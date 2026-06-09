package com.polyglider

import cats.effect.*
import cats.syntax.all.*
import doobie.*
import doobie.hikari.HikariTransactor
import doobie.implicits.*
import io.circe.*
import io.circe.generic.auto.*
import io.circe.parser.*
import org.flywaydb.core.Flyway
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.rabbitmq.client.{ConnectionFactory, DefaultConsumer, Envelope, AMQP}
import cats.effect.unsafe.implicits.global

import java.nio.charset.StandardCharsets
import scala.concurrent.duration.*

case class OrderPlaced(eventId: String, sku: String, quantity: Int, customerId: String, timestamp: String)

object OrderProcessor extends IOApp.Simple {

  given Logger[IO] = Slf4jLogger.getLoggerFromName[IO]("OrderProcessor")

  private val rabbitHost = sys.env.getOrElse("RABBIT_HOST", "127.0.0.1")
  private val rabbitPort = sys.env.getOrElse("RABBIT_PORT", "5672").toInt
  private val rabbitUser = sys.env.getOrElse("RABBIT_USER", "guest")
  private val rabbitPass = sys.env.getOrElse("RABBIT_PASS", "guest")

  def transactorResource: Resource[IO, HikariTransactor[IO]] = for {
    ec <- ExecutionContexts.fixedThreadPool[IO](32)
    xa <- HikariTransactor.newHikariTransactor[IO](
      "org.postgresql.Driver",
      sys.env.getOrElse("PG_URL", "jdbc:postgresql://localhost:5432/polyglider"),
      sys.env.getOrElse("PG_USER", "postgres"),
      sys.env.getOrElse("PG_PASSWORD", "postgres"),
      ec
    )
  } yield xa

  def runMigrations(): IO[Unit] = IO.blocking {
    val url = sys.env.getOrElse("PG_URL", "jdbc:postgresql://localhost:5432/polyglider")
    val user = sys.env.getOrElse("PG_USER", "postgres")
    val password = sys.env.getOrElse("PG_PASSWORD", "postgres")
    val flyway = Flyway.configure().dataSource(url, user, password).load()
    flyway.migrate()
    ()
  }

  def upsertSku(xa: Transactor[IO], sku: String, delta: Int): IO[Unit] =
    sql"""
      INSERT INTO ledger (sku, qty) VALUES ($sku, $delta)
      ON CONFLICT (sku) DO UPDATE SET qty = ledger.qty + EXCLUDED.qty
    """.update.run.transact(xa).void

  def processEvent(order: OrderPlaced, xa: Transactor[IO]): IO[Unit] =
    for {
      _ <- Logger[IO].info(s"Processing order ${order.eventId} sku=${order.sku} qty=${order.quantity}")
      _ <- upsertSku(xa, order.sku, order.quantity)
      _ <- Logger[IO].info(s"Persisted ledger update for sku=${order.sku}")
    } yield ()

  def parsePayload(body: Array[Byte]): Either[Error, OrderPlaced] =
    parse(new String(body, StandardCharsets.UTF_8)).flatMap(_.as[OrderPlaced])

  def withRetries[A](ioa: IO[A], retries: Int = 3, delay: FiniteDuration = 1.second): IO[A] =
    ioa.handleErrorWith { err =>
      if (retries > 0)
        Logger[IO].warn(s"Operation failed (${err.getMessage}), retrying...") *>
          IO.sleep(delay) *>
          withRetries(ioa, retries - 1, delay * 2)
      else IO.raiseError(err)
    }

  def run: IO[Unit] =
    transactorResource.use { xa =>
      for {
        _ <- runMigrations()
        _ <- Logger[IO].info("Flyway migrations executed")

        _ <- IO.blocking {
          val factory = new ConnectionFactory()
          factory.setHost(rabbitHost)
          factory.setPort(rabbitPort)
          factory.setUsername(rabbitUser)
          factory.setPassword(rabbitPass)

          val conn = factory.newConnection()
          val ch = conn.createChannel()

          ch.exchangeDeclare("orders.exchange", "direct", true)
          ch.queueDeclare("orders.placed", true, false, false, null)
          ch.queueBind("orders.placed", "orders.exchange", "orders.placed")

          ch.basicQos(1)

          val consumer = new DefaultConsumer(ch) {
            override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte]): Unit = {
              val deliveryTag = envelope.getDeliveryTag
              val io = for {
                order <- IO.fromEither(parsePayload(body))
                _ <- withRetries(processEvent(order, xa))
                _ <- IO.blocking(ch.basicAck(deliveryTag, false))
              } yield ()

              io.handleErrorWith { err =>
                Logger[IO].error(err)("Failed processing message") *> IO.blocking(ch.basicNack(deliveryTag, false, false))
              }.unsafeRunAndForget()
            }
          }

          ch.basicConsume("orders.placed", false, consumer)
          ()
        }
      } yield ()
    }
}
