package com.polyglider

import cats.effect.std.{Console, Dispatcher}
import cats.effect.*
import cats.syntax.all.*
import cats.syntax.functor.*
import com.rabbitmq.client.{DefaultSaslConfig, SaslConfig}
import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import fs2.Stream
import io.circe.*
import io.circe.generic.auto.*
import io.circe.parser.*
import io.circe.syntax.*
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.concurrent.Executors
import doobie._
import doobie.implicits._
import doobie.hikari.HikariTransactor
import scala.concurrent.ExecutionContext
import javax.net.ssl.SSLContext
import scala.concurrent.duration.*

case class OrderPlaced(eventId: String, sku: String, quantity: Int, customerId: String, timestamp: String)

object OrderProcessor extends IOApp.Simple {

  // Correct contextual logger initialization for typelevel ecosystem
  given logger: org.typelevel.log4cats.Logger[IO] = Slf4jLogger.getLoggerFromName[IO]("OrderProcessor")

  // Explicit configuration pointing to our docker broker settings
  val config: Fs2RabbitConfig = Fs2RabbitConfig(
    virtualHost = "/",
    host = "127.0.0.1",
    username = Some("guest"),
    password = Some("guest"),
    port = 5672,
    ssl = false,
    connectionTimeout = 3.seconds,
    requeueOnNack = false,
    requeueOnReject = false,
    automaticTopologyRecovery = true,
    internalQueueSize = Some(500)
  )

  // Postgres-backed ledger via Doobie
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

  def initDb(xa: Transactor[IO]): IO[Unit] =
    // With Flyway migrations present we prefer running Flyway instead of ad-hoc DDL.
    IO.unit

  def runMigrations(): IO[Unit] = IO.blocking {
    val url = sys.env.getOrElse("PG_URL", "jdbc:postgresql://localhost:5432/polyglider")
    val user = sys.env.getOrElse("PG_USER", "postgres")
    val password = sys.env.getOrElse("PG_PASSWORD", "postgres")
    val flyway = org.flywaydb.core.Flyway.configure().dataSource(url, user, password).load()
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
      _ <- logger.info(s"Processing order ${order.eventId} sku=${order.sku} qty=${order.quantity}")
      _ <- upsertSku(xa, order.sku, order.quantity)
      _ <- logger.info(s"Persisted ledger update for sku=${order.sku}")
    } yield ()

  def withRetries[A](ioa: IO[A], retries: Int = 3, delay: FiniteDuration = 1.second): IO[A] =
    ioa.handleErrorWith { err =>
      if (retries > 0) logger.warn(s"Operation failed (${err.getMessage}), retrying... ($retries left)") *> IO.sleep(delay) *> withRetries(ioa, retries - 1, delay * 2)
      else IO.raiseError(err)
    }

  def parsePayload(body: Array[Byte]): Either[Error, OrderPlaced] =
    parse(new String(body, "UTF-8")).flatMap(_.as[OrderPlaced])

  def run: IO[Unit] =
    for {
      rabbit <- RabbitClient[IO].default(config)
      _ <- logger.info("Starting OrderProcessor")

      _ <- transactorResource.use { xa =>
        for {
              _ <- runMigrations()
              _ <- logger.info("Flyway migrations executed")

          _ <- rabbit.createConnectionChannel.use { implicit ch =>
        for {
          // Declare the queue matching the name in our contract
          _ <- rabbit.declareQueue(DeclarationQueueConfig.default(QueueName("orders.placed")))

          // CRITICAL: Bind the queue to the exchange created by C# gateway
          _ <- rabbit.bindQueue(
            QueueName("orders.placed"),
            ExchangeName("orders.exchange"),
            RoutingKey("orders.placed")
          )

          consumer <- rabbit.createManualAckConsumer(QueueName("orders.placed"))
          (stream, _) = consumer

          _ <- stream.evalMap { amqpMsg =>
            val action = for {
              order <- IO.fromEither(parsePayload(amqpMsg.payload))
              _ <- processEvent(order, xa)
              _ <- rabbit.ack(amqpMsg.deliveryTag)
            } yield ()

            withRetries(action).handleErrorWith { err =>
              logger.error(s"Failed to process message after retries: ${err.getMessage}") *>
                rabbit.nack(amqpMsg.deliveryTag, requeue = false)
            }
          }.compile.drain
        } yield ()
          }
        } yield ()
      }
    } yield ()
}