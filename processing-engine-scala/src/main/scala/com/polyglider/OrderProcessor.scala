package com.polyglider

import cats.effect.*
import cats.syntax.all.*
import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
import dev.profunktor.fs2rabbit.config.declaration.*
import dev.profunktor.fs2rabbit.interpreter.RabbitClient
import dev.profunktor.fs2rabbit.model.*
import doobie.*
import doobie.hikari.HikariTransactor
import doobie.implicits.*
import io.circe.*
import io.circe.generic.auto.*
import io.circe.parser.*
import org.flywaydb.core.Flyway
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.nio.charset.StandardCharsets
import scala.concurrent.duration.*

case class OrderPlaced(
                        eventId: String,
                        sku: String,
                        quantity: Int,
                        customerId: String,
                        timestamp: String
                      )

object OrderProcessor extends IOApp.Simple {

  given Logger[IO] =
    Slf4jLogger.getLoggerFromName[IO]("OrderProcessor")

  private val config = Fs2RabbitConfig(
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

  def transactorResource: Resource[IO, HikariTransactor[IO]] =
    for {
      ec <- ExecutionContexts.fixedThreadPool[IO](32)
      xa <- HikariTransactor.newHikariTransactor[IO](
        "org.postgresql.Driver",
        sys.env.getOrElse(
          "PG_URL",
          "jdbc:postgresql://localhost:5432/polyglider"
        ),
        sys.env.getOrElse("PG_USER", "postgres"),
        sys.env.getOrElse("PG_PASSWORD", "postgres"),
        ec
      )
    } yield xa

  def runMigrations(): IO[Unit] =
    IO.blocking {
      val flyway =
        Flyway
          .configure()
          .dataSource(
            sys.env.getOrElse(
              "PG_URL",
              "jdbc:postgresql://localhost:5432/polyglider"
            ),
            sys.env.getOrElse("PG_USER", "postgres"),
            sys.env.getOrElse("PG_PASSWORD", "postgres")
          )
          .load()

      flyway.migrate()
      ()
    }

  def upsertSku(
                 xa: Transactor[IO],
                 sku: String,
                 delta: Int
               ): IO[Unit] =
    sql"""
      INSERT INTO ledger (sku, qty)
      VALUES ($sku, $delta)
      ON CONFLICT (sku)
      DO UPDATE SET qty = ledger.qty + EXCLUDED.qty
    """.update.run.transact(xa).void

  def processEvent(
                    order: OrderPlaced,
                    xa: Transactor[IO]
                  ): IO[Unit] =
    for {
      _ <- Logger[IO].info(
        s"Processing order ${order.eventId} sku=${order.sku}"
      )
      _ <- upsertSku(xa, order.sku, order.quantity)
      _ <- Logger[IO].info(
        s"Persisted ledger update for sku=${order.sku}"
      )
    } yield ()

  def parsePayload(
                    body: Array[Byte]
                  ): Either[Error, OrderPlaced] =
    parse(new String(body, StandardCharsets.UTF_8))
      .flatMap(_.as[OrderPlaced])

  def withRetries[A](
                      ioa: IO[A],
                      retries: Int = 3,
                      delay: FiniteDuration = 1.second
                    ): IO[A] =
    ioa.handleErrorWith { err =>
      if (retries > 0)
        Logger[IO]
          .warn(
            s"Operation failed (${err.getMessage}), retrying..."
          ) *>
          IO.sleep(delay) *>
          withRetries(ioa, retries - 1, delay * 2)
      else
        IO.raiseError(err)
    }

  def run: IO[Unit] =
    transactorResource.use { xa =>
      for {
        _ <- runMigrations()
        _ <- Logger[IO].info("Flyway migrations executed")

        // THIS LINE DEPENDS ON YOUR FS2-RABBIT VERSION
        rabbit <- RabbitClient[IO](config).use { rabbit =>
          rabbit.createConnectionChannel.use { implicit ch =>
            for {
              _ <- rabbit.declareQueue(
                DeclarationQueueConfig.default(
                  QueueName("orders.placed")
                )
              )

              _ <- rabbit.bindQueue(
                QueueName("orders.placed"),
                ExchangeName("orders.exchange"),
                RoutingKey("orders.placed")
              )

              consumer <- rabbit.createManualAckConsumer(
                QueueName("orders.placed")
              )

              (stream, _) = consumer

              _ <- stream.evalMap { msg =>
                val action =
                  for {
                    order <- IO.fromEither(
                      parsePayload(msg.payload)
                    )
                    _ <- processEvent(order, xa)
                    _ <- rabbit.ack(msg.deliveryTag)
                  } yield ()

                withRetries(action).handleErrorWith { err =>
                  Logger[IO].error(err)(
                    s"Failed processing message"
                  ) *>
                    rabbit.nack(
                      msg.deliveryTag,
                      requeue = false
                    )
                }
              }.compile.drain
            } yield ()
          }
        }

      } yield ()
    }
}