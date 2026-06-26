package com.polyglider

import cats.effect.*
import com.polyglider.consumer.{OrderPlacedHandler, RabbitConsumer}
import com.polyglider.reprocessor.DlqReprocessor
import com.polyglider.resilience.CircuitBreaker
import com.polyglider.metrics.Metrics
import com.polyglider.db.Database
import com.polyglider.storage.{CircuitBreakerSkuStorage, DoobieSkuStorage}
import com.typesafe.config.ConfigFactory
import com.polyglider.model.OrderPlaced
import io.circe.generic.auto.*
import io.circe.parser
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.nio.charset.StandardCharsets
import scala.concurrent.duration.*

object OrderProcessor {
  given Logger[IO] = Slf4jLogger.getLoggerFromName[IO]("OrderProcessor")

  def process: IO[Unit] =
    IO.fromEither(AppConfig.load(ConfigFactory.load()).left.map(new RuntimeException(_)))
      .flatMap { cfg =>
        Logger[IO].info(s"Effective config: ${cfg.effectiveConfigLog}") *> run(cfg)
      }

  private def run(cfg: AppConfig): IO[Unit] =
    // Build a combined Resource: transactor -> run migrations -> consumer resource
    val res: Resource[IO, Unit] = for {
      // First acquired, last released: this finalizer only logs once every consumer fiber,
      // the DLQ reprocessor, the metrics server, and the transactor have actually finished
      // closing -- confirming shutdown was clean rather than just claiming to be.
      _ <- Resource.onFinalize(Logger[IO].info("Shutdown complete: all consumers, the DLQ reprocessor, and the database connection are closed"))
      xa <- Database.transactorResource
      // Flyway takes a Postgres advisory lock for the duration of migrate(), so concurrent
      // migration attempts from multiple engine replicas are safe by default -- no code change needed.
      _ <- Resource.eval(
        if cfg.runMigrations then Database.runMigrations() *> Logger[IO].info("Flyway migrations executed")
        else Logger[IO].info("Skipping Flyway migrations (app.db.runMigrations=false)")
      )
      _ <- Metrics.startServer(cfg.metricsPort)
      breaker <- Resource.eval(CircuitBreaker.create("postgres-write", cfg.circuitBreakerMaxFailures, cfg.circuitBreakerResetTimeoutMs.millis, Logger[IO], Metrics.onCircuitBreakerStateChange("postgres-write")))
      storage = new CircuitBreakerSkuStorage(new DoobieSkuStorage(xa), breaker)
      handler = new OrderPlacedHandler(storage)
      _ <- RabbitConsumer.start(handler, Logger[IO], workerCount = cfg.workers, queueSize = cfg.queueSize, summaryEvery = cfg.summaryEvery, retryPolicy = cfg.retryPolicy, queueDepthPollInterval = cfg.dlqPollIntervalMs.millis, snapshotFn = storage.snapshot)
      _ <- DlqReprocessor.start(handler, Logger[IO], retryPolicy = cfg.reprocessorPolicy, dlqDepthPollInterval = cfg.dlqPollIntervalMs.millis)
    } yield ()

    // Use the resource and keep the app running. IOApp installs a JVM shutdown hook that
    // cancels this IO on SIGTERM/SIGINT; cancelling Resource.use runs every finalizer above
    // (closing the RabbitMQ channel/connection, cancelling consumer fibers, closing the
    // Hikari transactor) before this IO completes. The onCancel here just makes that visible.
    res.use(_ => IO.never.onCancel(Logger[IO].info("Shutdown signal received: draining in-flight messages and closing connections...")))

  // Helper used by tests
  def parsePayload(bytes: Array[Byte]): Either[io.circe.Error, OrderPlaced] =
    parser.parse(new String(bytes, StandardCharsets.UTF_8)).flatMap(_.as[OrderPlaced])
}
