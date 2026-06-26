package com.polyglider

import cats.effect.*
import com.polyglider.consumer.{MessageHandler, OrderPlacedHandler, RabbitConsumer, RetryPolicy}
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

  private val conf = ConfigFactory.load()

  def process: IO[Unit] =
    // Build a combined Resource: transactor -> run migrations -> consumer resource
    val res: Resource[IO, Unit] = for {
      // First acquired, last released: this finalizer only logs once every consumer fiber,
      // the DLQ reprocessor, the metrics server, and the transactor have actually finished
      // closing -- confirming shutdown was clean rather than just claiming to be.
      _ <- Resource.onFinalize(Logger[IO].info("Shutdown complete: all consumers, the DLQ reprocessor, and the database connection are closed"))
      xa <- Database.transactorResource
      // Conditionally run migrations based on config flag `app.db.runMigrations` (default true)
      runMigs = try conf.getConfig("app.db").getBoolean("runMigrations") catch {
        case _: Exception => true
      }
      // Flyway takes a Postgres advisory lock for the duration of migrate(), so concurrent
      // migration attempts from multiple engine replicas are safe by default -- no code change needed.
      _ <- Resource.eval(
        if runMigs then Database.runMigrations() *> Logger[IO].info("Flyway migrations executed")
        else Logger[IO].info("Skipping Flyway migrations (app.db.runMigrations=false)")
      )
      summaryEvery = try conf.getConfig("app.consumer").getLong("summary-every") catch {
        case _: Exception => 10L
      }
      queueSize = try conf.getConfig("app.consumer").getInt("queue-size") catch {
        case _: Exception => 1000
      }
      workerCount = try conf.getConfig("app.consumer").getInt("workers") catch {
        case _: Exception => 4
      }
      retryPolicy = try {
        val retryConf = conf.getConfig("app.consumer.retry")
        RetryPolicy(
          maxRetries = retryConf.getInt("max-retries"),
          baseDelay = retryConf.getLong("base-delay-ms").millis,
          multiplier = retryConf.getDouble("backoff-multiplier"),
          maxJitter = retryConf.getLong("max-jitter-ms").millis
        )
      } catch {
        case _: Exception => RetryPolicy.default
      }
      reprocessorPolicy = try {
        val reprocessorConf = conf.getConfig("app.reprocessor")
        DlqReprocessor.defaultRetryPolicy.copy(
          maxRetries = reprocessorConf.getInt("max-retries"),
          baseDelay = reprocessorConf.getLong("base-delay-ms").millis,
          multiplier = reprocessorConf.getDouble("backoff-multiplier"),
          maxJitter = reprocessorConf.getLong("max-jitter-ms").millis
        )
      } catch {
        case _: Exception => DlqReprocessor.defaultRetryPolicy
      }
      circuitBreakerConf = try {
        val cbConf = conf.getConfig("app.circuit-breaker")
        (cbConf.getInt("max-failures"), cbConf.getLong("reset-timeout-ms").millis)
      } catch {
        case _: Exception => (5, 30.seconds)
      }
      metricsPort = try conf.getConfig("app.metrics").getInt("port") catch {
        case _: Exception => 9100
      }
      dlqPollInterval = try conf.getConfig("app.metrics").getLong("dlq-poll-interval-ms").millis catch {
        case _: Exception => 15.seconds
      }
      _ <- Metrics.startServer(metricsPort)
      breaker <- Resource.eval(CircuitBreaker.create("postgres-write", circuitBreakerConf._1, circuitBreakerConf._2, Logger[IO], Metrics.onCircuitBreakerStateChange("postgres-write")))
      storage = new CircuitBreakerSkuStorage(new DoobieSkuStorage(xa), breaker)
      handler = new OrderPlacedHandler(storage)
      _ <- RabbitConsumer.start(handler, Logger[IO], workerCount = workerCount, queueSize = queueSize, summaryEvery = summaryEvery, retryPolicy = retryPolicy, queueDepthPollInterval = dlqPollInterval, snapshotFn = storage.snapshot)
      _ <- DlqReprocessor.start(handler, Logger[IO], retryPolicy = reprocessorPolicy, dlqDepthPollInterval = dlqPollInterval)
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
