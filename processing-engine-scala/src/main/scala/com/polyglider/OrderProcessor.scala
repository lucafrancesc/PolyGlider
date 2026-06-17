package com.polyglider

import cats.effect.*
import com.polyglider.consumer.{RabbitConsumer, RetryPolicy}
import com.polyglider.reprocessor.DlqReprocessor
import com.polyglider.resilience.CircuitBreaker
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
      xa <- Database.transactorResource
      // Conditionally run migrations based on config flag `app.db.runMigrations` (default true)
      runMigs = try conf.getConfig("app.db").getBoolean("runMigrations") catch {
        case _: Exception => true
      }
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
      breaker <- Resource.eval(CircuitBreaker.create("postgres-write", circuitBreakerConf._1, circuitBreakerConf._2, Logger[IO]))
      storage = new CircuitBreakerSkuStorage(new DoobieSkuStorage(xa), breaker)
      _ <- RabbitConsumer.start(storage, Logger[IO], workerCount = workerCount, queueSize = queueSize, summaryEvery = summaryEvery, retryPolicy = retryPolicy)
      _ <- DlqReprocessor.start(storage, Logger[IO], retryPolicy = reprocessorPolicy)
    } yield ()

    // Use the resource and keep the app running
    res.use(_ => IO.never)

  // Helper used by tests
  def parsePayload(bytes: Array[Byte]): Either[io.circe.Error, OrderPlaced] =
    parser.parse(new String(bytes, StandardCharsets.UTF_8)).flatMap(_.as[OrderPlaced])

  def withRetries[A](ioa: IO[A], retries: Int, delay: scala.concurrent.duration.FiniteDuration): IO[A] = {
    ioa.handleErrorWith { err =>
      if (retries <= 0) IO.raiseError(err)
      else IO.sleep(delay) *> withRetries(ioa, retries - 1, delay)
    }
  }
}
