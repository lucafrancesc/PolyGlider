package com.polyglider

import com.polyglider.consumer.RetryPolicy
import com.polyglider.reprocessor.DlqReprocessor
import com.typesafe.config.{Config => TypesafeConfig}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

final case class AppConfig(
  runMigrations: Boolean,
  summaryEvery: Long,
  queueSize: Int,
  workers: Int,
  retryPolicy: RetryPolicy,
  reprocessorPolicy: RetryPolicy,
  circuitBreakerMaxFailures: Int,
  circuitBreakerResetTimeoutMs: Long,
  metricsPort: Int,
  dlqPollIntervalMs: Long,
  rabbitPort: Int
) {
  def effectiveConfigLog: String =
    s"workers=$workers queueSize=$queueSize summaryEvery=$summaryEvery " +
    s"maxRetries=${retryPolicy.maxRetries} baseDelayMs=${retryPolicy.baseDelay.toMillis} " +
    s"reprocessorMaxRetries=${reprocessorPolicy.maxRetries} " +
    s"cbMaxFailures=$circuitBreakerMaxFailures cbResetMs=$circuitBreakerResetTimeoutMs " +
    s"metricsPort=$metricsPort dlqPollMs=$dlqPollIntervalMs rabbitPort=$rabbitPort " +
    s"runMigrations=$runMigrations"
}

object AppConfig {
  def load(conf: TypesafeConfig): Either[String, AppConfig] = {
    val errors = ArrayBuffer[String]()

    // Only validates when the path exists: a missing key falls back to `default` silently
    // (by design — operators may omit optional sections), but a present-but-malformed value
    // is always an error.
    def get[A](path: String, default: A)(f: TypesafeConfig => A): A =
      if conf.hasPath(path) then
        try f(conf)
        catch { case e: Exception => errors += s"$path: ${e.getMessage}"; default }
      else default

    val runMigrations = get("app.db.runMigrations", true)(_.getBoolean("app.db.runMigrations"))
    val summaryEvery  = get("app.consumer.summary-every", 10L)(_.getLong("app.consumer.summary-every"))
    val queueSize     = get("app.consumer.queue-size", 1000)(_.getInt("app.consumer.queue-size"))
    val workers       = get("app.consumer.workers", 4)(_.getInt("app.consumer.workers"))

    val maxRetries         = get("app.consumer.retry.max-retries", RetryPolicy.default.maxRetries)(_.getInt("app.consumer.retry.max-retries"))
    val baseDelayMs        = get("app.consumer.retry.base-delay-ms", RetryPolicy.default.baseDelay.toMillis)(_.getLong("app.consumer.retry.base-delay-ms"))
    val backoffMultiplier  = get("app.consumer.retry.backoff-multiplier", RetryPolicy.default.multiplier)(_.getDouble("app.consumer.retry.backoff-multiplier"))
    val maxJitterMs        = get("app.consumer.retry.max-jitter-ms", RetryPolicy.default.maxJitter.toMillis)(_.getLong("app.consumer.retry.max-jitter-ms"))

    val reprocessorMaxRetries        = get("app.reprocessor.max-retries", DlqReprocessor.defaultRetryPolicy.maxRetries)(_.getInt("app.reprocessor.max-retries"))
    val reprocessorBaseDelayMs       = get("app.reprocessor.base-delay-ms", DlqReprocessor.defaultRetryPolicy.baseDelay.toMillis)(_.getLong("app.reprocessor.base-delay-ms"))
    val reprocessorBackoffMultiplier = get("app.reprocessor.backoff-multiplier", DlqReprocessor.defaultRetryPolicy.multiplier)(_.getDouble("app.reprocessor.backoff-multiplier"))
    val reprocessorMaxJitterMs       = get("app.reprocessor.max-jitter-ms", DlqReprocessor.defaultRetryPolicy.maxJitter.toMillis)(_.getLong("app.reprocessor.max-jitter-ms"))

    val cbMaxFailures      = get("app.circuit-breaker.max-failures", 5)(_.getInt("app.circuit-breaker.max-failures"))
    val cbResetTimeoutMs   = get("app.circuit-breaker.reset-timeout-ms", 30000L)(_.getLong("app.circuit-breaker.reset-timeout-ms"))

    val metricsPort        = get("app.metrics.port", 9100)(_.getInt("app.metrics.port"))
    val dlqPollIntervalMs  = get("app.metrics.dlq-poll-interval-ms", 15000L)(_.getLong("app.metrics.dlq-poll-interval-ms"))

    val rabbitPort         = get("app.rabbitmq.port", 5672)(_.getInt("app.rabbitmq.port"))

    if errors.nonEmpty then
      Left(s"Configuration errors:\n${errors.mkString("\n")}")
    else
      Right(AppConfig(
        runMigrations = runMigrations,
        summaryEvery = summaryEvery,
        queueSize = queueSize,
        workers = workers,
        retryPolicy = RetryPolicy(
          maxRetries = maxRetries,
          baseDelay = baseDelayMs.millis,
          multiplier = backoffMultiplier,
          maxJitter = maxJitterMs.millis
        ),
        reprocessorPolicy = DlqReprocessor.defaultRetryPolicy.copy(
          maxRetries = reprocessorMaxRetries,
          baseDelay = reprocessorBaseDelayMs.millis,
          multiplier = reprocessorBackoffMultiplier,
          maxJitter = reprocessorMaxJitterMs.millis
        ),
        circuitBreakerMaxFailures = cbMaxFailures,
        circuitBreakerResetTimeoutMs = cbResetTimeoutMs,
        metricsPort = metricsPort,
        dlqPollIntervalMs = dlqPollIntervalMs,
        rabbitPort = rabbitPort
      ))
  }
}
