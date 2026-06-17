package com.polyglider.consumer

import scala.concurrent.duration._

/** Backoff policy for transient failures.
  *
  * Each retry tier maps to a dedicated RabbitMQ queue (`<queuePrefix>.<tier>`) with a
  * fixed `x-message-ttl` and `x-dead-letter-exchange` pointing back at the main exchange, so
  * RabbitMQ itself delays redelivery — no in-process scheduler or timer thread needed.
  *
  * `delayFor(tier) = baseDelay * multiplier^(tier-1)`, tier is 1-indexed (the first retry is
  * tier 1). Callers should additionally sleep a small random jitter (`0..maxJitter`) before
  * publishing to the tier queue, so that many messages failing at the same instant don't all
  * land back on the broker/DB in lockstep.
  *
  * Once a message has been retried `maxRetries` times, it is routed to the DLX instead of
  * being retried again — an unbounded retry loop is its own failure mode.
  *
  * `queuePrefix` lets independent retry loops (e.g. the main consumer vs. the DLQ reprocessor)
  * use the same policy shape without colliding on queue names.
  */
final case class RetryPolicy(
  maxRetries: Int,
  baseDelay: FiniteDuration,
  multiplier: Double,
  maxJitter: FiniteDuration,
  queuePrefix: String = "retry.orders.placed"
) {
  def delayFor(tier: Int): FiniteDuration = {
    require(tier >= 1, "tier is 1-indexed")
    val millis = baseDelay.toMillis * math.pow(multiplier, (tier - 1).toDouble)
    millis.toLong.millis
  }

  def retryQueueName(tier: Int): String = s"$queuePrefix.$tier"
}

object RetryPolicy {
  val default: RetryPolicy = RetryPolicy(
    maxRetries = 5,
    baseDelay = 1.second,
    multiplier = 3.0,
    maxJitter = 250.millis
  )
}
