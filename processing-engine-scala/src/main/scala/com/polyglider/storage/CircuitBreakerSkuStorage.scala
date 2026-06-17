package com.polyglider.storage

import cats.effect.IO
import com.polyglider.resilience.CircuitBreaker

/** Wraps the Postgres write path (`upsertSku`) with a circuit breaker so a Postgres outage
  * fails fast instead of letting every in-flight message hit (and fail against) the database
  * individually. `snapshot` is a periodic read used only for log output, not on the
  * message-processing hot path, so it is left unguarded.
  */
final class CircuitBreakerSkuStorage(underlying: SkuStorage, breaker: CircuitBreaker) extends SkuStorage {
  def upsertSku(eventId: String, sku: String, delta: Int): IO[Unit] =
    breaker.protect(underlying.upsertSku(eventId, sku, delta))

  def snapshot: IO[List[SkuStats]] = underlying.snapshot
}
