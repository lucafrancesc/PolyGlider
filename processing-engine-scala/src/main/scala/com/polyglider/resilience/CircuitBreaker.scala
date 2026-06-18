package com.polyglider.resilience

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.FiniteDuration

/** Thrown instead of calling through when the breaker is open. Classified as a
  * [[com.polyglider.consumer.ProcessingFailure.TransientFailure]] (see `ProcessingFailure.classify`)
  * so callers requeue-with-backoff rather than routing straight to the DLQ — the underlying
  * outage is expected to clear, and the message itself was never the problem.
  */
final case class CircuitBreakerOpenException(name: String)
    extends Exception(s"Circuit breaker '$name' is open; failing fast")

private sealed trait BreakerState
private object BreakerState {
  final case class Closed(consecutiveFailures: Int) extends BreakerState
  final case class Open(openedAt: FiniteDuration) extends BreakerState
  case object HalfOpen extends BreakerState
}

/** Minimal circuit breaker guarding a single failure-prone operation (e.g. a Postgres write).
  *
  * Closed: calls pass through; `maxFailures` consecutive failures trip the breaker to Open.
  * Open: calls fail fast with [[CircuitBreakerOpenException]] without touching the underlying
  *   operation, until `resetTimeout` has elapsed since it tripped.
  * HalfOpen: exactly one trial call is let through; success closes the breaker, failure
  *   reopens it (restarting the cooldown).
  *
  * This avoids flooding a downed dependency (and, downstream, the DLQ) with one failure per
  * in-flight message when the real problem is a single outage rather than N independent ones.
  */
final class CircuitBreaker private (
  name: String,
  state: Ref[IO, BreakerState],
  maxFailures: Int,
  resetTimeout: FiniteDuration,
  logger: Logger[IO],
  onStateChange: String => IO[Unit]
) {
  import BreakerState.*

  def protect[A](ioa: IO[A]): IO[A] =
    IO.monotonic.flatMap { now =>
      state.modify {
        case s @ Closed(_) => (s, (true, None))
        case Open(openedAt) if now - openedAt >= resetTimeout => (HalfOpen, (true, Some("half-open")))
        case s @ Open(_) => (s, (false, None))
        case HalfOpen => (HalfOpen, (false, None))
      }
    }.flatMap { case (allowed, transition) =>
      transition.traverse_(onStateChange) *> {
        if (!allowed) IO.raiseError(CircuitBreakerOpenException(name))
        else ioa.attempt.flatMap(_.fold(onFailure, onSuccess))
      }
    }

  private def onSuccess[A](a: A): IO[A] =
    state.modify {
      case HalfOpen => (Closed(0), true)
      case _         => (Closed(0), false)
    }.flatMap { reclosed =>
      logger.info(s"Circuit breaker '$name' closed after a successful call").whenA(reclosed) *>
        onStateChange("closed").whenA(reclosed)
    }.as(a)

  private def onFailure[A](err: Throwable): IO[A] =
    IO.monotonic.flatMap { now =>
      state.modify {
        case Closed(failures) =>
          val nextFailures = failures + 1
          if (nextFailures >= maxFailures) (Open(now), true) else (Closed(nextFailures), false)
        case HalfOpen => (Open(now), true)
        case s @ Open(_) => (s, false)
      }
    }.flatMap { tripped =>
      logger.warn(err)(s"Circuit breaker '$name' tripped open after $maxFailures consecutive failures").whenA(tripped) *>
        onStateChange("open").whenA(tripped)
    }.flatMap(_ => IO.raiseError(err))
}

object CircuitBreaker {
  def create(
    name: String,
    maxFailures: Int,
    resetTimeout: FiniteDuration,
    logger: Logger[IO],
    onStateChange: String => IO[Unit] = _ => IO.unit
  ): IO[CircuitBreaker] =
    Ref[IO].of[BreakerState](BreakerState.Closed(0)).map(new CircuitBreaker(name, _, maxFailures, resetTimeout, logger, onStateChange))
}
