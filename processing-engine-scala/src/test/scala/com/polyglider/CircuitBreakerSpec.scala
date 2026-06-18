package com.polyglider

import cats.effect.IO
import munit.CatsEffectSuite
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.polyglider.resilience.{CircuitBreaker, CircuitBreakerOpenException}

import scala.concurrent.duration.*

class CircuitBreakerSpec extends CatsEffectSuite {
  private given Logger[IO] = Slf4jLogger.getLoggerFromClass[IO](classOf[CircuitBreakerSpec])

  private val boom = new RuntimeException("postgres down")

  test("stays closed and passes through calls while failures are below the threshold") {
    for {
      breaker <- CircuitBreaker.create("test", maxFailures = 3, resetTimeout = 1.minute, summon[Logger[IO]])
      _       <- breaker.protect(IO.raiseError(boom)).attempt
      _       <- breaker.protect(IO.raiseError(boom)).attempt
      result  <- breaker.protect(IO.pure(42))
    } yield assertEquals(result, 42)
  }

  test("trips open after maxFailures consecutive failures and fails fast without calling through") {
    for {
      breaker <- CircuitBreaker.create("test", maxFailures = 2, resetTimeout = 1.minute, summon[Logger[IO]])
      calls   <- IO.ref(0)
      failing  = calls.update(_ + 1) *> IO.raiseError(boom)
      _       <- breaker.protect(failing).attempt
      _       <- breaker.protect(failing).attempt // trips here
      result  <- breaker.protect(failing).attempt
      callCount <- calls.get
    } yield {
      assert(result.left.exists(_.isInstanceOf[CircuitBreakerOpenException]), "expected CircuitBreakerOpenException")
      assertEquals(callCount, 2, "the underlying operation must not run once the breaker is open")
    }
  }

  test("a successful call resets the consecutive failure count") {
    for {
      breaker <- CircuitBreaker.create("test", maxFailures = 2, resetTimeout = 1.minute, summon[Logger[IO]])
      _       <- breaker.protect(IO.raiseError(boom)).attempt
      _       <- breaker.protect(IO.pure(1))
      _       <- breaker.protect(IO.raiseError(boom)).attempt
      result  <- breaker.protect(IO.pure(2)) // breaker should still be closed; only 1 consecutive failure
    } yield assertEquals(result, 2)
  }

  test("transitions to half-open after resetTimeout and closes again on a successful trial") {
    for {
      breaker <- CircuitBreaker.create("test", maxFailures = 1, resetTimeout = 50.millis, summon[Logger[IO]])
      _       <- breaker.protect(IO.raiseError(boom)).attempt // trips open
      blocked <- breaker.protect(IO.pure("should not run")).attempt
      _       <- IO.sleep(100.millis)
      result  <- breaker.protect(IO.pure("recovered"))
      closed  <- breaker.protect(IO.pure("still closed"))
    } yield {
      assert(blocked.left.exists(_.isInstanceOf[CircuitBreakerOpenException]))
      assertEquals(result, "recovered")
      assertEquals(closed, "still closed")
    }
  }

  test("a failed half-open trial reopens the breaker") {
    for {
      breaker <- CircuitBreaker.create("test", maxFailures = 1, resetTimeout = 50.millis, summon[Logger[IO]])
      _       <- breaker.protect(IO.raiseError(boom)).attempt // trips open
      _       <- IO.sleep(100.millis)
      trial   <- breaker.protect(IO.raiseError(boom)).attempt // half-open trial fails
      blocked <- breaker.protect(IO.pure("should not run")).attempt
    } yield {
      assert(trial.isLeft)
      assert(blocked.left.exists(_.isInstanceOf[CircuitBreakerOpenException]), "must reopen after a failed trial")
    }
  }

  test("onStateChange reports open, half-open, and closed transitions for observability") {
    for {
      events  <- IO.ref(List.empty[String])
      breaker <- CircuitBreaker.create(
                   "test", maxFailures = 1, resetTimeout = 50.millis, summon[Logger[IO]],
                   onStateChange = s => events.update(_ :+ s)
                 )
      _       <- breaker.protect(IO.raiseError(boom)).attempt // trips open
      _       <- IO.sleep(100.millis)
      _       <- breaker.protect(IO.pure("recovered")) // half-open trial succeeds, then closes
      seen    <- events.get
    } yield assertEquals(seen, List("closed", "open", "half-open", "closed"))
  }

  test("onStateChange reports the initial closed state but no further transitions below the threshold") {
    for {
      events  <- IO.ref(List.empty[String])
      breaker <- CircuitBreaker.create(
                   "test", maxFailures = 3, resetTimeout = 1.minute, summon[Logger[IO]],
                   onStateChange = s => events.update(_ :+ s)
                 )
      _       <- breaker.protect(IO.raiseError(boom)).attempt
      seen    <- events.get
    } yield assertEquals(seen, List("closed"))
  }
}
