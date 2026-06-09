package com.polyglider

import cats.effect.IO
import munit.CatsEffectSuite
import scala.concurrent.duration._

class OrderProcessorSpec extends CatsEffectSuite {

  test("parsePayload should fail on malformed JSON") {
    val bad = "{ not: valid json }".getBytes("UTF-8")
    val res = OrderProcessor.parsePayload(bad)
    assert(res.isLeft)
  }

  test("withRetries should retry and eventually succeed") {
    var attempts = 0
    val action = IO {
      attempts += 1
      if (attempts < 3) throw new RuntimeException("transient")
      else 42
    }

    OrderProcessor.withRetries(action, retries = 5, delay = 10.millis).map { out =>
      assertEquals(out, 42)
      assert(attempts >= 3)
    }
  }

  test("withRetries should fail after exhausted retries") {
    val action = IO.raiseError[Int](new RuntimeException("permanent"))
    OrderProcessor.withRetries(action, retries = 2, delay = 10.millis).attempt.map {
      case Left(ex) => assert(ex.getMessage.contains("permanent"))
      case Right(_) => fail("expected failure")
    }
  }
}
