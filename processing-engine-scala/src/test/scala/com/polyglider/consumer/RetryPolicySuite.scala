package com.polyglider.consumer

import munit.FunSuite
import scala.concurrent.duration._

class RetryPolicySuite extends FunSuite {
  private val policy = RetryPolicy(maxRetries = 5, baseDelay = 1.second, multiplier = 3.0, maxJitter = 250.millis)

  test("delayFor grows exponentially with the configured multiplier") {
    assertEquals(policy.delayFor(1), 1.second)
    assertEquals(policy.delayFor(2), 3.seconds)
    assertEquals(policy.delayFor(3), 9.seconds)
    assertEquals(policy.delayFor(4), 27.seconds)
    assertEquals(policy.delayFor(5), 81.seconds)
  }

  test("delayFor rejects tier below 1") {
    intercept[IllegalArgumentException](policy.delayFor(0))
  }

  test("retryQueueName is distinct per tier") {
    assertEquals(policy.retryQueueName(1), "retry.orders.placed.1")
    assertEquals(policy.retryQueueName(2), "retry.orders.placed.2")
    assertNotEquals(policy.retryQueueName(1), policy.retryQueueName(2))
  }
}
