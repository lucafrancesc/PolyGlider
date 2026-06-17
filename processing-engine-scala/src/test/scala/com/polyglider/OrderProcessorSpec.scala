package com.polyglider

import munit.CatsEffectSuite

class OrderProcessorSpec extends CatsEffectSuite {

  test("parsePayload should fail on malformed JSON") {
    val bad = "{ not: valid json }".getBytes("UTF-8")
    val res = OrderProcessor.parsePayload(bad)
    assert(res.isLeft)
  }
}
