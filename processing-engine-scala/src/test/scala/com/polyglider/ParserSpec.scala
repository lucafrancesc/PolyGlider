package com.polyglider

import munit.CatsEffectSuite
import io.circe.parser.*
import io.circe.generic.auto._
import com.polyglider.model.OrderPlaced
import cats.effect.IO

class ParserSpec extends CatsEffectSuite {
  test("parse valid OrderPlaced JSON") {
    val json = """{ "eventId": "e1", "sku": "SKU-1", "quantity": 3, "customerId": "c1", "timestamp": "t" }"""
    val parsed = parse(json).flatMap(_.as[OrderPlaced])
    assert(parsed.isRight)
    val order = parsed.toOption.get
    assertEquals(order.sku, "SKU-1")
    assertEquals(order.quantity, 3)
  }

  test("fail on malformed JSON") {
    val bad = "{ this is not json }"
    val parsed = parse(bad)
    assert(parsed.isLeft)
  }
}
