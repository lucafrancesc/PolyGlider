package com.polyglider.consumer

import cats.effect.IO
import munit.CatsEffectSuite
import com.polyglider.model.OrderPlaced
import com.polyglider.storage.{SkuStats, SkuStorage, UpsertResult}

/** Proves the MessageHandler seam is real: decode/validate/process exercise domain logic
  * against a stub SkuStorage with no live RabbitMQ connection required.
  */
class MessageHandlerSpec extends CatsEffectSuite {

  private val stubStorage = new SkuStorage {
    def upsertSku(eventId: String, sku: String, delta: Int): IO[UpsertResult] = IO.pure(UpsertResult.Applied)
    def snapshot: IO[List[SkuStats]] = IO.pure(Nil)
  }

  private val handler: MessageHandler[OrderPlaced] = new OrderPlacedHandler(stubStorage)

  private val validBody =
    """{"eventId":"11111111-1111-4111-8111-111111111111","sku":"SKU-1","quantity":2,"customerId":"22222222-2222-4222-8222-222222222222","timestamp":"2024-01-01T00:00:00Z","version":"1"}"""
      .getBytes("UTF-8")

  test("decode parses valid JSON into an OrderPlaced") {
    val result = handler.decode(validBody)
    assert(result.isRight)
    assertEquals(result.toOption.map(_.sku), Some("SKU-1"))
    assertEquals(result.toOption.map(_.quantity), Some(2))
  }

  test("decode fails on malformed JSON without throwing") {
    val result = handler.decode("{ not valid json }".getBytes("UTF-8"))
    assert(result.isLeft)
  }

  test("validate accepts an order with valid UUIDs and a supported version") {
    val order = handler.decode(validBody).toOption.get
    assert(handler.validate(order).isRight)
  }

  test("validate rejects an order with an invalid eventId") {
    val order = OrderPlaced("not-a-uuid", "SKU-1", 2, "22222222-2222-4222-8222-222222222222", "2024-01-01T00:00:00Z", "1")
    assert(handler.validate(order).isLeft)
  }

  test("validate rejects an order with an invalid customerId") {
    val order = OrderPlaced("11111111-1111-4111-8111-111111111111", "SKU-1", 2, "not-a-uuid", "2024-01-01T00:00:00Z", "1")
    assert(handler.validate(order).isLeft)
  }

  test("validate rejects an order with an unsupported schema version") {
    val order = OrderPlaced("11111111-1111-4111-8111-111111111111", "SKU-1", 2, "22222222-2222-4222-8222-222222222222", "2024-01-01T00:00:00Z", "99")
    assert(handler.validate(order).isLeft)
  }

  test("eventIdOf extracts the eventId from a valid body") {
    assertEquals(handler.eventIdOf(validBody), "11111111-1111-4111-8111-111111111111")
  }

  test("eventIdOf returns \"unknown\" for a body that cannot be parsed") {
    assertEquals(handler.eventIdOf("garbage".getBytes("UTF-8")), "unknown")
  }

  test("process calls storage.upsertSku and returns its result without a live broker") {
    val order = handler.decode(validBody).toOption.get
    for {
      result <- handler.process(order)
    } yield assertEquals(result, UpsertResult.Applied)
  }

  test("process propagates DuplicateSkipped from storage") {
    val dedupStorage = new SkuStorage {
      def upsertSku(eventId: String, sku: String, delta: Int): IO[UpsertResult] = IO.pure(UpsertResult.DuplicateSkipped)
      def snapshot: IO[List[SkuStats]] = IO.pure(Nil)
    }
    val dedupHandler: MessageHandler[OrderPlaced] = new OrderPlacedHandler(dedupStorage)
    val order = handler.decode(validBody).toOption.get
    for {
      result <- dedupHandler.process(order)
    } yield assertEquals(result, UpsertResult.DuplicateSkipped)
  }
}
