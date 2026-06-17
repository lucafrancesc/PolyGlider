package com.polyglider

import cats.effect.IO
import cats.effect.std.Queue
import munit.CatsEffectSuite
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.polyglider.consumer.RabbitConsumer

class RabbitConsumerSpec extends CatsEffectSuite {
  type Delivery = RabbitConsumer.Delivery

  private given Logger[IO] = Slf4jLogger.getLoggerFromClass[IO](classOf[RabbitConsumerSpec])

  test("handleOrDrop enqueues message when queue has space") {
    for {
      nacked <- IO.ref(false)
      queue  <- Queue.bounded[IO, Delivery](10)
      d       = RabbitConsumer.Delivery(null, 1L, Array.emptyByteArray)
      _      <- RabbitConsumer.handleOrDrop(d, queue, nacked.set(true), summon[Logger[IO]])
      item   <- queue.tryTake
      wasNacked <- nacked.get
    } yield {
      assert(item.isDefined, "message should be enqueued")
      assert(!wasNacked, "nack must not be called when queue has space")
    }
  }

  test("handleOrDrop nacks immediately when queue is full") {
    for {
      nacked <- IO.ref(false)
      queue  <- Queue.bounded[IO, Delivery](1)
      // fill the queue so tryOffer returns false
      _      <- queue.offer(RabbitConsumer.Delivery(null, 0L, Array.emptyByteArray))
      d       = RabbitConsumer.Delivery(null, 42L, Array.emptyByteArray)
      _      <- RabbitConsumer.handleOrDrop(d, queue, nacked.set(true), summon[Logger[IO]])
      wasNacked <- nacked.get
      // the original filler is still in the queue; the dropped message was never enqueued
      size   <- queue.size
    } yield {
      assert(wasNacked, "nack must be called when queue is full")
      assertEquals(size, 1, "dropped message must not be added to the full queue")
    }
  }

  test("retryCountOf defaults to 0 when no header is present") {
    val props = new com.rabbitmq.client.AMQP.BasicProperties()
    assertEquals(RabbitConsumer.retryCountOf(props), 0)
  }

  test("withRetryCount round-trips through retryCountOf") {
    val props      = new com.rabbitmq.client.AMQP.BasicProperties()
    val withCount1 = RabbitConsumer.withRetryCount(props, 1)
    assertEquals(RabbitConsumer.retryCountOf(withCount1), 1)

    val withCount2 = RabbitConsumer.withRetryCount(withCount1, 2)
    assertEquals(RabbitConsumer.retryCountOf(withCount2), 2)
  }

  test("withRetryCount preserves other existing headers") {
    val headers = new java.util.HashMap[String, Object]()
    headers.put("custom-header", "value")
    val props   = new com.rabbitmq.client.AMQP.BasicProperties.Builder().headers(headers).build()
    val updated = RabbitConsumer.withRetryCount(props, 3)
    assertEquals(RabbitConsumer.retryCountOf(updated), 3)
    assertEquals(updated.getHeaders.get("custom-header"), "value")
  }

  private def order(eventId: String = "11111111-1111-4111-8111-111111111111", customerId: String = "22222222-2222-4222-8222-222222222222") =
    com.polyglider.model.OrderPlaced(eventId, "SKU-1", 2, customerId, "2024-01-01T00:00:00Z")

  test("validateUuids accepts an order with valid eventId and customerId") {
    assertEquals(RabbitConsumer.validateUuids(order()), Right(order()))
  }

  test("validateUuids rejects an invalid eventId") {
    val bad = order(eventId = "not-a-uuid")
    assertEquals(RabbitConsumer.validateUuids(bad), Left(RabbitConsumer.InvalidUuidException("eventId", "not-a-uuid")))
  }

  test("validateUuids rejects an invalid customerId") {
    val bad = order(customerId = "not-a-uuid")
    assertEquals(RabbitConsumer.validateUuids(bad), Left(RabbitConsumer.InvalidUuidException("customerId", "not-a-uuid")))
  }
}
