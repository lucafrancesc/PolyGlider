package com.polyglider

import cats.effect.IO
import cats.effect.std.Queue
import com.rabbitmq.client.AMQP
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.`export`.SimpleSpanProcessor
import munit.CatsEffectSuite
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.polyglider.consumer.RabbitConsumer

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

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

  private def order(
    eventId: String = "11111111-1111-4111-8111-111111111111",
    customerId: String = "22222222-2222-4222-8222-222222222222",
    version: String = "1"
  ) =
    com.polyglider.model.OrderPlaced(eventId, "SKU-1", 2, customerId, "2024-01-01T00:00:00Z", version)

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

  test("validateVersion accepts a supported version") {
    assertEquals(RabbitConsumer.validateVersion(order(version = "1")), Right(order(version = "1")))
  }

  test("validateVersion rejects an unrecognized version") {
    val bad = order(version = "2")
    assertEquals(RabbitConsumer.validateVersion(bad), Left(RabbitConsumer.UnsupportedSchemaVersionException("2")))
  }

  test("a message with an unrecognized version is classified as a permanent failure (straight to DLX, no retry)") {
    import com.polyglider.consumer.ProcessingFailure
    import com.polyglider.consumer.ProcessingFailure.PermanentFailure

    val bad = order(version = "2")
    val result = RabbitConsumer.validateUuids(bad).flatMap(RabbitConsumer.validateVersion)
    val err = result.swap.getOrElse(fail("expected validation to reject the unrecognized version"))
    assertEquals(ProcessingFailure.classify(err), PermanentFailure(err))
  }

  test("eventIdOf extracts eventId from a valid OrderPlaced body") {
    val body =
      """{"eventId":"11111111-1111-1111-1111-111111111111","sku":"SKU-1","quantity":2,"customerId":"22222222-2222-2222-2222-222222222222","timestamp":"2024-01-01T00:00:00Z"}"""
        .getBytes("UTF-8")
    assertEquals(RabbitConsumer.eventIdOf(body), "11111111-1111-1111-1111-111111111111")
  }

  test("eventIdOf returns \"unknown\" for malformed JSON") {
    val body = "{ not: valid json }".getBytes("UTF-8")
    assertEquals(RabbitConsumer.eventIdOf(body), "unknown")
  }

  test("retryWithBackoff succeeds immediately without retrying when the action succeeds") {
    for {
      attempts <- IO.ref(0)
      result   <- RabbitConsumer.retryWithBackoff(
        attempts.updateAndGet(_ + 1),
        summon[Logger[IO]],
        baseDelay = 1.millis, maxDelay = 5.millis, maxJitter = 1.millis
      )
    } yield assertEquals(result, 1)
  }

  test("retryWithBackoff retries on failure until the action eventually succeeds") {
    for {
      attempts <- IO.ref(0)
      action    = attempts.updateAndGet(_ + 1).flatMap { n =>
        if (n < 3) IO.raiseError(new RuntimeException(s"boom $n")) else IO.pure(n)
      }
      result   <- RabbitConsumer.retryWithBackoff(
        action,
        summon[Logger[IO]],
        baseDelay = 1.millis, maxDelay = 5.millis, maxJitter = 1.millis
      )
      finalAttempts <- attempts.get
    } yield {
      assertEquals(result, 3)
      assertEquals(finalAttempts, 3)
    }
  }

  private def withTestTracer[A](f: (io.opentelemetry.api.trace.Tracer, InMemorySpanExporter) => IO[A]): IO[A] = {
    val exporter = InMemorySpanExporter.create()
    val tracerProvider = SdkTracerProvider.builder()
      .addSpanProcessor(SimpleSpanProcessor.create(exporter))
      .build()
    val tracer = tracerProvider.get("test")
    f(tracer, exporter).guarantee(IO.blocking(tracerProvider.shutdown()).void)
  }

  test("withSpan ends the span with no error status when the wrapped IO succeeds") {
    withTestTracer { (tracer, exporter) =>
      for {
        result <- RabbitConsumer.withSpan(new AMQP.BasicProperties(), tracer)(IO.pure(42))
        spans  <- IO.blocking(exporter.getFinishedSpanItems.asScala.toList)
      } yield {
        assertEquals(result, 42)
        assertEquals(spans.map(_.getName), List("process orders.placed"))
        assertEquals(spans.head.getStatus.getStatusCode, StatusCode.UNSET)
      }
    }
  }

  test("withSpan records the exception and sets ERROR status when the wrapped IO fails") {
    withTestTracer { (tracer, exporter) =>
      val boom = new RuntimeException("boom")
      for {
        result <- RabbitConsumer.withSpan(new AMQP.BasicProperties(), tracer)(IO.raiseError[Int](boom)).attempt
        spans  <- IO.blocking(exporter.getFinishedSpanItems.asScala.toList)
      } yield {
        assert(result.isLeft, "the original error must still propagate")
        assertEquals(spans.map(_.getName), List("process orders.placed"))
        assertEquals(spans.head.getStatus.getStatusCode, StatusCode.ERROR)
        assertEquals(spans.head.getEvents.asScala.map(_.getName).toList, List("exception"))
      }
    }
  }
}
