package com.polyglider

import cats.effect.IO
import cats.effect.std.Queue
import munit.CatsEffectSuite
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.polyglider.consumer.Delivery
import com.polyglider.reprocessor.DlqReprocessor

class DlqReprocessorSpec extends CatsEffectSuite {

  private given Logger[IO] = Slf4jLogger.getLoggerFromClass[IO](classOf[DlqReprocessorSpec])

  test("reprocessCountOf defaults to 0 when no header is present") {
    val props = new com.rabbitmq.client.AMQP.BasicProperties()
    assertEquals(DlqReprocessor.reprocessCountOf(props), 0)
  }

  test("withReprocessAttempt round-trips through reprocessCountOf") {
    val props      = new com.rabbitmq.client.AMQP.BasicProperties()
    val withCount1 = DlqReprocessor.withReprocessAttempt(props, 1, "postgres connection refused")
    assertEquals(DlqReprocessor.reprocessCountOf(withCount1), 1)

    val withCount2 = DlqReprocessor.withReprocessAttempt(withCount1, 2, "postgres connection refused")
    assertEquals(DlqReprocessor.reprocessCountOf(withCount2), 2)
  }

  test("withReprocessAttempt records the failure reason header") {
    val props   = new com.rabbitmq.client.AMQP.BasicProperties()
    val updated = DlqReprocessor.withReprocessAttempt(props, 1, "broker unreachable")
    assertEquals(updated.getHeaders.get("x-last-failure-reason"), "broker unreachable")
  }

  test("withReprocessAttempt preserves other existing headers") {
    val headers = new java.util.HashMap[String, Object]()
    headers.put("custom-header", "value")
    val props   = new com.rabbitmq.client.AMQP.BasicProperties.Builder().headers(headers).build()
    val updated = DlqReprocessor.withReprocessAttempt(props, 1, "timeout")
    assertEquals(updated.getHeaders.get("custom-header"), "value")
  }

  test("handleOrRequeue enqueues message when queue has space") {
    for {
      requeued <- IO.ref(false)
      queue    <- Queue.bounded[IO, Delivery](10)
      d         = Delivery(null,1L, Array.emptyByteArray)
      _        <- DlqReprocessor.handleOrRequeue(d, queue, requeued.set(true), summon[Logger[IO]])
      item     <- queue.tryTake
      wasRequeued <- requeued.get
    } yield {
      assert(item.isDefined, "message should be enqueued")
      assert(!wasRequeued, "requeue must not be called when queue has space")
    }
  }

  test("handleOrRequeue requeues immediately when queue is full") {
    for {
      requeued <- IO.ref(false)
      queue    <- Queue.bounded[IO, Delivery](1)
      _        <- queue.offer(Delivery(null,0L, Array.emptyByteArray))
      d         = Delivery(null,42L, Array.emptyByteArray)
      _        <- DlqReprocessor.handleOrRequeue(d, queue, requeued.set(true), summon[Logger[IO]])
      wasRequeued <- requeued.get
      size     <- queue.size
    } yield {
      assert(wasRequeued, "requeue must be called when queue is full")
      assertEquals(size, 1, "dropped message must not be added to the full queue")
    }
  }
}
