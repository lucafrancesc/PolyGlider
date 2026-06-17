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
}
