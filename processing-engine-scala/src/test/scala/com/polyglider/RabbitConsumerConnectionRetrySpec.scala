package com.polyglider

import cats.effect.IO
import com.dimafeng.testcontainers.RabbitMQContainer
import com.rabbitmq.client.ConnectionFactory
import munit.CatsEffectSuite
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.polyglider.consumer.RabbitConsumer

import scala.concurrent.duration._

/** Exercises `RabbitConsumer.retryWithBackoff` against a real, paused RabbitMQ broker —
  * i.e. one that accepts the TCP connection but never completes the AMQP handshake, unlike a
  * closed port (immediate connection-refused). `RabbitConsumerSpec` covers the backoff/retry
  * loop with a fake IO action; this covers the actual `ConnectionFactory.newConnection()` path
  * that loop wraps in production.
  */
class RabbitConsumerConnectionRetrySpec extends CatsEffectSuite {
  private val container = RabbitMQContainer()

  private given Logger[IO] = Slf4jLogger.getLoggerFromClass[IO](classOf[RabbitConsumerConnectionRetrySpec])

  override def beforeAll(): Unit = {
    super.beforeAll()
    container.start()
  }

  override def afterAll(): Unit = {
    container.stop()
    super.afterAll()
  }

  private def connectIO(attempts: cats.effect.Ref[IO, Int]): IO[com.rabbitmq.client.Connection] =
    attempts.update(_ + 1) *> IO.blocking {
      val factory = new ConnectionFactory()
      factory.setHost(container.container.getHost)
      factory.setPort(container.container.getMappedPort(5672))
      factory.setUsername("guest")
      factory.setPassword("guest")
      // Bound each attempt so a paused broker fails fast enough for the test to observe
      // multiple retries within a reasonable wall-clock time, instead of relying on the
      // RabbitMQ client's much longer default timeouts.
      factory.setConnectionTimeout(2000)
      factory.setHandshakeTimeout(2000)
      factory.newConnection()
    }

  test("retryWithBackoff keeps retrying against a paused (unreachable) broker, then connects once unpaused") {
    val dockerClient = container.container.getDockerClient
    val containerId = container.container.getContainerId

    for {
      attempts <- IO.ref(0)
      _        <- IO.blocking(dockerClient.pauseContainerCmd(containerId).exec())
      fiber    <- RabbitConsumer.retryWithBackoff(
        connectIO(attempts),
        summon[Logger[IO]],
        baseDelay = 50.millis,
        maxDelay = 200.millis,
        maxJitter = 50.millis
      ).start
      // Give it time to fail and retry a few times while genuinely unreachable.
      _ <- IO.sleep(3.seconds)
      attemptsWhilePaused <- attempts.get
      _ <- IO.blocking(dockerClient.unpauseContainerCmd(containerId).exec())
      conn <- fiber.joinWithNever
      isOpen = conn.isOpen
      closeReason = if (isOpen) None else Option(conn.getCloseReason)
      _ <- IO.whenA(isOpen)(IO.blocking(conn.close()))
    } yield {
      assert(attemptsWhilePaused >= 2, s"expected at least 2 retry attempts while paused, got $attemptsWhilePaused")
      assert(isOpen, s"connection should be open once the broker is unpaused (closeReason=$closeReason)")
    }
  }
}
