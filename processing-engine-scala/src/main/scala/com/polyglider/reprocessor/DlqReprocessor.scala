package com.polyglider.reprocessor

import cats.effect.*
import cats.effect.std.{Dispatcher, Mutex, Queue}
import cats.syntax.all.*
import org.typelevel.log4cats.Logger
import io.circe.generic.auto._
import com.rabbitmq.client.{ConnectionFactory, DefaultConsumer, Envelope, AMQP, Channel}
import com.polyglider.model.OrderPlaced
import com.polyglider.storage.SkuStorage
import com.polyglider.consumer.{RabbitConsumer, RetryPolicy}
import com.polyglider.consumer.ProcessingFailure
import com.polyglider.consumer.ProcessingFailure.{PermanentFailure, TransientFailure}
import com.polyglider.metrics.Metrics

import java.nio.charset.StandardCharsets
import java.time.Instant
import scala.concurrent.duration.{DurationLong, FiniteDuration}

/** Automated DLQ reprocessor.
  *
  * The main consumer's DLX (`dlx.orders.placed`) used to be a dead end requiring a human to
  * look at it. This worker polls that queue, applies its own bounded retry-with-backoff
  * (separate budget from the main consumer's `RetryPolicy`), and only escalates to the
  * genuinely-manual `needs-attention.orders.placed` queue once that budget is exhausted —
  * or immediately, for failures classified as permanent (retrying a malformed payload is
  * never going to help).
  *
  * Every attempt is logged with the original failure reason, the attempt count, and a
  * timestamp, so a human looking at `needs-attention.orders.placed` later can reconstruct
  * what happened without needing to re-run anything.
  */
object DlqReprocessor {
  private val ReprocessCountHeader = "x-reprocess-count"
  private val FailureReasonHeader  = "x-last-failure-reason"

  val defaultRetryPolicy: RetryPolicy = RetryPolicy(
    maxRetries = 3,
    baseDelay = 10.seconds,
    multiplier = 2.0,
    maxJitter = 1.second,
    queuePrefix = "dlq-reprocess.orders.placed"
  )

  // private[polyglider] so tests in com.polyglider can reference the type
  private[polyglider] case class Delivery(
    channel: Channel,
    deliveryTag: Long,
    body: Array[Byte],
    properties: AMQP.BasicProperties = new AMQP.BasicProperties()
  )

  private[polyglider] def reprocessCountOf(properties: AMQP.BasicProperties): Int =
    Option(properties.getHeaders)
      .flatMap(h => Option(h.get(ReprocessCountHeader)))
      .collect { case n: Number => n.intValue() }
      .getOrElse(0)

  private[polyglider] def withReprocessAttempt(properties: AMQP.BasicProperties, count: Int, failureReason: String): AMQP.BasicProperties = {
    val headers = Option(properties.getHeaders)
      .map(h => new java.util.HashMap[String, Object](h))
      .getOrElse(new java.util.HashMap[String, Object]())
    headers.put(ReprocessCountHeader, Integer.valueOf(count))
    headers.put(FailureReasonHeader, failureReason)
    properties.builder().headers(headers).build()
  }

  // Extracted for unit testing: enqueues d, or requeues + logs if the internal queue is full.
  // dlx.orders.placed has no dead-letter-exchange of its own, so on overflow we requeue
  // (rather than nack-without-requeue, which would discard the message permanently).
  private[polyglider] def handleOrRequeue(
    d: Delivery,
    queue: Queue[IO, Delivery],
    requeue: IO[Unit],
    logger: Logger[IO]
  ): IO[Unit] =
    queue.tryOffer(d).flatMap {
      case true  => IO.unit
      case false => requeue *> logger.warn(s"DLQ reprocessor queue full; requeueing tag=${d.deliveryTag}")
    }

  def start(
    storage: SkuStorage,
    logger: Logger[IO],
    retryPolicy: RetryPolicy = defaultRetryPolicy,
    dlqDepthPollInterval: FiniteDuration = 15.seconds
  ): Resource[IO, Unit] =
    for {
      queue        <- Resource.eval(Queue.bounded[IO, Delivery](1000))
      channelMutex <- Resource.eval(Mutex[IO])
      dispatcher   <- Dispatcher.parallel[IO]
      connRes <- Resource.make(IO.blocking {
        val host = sys.env.getOrElse("RABBIT_HOST", "127.0.0.1")
        val ssl  = sys.env.get("RABBIT_SSL").contains("true")
        val port = sys.env.getOrElse("RABBIT_PORT", if (ssl) "5671" else "5672").toInt
        val user = sys.env.getOrElse("RABBIT_USER", "guest")
        val pass = sys.env.getOrElse("RABBIT_PASS", "guest")

        val factory = new ConnectionFactory()
        factory.setHost(host)
        factory.setPort(port)
        factory.setUsername(user)
        factory.setPassword(pass)
        if (ssl) factory.useSslProtocol()

        val conn = factory.newConnection()
        val ch = conn.createChannel()

        // Same topology the main consumer declares; redeclaring with identical args is a no-op.
        ch.exchangeDeclare("dlx.orders.exchange", "fanout", true)
        ch.queueDeclare("dlx.orders.placed", true, false, false, null)
        ch.queueBind("dlx.orders.placed", "dlx.orders.exchange", "")

        // Genuinely-manual escalation path: a human has to look at this queue.
        ch.exchangeDeclare("needs-attention.exchange", "fanout", true)
        ch.queueDeclare("needs-attention.orders.placed", true, false, false, null)
        ch.queueBind("needs-attention.orders.placed", "needs-attention.exchange", "")

        // One retry-tier queue per attempt: fixed TTL delays redelivery, then dead-letters
        // back into dlx.orders.exchange (fanout, so the routing key is irrelevant) where this
        // same consumer picks it up again.
        (1 to retryPolicy.maxRetries).foreach { tier =>
          val retryArgs = new java.util.HashMap[String, AnyRef]()
          retryArgs.put("x-message-ttl", java.lang.Long.valueOf(retryPolicy.delayFor(tier).toMillis))
          retryArgs.put("x-dead-letter-exchange", "dlx.orders.exchange")
          ch.queueDeclare(retryPolicy.retryQueueName(tier), true, false, false, retryArgs)
        }
        ch.basicQos(1)

        val consumer = new DefaultConsumer(ch) {
          override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte]): Unit = {
            val d       = Delivery(ch, envelope.getDeliveryTag, body, properties)
            val requeue = channelMutex.lock.surround(IO.blocking(ch.basicNack(d.deliveryTag, false, true)))
            dispatcher.unsafeRunAndForget(handleOrRequeue(d, queue, requeue, logger))
          }
        }

        ch.basicConsume("dlx.orders.placed", false, consumer)
        (conn, ch, host, port)
      })( { case (conn, ch, _, _) => IO.blocking(ch.close()) *> IO.blocking(conn.close()) })

      _ <- Resource.eval(connRes match { case (_, _, host, port) => logger.info(s"DLQ reprocessor connected to RabbitMQ at $host:$port, consuming from dlx.orders.placed") })

      dlqChannel = connRes._2
      // needs-attention.orders.placed is meant to be empty in steady state, same as
      // dlx.orders.placed — anything sitting in either is a signal something needs a human.
      _ <- Resource.make(
        (List("dlx.orders.placed", "needs-attention.orders.placed").traverse_ { queueName =>
          channelMutex.lock.surround(IO.blocking(dlqChannel.queueDeclarePassive(queueName).getMessageCount))
            .flatMap(depth => IO.delay(Metrics.dlqDepth.labels(queueName).set(depth.toDouble)))
            .handleErrorWith(e => logger.warn(e)(s"Failed to poll $queueName depth"))
        } *> IO.sleep(dlqDepthPollInterval)).foreverM.start
      )(_.cancel)

      fiber <- Resource.make(
        queue.take.flatMap { d =>
          val ack = channelMutex.lock.surround(IO.blocking(d.channel.basicAck(d.deliveryTag, false)))

          def escalate(reason: String, attempts: Int): IO[Unit] =
            channelMutex.lock.surround(IO.blocking {
              val props = withReprocessAttempt(d.properties, attempts, reason)
              d.channel.basicPublish("", "needs-attention.orders.placed", props, d.body)
            }) *> ack

          def retryReprocess(eventId: String, reason: String): IO[Unit] = {
            val attemptsSoFar = reprocessCountOf(d.properties)
            if (attemptsSoFar >= retryPolicy.maxRetries) {
              logger.error(s"[${Instant.now}] DLQ reprocess exhausted for eventId=$eventId after $attemptsSoFar attempt(s) (reason=$reason); escalating to needs-attention.orders.placed") *>
                escalate(reason, attemptsSoFar)
            } else {
              val tier = attemptsSoFar + 1
              for {
                jitterMs <- IO(scala.util.Random.nextLong(retryPolicy.maxJitter.toMillis + 1))
                _        <- IO.sleep(jitterMs.millis)
                _        <- channelMutex.lock.surround(IO.blocking {
                              val retryProps = withReprocessAttempt(d.properties, tier, reason)
                              d.channel.basicPublish("", retryPolicy.retryQueueName(tier), retryProps, d.body)
                            })
                _        <- ack
                _        <- logger.warn(s"[${Instant.now}] DLQ reprocess attempt $tier/${retryPolicy.maxRetries} scheduled for eventId=$eventId (reason=$reason); retrying in ~${retryPolicy.delayFor(tier)}")
              } yield ()
            }
          }

          val task = for {
            parsed <- IO.fromEither(_root_.io.circe.parser.parse(new String(d.body, StandardCharsets.UTF_8)).flatMap(_.as[OrderPlaced]))
            order  <- IO.fromEither(RabbitConsumer.validateUuids(parsed))
            _     <- storage.upsertSku(order.eventId, order.sku, order.quantity)
            _     <- logger.info(s"[${Instant.now}] DLQ reprocess succeeded for eventId=${order.eventId} sku=${order.sku} after ${reprocessCountOf(d.properties)} prior attempt(s)")
            _     <- ack
          } yield ()

          task.handleErrorWith { err =>
            val eventId = RabbitConsumer.eventIdOf(d.body)
            ProcessingFailure.classify(err) match {
              case PermanentFailure(cause) =>
                logger.error(cause)(s"[${Instant.now}] DLQ reprocess hit a permanent failure for eventId=$eventId; escalating immediately") *>
                  escalate(cause.getMessage, reprocessCountOf(d.properties))
              case TransientFailure(cause) =>
                logger.warn(cause)(s"[${Instant.now}] DLQ reprocess hit a transient failure for eventId=$eventId") *> retryReprocess(eventId, cause.getMessage)
            }
          }
        }.foreverM.start
      )(_.cancel)
    } yield ()
}
