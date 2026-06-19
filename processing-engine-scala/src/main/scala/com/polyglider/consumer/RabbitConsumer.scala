package com.polyglider.consumer

import cats.effect.*
import cats.effect.std.{Dispatcher, Mutex, Queue}
import cats.syntax.all.*
import org.typelevel.log4cats.Logger
import io.circe.generic.auto._
import com.rabbitmq.client.{ConnectionFactory, DefaultConsumer, Envelope, AMQP, Channel}
import com.polyglider.model.OrderPlaced
import com.polyglider.storage.SkuStorage
import com.polyglider.consumer.ProcessingFailure.{PermanentFailure, TransientFailure}
import com.polyglider.UuidUtils
import com.polyglider.metrics.Metrics

import java.nio.charset.StandardCharsets
import scala.concurrent.duration.{DurationLong, FiniteDuration}

object RabbitConsumer {
  private val RetryCountHeader = "x-retry-count"

  /** A malformed UUID is a property of this specific payload, not the infrastructure, so it
    * falls into the default-Permanent bucket in `ProcessingFailure.classify` — retrying would
    * just reproduce the same invalid value.
    */
  private[polyglider] final case class InvalidUuidException(field: String, value: String)
    extends RuntimeException(s"Invalid UUID in field '$field': '$value'")

  private[polyglider] def validateUuids(order: OrderPlaced): Either[InvalidUuidException, OrderPlaced] =
    if (!UuidUtils.isValidUuid(order.eventId)) Left(InvalidUuidException("eventId", order.eventId))
    else if (!UuidUtils.isValidUuid(order.customerId)) Left(InvalidUuidException("customerId", order.customerId))
    else Right(order)

  // private[polyglider] so tests in com.polyglider can reference the type
  private[polyglider] case class Delivery(
    channel: Channel,
    deliveryTag: Long,
    body: Array[Byte],
    properties: AMQP.BasicProperties = new AMQP.BasicProperties()
  )

  private[polyglider] def retryCountOf(properties: AMQP.BasicProperties): Int =
    Option(properties.getHeaders)
      .flatMap(h => Option(h.get(RetryCountHeader)))
      .collect {
        case n: Number => n.intValue()
      }
      .getOrElse(0)

  private[polyglider] def withRetryCount(properties: AMQP.BasicProperties, count: Int): AMQP.BasicProperties = {
    val headers = Option(properties.getHeaders)
      .map(h => new java.util.HashMap[String, Object](h))
      .getOrElse(new java.util.HashMap[String, Object]())
    headers.put(RetryCountHeader, Integer.valueOf(count))
    properties.builder().headers(headers).build()
  }

  /** Best-effort extraction of `eventId` for log correlation, even on the failure path where the
    * body may not have been (re-)parsed yet. `eventId` is generated once by the gateway and
    * carried unchanged through every retry/DLX/reprocess hop, so it doubles as the end-to-end
    * correlation id without needing a separate `traceId` field on the wire.
    */
  private[polyglider] def eventIdOf(body: Array[Byte]): String =
    _root_.io.circe.parser.parse(new String(body, StandardCharsets.UTF_8))
      .flatMap(_.as[OrderPlaced])
      .map(_.eventId)
      .getOrElse("unknown")

  // Extracted for unit testing: enqueues d, or nacks + logs if the internal queue is full.
  private[polyglider] def handleOrDrop(
    d: Delivery,
    queue: Queue[IO, Delivery],
    nack: IO[Unit],
    logger: Logger[IO]
  ): IO[Unit] =
    queue.tryOffer(d).flatMap {
      case true  => IO.unit
      case false => nack *> logger.warn(s"Consumer queue full; nacking tag=${d.deliveryTag} — message routed to DLX")
    }

  def start(
    storage: SkuStorage,
    logger: Logger[IO],
    workerCount: Int = 4,
    queueSize: Int = 1000,
    summaryEvery: Long = 10,
    retryPolicy: RetryPolicy = RetryPolicy.default,
    queueDepthPollInterval: FiniteDuration = 15.seconds
  ): Resource[IO, Unit] =
    for {
      queue      <- Resource.eval(Queue.bounded[IO, Delivery](queueSize))
      counter    <- Resource.eval(Ref[IO].of(0L))
      // RabbitMQ Channel is not thread-safe; serialize all ack/nack/publish calls across worker fibers
      channelMutex <- Resource.eval(Mutex[IO])
      dispatcher <- Dispatcher.parallel[IO]
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
        // Enable AMQPS when RABBIT_SSL=true; accepts self-signed certs in dev
        if (ssl) factory.useSslProtocol()

        val conn = factory.newConnection()
        val ch = conn.createChannel()

        ch.exchangeDeclare("orders.exchange", "topic", true)
        ch.exchangeDeclare("dlx.orders.exchange", "fanout", true)
        ch.queueDeclare("dlx.orders.placed", true, false, false, null)
        ch.queueBind("dlx.orders.placed", "dlx.orders.exchange", "")
        val queueArgs = new java.util.HashMap[String, AnyRef]()
        queueArgs.put("x-dead-letter-exchange", "dlx.orders.exchange")
        ch.queueDeclare("orders.placed", true, false, false, queueArgs)
        ch.queueBind("orders.placed", "orders.exchange", "orders.placed")
        // Prefetch must scale with workerCount: a per-consumer prefetch of 1 lets RabbitMQ
        // deliver only one unacked message at a time regardless of how many worker fibers are
        // waiting on the internal queue, serializing throughput to one in-flight message no
        // matter how many fibers are configured (see #70 — this previously meant a Postgres
        // outage could only ever produce one consecutive failure every ~30s, never enough to
        // trip the circuit breaker's default threshold within a realistic outage window).
        ch.basicQos(workerCount)

        // One queue per backoff tier: a fixed x-message-ttl delays redelivery, and
        // x-dead-letter-exchange routes the expired message back to the main queue once the
        // delay elapses. RabbitMQ handles the timing — no in-process scheduler needed.
        (1 to retryPolicy.maxRetries).foreach { tier =>
          val retryArgs = new java.util.HashMap[String, AnyRef]()
          retryArgs.put("x-message-ttl", java.lang.Long.valueOf(retryPolicy.delayFor(tier).toMillis))
          retryArgs.put("x-dead-letter-exchange", "orders.exchange")
          retryArgs.put("x-dead-letter-routing-key", "orders.placed")
          ch.queueDeclare(retryPolicy.retryQueueName(tier), true, false, false, retryArgs)
        }

        val consumer = new DefaultConsumer(ch) {
          override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte]): Unit = {
            val d    = Delivery(ch, envelope.getDeliveryTag, body, properties)
            val nack = channelMutex.lock.surround(IO.blocking(ch.basicNack(d.deliveryTag, false, false)))
            dispatcher.unsafeRunAndForget(handleOrDrop(d, queue, nack, logger))
          }
        }

        ch.basicConsume("orders.placed", false, consumer)
        (conn, ch, host, port)
      })( { case (conn, ch, _, _) => IO.blocking(ch.close()) *> IO.blocking(conn.close()) })

      _ <- Resource.eval(connRes match { case (_, _, host, port) => logger.info(s"Connected to RabbitMQ at $host:$port, consuming from orders.placed") })

      mainChannel = connRes._2
      // Consumer-lag proxy: how many messages are sitting in orders.placed waiting to be
      // delivered. Unlike dlx.orders.placed/needs-attention.orders.placed (which should be
      // near-zero in steady state), this one is expected to fluctuate with normal traffic --
      // it's useful as a rate-of-change/trend signal rather than a fixed alert threshold.
      _ <- Resource.make(
        (channelMutex.lock.surround(IO.blocking(mainChannel.queueDeclarePassive("orders.placed").getMessageCount))
          .flatMap(depth => IO.delay(Metrics.dlqDepth.labels("orders.placed").set(depth.toDouble)))
          .handleErrorWith(e => logger.warn(e)("Failed to poll orders.placed depth"))
          *> IO.sleep(queueDepthPollInterval)).foreverM.start
      )(_.cancel)

      fibers <- Resource.make(
        List.fill(workerCount)(
          queue.take.flatMap { d =>
            val ack       = channelMutex.lock.surround(IO.blocking(d.channel.basicAck(d.deliveryTag, false)))
            val nackToDlx = channelMutex.lock.surround(IO.blocking(d.channel.basicNack(d.deliveryTag, false, false)))

            def retryWithBackoff: IO[Unit] = {
              val attemptsSoFar = retryCountOf(d.properties)
              val eventId = eventIdOf(d.body)
              if (attemptsSoFar >= retryPolicy.maxRetries) {
                logger.warn(s"Exceeded max retries (${retryPolicy.maxRetries}) for eventId=$eventId tag=${d.deliveryTag}; routing to DLX") *> nackToDlx
              } else {
                val tier = attemptsSoFar + 1
                for {
                  jitterMs <- IO(scala.util.Random.nextLong(retryPolicy.maxJitter.toMillis + 1))
                  _        <- IO.sleep(jitterMs.millis)
                  _        <- channelMutex.lock.surround(IO.blocking {
                                val retryProps = withRetryCount(d.properties, tier)
                                d.channel.basicPublish("", retryPolicy.retryQueueName(tier), retryProps, d.body)
                              })
                  // The retry queue now owns redelivery; ack the original so it isn't redelivered too.
                  _        <- ack
                  _        <- IO.delay(Metrics.retries.inc())
                  _        <- logger.warn(s"Transient failure for eventId=$eventId tag=${d.deliveryTag}; scheduled retry $tier/${retryPolicy.maxRetries} in ~${retryPolicy.delayFor(tier)}")
                } yield ()
              }
            }

            val task = for {
              parsed <- IO.fromEither(_root_.io.circe.parser.parse(new String(d.body, StandardCharsets.UTF_8)).flatMap(_.as[OrderPlaced]))
              order  <- IO.fromEither(validateUuids(parsed))
              _ <- logger.info(s"Message received: eventId=${order.eventId} sku=${order.sku} qty=${order.quantity}")
              _ <- storage.upsertSku(order.eventId, order.sku, order.quantity)
              _ <- logger.info(s"Stored to ledger: eventId=${order.eventId} sku=${order.sku} qty=${order.quantity}")
              _ <- ack
              _ <- IO.delay(Metrics.messagesProcessed.inc())
              n <- counter.updateAndGet(_ + 1)
              _ <- if (n % summaryEvery == 0) logSnapshot(storage, logger) else IO.unit
            } yield ()

            task.handleErrorWith { err =>
              val eventId = eventIdOf(d.body)
              ProcessingFailure.classify(err) match {
                case PermanentFailure(cause) =>
                  IO.delay(Metrics.permanentFailures.inc()) *>
                    logger.error(cause)(s"Permanent failure processing message eventId=$eventId tag=${d.deliveryTag}; routing to DLX") *> nackToDlx
                case TransientFailure(cause) =>
                  IO.delay(Metrics.transientFailures.inc()) *>
                    logger.warn(cause)(s"Transient failure processing message eventId=$eventId tag=${d.deliveryTag}; scheduling backoff retry") *> retryWithBackoff
              }
            }
          }.foreverM.start
        ).sequence
      )(fs => fs.traverse_(_.cancel))
    } yield ()

  private def logSnapshot(storage: SkuStorage, logger: Logger[IO]): IO[Unit] =
    for {
      stats  <- storage.snapshot
      total   = stats.foldLeft((0L, 0L)) { case ((orders, units), s) => (orders + s.orderCount, units + s.qty) }
      _      <- logger.info(s"── Analytics snapshot (${total._1} orders, ${total._2} units) ──")
      _      <- stats.traverse_(s => logger.info(f"  ${s.sku}%-22s orders=${s.orderCount}%-8d units=${s.qty}"))
    } yield ()
}
