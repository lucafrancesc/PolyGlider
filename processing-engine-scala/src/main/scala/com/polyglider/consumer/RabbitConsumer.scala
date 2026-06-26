package com.polyglider.consumer

import cats.effect.*
import cats.effect.std.{Dispatcher, Mutex, Queue}
import cats.syntax.all.*
import org.typelevel.log4cats.Logger
import io.circe.generic.auto._
import com.rabbitmq.client.{ConnectionFactory, DefaultConsumer, Envelope, AMQP, Channel}
import com.polyglider.model.OrderPlaced
import com.polyglider.storage.{SkuStats, UpsertResult}
import com.polyglider.consumer.ProcessingFailure.{PermanentFailure, TransientFailure}
import com.polyglider.UuidUtils
import com.polyglider.metrics.Metrics
import com.polyglider.tracing.Tracing
import io.opentelemetry.api.trace.{SpanKind, StatusCode, Tracer}

import java.nio.charset.StandardCharsets
import java.time.Instant
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

  /** See ADR-006: an unrecognized version means this consumer doesn't know how to interpret the
    * payload's shape, which retrying can't fix — same default-Permanent reasoning as a malformed
    * UUID above.
    */
  private[polyglider] final case class UnsupportedSchemaVersionException(version: String)
    extends RuntimeException(s"Unsupported event schema version: '$version'")

  private[polyglider] val SupportedVersions: Set[String] = Set("1")

  private[polyglider] def validateVersion(order: OrderPlaced): Either[UnsupportedSchemaVersionException, OrderPlaced] =
    if (SupportedVersions.contains(order.version)) Right(order)
    else Left(UnsupportedSchemaVersionException(order.version))

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

  /** Backs the `polyglider_order_processing_duration_seconds` SLO histogram (see ADR-007). A
    * malformed/unparseable timestamp is an observability gap, not a processing failure -- it
    * must never fail the message itself, so a parse error is logged and skipped rather than
    * propagated.
    */
  private[polyglider] def recordProcessingLatency(timestamp: String, now: Instant, logger: Logger[IO]): IO[Unit] =
    IO(Instant.parse(timestamp)).attempt.flatMap {
      case Right(emittedAt) =>
        val seconds = java.time.Duration.between(emittedAt, now).toMillis / 1000.0
        IO.delay(Metrics.orderProcessingDuration.observe(seconds))
      case Left(err) =>
        logger.warn(err)(s"Could not parse order timestamp '$timestamp' for the processing-latency metric; skipping observation")
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

  /** Starts a span parented onto the trace context carried in the message's `traceparent`
    * header (injected by the C# gateway at publish time), runs `io` inside it, and always ends
    * the span -- recording the exception and an ERROR status on failure so it doesn't just
    * silently end as if it succeeded.
    */
  private[polyglider] def withSpan[A](properties: AMQP.BasicProperties, tracer: Tracer = Tracing.tracer)(io: IO[A]): IO[A] =
    IO.blocking {
      val parentCtx = Tracing.extract(properties.getHeaders)
      tracer.spanBuilder("process orders.placed")
        .setParent(parentCtx)
        .setSpanKind(SpanKind.CONSUMER)
        .startSpan()
    }.flatMap { span =>
      io.guaranteeCase {
        case Outcome.Succeeded(_) => IO.blocking(span.end())
        case Outcome.Errored(e)   => IO.blocking { span.recordException(e); span.setStatus(StatusCode.ERROR); span.end() }
        case Outcome.Canceled()   => IO.blocking { span.setStatus(StatusCode.ERROR, "cancelled"); span.end() }
      }
    }

  /** Retries `io` with capped exponential backoff + jitter until it succeeds, logging each
    * failure. The initial RabbitMQ connection used to be a single attempt -- if it raced
    * RabbitMQ's own startup (e.g. in the containerized stack, where `depends_on` only waits for
    * the container to start, not for the broker to accept connections) it threw and the whole
    * consumer Resource unwound, exiting the process with no way back. Retrying indefinitely
    * here matches the gateway's own RabbitMQ reconnect behavior (RabbitMqPublisherWorker) and
    * this app's general fail-open-and-keep-trying posture (see CircuitBreaker, retry-with-backoff
    * for message processing) rather than giving up.
    */
  private[polyglider] def retryWithBackoff[A](
    io: IO[A],
    logger: Logger[IO],
    baseDelay: FiniteDuration = 1.second,
    maxDelay: FiniteDuration = 30.seconds,
    multiplier: Double = 2.0,
    maxJitter: FiniteDuration = 1.second
  ): IO[A] = {
    def loop(attempt: Int): IO[A] =
      io.handleErrorWith { err =>
        for {
          jitterMs <- IO(scala.util.Random.nextLong(maxJitter.toMillis + 1))
          delay     = (baseDelay.toMillis * math.pow(multiplier, attempt.toDouble)).toLong.min(maxDelay.toMillis).millis + jitterMs.millis
          _        <- logger.warn(err)(s"Failed to connect to RabbitMQ (attempt ${attempt + 1}); retrying in $delay")
          _        <- IO.sleep(delay)
          result   <- loop(attempt + 1)
        } yield result
      }
    loop(0)
  }

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
    handler: MessageHandler[OrderPlaced],
    logger: Logger[IO],
    workerCount: Int = 4,
    queueSize: Int = 1000,
    summaryEvery: Long = 10,
    retryPolicy: RetryPolicy = RetryPolicy.default,
    queueDepthPollInterval: FiniteDuration = 15.seconds,
    snapshotFn: IO[List[SkuStats]] = IO.pure(Nil),
    mainQueue: String = "orders.placed",
    mainExchange: String = "orders.exchange",
    dlxExchange: String = "dlx.orders.exchange",
    dlxQueue: String = "dlx.orders.placed"
  ): Resource[IO, Unit] =
    for {
      queue      <- Resource.eval(Queue.bounded[IO, Delivery](queueSize))
      counter    <- Resource.eval(Ref[IO].of(0L))
      // RabbitMQ Channel is not thread-safe; serialize all ack/nack/publish calls across worker fibers
      channelMutex <- Resource.eval(Mutex[IO])
      dispatcher <- Dispatcher.parallel[IO]
      connRes <- Resource.make(retryWithBackoff(IO.blocking {
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

        ch.exchangeDeclare(mainExchange, "topic", true)
        ch.exchangeDeclare(dlxExchange, "fanout", true)
        ch.queueDeclare(dlxQueue, true, false, false, null)
        ch.queueBind(dlxQueue, dlxExchange, "")
        val queueArgs = new java.util.HashMap[String, AnyRef]()
        queueArgs.put("x-dead-letter-exchange", dlxExchange)
        ch.queueDeclare(mainQueue, true, false, false, queueArgs)
        ch.queueBind(mainQueue, mainExchange, mainQueue)
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
          retryArgs.put("x-dead-letter-exchange", mainExchange)
          retryArgs.put("x-dead-letter-routing-key", mainQueue)
          ch.queueDeclare(retryPolicy.retryQueueName(tier), true, false, false, retryArgs)
        }

        val consumer = new DefaultConsumer(ch) {
          override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte]): Unit = {
            val d    = Delivery(ch, envelope.getDeliveryTag, body, properties)
            val nack = channelMutex.lock.surround(IO.blocking(ch.basicNack(d.deliveryTag, false, false)))
            dispatcher.unsafeRunAndForget(handleOrDrop(d, queue, nack, logger))
          }
        }

        ch.basicConsume(mainQueue, false, consumer)
        (conn, ch, host, port)
      }, logger))( { case (conn, ch, _, _) => IO.blocking(ch.close()) *> IO.blocking(conn.close()) })

      _ <- Resource.eval(connRes match { case (_, _, host, port) => logger.info(s"Connected to RabbitMQ at $host:$port, consuming from $mainQueue") })

      mainChannel = connRes._2
      // Consumer-lag proxy: how many messages are sitting in mainQueue waiting to be
      // delivered. Unlike dlx.orders.placed/needs-attention.orders.placed (which should be
      // near-zero in steady state), this one is expected to fluctuate with normal traffic --
      // it's useful as a rate-of-change/trend signal rather than a fixed alert threshold.
      _ <- Resource.make(
        (channelMutex.lock.surround(IO.blocking(mainChannel.queueDeclarePassive(mainQueue).getMessageCount))
          .flatMap(depth => IO.delay(Metrics.dlqDepth.labels(mainQueue).set(depth.toDouble)))
          .handleErrorWith(e => logger.warn(e)(s"Failed to poll $mainQueue depth"))
          *> IO.sleep(queueDepthPollInterval)).foreverM.start
      )(_.cancel)

      fibers <- Resource.make(
        List.fill(workerCount)(
          queue.take.flatMap { d =>
            val ack       = channelMutex.lock.surround(IO.blocking(d.channel.basicAck(d.deliveryTag, false)))
            val nackToDlx = channelMutex.lock.surround(IO.blocking(d.channel.basicNack(d.deliveryTag, false, false)))

            def retryWithBackoff: IO[Unit] = {
              val attemptsSoFar = retryCountOf(d.properties)
              val eventId = handler.eventIdOf(d.body)
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

            val task = withSpan(d.properties) {
              for {
                parsed <- IO.fromEither(handler.decode(d.body))
                order  <- IO.fromEither(handler.validate(parsed))
                _ <- logger.info(s"Message received: eventId=${order.eventId} sku=${order.sku} qty=${order.quantity}")
                result <- handler.process(order)
                _ <- result match {
                  case UpsertResult.Applied =>
                    logger.info(s"Stored to ledger: eventId=${order.eventId} sku=${order.sku} qty=${order.quantity}")
                  case UpsertResult.DuplicateSkipped =>
                    logger.info(s"Duplicate eventId=${order.eventId} — skipped (already applied)")
                }
                _ <- ack
                now <- IO.realTimeInstant
                _ <- recordProcessingLatency(order.timestamp, now, logger)
                _ <- IO.delay(Metrics.messagesProcessed.inc())
                n <- counter.updateAndGet(_ + 1)
                _ <- if (n % summaryEvery == 0) logSnapshot(snapshotFn, logger) else IO.unit
              } yield ()
            }

            task.handleErrorWith { err =>
              val eventId = handler.eventIdOf(d.body)
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

  private def logSnapshot(snapshotFn: IO[List[SkuStats]], logger: Logger[IO]): IO[Unit] =
    for {
      stats  <- snapshotFn
      total   = stats.foldLeft((0L, 0L)) { case ((orders, units), s) => (orders + s.orderCount, units + s.qty) }
      _      <- logger.info(s"── Analytics snapshot (${total._1} orders, ${total._2} units) ──")
      _      <- stats.traverse_(s => logger.info(f"  ${s.sku}%-22s orders=${s.orderCount}%-8d units=${s.qty}"))
    } yield ()
}
