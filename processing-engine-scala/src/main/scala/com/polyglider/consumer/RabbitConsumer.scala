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

import java.nio.charset.StandardCharsets

object RabbitConsumer {
  // private[polyglider] so tests in com.polyglider can reference the type
  private[polyglider] case class Delivery(channel: Channel, deliveryTag: Long, body: Array[Byte])

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

  def start(storage: SkuStorage, logger: Logger[IO], workerCount: Int = 4, summaryEvery: Long = 10): Resource[IO, Unit] =
    for {
      queue      <- Resource.eval(Queue.bounded[IO, Delivery](1000))
      counter    <- Resource.eval(Ref[IO].of(0L))
      // RabbitMQ Channel is not thread-safe; serialize all ack/nack calls across worker fibers
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
        ch.basicQos(1)

        val consumer = new DefaultConsumer(ch) {
          override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte]): Unit = {
            val d    = Delivery(ch, envelope.getDeliveryTag, body)
            val nack = channelMutex.lock.surround(IO.blocking(ch.basicNack(d.deliveryTag, false, false)))
            dispatcher.unsafeRunAndForget(handleOrDrop(d, queue, nack, logger))
          }
        }

        ch.basicConsume("orders.placed", false, consumer)
        (conn, ch, host, port)
      })( { case (conn, ch, _, _) => IO.blocking(ch.close()) *> IO.blocking(conn.close()) })

      _ <- Resource.eval(connRes match { case (_, _, host, port) => logger.info(s"Connected to RabbitMQ at $host:$port, consuming from orders.placed") })

      fibers <- Resource.make(
        List.fill(workerCount)(
          queue.take.flatMap { d =>
            val ack          = channelMutex.lock.surround(IO.blocking(d.channel.basicAck(d.deliveryTag, false)))
            val nackToDlx    = channelMutex.lock.surround(IO.blocking(d.channel.basicNack(d.deliveryTag, false, false)))
            val nackRequeue  = channelMutex.lock.surround(IO.blocking(d.channel.basicNack(d.deliveryTag, false, true)))

            val task = for {
              order <- IO.fromEither(_root_.io.circe.parser.parse(new String(d.body, StandardCharsets.UTF_8)).flatMap(_.as[OrderPlaced]))
              _ <- logger.info(s"Message received: eventId=${order.eventId} sku=${order.sku} qty=${order.quantity}")
              _ <- storage.upsertSku(order.eventId, order.sku, order.quantity)
              _ <- logger.info(s"Stored to ledger: sku=${order.sku} qty=${order.quantity}")
              _ <- ack
              n <- counter.updateAndGet(_ + 1)
              _ <- if (n % summaryEvery == 0) logSnapshot(storage, logger) else IO.unit
            } yield ()

            task.handleErrorWith { err =>
              ProcessingFailure.classify(err) match {
                case PermanentFailure(cause) =>
                  logger.error(cause)("Permanent failure processing message; routing to DLX") *> nackToDlx
                case TransientFailure(cause) =>
                  logger.warn(cause)("Transient failure processing message; requeueing for retry") *> nackRequeue
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
