package com.polyglider.consumer

import cats.effect.*
import cats.effect.std.{Dispatcher, Queue}
import cats.syntax.all.*
import org.typelevel.log4cats.Logger
import io.circe.generic.auto._
import com.rabbitmq.client.{ConnectionFactory, DefaultConsumer, Envelope, AMQP, Channel}
import com.polyglider.model.OrderPlaced
import com.polyglider.storage.SkuStorage

import java.nio.charset.StandardCharsets

object RabbitConsumer {
  private case class Delivery(channel: Channel, deliveryTag: Long, body: Array[Byte])

  def start(storage: SkuStorage, logger: Logger[IO], workerCount: Int = 4): Resource[IO, Unit] =
    for {
      queue      <- Resource.eval(Queue.bounded[IO, Delivery](1000))
      dispatcher <- Dispatcher.parallel[IO]
      connRes <- Resource.make(IO.blocking {
        val host = sys.env.getOrElse("RABBIT_HOST", "127.0.0.1")
        val port = sys.env.getOrElse("RABBIT_PORT", "5672").toInt
        val user = sys.env.getOrElse("RABBIT_USER", "guest")
        val pass = sys.env.getOrElse("RABBIT_PASS", "guest")

        val factory = new ConnectionFactory()
        factory.setHost(host)
        factory.setPort(port)
        factory.setUsername(user)
        factory.setPassword(pass)

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
            val d = Delivery(ch, envelope.getDeliveryTag, body)
            dispatcher.unsafeRunAndForget(queue.offer(d))
          }
        }

        ch.basicConsume("orders.placed", false, consumer)
        (conn, ch, host, port)
      })( { case (conn, ch, _, _) => IO.blocking(ch.close()) *> IO.blocking(conn.close()) })

      _ <- Resource.eval(connRes match { case (_, _, host, port) => logger.info(s"Connected to RabbitMQ at $host:$port, consuming from orders.placed") })

      fibers <- Resource.make(
        List.fill(workerCount)(
          queue.take.flatMap { d =>
            val task = for {
              order <- IO.fromEither(_root_.io.circe.parser.parse(new String(d.body, StandardCharsets.UTF_8)).flatMap(_.as[OrderPlaced]))
              _ <- logger.info(s"Message received: eventId=${order.eventId} sku=${order.sku} qty=${order.quantity}")
              _ <- storage.upsertSku(order.eventId, order.sku, order.quantity)
              _ <- logger.info(s"Stored to ledger: sku=${order.sku} qty=${order.quantity}")
              _ <- IO.blocking(d.channel.basicAck(d.deliveryTag, false))
            } yield ()

            task.handleErrorWith { err =>
              logger.error(err)("Failed processing message") *> IO.blocking(d.channel.basicNack(d.deliveryTag, false, false))
            }
          }.foreverM.start
        ).sequence
      )(fs => fs.traverse_(_.cancel))
    } yield ()
}
