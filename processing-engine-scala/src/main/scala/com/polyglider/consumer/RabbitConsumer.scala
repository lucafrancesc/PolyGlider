package com.polyglider.consumer

import cats.effect.*
import cats.effect.unsafe.implicits.global
import org.typelevel.log4cats.Logger
import io.circe.generic.auto._
import com.rabbitmq.client.{ConnectionFactory, DefaultConsumer, Envelope, AMQP}
import doobie.Transactor
import com.polyglider.model.OrderPlaced
import com.polyglider.db.Database

import java.nio.charset.StandardCharsets

object RabbitConsumer {
  def start(xa: Transactor[IO], logger: Logger[IO]): IO[Unit] = IO.blocking {
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

    ch.exchangeDeclare("orders.exchange", "direct", true)
    ch.queueDeclare("orders.placed", true, false, false, null)
    ch.queueBind("orders.placed", "orders.exchange", "orders.placed")
    ch.basicQos(1)

    val consumer = new DefaultConsumer(ch) {
      override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties, body: Array[Byte]): Unit = {
        val deliveryTag = envelope.getDeliveryTag
        val task = for {
          order <- IO.fromEither(_root_.io.circe.parser.parse(new String(body, StandardCharsets.UTF_8)).flatMap(_.as[OrderPlaced]))
          _ <- Database.upsertSku(xa, order.sku, order.quantity)
          _ <- IO.blocking(ch.basicAck(deliveryTag, false))
        } yield ()

        task.handleErrorWith { err =>
          logger.error(err)("Failed processing message") *> IO.blocking(ch.basicNack(deliveryTag, false, false))
        }.unsafeRunAndForget()
      }
    }

    ch.basicConsume("orders.placed", false, consumer)
    ()
  }
}
