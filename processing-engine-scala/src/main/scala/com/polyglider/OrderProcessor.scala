package com.polyglider

import cats.effect.{IO, IOApp, Resource}
import cats.effect.std.Console
import cats.syntax.all._
import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
import dev.profunktor.fs2rabbit.interpreter.Fs2Rabbit
import dev.profunktor.fs2rabbit.model.*
import fs2.Stream
import fs2.concurrent.SignallingRef
import io.circe._, io.circe.parser._, io.circe.generic.auto._
import org.typelevel.log4cats.slf4j.Slf4jLogger
import cats.effect.Ref

case class OrderPlaced(eventId: String, sku: String, quantity: Int, customerId: String, timestamp: String)

object OrderProcessor extends IOApp.Simple {

  given logger = Slf4jLogger.getLogger[IO]

  val config = Fs2RabbitConfig.default

  def processEvent(order: OrderPlaced, ledger: Ref[IO, Map[String, Int]]): IO[Unit] =
    for {
      _ <- logger.info(s"Processing order ${order.eventId} sku=${order.sku} qty=${order.quantity}")
      _ <- ledger.update(map => map.updatedWith(order.sku)(
        case None => Some(order.quantity)
        case Some(prev) => Some(prev + order.quantity)
      ))
      _ <- logger.info(s"Ledger updated for sku=${order.sku}")
    } yield ()

  def parsePayload(body: Array[Byte]): Either[Error, OrderPlaced] =
    parse(new String(body, "UTF-8")).flatMap(_.as[OrderPlaced])

  def run: IO[Unit] =
    for {
      ledger <- Ref.of[IO, Map[String, Int]](Map.empty)
      rabbit <- Fs2Rabbit[IO](config)
      connection <- rabbit.createConnectionChannel.use { implicit ch =>
        for {
          _ <- rabbit.declareQueue(QueueConfig(name = "orders.placed", durable = true))
          consumerTag <- rabbit.createAutoAckConsumer(QueueName("orders.placed"))
            .flatMap { case (stream, cancel) =>
              stream.evalMap { amqpMsg =>
                IO.fromEither(parsePayload(amqpMsg.payload))
                  .flatMap(order => processEvent(order, ledger))
                  .handleErrorWith(err => logger.error(err)("Failed to process message"))
              }.compile.drain
            }
        } yield ()
      }
    } yield ()
}
