package com.polyglider

import cats.effect.{IO, IOApp, Resource}
import cats.effect.std.Console
import cats.syntax.all._
import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
import dev.profunktor.fs2rabbit.interpreter.Fs2Rabbit
import dev.profunktor.fs2rabbit.model.*
import fs2.Stream
import fs2.concurrent.SignallingRef
import io.circe._, io.circe.parser._, io.circe.generic.auto._, io.circe.syntax._
import org.typelevel.log4cats.slf4j.Slf4jLogger
import cats.effect.Ref
import scala.concurrent.duration._
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.nio.charset.StandardCharsets

case class OrderPlaced(eventId: String, sku: String, quantity: Int, customerId: String, timestamp: String)

object OrderProcessor extends IOApp.Simple {

  given logger = Slf4jLogger.getLogger[IO]

  val config = Fs2RabbitConfig.default

  val ledgerFile = Paths.get("data/ledger.json")

  def saveLedger(map: Map[String, Int]): IO[Unit] = IO {
    val json = map.asJson.spaces2
    val dir = ledgerFile.getParent
    if (dir != null && !Files.exists(dir)) Files.createDirectories(dir)
    Files.write(ledgerFile, json.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
  }.handleErrorWith(e => logger.error(e)("Failed to persist ledger"))

  def loadLedger: IO[Map[String, Int]] = IO {
    if (!Files.exists(ledgerFile)) Map.empty[String, Int]
    else {
      val bytes = Files.readAllBytes(ledgerFile)
      val json = new String(bytes, StandardCharsets.UTF_8)
      parser.parse(json).flatMap(_.as[Map[String, Int]]) match {
        case Right(m) => m
        case Left(_)  => Map.empty[String, Int]
      }
    }
  }.handleError(_ => Map.empty[String, Int])

  def processEvent(order: OrderPlaced, ledger: Ref[IO, Map[String, Int]]): IO[Unit] =
    for {
      _ <- logger.info(s"Processing order ${order.eventId} sku=${order.sku} qty=${order.quantity}")
      _ <- ledger.update(map => map.updatedWith(order.sku)(
        case None => Some(order.quantity)
        case Some(prev) => Some(prev + order.quantity)
      ))
      snapshot <- ledger.get
      _ <- saveLedger(snapshot)
      _ <- logger.info(s"Ledger updated and persisted for sku=${order.sku}")
    } yield ()

  // Simple retry helper: attempt up to `retries` times with delay
  def withRetries[A](ioa: IO[A], retries: Int = 3, delay: FiniteDuration = 1.second): IO[A] =
    ioa.handleErrorWith { err =>
      if (retries > 0) logger.warn(s"Operation failed, retrying... (${retries} left)") *> IO.sleep(delay) *> withRetries(ioa, retries - 1, delay * 2)
      else IO.raiseError(err)
    }

  def parsePayload(body: Array[Byte]): Either[Error, OrderPlaced] =
    parse(new String(body, "UTF-8")).flatMap(_.as[OrderPlaced])

  def run: IO[Unit] =
    for {
      ledger <- Ref.of[IO, Map[String, Int]](Map.empty)
      rabbit <- Fs2Rabbit[IO](config)
      _ <- logger.info("Starting OrderProcessor")
      // load persisted ledger into Ref
      initial <- loadLedger
      _ <- ledger.set(initial)
      _ <- logger.info(s"Loaded ledger with ${initial.size} entries")
      _ <- rabbit.createConnectionChannel.use { implicit ch =>
        for {
          _ <- rabbit.declareQueue(QueueConfig(name = "orders.placed", durable = true))
          consumer <- rabbit.createManualAckConsumer(QueueName("orders.placed"))
          (stream, cancel) = consumer
          _ <- stream.evalMap { amqpMsg =>
            val action = for {
              order <- IO.fromEither(parsePayload(amqpMsg.payload))
              _ <- processEvent(order, ledger)
              _ <- rabbit.ack(amqpMsg.deliveryTag)
            } yield ()

            withRetries(action).handleErrorWith { err =>
              logger.error(err)("Failed to process message after retries") *> rabbit.nack(amqpMsg.deliveryTag, requeue = false)
            }
          }.compile.drain
        } yield ()
      }
    } yield ()
}
