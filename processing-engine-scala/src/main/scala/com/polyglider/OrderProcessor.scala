package com.polyglider

import cats.effect.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.polyglider.db.Database
import com.polyglider.consumer.RabbitConsumer
import io.circe.parser
import io.circe.generic.auto._
import com.polyglider.model.OrderPlaced
import scala.concurrent.duration.*
import cats.syntax.all.*

object OrderProcessor extends IOApp.Simple {
  given Logger[IO] = Slf4jLogger.getLoggerFromName[IO]("OrderProcessor")

  def run: IO[Unit] =
    // Build a combined Resource: transactor -> run migrations -> consumer resource
    val res: Resource[IO, Unit] = for {
      xa <- Database.transactorResource
      _  <- Resource.eval(Database.runMigrations())
      _  <- Resource.eval(Logger[IO].info("Flyway migrations executed"))
      _  <- RabbitConsumer.start(xa, Logger[IO])
    } yield ()

    // Use the resource and keep the app running
    res.use(_ => IO.never)

  // Helper used by tests
  def parsePayload(bytes: Array[Byte]): Either[io.circe.Error, OrderPlaced] =
    parser.parse(new String(bytes, java.nio.charset.StandardCharsets.UTF_8)).flatMap(_.as[OrderPlaced])

  def withRetries[A](ioa: IO[A], retries: Int, delay: scala.concurrent.duration.FiniteDuration): IO[A] = {
    ioa.handleErrorWith { err =>
      if (retries <= 0) IO.raiseError(err)
      else IO.sleep(delay) *> withRetries(ioa, retries - 1, delay)
    }
  }
}
