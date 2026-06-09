package com.polyglider

import cats.effect.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.polyglider.db.Database
import com.polyglider.consumer.RabbitConsumer

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
}
