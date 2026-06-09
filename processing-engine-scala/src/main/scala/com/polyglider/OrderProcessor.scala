package com.polyglider

import cats.effect.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.polyglider.db.Database
import com.polyglider.consumer.RabbitConsumer

object OrderProcessor extends IOApp.Simple {
  given Logger[IO] = Slf4jLogger.getLoggerFromName[IO]("OrderProcessor")

  def run: IO[Unit] =
    Database.transactorResource.use { xa =>
      for {
        _ <- Database.runMigrations()
        _ <- Logger[IO].info("Flyway migrations executed")
        _ <- RabbitConsumer.start(xa, Logger[IO])
        // keep running; RabbitConsumer.start returns after registering consumer
        _ <- IO.never
      } yield ()
    }
}
