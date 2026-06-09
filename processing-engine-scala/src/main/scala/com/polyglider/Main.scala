package com.polyglider

import cats.effect.{IO, IOApp}

object Main extends IOApp.Simple {
  val run: IO[Unit] = OrderProcessor.run
}
