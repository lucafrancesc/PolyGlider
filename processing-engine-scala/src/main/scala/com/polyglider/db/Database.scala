package com.polyglider.db

import cats.effect.*
import doobie.*
import doobie.hikari.HikariTransactor
import org.flywaydb.core.Flyway

object Database {
  import com.typesafe.config.ConfigFactory

  private val conf = ConfigFactory.load().getConfig("app.db")

  def transactorResource: Resource[IO, HikariTransactor[IO]] = for {
    ec <- ExecutionContexts.fixedThreadPool[IO](32)
    xa <- HikariTransactor.newHikariTransactor[IO](
      "org.postgresql.Driver",
      conf.getString("url"),
      conf.getString("user"),
      conf.getString("password"),
      ec
    )
  } yield xa

  def runMigrations(): IO[Unit] = IO.blocking {
    val url = conf.getString("url")
    val user = conf.getString("user")
    val password = conf.getString("password")
    val flyway = Flyway.configure().dataSource(url, user, password).load()
    flyway.migrate()
    ()
  }

}
