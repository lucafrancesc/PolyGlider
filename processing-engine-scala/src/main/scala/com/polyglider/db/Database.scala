package com.polyglider.db

import cats.effect.*
import doobie.*
import doobie.hikari.HikariTransactor
import org.flywaydb.core.Flyway

object Database {
  import com.typesafe.config.ConfigFactory

  private val conf = ConfigFactory.load().getConfig("app.db")

  private def jdbcUrl: String = {
    val base    = conf.getString("url")
    val sslMode = conf.getString("ssl-mode")
    if (sslMode == "disable") base
    else s"$base?sslmode=$sslMode"
  }

  def transactorResource: Resource[IO, HikariTransactor[IO]] = for {
    ec <- ExecutionContexts.fixedThreadPool[IO](conf.getInt("pool-size"))
    xa <- HikariTransactor.newHikariTransactor[IO](
      "org.postgresql.Driver",
      jdbcUrl,
      conf.getString("user"),
      conf.getString("password"),
      ec
    )
  } yield xa

  def runMigrations(): IO[Unit] = IO.blocking {
    val flyway = Flyway.configure().dataSource(jdbcUrl, conf.getString("user"), conf.getString("password")).load()
    flyway.migrate()
    ()
  }

}
