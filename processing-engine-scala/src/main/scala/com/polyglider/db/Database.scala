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
    xa <- HikariTransactor.initial[IO](ec)
    _  <- Resource.eval(xa.configure { ds =>
      IO.delay {
        ds.setDriverClassName("org.postgresql.Driver")
        ds.setJdbcUrl(jdbcUrl)
        ds.setUsername(conf.getString("user"))
        ds.setPassword(conf.getString("password"))
        ds.setMaximumPoolSize(conf.getInt("pool-size"))
        ds.setConnectionTimeout(conf.getLong("connection-timeout-ms"))
      }
    })
  } yield xa

  def runMigrations(): IO[Unit] = IO.blocking {
    val flyway = Flyway.configure().dataSource(jdbcUrl, conf.getString("user"), conf.getString("password")).load()
    flyway.migrate()
    ()
  }

}
