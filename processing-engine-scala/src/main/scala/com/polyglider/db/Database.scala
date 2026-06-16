package com.polyglider.db

import cats.effect.*
import cats.syntax.all.*
import doobie.*
import doobie.hikari.HikariTransactor
import doobie.implicits.*
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

  def upsertSku(xa: Transactor[IO], eventId: String, sku: String, delta: Int): IO[Unit] = {
    val insertEvent = sql"INSERT INTO processed_events (event_id) VALUES ($eventId)".update.run
    val upsertLedger = sql"""
      INSERT INTO ledger (sku, qty) VALUES ($sku, $delta)
      ON CONFLICT (sku) DO UPDATE SET qty = ledger.qty + EXCLUDED.qty
    """.update.run
    // Both run in one transaction; duplicate event_id causes a PK violation
    // which rolls back the ledger upsert, giving exactly-once ledger semantics.
    (insertEvent *> upsertLedger).transact(xa).void
  }
}
