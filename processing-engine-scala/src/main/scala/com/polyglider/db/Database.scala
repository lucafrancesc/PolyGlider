package com.polyglider.db

import cats.effect.*
import doobie.*
import doobie.hikari.HikariTransactor
import doobie.implicits.*
import org.flywaydb.core.Flyway

object Database {
  def transactorResource: Resource[IO, HikariTransactor[IO]] = for {
    ec <- ExecutionContexts.fixedThreadPool[IO](32)
    xa <- HikariTransactor.newHikariTransactor[IO](
      "org.postgresql.Driver",
      sys.env.getOrElse("PG_URL", "jdbc:postgresql://localhost:5432/polyglider"),
      sys.env.getOrElse("PG_USER", "postgres"),
      sys.env.getOrElse("PG_PASSWORD", "postgres"),
      ec
    )
  } yield xa

  def runMigrations(): IO[Unit] = IO.blocking {
    val url = sys.env.getOrElse("PG_URL", "jdbc:postgresql://localhost:5432/polyglider")
    val user = sys.env.getOrElse("PG_USER", "postgres")
    val password = sys.env.getOrElse("PG_PASSWORD", "postgres")
    val flyway = Flyway.configure().dataSource(url, user, password).load()
    flyway.migrate()
    ()
  }

  def upsertSku(xa: Transactor[IO], sku: String, delta: Int): IO[Unit] =
    sql"""
      INSERT INTO ledger (sku, qty) VALUES ($sku, $delta)
      ON CONFLICT (sku) DO UPDATE SET qty = ledger.qty + EXCLUDED.qty
    """.update.run.transact(xa).void
}
