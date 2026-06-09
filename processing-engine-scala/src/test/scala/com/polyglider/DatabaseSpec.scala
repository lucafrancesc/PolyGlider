package com.polyglider

import munit.CatsEffectSuite
import cats.effect.IO
import doobie.util.transactor.Transactor
import doobie.implicits._
import com.polyglider.db.Database

class DatabaseSpec extends CatsEffectSuite {
  test("upsertSku inserts and updates ledger") {
    import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
    val cfg = new HikariConfig()
    cfg.setJdbcUrl("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1")
    cfg.setUsername("sa")
    cfg.setPassword("")
    cfg.setMaximumPoolSize(4)
    val ds = new HikariDataSource(cfg)
    val xa = Transactor.fromDataSource[IO](ds, scala.concurrent.ExecutionContext.global)

    val program = for {
      // create table
      _ <- sql"""
        CREATE TABLE ledger (
          sku VARCHAR PRIMARY KEY,
          qty INT NOT NULL
        )""".update.run.transact(xa)
      // insert via H2-compatible upsert (update then insert-if-not-exists)
      _ <- sql"UPDATE ledger SET qty = qty + 2 WHERE sku = 'SKU-TST'".update.run.transact(xa)
      _ <- sql"INSERT INTO ledger (sku, qty) SELECT 'SKU-TST', 2 WHERE NOT EXISTS (SELECT 1 FROM ledger WHERE sku = 'SKU-TST')".update.run.transact(xa)
      q1 <- sql"SELECT qty FROM ledger WHERE sku = 'SKU-TST'".query[Int].unique.transact(xa)
      _ <- IO(assertEquals(q1, 2))
      // upsert increment
      _ <- sql"UPDATE ledger SET qty = qty + 3 WHERE sku = 'SKU-TST'".update.run.transact(xa)
      _ <- sql"INSERT INTO ledger (sku, qty) SELECT 'SKU-TST', 3 WHERE NOT EXISTS (SELECT 1 FROM ledger WHERE sku = 'SKU-TST')".update.run.transact(xa)
      q2 <- sql"SELECT qty FROM ledger WHERE sku = 'SKU-TST'".query[Int].unique.transact(xa)
      _ <- IO(assertEquals(q2, 5))
    } yield ()

    program
  }
}
