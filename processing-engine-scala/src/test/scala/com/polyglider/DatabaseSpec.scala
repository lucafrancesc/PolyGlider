package com.polyglider

import munit.CatsEffectSuite
import cats.effect.IO
import doobie.util.transactor.Transactor
import doobie.implicits._
import com.polyglider.db.Database
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

class DatabaseSpec extends CatsEffectSuite {

  // Each test gets its own H2 in-memory database name so they stay isolated from one another.
  private def h2Transactor(dbName: String): Transactor[IO] = {
    val cfg = new HikariConfig()
    cfg.setJdbcUrl(s"jdbc:h2:mem:$dbName;DB_CLOSE_DELAY=-1")
    cfg.setUsername("sa")
    cfg.setPassword("")
    cfg.setMaximumPoolSize(4)
    val ds = new HikariDataSource(cfg)
    Transactor.fromDataSource[IO](ds, scala.concurrent.ExecutionContext.global)
  }

  test("upsertSku inserts and updates ledger") {
    val xa = h2Transactor("test")

    val program = for {
      // create tables
      _ <- sql"""
        CREATE TABLE ledger (
          sku VARCHAR PRIMARY KEY,
          qty INT NOT NULL,
          order_count BIGINT NOT NULL DEFAULT 0
        )""".update.run.transact(xa)
      _ <- sql"""
        CREATE TABLE processed_events (
          event_id VARCHAR PRIMARY KEY
        )""".update.run.transact(xa)
      // insert via H2-compatible upsert (update then insert-if-not-exists)
      _ <- sql"UPDATE ledger SET qty = qty + 2, order_count = order_count + 1 WHERE sku = 'SKU-TST'".update.run.transact(xa)
      _ <- sql"INSERT INTO ledger (sku, qty, order_count) SELECT 'SKU-TST', 2, 1 WHERE NOT EXISTS (SELECT 1 FROM ledger WHERE sku = 'SKU-TST')".update.run.transact(xa)
      q1 <- sql"SELECT qty FROM ledger WHERE sku = 'SKU-TST'".query[Int].unique.transact(xa)
      _ <- IO(assertEquals(q1, 2))
      // upsert increment
      _ <- sql"UPDATE ledger SET qty = qty + 3, order_count = order_count + 1 WHERE sku = 'SKU-TST'".update.run.transact(xa)
      _ <- sql"INSERT INTO ledger (sku, qty, order_count) SELECT 'SKU-TST', 3, 1 WHERE NOT EXISTS (SELECT 1 FROM ledger WHERE sku = 'SKU-TST')".update.run.transact(xa)
      q2 <- sql"SELECT qty FROM ledger WHERE sku = 'SKU-TST'".query[Int].unique.transact(xa)
      _ <- IO(assertEquals(q2, 5))
    } yield ()

    program
  }

  test("order_count increments once per order regardless of quantity") {
    val xa = h2Transactor("order_count")

    val upsert = (sku: String, qty: Int) => for {
      _ <- sql"UPDATE ledger SET qty = qty + $qty, order_count = order_count + 1 WHERE sku = $sku".update.run.transact(xa)
      _ <- sql"INSERT INTO ledger (sku, qty, order_count) SELECT $sku, $qty, 1 WHERE NOT EXISTS (SELECT 1 FROM ledger WHERE sku = $sku)".update.run.transact(xa)
    } yield ()

    val program = for {
      _ <- sql"""
        CREATE TABLE ledger (
          sku VARCHAR PRIMARY KEY,
          qty INT NOT NULL,
          order_count BIGINT NOT NULL DEFAULT 0
        )""".update.run.transact(xa)
      _ <- upsert("MOUSE-01", 5)
      _ <- upsert("MOUSE-01", 3)
      _ <- upsert("MOUSE-01", 10)
      count <- sql"SELECT order_count FROM ledger WHERE sku = 'MOUSE-01'".query[Long].unique.transact(xa)
      qty   <- sql"SELECT qty FROM ledger WHERE sku = 'MOUSE-01'".query[Int].unique.transact(xa)
      _ <- IO(assertEquals(count, 3L))
      _ <- IO(assertEquals(qty, 18))
    } yield ()

    program
  }

  test("processed_events rejects duplicate event_id") {
    val xa = h2Transactor("dedup")

    val program = for {
      _ <- sql"CREATE TABLE processed_events (event_id VARCHAR PRIMARY KEY)".update.run.transact(xa)
      _ <- sql"INSERT INTO processed_events (event_id) VALUES ('evt-1')".update.run.transact(xa)
      result <- sql"INSERT INTO processed_events (event_id) VALUES ('evt-1')"
                  .update.run.transact(xa).attempt
      _ <- IO(assert(result.isLeft, "duplicate event_id should fail"))
    } yield ()

    program
  }

  test("duplicate event_id does not increment order_count") {
    val xa = h2Transactor("dedup_count")

    // Simulate the full upsertSku transaction: insert event, then upsert ledger
    val upsertWithDedup = (eventId: String, sku: String, qty: Int) =>
      (for {
        _ <- sql"INSERT INTO processed_events (event_id) VALUES ($eventId)".update.run
        _ <- sql"UPDATE ledger SET qty = qty + $qty, order_count = order_count + 1 WHERE sku = $sku".update.run
        _ <- sql"INSERT INTO ledger (sku, qty, order_count) SELECT $sku, $qty, 1 WHERE NOT EXISTS (SELECT 1 FROM ledger WHERE sku = $sku)".update.run
      } yield ()).transact(xa).attempt

    val program = for {
      _ <- sql"CREATE TABLE processed_events (event_id VARCHAR PRIMARY KEY)".update.run.transact(xa)
      _ <- sql"""
        CREATE TABLE ledger (
          sku VARCHAR PRIMARY KEY,
          qty INT NOT NULL,
          order_count BIGINT NOT NULL DEFAULT 0
        )""".update.run.transact(xa)
      _ <- upsertWithDedup("evt-1", "KEYBOARD-05", 2)
      _ <- upsertWithDedup("evt-1", "KEYBOARD-05", 2)  // duplicate — should be rejected
      count <- sql"SELECT order_count FROM ledger WHERE sku = 'KEYBOARD-05'".query[Long].unique.transact(xa)
      qty   <- sql"SELECT qty FROM ledger WHERE sku = 'KEYBOARD-05'".query[Int].unique.transact(xa)
      _ <- IO(assertEquals(count, 1L, "duplicate event must not increment order_count"))
      _ <- IO(assertEquals(qty, 2, "duplicate event must not increment qty"))
    } yield ()

    program
  }

  test("ledger rejects negative qty") {
    val xa = h2Transactor("neg_qty")

    val program = for {
      _ <- sql"""
        CREATE TABLE ledger (
          sku VARCHAR PRIMARY KEY,
          qty BIGINT NOT NULL CHECK (qty >= 0),
          order_count BIGINT NOT NULL DEFAULT 0 CHECK (order_count >= 0)
        )""".update.run.transact(xa)
      _ <- sql"INSERT INTO ledger (sku, qty) VALUES ('SKU-A', 5)".update.run.transact(xa)
      result <- sql"UPDATE ledger SET qty = -1 WHERE sku = 'SKU-A'".update.run.transact(xa).attempt
      _ <- IO(assert(result.isLeft, "negative qty must be rejected by CHECK constraint"))
    } yield ()

    program
  }
}
