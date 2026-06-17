package com.polyglider.storage

import cats.effect.IO
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*

class DoobieSkuStorage(xa: Transactor[IO]) extends SkuStorage {
  def upsertSku(eventId: String, sku: String, delta: Int): IO[Unit] = {
    val insertEvent = sql"INSERT INTO processed_events (event_id) VALUES ($eventId)".update.run
    val upsertLedger = sql"""
      INSERT INTO ledger (sku, qty, order_count) VALUES ($sku, $delta, 1)
      ON CONFLICT (sku) DO UPDATE SET
        qty = ledger.qty + EXCLUDED.qty,
        order_count = ledger.order_count + 1
    """.update.run
    // Both run in one transaction; duplicate event_id causes a PK violation
    // which rolls back the ledger upsert, giving exactly-once ledger semantics.
    (insertEvent *> upsertLedger).transact(xa).void
  }

  def snapshot: IO[List[SkuStats]] =
    sql"SELECT sku, qty, order_count FROM ledger ORDER BY sku"
      .query[SkuStats]
      .to[List]
      .transact(xa)
}
