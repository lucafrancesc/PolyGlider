package com.polyglider.storage

import cats.effect.IO
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*

class DoobieSkuStorage(xa: Transactor[IO]) extends SkuStorage {
  def upsertSku(eventId: String, sku: String, delta: Int): IO[Unit] = {
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
