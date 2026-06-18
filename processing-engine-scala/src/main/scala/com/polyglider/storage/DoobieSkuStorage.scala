package com.polyglider.storage

import cats.effect.IO
import cats.syntax.all.*
import doobie.*
import doobie.implicits.*

class DoobieSkuStorage(xa: Transactor[IO]) extends SkuStorage {
  def upsertSku(eventId: String, sku: String, delta: Int): IO[Unit] = {
    val insertEvent = sql"INSERT INTO processed_events (event_id) VALUES ($eventId) ON CONFLICT (event_id) DO NOTHING".update.run
    val upsertLedger = sql"""
      INSERT INTO ledger (sku, qty, order_count) VALUES ($sku, $delta, 1)
      ON CONFLICT (sku) DO UPDATE SET
        qty = ledger.qty + EXCLUDED.qty,
        order_count = ledger.order_count + 1
    """.update.run
    // insertEvent reports 0 affected rows on a duplicate eventId rather than throwing a PK
    // violation. That matters for redelivery: if a prior attempt's commit actually succeeded
    // but its ack never reached the broker (e.g. the connection dropped right after COMMIT),
    // the ledger update already happened. Throwing on redelivery would roll the whole
    // transaction back into a permanent failure even though there's nothing left to do;
    // skipping the ledger upsert when the event is already recorded makes redelivery an
    // idempotent no-op instead.
    insertEvent.flatMap {
      case 0 => ().pure[ConnectionIO]
      case _ => upsertLedger.void
    }.transact(xa).void
  }

  def snapshot: IO[List[SkuStats]] =
    sql"SELECT sku, qty, order_count FROM ledger ORDER BY sku"
      .query[SkuStats]
      .to[List]
      .transact(xa)
}
