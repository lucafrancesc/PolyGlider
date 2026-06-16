-- Deduplication table: one row per event_id already applied to the ledger.
-- Inserting a duplicate event_id violates the primary key and aborts the
-- transaction, so the ledger upsert in the same transaction is also rolled back.
CREATE TABLE IF NOT EXISTS processed_events (
  event_id TEXT PRIMARY KEY,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
