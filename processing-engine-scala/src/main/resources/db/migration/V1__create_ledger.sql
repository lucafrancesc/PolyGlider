-- Create ledger table for SKU quantities
CREATE TABLE IF NOT EXISTS ledger (
  sku TEXT PRIMARY KEY,
  qty BIGINT NOT NULL
);
