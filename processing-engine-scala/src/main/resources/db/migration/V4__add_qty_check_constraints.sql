ALTER TABLE ledger ADD CONSTRAINT ledger_qty_non_negative CHECK (qty >= 0);
ALTER TABLE ledger ADD CONSTRAINT ledger_order_count_non_negative CHECK (order_count >= 0);
