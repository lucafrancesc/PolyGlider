# Runbook: `processed_events` archival

## When to run this

When `pg_total_relation_size('processed_events')` approaches or exceeds `shared_buffers` (default 128 MB), index pages stop fitting in the buffer cache and write throughput degrades. Check current size first:

```sql
SELECT pg_size_pretty(pg_total_relation_size('processed_events'));
```

If the result is well below `shared_buffers`, no action is needed. See ADR-002's retention section for the growth-rate estimates at different order volumes.

## Safety window

A row in `processed_events` is safe to delete once no in-flight redelivery of that `event_id` can still arrive. The worst-case window is the full main-consumer retry budget (~121 s) plus the DLQ reprocessor's full retry budget (~70 s) plus a 60 s safety margin — approximately 4 minutes in total. Rows older than **10 minutes** are safe with a comfortable margin.

## Step 1: dry run (SELECT before DELETE)

Always verify what will be deleted before running the actual DELETE:

```sql
SELECT COUNT(*), min(processed_at), max(processed_at)
FROM processed_events
WHERE processed_at < now() - interval '10 minutes';
```

Confirm the count looks plausible (roughly: orders/day × days old) and that `max(processed_at)` is at least 10 minutes in the past.

## Step 2: delete in batches

Delete in bounded batches to avoid a single long-lived transaction that holds a table lock and blocks concurrent inserts from the consumer:

```sql
-- Repeat until 0 rows deleted
DELETE FROM processed_events
WHERE event_id IN (
  SELECT event_id FROM processed_events
  WHERE processed_at < now() - interval '10 minutes'
  LIMIT 10000
);
```

Run this in a loop until `DELETE 0` is reported. Each batch holds a row-level lock only on the 10,000 rows being deleted — the consumer's `INSERT` into `processed_events` on concurrent deliveries is not blocked.

## Step 3: reclaim space (optional)

After a large deletion, Postgres marks pages as reusable but doesn't immediately return space to the OS. If disk is the constraint (not just cache efficiency), run:

```sql
VACUUM (VERBOSE, ANALYZE) processed_events;
```

This is safe to run while the consumer is running. `VACUUM FULL` would reclaim more space but holds an exclusive lock for its duration — avoid it on a live consumer.

## Verification

```sql
SELECT pg_size_pretty(pg_total_relation_size('processed_events'));
```

Confirm the size has decreased to the expected post-archival level.

Also verify the consumer is still processing normally: `polyglider_messages_processed_total` rate should be unchanged, and `polyglider_permanent_failures_total` should not have spiked (a spike would indicate the archival window was set too short and deleted rows for events that were redelivered after archival — if this happens, restore from backup or re-process the affected messages from `needs-attention.orders.placed`).
