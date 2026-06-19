# Postmortem: malformed payload and duplicate-message handling

**Date:** 2026-06-18
**Scenarios:** `tools/chaos/chaos.sh malformed-payload`, `tools/chaos/chaos.sh duplicate-message`
**Severity:** Low — both paths behaved exactly as designed. Recorded as a postmortem anyway because the timing revealed something worth knowing about the DLQ reprocessor.

**Update (since superseded):** the duplicate-message behavior described below (PK violation → permanent failure → DLX) was intentionally changed by #78, *after* this postmortem was written: the dedup insert is now `ON CONFLICT (event_id) DO NOTHING`, so a duplicate is an idempotent no-op (ledger upsert skipped, message still acks normally) rather than a thrown failure. This doc is left as-is as a historical record of the behavior *at the time*; see #78's description for why it changed, and re-verify against current code/logs before relying on the specifics below.

## What we did

`tools/chaos/publish_chaos_message.py` publishes directly to `orders.exchange`, bypassing the gateway's input validation (the gateway would reject genuinely malformed input with a `400` before it ever reached the broker, so this is the only way to exercise the Scala consumer's own failure-classification path):

- **malformed**: published `{"eventId": "not-json-from-here-on...` (truncated, unparseable JSON)
- **duplicate**: published the same `eventId` three times

## What happened

**Malformed payload:** `ProcessingFailure.classify` correctly identified the `circe.ParsingFailure` as permanent. The message was nacked straight to `dlx.orders.placed`, picked up by `DlqReprocessor`, classified as permanent there too, and escalated immediately to `needs-attention.orders.placed` — all within milliseconds (09:14:54.559 → 09:14:54.567 in the consumer log). `polyglider_permanent_failures_total` incremented by 1.

**Duplicate message:** the first copy stored normally (`Stored to ledger: eventId=...`). The second and third copies hit the `processed_events` primary-key constraint (`Key (event_id)=(...) already exists`), were classified permanent, routed to `dlx.orders.placed`, and escalated to `needs-attention.orders.placed` just as fast. `polyglider_permanent_failures_total` incremented by 2. The ledger was not double-counted — confirmed via `polyglider_messages_processed_total` (incremented by exactly 1, not 3) and a direct check that `qty`/`order_count` only reflected the first copy.

## What recovered automatically

Both. No retries were attempted for either scenario (correctly — retrying a malformed payload or a duplicate `eventId` would just reproduce the same outcome), and no ledger corruption occurred.

## What needed manual intervention

Both ended up in `needs-attention.orders.placed` (verified via the RabbitMQ management API: depth 3 — 1 malformed + 2 duplicates — after these two tests), which is the intended manual-triage point. Nobody had to do anything during the test itself, but a human would eventually need to look at that queue and decide: discard the malformed message, and confirm the duplicate copies are safe to discard (they are, since the dedup already protected the ledger).

## Gaps found / what we'd change

- **The DLQ reprocessor's retry-then-escalate path never actually gets exercised by either of these scenarios**, because both failures are classified `Permanent` and skip straight to `needs-attention.orders.placed` (see `DlqReprocessor.scala`'s `escalate` call inside the `PermanentFailure` branch). The reprocessor's bounded-retry budget only matters for failures that are transient *at the DLQ layer* — e.g. if Postgres happened to be down at the exact moment a message was being reprocessed. We didn't manage to trigger that combination in this round of testing; it's worth a follow-up chaos run that overlaps a `postgres-kill` window with messages already sitting in `dlx.orders.placed`.
- **`needs-attention.orders.placed` has no consumer or alerting** (tracked separately as issue #18). After this test it sat at depth 3 with nothing watching it — the only way we noticed was by querying the RabbitMQ management API by hand. The `DlqDepthHigh` alert in `observability/prometheus/alerts.yml` only watches `dlx.orders.placed`, not the escalation queue; worth adding a parallel alert once #18 is scoped.
