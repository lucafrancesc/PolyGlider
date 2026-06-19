# Processing Engine — Scala 3

The core consumer and ledger. It reads `OrderPlaced` events from RabbitMQ, upserts SKU quantities into a Postgres ledger, and routes failures to a Dead Letter Exchange for manual triage.

**How it works:**
- Declares `orders.placed` queue bound to `orders.exchange` with an `x-dead-letter-exchange` argument pointing to `dlx.orders.exchange`.
- Runs 4 parallel worker fibers (Cats Effect) each pulling from a bounded in-memory queue (capacity 1,000).
- Each message: parse JSON → record `event_id` in `processed_events` + upsert ledger qty (and increment `order_count`) in a single transaction → ack. A duplicate `event_id` causes a PK violation that rolls back the ledger write (exactly-once semantics). The whole step runs inside an OpenTelemetry span parented onto the `traceparent` the gateway injected into the RabbitMQ message headers — see the repo root README's [Observability](../README.md#observability) section.
- On failure, `ProcessingFailure.classify` distinguishes malformed payloads / constraint violations (**permanent**, never going to succeed on retry) from DB/network blips (**transient**, worth retrying):
  - Permanent failures are nacked without requeue, routing straight to the DLX.
  - Transient failures are republished to a per-tier retry queue (`retry.orders.placed.<tier>`) with exponential backoff + jitter (`RetryPolicy`, default: 1s base delay, ×3 multiplier, up to 5 retries, ≤250ms jitter). Each retry queue has a fixed `x-message-ttl` and dead-letters back into `orders.exchange` once the delay elapses — RabbitMQ handles the timing, no in-process scheduler needed. Once `max-retries` is exceeded, the message is routed to the DLX instead of retrying forever.
- A separate `DlqReprocessor` worker polls `dlx.orders.placed` so the DLX is no longer a dead end requiring manual triage. It applies its own bounded `RetryPolicy` (default: 10s base delay, ×2 multiplier, up to 3 retries, ≤1s jitter, queue prefix `dlq-reprocess.orders.placed`) — separate from the main consumer's budget — and reclassifies each failure the same way: permanent failures escalate immediately, transient failures get bounded retries. Once the reprocessor's retry budget is exhausted (or on a permanent failure), the message is routed to `needs-attention.orders.placed` for genuinely manual triage, with the original failure reason and attempt count attached as headers and logged.
- The Postgres write path (`upsertSku`) is wrapped in a `CircuitBreaker` (`app.circuit-breaker`, default: trip open after 5 consecutive failures, 30s cooldown). When Postgres is down, this stops every in-flight message from individually hitting (and failing against) the database — the breaker fails fast with `CircuitBreakerOpenException`, classified as transient so the message still gets a backoff retry rather than going straight to the DLX. After the cooldown, a single half-open trial call decides whether to close the breaker again or reopen it.
- Flyway runs all four migrations at startup when `app.db.runMigrations = true`: `V1__create_ledger.sql` (creates `ledger`), `V2__add_processed_events.sql` (dedup table), `V3__add_order_count.sql` (adds `order_count BIGINT NOT NULL DEFAULT 0` to `ledger`), `V4__add_qty_check_constraints.sql` (adds `CHECK (qty >= 0)` and `CHECK (order_count >= 0)`).
- Exposes Prometheus metrics at `:9100/metrics` (`app.metrics.port` / `METRICS_PORT`) and exports OpenTelemetry traces via OTLP — see the repo root README's [Observability](../README.md#observability) section for both.

---

## Run

```bash
# Start RabbitMQ and Postgres first
docker compose up -d

cd processing-engine-scala
sbt run
```

### Configuration

**`src/main/resources/application.conf`** — DB settings (already aligned with docker-compose defaults):

| Key | Default |
|-----|---------|
| `app.db.url` | `jdbc:postgresql://localhost:5432/polyglider_inventory` |
| `app.db.user` | `postgres` |
| `app.db.password` | `postgres` |
| `app.db.runMigrations` | `true` |

**RabbitMQ** — read from environment variables (not `application.conf`):

| Variable | Default |
|----------|---------|
| `RABBIT_HOST` | `127.0.0.1` |
| `RABBIT_PORT` | `5672` |
| `RABBIT_USER` | `guest` |
| `RABBIT_PASS` | `guest` |
| `METRICS_PORT` | `9100` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` (Jaeger OTLP) |
| `OTEL_SERVICE_NAME` | `processing-engine-scala` |

---

## Test

```bash
cd processing-engine-scala
sbt test
```

Unit tests run against H2 in-memory — no Docker required. They cover JSON parsing, retry logic, UUID validation, ledger upserts, and duplicate event rejection.

`sbt test` also runs the **Pact consumer contract test** (`OrderPlacedEventContractSpec`), which defines the expected `OrderPlaced` message shape and writes the pact file to `contracts/pacts/scala-engine-cs-gateway.json`. The C# gateway's provider tests read this file — run `sbt test` first whenever the message schema changes.
