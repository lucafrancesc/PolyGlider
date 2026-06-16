# PolyGlider

A polyglot, event-driven order-processing system built to demonstrate how multiple language runtimes can collaborate over a shared message broker. An HTTP gateway (C#) accepts orders, a processing engine (Scala) maintains a ledger in Postgres, and an analytics worker (Python) streams aggregated metrics — all wired together through RabbitMQ.

```
POST /api/orders
      │
      ▼
 C# Gateway ──► Channel<T> buffer ──► RabbitMqPublisherWorker
                                              │
                                    orders.exchange (topic)
                                         routing key: orders.placed
                                              │
                          ┌───────────────────┴──────────────────────┐
                          ▼                                           ▼
               queue: orders.placed                  queue: analytics.orders.placed
                          │                                           │
                    Scala Engine                             Python analytics worker
                  (ledger → Postgres)                        (in-memory aggregation)
                          │
                   nack → DLX
             (dlx.orders.placed)
```

---

## Prerequisites

| Tool | Version | Used by |
|------|---------|---------|
| Docker & Docker Compose | any recent | infrastructure |
| .NET SDK | 9 | C# gateway |
| Java + sbt | 17+ / 1.9+ | Scala engine |
| Python | 3.11+ | analytics worker, load tester |

---

## Run

```bash
# Start RabbitMQ and Postgres
docker compose up -d

# Start all services with colour-coded output
./run-all.sh

# Also start the Python analytics worker
./run-all.sh --analytics
```

Press `Ctrl+C` to stop everything. The script waits for RabbitMQ and Postgres to be healthy before starting the services.

**Or start services individually** — see each component's README.

---

## Test

```bash
# All suites (requires Docker for the Testcontainers integration test)
./test-all.sh

# Unit tests only — no Docker needed
./test-all.sh --no-integration
```

---

## Send an order

```bash
curl -X POST http://localhost:5187/api/orders \
  -H 'Content-Type: application/json' \
  -d '{"sku":"ABC123","quantity":2,"customerId":"22222222-2222-4222-8222-222222222222"}'
# → HTTP 202 Accepted  {"message":"Order queued successfully","eventId":"..."}
```

Or fire a batch and get a pass/fail summary:

```bash
./tools/smoke-test.sh        # 20 orders
./tools/smoke-test.sh 50     # custom count
```

---

## Services

| Directory | Language | What it does |
|-----------|----------|--------------|
| [`gateway-api-cs/`](gateway-api-cs/README.md) | C# .NET 9 | Accepts `POST /api/orders`, buffers events, publishes to RabbitMQ |
| [`processing-engine-scala/`](processing-engine-scala/README.md) | Scala 3 | Consumes events, upserts SKU quantities in Postgres, routes failures to DLX |
| [`analytics-worker-python/`](analytics-worker-python/README.md) | Python 3.11 | Aggregates order counts and units per SKU, logs periodic snapshots |
| [`tools/load-tester/`](tools/load-tester/README.md) | Python / Locust | Simulates concurrent users placing orders |

---

## Event schema

The gateway generates `eventId` and `timestamp`; clients only send the three fields below.

**HTTP request body:**
```json
{ "sku": "string", "quantity": 1, "customerId": "uuid" }
```

**Message published to RabbitMQ** (`orders.exchange`, routing key `orders.placed`):
```json
{
  "eventId": "uuid",
  "sku": "string",
  "quantity": 1,
  "customerId": "uuid",
  "timestamp": "2026-01-01T00:00:00Z"
}
```

---

## Accessing infrastructure

**RabbitMQ management UI** — http://localhost:15672 (credentials: `guest` / `guest`)

**Postgres**
```bash
psql -h localhost -p 5432 -U postgres -d polyglider_inventory
# password: postgres
```

---

## Resiliency

| Risk | Mitigation |
|------|-----------|
| Broker unavailable | Gateway buffers up to 10,000 events in `Channel<T>`; `RabbitMqPublisherWorker` reconnects automatically |
| Consumer failure | Scala engine uses manual ack/nack — message stays on the queue until the DB write succeeds |
| Poison-pill messages | Scala consumer nacks without requeue → message routed to `dlx.orders.placed` for manual triage |
| Duplicate delivery | `processed_events` table guards the ledger; duplicate `eventId` rolls back the transaction |
