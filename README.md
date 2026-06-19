# PolyGlider

A polyglot, event-driven order-processing system built to demonstrate how multiple language runtimes can collaborate over a shared message broker. An HTTP gateway (C#) accepts orders and a processing engine (Scala) maintains a ledger in Postgres — all wired together through RabbitMQ.

```
HTTP client
    │
    ▼ POST /api/orders
C# Gateway ── ApiKeyFilter ── RateLimiter
    │
    ▼ Channel<T> (10k cap)
RabbitMqPublisherWorker ── auto-reconnect (5s backoff)
    │
    ▼ orders.exchange (topic) / routing key: orders.placed
queue: orders.placed
    │
    ▼
Scala Engine (4 worker fibers)
    │   ├─ dedup (processed_events)
    │   ├─ upsert ledger (sku, qty, order_count)
    │   └─ analytics snapshot every 10 messages
    │
    ├─ nack (transient / queue full) ──► dlx.orders.placed ──► DlqReprocessor (automated retry)
    │                                                              └─ exhausted/permanent ──► needs-attention.orders.placed (manual triage)
    └─ ack ──► done
```

---

## Prerequisites

| Tool | Version | Used by |
|------|---------|---------|
| Docker & Docker Compose | any recent | infrastructure |
| .NET SDK | 9 | C# gateway |
| Java + sbt | 17+ / 1.9+ | Scala engine |
| Python | 3.11+ | load tester, MCP server |

---

## Run

```bash
# Start RabbitMQ and Postgres
docker compose up -d

# Start all services with colour-coded output
./run-all.sh
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

## Contract tests

Cross-service contracts live in `contracts/`:

| File | What it covers |
|------|---------------|
| `contracts/order-api.json` | JSON Schema for `POST /api/orders` request and 202 response |
| `contracts/pacts/scala-engine-cs-gateway.json` | Pact V3 — RabbitMQ message shape agreed between Scala consumer and C# provider |

Run Scala first (generates the pact file), then C#:

```bash
cd processing-engine-scala && sbt test
cd ../gateway-api-cs-tests && dotnet test --filter "Category=Contract"
```

---

## Send an order

```bash
curl -X POST http://localhost:5187/api/orders \
  -H 'Content-Type: application/json' \
  -d '{"sku":"ABC123","quantity":2,"customerId":"22222222-2222-4222-8222-222222222222"}'
# → HTTP 202 Accepted  {"message":"Order queued successfully","eventId":"..."}
```

If API key auth is enabled (see [Security](#security)):

```bash
curl -X POST http://localhost:5187/api/orders \
  -H 'Content-Type: application/json' \
  -H 'X-Api-Key: your-key' \
  -d '{"sku":"ABC123","quantity":2,"customerId":"22222222-2222-4222-8222-222222222222"}'
```

Or fire a batch and get a pass/fail summary:

```bash
./tools/smoke-test.sh        # 20 orders
./tools/smoke-test.sh 50     # custom count
```

---

## Health check

```bash
curl http://localhost:5187/health
# Healthy  → 200  {"status":"healthy","rabbitmq":"ok","bufferUsed":0}
# Degraded → 503  {"status":"unhealthy","rabbitmq":"unreachable","bufferUsed":0}
```

The health endpoint probes a live RabbitMQ connection on every call (3 s timeout). It is not gated by API key auth or rate limiting, so load balancers and container orchestrators can call it freely.

---

## Services

| Directory | Language | What it does |
|-----------|----------|--------------|
| [`gateway-api-cs/`](gateway-api-cs/README.md) | C# .NET 9 | Accepts `POST /api/orders`, buffers events, publishes to RabbitMQ |
| [`processing-engine-scala/`](processing-engine-scala/README.md) | Scala 3 | Consumes events, upserts SKU quantities and order counts in Postgres, logs periodic analytics snapshots, routes failures to DLX |
| [`tools/load-tester/`](tools/load-tester/README.md) | Python / Locust | Simulates concurrent users placing orders |
| [`tools/mcp-server/`](tools/mcp-server/README.md) | Python / MCP | Exposes inventory and order tools to AI assistants via the Model Context Protocol |

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

**RabbitMQ management UI** — http://localhost:15672

**Postgres**
```bash
psql -h localhost -p 5432 -U postgres -d polyglider_inventory
```

Default credentials are in `.env.example`. Never commit a `.env` file with real secrets.

---

## Observability

The Scala engine exposes Prometheus metrics on `:9100/metrics` (port configurable via `app.metrics.port` / `METRICS_PORT`): messages processed, transient/permanent failure counts, retry counts, circuit breaker state (`postgres-write`), and `dlx.orders.placed` queue depth (polled every 15s by default, `app.metrics.dlq-poll-interval-ms` / `METRICS_DLQ_POLL_INTERVAL_MS`).

`docker compose up -d` also starts Prometheus and Grafana, provisioned from `observability/`:

- **Prometheus** — http://localhost:9090, scrapes the Scala engine at `host.docker.internal:9100` (config: `observability/prometheus/prometheus.yml`); alert rules in `observability/prometheus/alerts.yml` fire on `DlqDepthHigh` (depth > 50 for 2m) and `CircuitBreakerOpenTooLong` (open > 60s)
- **Grafana** — http://localhost:3000 (default `admin`/`admin`, override via `GRAFANA_USER`/`GRAFANA_PASSWORD`), pre-loaded with the "PolyGlider Resilience" dashboard (`observability/grafana/dashboards/polyglider-resilience.json`)

The Scala engine itself runs on the host (`sbt run`), not inside `docker-compose.yml`, so Prometheus reaches it through the docker-to-host gateway rather than a compose service name.

---

## Security

API key auth and rate limiting are both opt-in, configured via environment variables.

### API key auth

Set `GATEWAY__API_KEY` (or `appsettings.json` key `Gateway:ApiKey`) to a non-empty value to enable:

```bash
GATEWAY__API_KEY=my-secret dotnet run --project gateway-api-cs
```

When enabled, every `POST /api/orders` request must include `X-Api-Key: <value>` — missing or wrong keys return `401`. The `/health` endpoint is never gated.

### Rate limiting

A fixed-window rate limiter caps `POST /api/orders` at 100 requests per minute by default. Requests over the limit receive `429 Too Many Requests`. Override with:

```bash
GATEWAY__RATELIMITPERMINUTE=200 dotnet run --project gateway-api-cs
```

---

## Configuration

All tuneable settings are documented in [`.env.example`](.env.example). Copy it to `.env` (gitignored) and docker-compose and both services will pick it up automatically.

Key variables:

| Variable | Default | Effect |
|----------|---------|--------|
| `GATEWAY__API_KEY` | _(empty — auth disabled)_ | Enables `X-Api-Key` enforcement on `POST /api/orders` |
| `GATEWAY__RATELIMITPERMINUTE` | `100` | Fixed-window rate limit on `POST /api/orders` |
| `RABBITMQ__HOST` / `RABBIT_HOST` | `127.0.0.1` | Broker address (C# / Scala env var names differ) |
| `RABBITMQ__SSL` / `RABBIT_SSL` | `false` | Enable AMQPS on port 5671 |
| `DB_SSL_MODE` | `disable` | Postgres `sslmode` (e.g. `require`, `verify-full`) |

---

## Resiliency

| Risk | Current mitigation |
|------|-----------|
| Broker unreachable | Gateway buffers up to 10,000 events in `Channel<T>`; `RabbitMqPublisherWorker` reconnects automatically with 5 s backoff |
| Buffer full | `POST /api/orders` returns `503` with `Retry-After: 1` — no silent drops |
| Consumer queue full | Scala engine uses `tryOffer`; a full internal queue (1,000) causes an immediate nack → DLX rather than piling up suspended fibers |
| RabbitMQ channel thread safety | All `basicAck` / `basicNack` calls across 4 worker fibers are serialised through a `Mutex[IO]` |
| Consumer processing failure | Scala engine uses manual ack/nack — a failed DB write nacks the message, keeping it off the queue until the engine recovers |
| Poison-pill messages | Malformed JSON or permanently unprocessable events are nacked without requeue → `dlx.orders.placed`; `DlqReprocessor` classifies them as permanent and escalates immediately to `needs-attention.orders.placed` for manual triage (transient DLQ failures instead get their own bounded retry budget before escalating — see Design doc below) |
| Duplicate delivery | `processed_events` table deduplicates by `eventId`; duplicate events are rolled back without touching the ledger |
| Negative inventory | DB-level `CHECK (qty >= 0)` and `CHECK (order_count >= 0)` constraints on the `ledger` table reject any update that would underflow |
| Broker / DB unreachable (health) | `GET /health` probes a live RabbitMQ connection and returns `503` when unreachable, enabling load balancers to pull broken instances |

---

## Ledger schema

```sql
-- ledger: one row per SKU
sku          TEXT PRIMARY KEY
qty          BIGINT NOT NULL CHECK (qty >= 0)          -- cumulative units
order_count  BIGINT NOT NULL DEFAULT 0
             CHECK (order_count >= 0)                  -- cumulative order count

-- processed_events: idempotency guard
event_id     TEXT PRIMARY KEY
```

The Scala engine logs a per-SKU analytics snapshot to stdout every 10 processed messages:

```
── Analytics snapshot (42 orders, 318 units) ──
  SKU-ALPHA              orders=28       units=210
  SKU-BETA               orders=14       units=108
```
