# PolyGlider

A polyglot, event-driven order-processing system built to demonstrate how multiple language runtimes can collaborate over a shared message broker. An HTTP gateway (C#) accepts orders and a processing engine (Scala) maintains a ledger in Postgres — all wired together through RabbitMQ.

Spanning three languages is a deliberate teaching choice, not a production recommendation — see [`docs/architecture.md`](docs/architecture.md) for what concept each language's role is meant to illustrate, and why the supporting infrastructure (RabbitMQ, Postgres, Redis, nginx, Prometheus, Grafana, Jaeger) exists to make those three lessons runnable and observable locally rather than being a fourth concept in its own right.

Two other docs worth knowing about: [`docs/technical-overview.md`](docs/technical-overview.md) (an engineer-facing map of the full request lifecycle and where to find deeper rationale for any given piece) and [`docs/overview-for-stakeholders.md`](docs/overview-for-stakeholders.md) (the same system explained without jargon, for a non-technical reader).

```
HTTP client
    │
    ▼ POST /api/orders
C# Gateway ── RedisRateLimitFilter (global, Redis-backed) ── ApiKeyFilter
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

**Or run the full stack containerized**, with nginx load-balancing across multiple gateway replicas:

```bash
docker compose --profile containerized up -d --build --scale gateway=3
```

`gateway`, `engine`, and `nginx` sit behind the `containerized` Compose profile, so a plain `docker compose up -d` (as used by `./run-all.sh` and the "Start RabbitMQ and Postgres" step above) only brings up shared infrastructure (RabbitMQ, Postgres, Redis, Prometheus, Grafana) and never starts these containerized app services alongside the host-based ones `run-all.sh` starts itself.

nginx (`nginx/nginx.conf`) is the **only** externally-exposed entry point in this mode — at http://localhost:80 — and round-robins across however many `gateway` replicas exist. It re-resolves the `gateway` hostname against Docker's embedded DNS every 10s (`resolver` + the `resolve` parameter on the upstream `server`), so scaling up or down is picked up automatically without restarting nginx. It has no active health-check directive though (an nginx Plus feature) — a dead replica is only routed around passively, after a request to it has already failed.

This mode is a separate code path from `./run-all.sh`, which always starts exactly one gateway instance directly on `:5187`, bypassing nginx entirely. The two are not meant to be run side by side.

---

## Test

```bash
# All suites (requires Docker for the Testcontainers integration test)
./test-all.sh

# Unit tests only — no Docker needed
./test-all.sh --no-integration

# Also run the full end-to-end pipeline test (builds + starts the containerized
# stack, posts an order, and verifies it all the way through to a Postgres ledger
# upsert and an MCP read-back — see tools/e2e-test.sh). Slow; not run by default.
./test-all.sh --e2e
```

**If the Testcontainers-backed tests hang or time out** (`DoobieSkuStorageSpec`, the C# gateway's
`Category=Integration` suite, or this script more generally), check whether a VPN client is
running before suspecting the test or Docker setup itself. A VPN's killswitch/firewall rules can
interfere with Docker's bridge networking in a way that looks identical to a broken test: the TCP
handshake to a container's port succeeds, but the actual data exchange afterward hangs. This
project hit exactly that with NordVPN (see [#131](https://github.com/lucafrancesc/PolyGlider/issues/131)) —
disconnecting it made every previously-flaky suite pass deterministically, no code changes needed.

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

## API docs

Swagger UI is at http://localhost:5187/swagger — generated live from the route definitions in `Program.cs` (`Swashbuckle.AspNetCore`), for interactive human browsing. It is enabled unconditionally, not gated to `Development`, since the whole point of this stack is to be inspectable by anyone who clones the repo.

This is **not** the same artifact as `contracts/order-api.json`: that hand-maintained JSON Schema (strict `additionalProperties: false`) is the contract-test source of truth, validated in `OrderApiSchemaTests.cs`. Swagger can drift if a route changes without updating its `.Produces<T>()` annotations; the JSON Schema cannot drift silently because the contract test fails. Neither is generated from the other.

In the containerized stack, nginx (`nginx/nginx.conf`) proxies every path to the gateway upstream — including `/swagger` — so Swagger UI is also reachable at http://localhost/swagger, not just on the gateway's direct port.

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

**RabbitMQ management UI** — http://localhost:15672 (`guest`/`guest`)

Useful things to check there: the `orders.placed` queue (consumer lag — messages ready vs. unacked), `dlx.orders.placed` (failed messages awaiting reprocessing), and `needs-attention.orders.placed` (escalated, permanent failures — should be empty in steady state). The "Connections" and "Channels" tabs show whether the gateway/engine are currently connected.

Equivalent from the CLI, if you'd rather not open the UI:
```bash
docker exec polyglider_broker rabbitmqctl list_queues name messages messages_ready messages_unacknowledged
docker exec polyglider_broker rabbitmqctl list_connections
```

**Postgres**
```bash
psql -h localhost -p 5432 -U postgres -d polyglider_inventory
```
```sql
select * from ledger order by sku;              -- current qty/order_count per SKU
select count(*) from processed_events;           -- dedup table size
```

**Redis** (rate-limit counters only — no persistence, safe to flush)
```bash
redis-cli -h localhost -p 6379
```
Keys follow `ratelimit:orders:<client-ip>` (set in `gateway-api-cs/Services/RedisRateLimitFilter.cs`), one fixed 60s window per client IP:
```
KEYS ratelimit:orders:*        # all clients currently being rate-limited
GET ratelimit:orders:<ip>      # request count so far in the current window
TTL ratelimit:orders:<ip>      # seconds left in that window (-2 means the key/window expired)
DBSIZE                         # number of distinct client IPs tracked right now
FLUSHDB                        # clear all counters — safe, per the note above
```
Note: `KEYS` is fine here given the tiny, short-lived keyspace; prefer `SCAN` for anything at production scale.

Default credentials are in `.env.example`. Never commit a `.env` file with real secrets.

---

## Observability

All three services expose Prometheus metrics:

- **Scala engine** — `:9100/metrics` (port via `app.metrics.port` / `METRICS_PORT`): messages processed, transient/permanent failure counts, retry counts, circuit breaker state (`postgres-write`), and queue depths for `orders.placed` (consumer lag), `dlx.orders.placed`, and `needs-attention.orders.placed` (polled every 15s by default, `app.metrics.dlq-poll-interval-ms` / `METRICS_DLQ_POLL_INTERVAL_MS`)
- **C# gateway** — `:5187/metrics` (same port the API itself listens on, via `prometheus-net.AspNetCore`): `gateway_orders_received_total`, `gateway_orders_rejected_total{reason}`, `gateway_order_buffer_used`, `gateway_rabbitmq_connected`, plus `http_request_duration_seconds` latency histograms for every route, for free
- **Python MCP server** — `:9101/metrics` (port via `METRICS_PORT`): `mcp_tool_calls_total{tool,outcome}` and `mcp_tool_call_duration_seconds{tool}`

`docker compose up -d` also starts Prometheus and Grafana, provisioned from `observability/`:

- **Prometheus** — http://localhost:9090, scrapes all three services at `host.docker.internal:<port>` (config: `observability/prometheus/prometheus.yml`); alert rules in `observability/prometheus/alerts.yml` fire on `DlqDepthHigh` (depth > 50 for 2m), `NeedsAttentionDepthNonZero` (`needs-attention.orders.placed` non-empty for 5m — that queue is meant to be empty in steady state), and `CircuitBreakerOpenTooLong` (open > 60s)
- **Grafana** — http://localhost:3000 (default `admin`/`admin`, override via `GRAFANA_USER`/`GRAFANA_PASSWORD`), pre-loaded with the "PolyGlider Resilience" dashboard (`observability/grafana/dashboards/polyglider-resilience.json`)

All three services run on the host (`run-all.sh`), not inside `docker-compose.yml`, so Prometheus reaches them through the docker-to-host gateway rather than compose service names. The gateway binds to `0.0.0.0:5187` (not `localhost`) in `Properties/launchSettings.json` specifically so that gateway is reachable — binding to loopback only would make it unreachable from inside the Prometheus container.

### Querying Prometheus directly

http://localhost:9090/graph lets you run PromQL ad hoc. A few starting points (paste into the query box, hit "Execute", then switch to the "Graph" tab):

```promql
rate(gateway_orders_received_total[1m])                 # orders/sec hitting the gateway
gateway_order_buffer_used                                # current in-process channel depth (cap 10,000)
rate(gateway_orders_rejected_total[1m])                   # rejections/sec, broken out by the `reason` label
histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m]))  # p99 HTTP latency
```

http://localhost:9090/targets shows scrape health for all three services — check this first if a Grafana panel is empty, since a red/down target means Prometheus isn't reaching that service at all (most often `run-all.sh` isn't running, or the docker-to-host gateway path described above is broken). http://localhost:9090/alerts shows the live state of `DlqDepthHigh`, `NeedsAttentionDepthNonZero`, `CircuitBreakerOpenTooLong`, and `ProcessingLatencySLOBudgetBurn` (see `docs/slo.md`).

To add a new graph/metric permanently rather than querying ad hoc, edit `observability/prometheus/prometheus.yml` (scrape targets) or `observability/prometheus/alerts.yml` / `slo-rules.yml` (alerting and recording rules), then `docker compose restart prometheus` to pick up the change — no rebuild needed since the files are volume-mounted.

### Using the Grafana dashboard

http://localhost:3000, default login `admin`/`admin` (override via `GRAFANA_USER`/`GRAFANA_PASSWORD` in `.env`). The "PolyGlider Resilience" dashboard is pre-provisioned and pinned in the default folder — no manual setup needed, it's loaded automatically from `observability/grafana/dashboards/polyglider-resilience.json` via the provisioning config in `observability/grafana/provisioning/`.

To build your own panel: "+" → "New dashboard" → "Add visualization" → pick the pre-wired "Prometheus" data source (already provisioned, no need to add one) → enter a PromQL query (the same ones as above work) → set panel title/unit → "Save dashboard". To edit the existing dashboard's JSON directly (e.g. to add a panel and keep it under version control), edit `observability/grafana/dashboards/polyglider-resilience.json` and restart Grafana (`docker compose restart grafana`) to reload it — editing in the UI alone won't persist across a container recreate, since it's provisioned read-only from that file by default.

### Distributed tracing

**Jaeger** — http://localhost:16686. Both `docker compose up -d` (shared infra, not behind the `containerized` profile) and `run-all.sh` bring it up, since host-based and containerized services both need it.

What it's for: Prometheus/Grafana tell you *that* something is slow or failing in aggregate (e.g. p99 latency spiked); Jaeger lets you pick one specific order and see exactly where its time went across all three services, in order, on one timeline. Useful when a metric looks bad and you want to find a concrete example to dig into, e.g. "find the slowest 1% of orders and see if they're all stuck in the same place."

How to use it:
1. Open http://localhost:16686
2. In "Service", pick `gateway-api-cs` (or whatever `OTEL_SERVICE_NAME`/`Otel:ServiceName` is set to for the Scala engine)
3. Optionally set "Tags" to filter, e.g. `http.method=POST`, or sort by "Longest First" to find slow outliers
4. Click "Find Traces", then click into one — each row in the waterfall view is a span; nesting shows you parent/child relationships across services

The C# gateway and Scala engine each export traces via OTLP/gRPC to Jaeger, defaulting to `http://localhost:4317` (overridable via `OTEL__EXPORTERENDPOINT` for the gateway, `OTEL_EXPORTER_OTLP_ENDPOINT` for the engine — see `.env.example`). A trace covers the full order pipeline:

- The gateway's `POST /api/orders` request span (`OpenTelemetry.Instrumentation.AspNetCore`, automatic)
- A `publish orders.placed` producer span around the RabbitMQ publish in `RabbitMqPublisherWorker` — parented onto the request span captured at enqueue time, since by the time the background worker dequeues and publishes, the original request has already completed and `Activity.Current` no longer points at it
- A `process orders.placed` consumer span in the Scala engine's `RabbitConsumer`, parented onto the W3C `traceparent` header the gateway injected into the RabbitMQ message, wrapping the dedup/upsert/ack

A message with no trace headers (e.g. a DLQ retry that's been through several backoff hops) still produces a span — just unparented — rather than failing.

### nginx logs

nginx (`polyglider_nginx`) only runs in the **containerized** run mode (`docker compose --profile containerized up -d --build --scale gateway=N`) — it's not part of `run-all.sh`, so there's nothing to tail there.
`nginx/nginx.conf` doesn't define a custom `log_format`, so the official nginx image's default access/error logs apply, and that image symlinks both to stdout/stderr — meaning `docker logs` is the only place to see them (there's no log file inside the container to `exec` in and `tail`):

```bash
docker logs -f polyglider_nginx          # follow both access + error logs live
docker logs --tail 100 polyglider_nginx  # last 100 lines
```

Access log lines show one entry per request proxied to a `gateway` replica, in the default nginx combined format (client IP, timestamp, request line, status, bytes, referer, user-agent); error log lines show upstream connection failures (e.g. a dead replica before `proxy_next_upstream` retries the next one).

---

## Security

API key auth and rate limiting are both opt-in, configured via environment variables.

### API key auth

Set `Gateway__ApiKey` (double underscore is ASP.NET Core's section separator; this binds to the `appsettings.json` key `Gateway:ApiKey` — note **no** underscore inside `ApiKey` itself, unlike most of this project's other env vars) to a non-empty value to enable:

```bash
Gateway__ApiKey=my-secret dotnet run --project gateway-api-cs
```

When enabled, every `POST /api/orders` request must include `X-Api-Key: <value>` — missing or wrong keys return `401`. The `/health` endpoint is never gated.

### Rate limiting

A Redis-backed fixed-window limiter (`RedisRateLimitFilter` + `RedisRateLimiter`, hand-rolled via a Lua `EVAL` doing an atomic `INCR`+`EXPIRE`, not a framework) caps `POST /api/orders` at 100 requests per minute per client IP by default. Requests over the limit receive `429 Too Many Requests`. Override with:

```bash
GATEWAY__RATELIMITPERMINUTE=200 dotnet run --project gateway-api-cs
```

The limit is enforced **globally** in Redis, not per gateway instance — a client round-robined across N replicas by nginx still only gets the configured limit, not N times it. Client IP is read from `HttpContext.Connection.RemoteIpAddress`, which `UseForwardedHeaders` populates from nginx's `X-Forwarded-For` — trusted unconditionally because the gateway has no host-exposed port, so nginx is the only thing that can ever call it directly. Point at a different Redis with `REDIS__CONNECTIONSTRING` (default `localhost:6379`).

The limiter **fails open**: if Redis is unreachable or slow, `RedisRateLimiter` logs a warning and lets the request through rather than rejecting it — a rate-limiter outage isn't allowed to take down order placement.

### Backpressure

The gateway also returns `429 Too Many Requests` (with `Retry-After: 1`) when the internal `Channel<T>` buffer is at or above a high-water mark (default 8,000 of the 10,000 capacity, i.e. 80%) or completely full — replacing what used to be a silent drop once the buffer hit capacity. Tune the threshold with:

```bash
GATEWAY__CHANNELHIGHWATERMARK=5000 dotnet run --project gateway-api-cs
```

---

## Configuration

All tuneable settings are documented in [`.env.example`](.env.example). Copy it to `.env` (gitignored) and docker-compose and both services will pick it up automatically.

Key variables:

| Variable | Default | Effect |
|----------|---------|--------|
| `Gateway__ApiKey` | _(empty — auth disabled)_ | Enables `X-Api-Key` enforcement on `POST /api/orders` |
| `GATEWAY__RATELIMITPERMINUTE` | `100` | Global (Redis-backed) per-client-IP rate limit on `POST /api/orders` |
| `REDIS__CONNECTIONSTRING` | `localhost:6379` | Redis instance backing the rate limiter |
| `GATEWAY__CHANNELHIGHWATERMARK` | `8000` | Buffer occupancy at/above which `POST /api/orders` returns `429` instead of `202` |
| `RABBITMQ__HOST` / `RABBIT_HOST` | `127.0.0.1` | Broker address (C# / Scala env var names differ) |
| `RABBITMQ__SSL` / `RABBIT_SSL` | `false` | Enable AMQPS on port 5671 |
| `DB_SSL_MODE` | `disable` | Postgres `sslmode` (e.g. `require`, `verify-full`) |

---

## Resiliency

| Risk | Current mitigation |
|------|-----------|
| Broker unreachable | Gateway buffers up to 10,000 events in `Channel<T>`; `RabbitMqPublisherWorker` reconnects automatically with 5 s backoff |
| Buffer full / near capacity | `POST /api/orders` returns `429` with `Retry-After: 1` once the buffer hits its high-water mark (default 80%) or is completely full — no silent drops |
| Consumer queue full | Scala engine uses `tryOffer`; a full internal queue (1,000) causes an immediate nack → DLX rather than piling up suspended fibers |
| RabbitMQ channel thread safety | All `basicAck` / `basicNack` calls across 4 worker fibers are serialised through a `Mutex[IO]` |
| Consumer processing failure | Scala engine uses manual ack/nack — a failed DB write nacks the message, keeping it off the queue until the engine recovers |
| Poison-pill messages | Malformed JSON or permanently unprocessable events are nacked without requeue → `dlx.orders.placed`; `DlqReprocessor` classifies them as permanent and escalates immediately to `needs-attention.orders.placed` for manual triage (transient DLQ failures instead get their own bounded retry budget before escalating — see Design doc below) |
| Duplicate delivery | `processed_events` table deduplicates by `eventId`; duplicate events are rolled back without touching the ledger |
| Negative inventory | DB-level `CHECK (qty >= 0)` and `CHECK (order_count >= 0)` constraints on the `ledger` table reject any update that would underflow |
| Broker / DB unreachable (health) | `GET /health` probes a live RabbitMQ connection and returns `503` when unreachable, enabling load balancers to pull broken instances |
| Redis (rate limiter) unreachable | `RedisRateLimiter` fails open — logs a warning and lets the request through rather than rejecting it; a rate-limiter outage doesn't block order placement |

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
