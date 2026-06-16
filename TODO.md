# PolyGlider — Improvement Backlog

Findings from a full-codebase audit. Grouped by severity, then area.

---

## Critical

### [C# Gateway] Silent order drop on buffer full
**File:** `gateway-api-cs/Services/ChannelOrderPublisher.cs`

When the bounded channel (cap 10,000) is full, `TryWrite` returns false and the event is silently dropped — but `PublishAsync` returns `CompletedTask` and the HTTP handler still replies `202 Accepted`. The client believes the order was accepted.

**Fix:** Return a failed `ValueTask` (or throw) when `TryWrite` returns false so the endpoint can return `503 Service Unavailable`.

---

### [Scala Engine] Thread-unsafe RabbitMQ channel access
**File:** `processing-engine-scala/src/main/scala/com/polyglider/RabbitConsumer.scala`

Four worker fibers all call `basicAck` / `basicNack` on the same `Channel` instance. The RabbitMQ Java client's `Channel` is not thread-safe; concurrent calls cause protocol violations, dropped ACKs, and connection resets under load.

**Fix:** Serialize ACK/NACK calls through a queue or mutex, or give each worker its own channel.

---

## High

### [Database] Negative inventory allowed
**File:** `processing-engine-scala/src/main/resources/db/migration/V1__create_ledger.sql`

No `CHECK (qty >= 0)` constraint on the `ledger` table. A negative delta (or replayed negative-quantity event) can make inventory go negative.

**Fix:** Add the constraint in a new Flyway migration. Validate `delta >= 0` in `DoobieSkuStorage.upsertSku`.

---

### [C# Gateway] No authentication or rate limiting
**File:** `gateway-api-cs/Program.cs`

`POST /api/orders` is completely open — no API key, no rate limit, no CORS policy, no request-size cap.

**Fix:** Add ASP.NET Core rate limiting middleware and API key validation. Define a CORS policy.

---

### [All] Hardcoded credentials
Defaults of `guest/guest` (RabbitMQ) and `postgres/postgres` (Postgres) are baked into `docker-compose.yml`, `application.conf`, and `RabbitMqPublisherWorker.cs`. Management UI (port 15672) and Postgres (port 5432) are bound to `0.0.0.0`.

**Fix:** Replace all defaults with environment variables backed by a secrets manager (Vault, AWS Secrets Manager, etc.). Restrict Docker port bindings to `127.0.0.1` or an internal network.

---

### [All] No TLS
RabbitMQ AMQP connection uses plain TCP. Postgres has no SSL mode. MCP server uses plain HTTP. All credentials and data travel in the clear.

**Fix:** Enable AMQPS, Postgres `sslmode=require`, and HTTPS for the gateway.

---

### [C# Gateway] Health check doesn't verify dependencies
**File:** `gateway-api-cs/Program.cs`

`GET /health` only checks channel-buffer availability. It returns "healthy" while the broker or database is unreachable.

**Fix:** Add `IHealthCheck` implementations for RabbitMQ and Postgres connectivity.

---

### [Scala Engine] `queue.offer` backpressure loss
**File:** `processing-engine-scala/src/main/scala/com/polyglider/RabbitConsumer.scala`

`queue.offer(d)` is fired via `unsafeRunAndForget` inside the consumer callback. When the bounded queue (1,000) is full, suspended `offer` effects pile up in the dispatcher with no backpressure signal. Under sustained burst traffic this silently exhausts memory.

**Fix:** Drop messages at the callback level when the queue is full (check `queue.tryOffer`) and emit a metric/log.

---

## Medium

### [Scala Engine] `withRetries` helper is never used
**File:** `processing-engine-scala/src/main/scala/com/polyglider/OrderProcessor.scala`

`OrderProcessor.withRetries` is defined but never called. Failed messages are nacked immediately with no retry delay, and retried messages arrive right back on the queue as a hot loop until they exhaust prefetch.

**Fix:** Wire `withRetries` into the processing path with exponential backoff and a max-attempts cap before nacking.

---

### [C# Gateway] Fixed 5 s reconnect delay, no jitter
**File:** `gateway-api-cs/Services/RabbitMqPublisherWorker.cs`

`await Task.Delay(TimeSpan.FromSeconds(5))` is constant. Under a broker restart, all gateway instances reconnect simultaneously (thundering herd).

**Fix:** Exponential backoff with jitter (e.g., `Polly.RetryPolicy`).

---

### [Scala Engine] Config values not read from `application.conf`
**File:** `processing-engine-scala/src/main/resources/application.conf`

`app.consumer.queue-size` and `app.consumer.workers` are defined but not read; code hardcodes `1000` and `4`.

**Fix:** Read values from `Config` at startup so operators can tune without recompilation.

---

### [MCP Server] New Postgres connection per request
**File:** `tools/mcp-server/main.py`

`_get_conn()` opens a new `psycopg2.connect(…)` on every tool call — no pooling.

**Fix:** Use `psycopg2.pool.SimpleConnectionPool` or switch to `asyncpg` with a connection pool.

---

### [MCP Server] `list_recent_events` has no upper bound on `limit`
**File:** `tools/mcp-server/main.py`

A caller can pass `limit=999999` and pull millions of rows into memory.

**Fix:** Clamp `limit` to `max(1, min(limit, 1000))` and document the cap.

---

### [MCP Server] Partial exception handling
**File:** `tools/mcp-server/main.py`

Only `psycopg2.OperationalError` is caught. `IntegrityError`, `ProgrammingError`, and others propagate uncaught. Error returns use a success-shaped dict (`{"error": "..."}`) indistinguishable from real results.

**Fix:** Catch `psycopg2.Error` (base class), log the exception, and raise an MCP-level error.

---

### [Gateway / Contract] No `maxLength` on `sku`
**File:** `contracts/order-api.json` and `gateway-api-cs/Program.cs`

The JSON Schema has no `maxLength` for `sku`, and the gateway only checks for empty/whitespace. A very long string passes validation.

**Fix:** Add `"maxLength": 100` to the schema. Enforce it in the gateway validator.

---

### [All] No correlation IDs across services
There is no request-scoped correlation ID passed from the HTTP call through the RabbitMQ event into the Scala log lines. End-to-end debugging requires guessing.

**Fix:** Generate a `traceId` in the gateway, embed it in `OrderPlacedEvent`, and propagate it through Scala logs.

---

### [All] No graceful shutdown
- Scala: `IO.never` runs forever; no SIGTERM handler drains in-flight messages before exit.
- C# gateway: no drain of the `Channel<>` buffer on shutdown.
- Python MCP server: no connection cleanup on SIGINT.

**Fix:** Register shutdown hooks that drain in-flight work, close connections, and flush logs before process exit.

---

### [Operations] DLQ has no consumer or alerting
The `dlx.orders.placed` queue is declared but nothing reads it. There is no metric on its depth and no runbook for manual triage.

**Fix:** Add a minimal DLQ consumer that logs poison messages and emits a metric. Alert when depth > 0.

---

### [All] No metrics or distributed tracing
No Prometheus endpoint, no OpenTelemetry instrumentation, no consumer-lag tracking, no latency percentiles.

**Fix:** Add `prometheus-net` to the C# gateway, `micrometer` to the Scala engine, and wire up a Grafana dashboard.

---

### [Scala Engine] UUIDs not validated before storage
**File:** `processing-engine-scala/src/main/scala/com/polyglider/UuidUtils.scala`

`UuidUtils.isValidUuid` is defined but never called on incoming `customerId` or `eventId` fields.

**Fix:** Validate UUIDs in the message deserialization step; nack and log on failure.

---

### [Scala Engine] No duplicate-event handling edge case
**File:** `processing-engine-scala/src/main/scala/com/polyglider/storage/DoobieSkuStorage.scala`

`insertEvent *> upsertLedger` is in one transaction. If the network drops after `insertEvent` succeeds but before `upsertLedger` commits, a redelivery will fail on the duplicate `event_id` insert without updating the ledger — leaving the order unprocessed permanently.

**Fix:** Use `INSERT … ON CONFLICT DO NOTHING` for the event deduplication insert and always attempt the upsert, making the transaction idempotent for redeliveries.

---

## Low / Quality

### [Scala Engine] Unused dependency: `fs2-rabbit`
Declared in `build.sbt` but not used; the consumer uses the raw RabbitMQ Java client.

**Fix:** Remove the unused dependency.

---

### [Scala Engine] Logging configuration is console-only
**File:** `processing-engine-scala/src/main/resources/logback.xml`

Synchronous console appender only; no file rotation, no async appender. Logging blocks message-processing threads under high throughput.

**Fix:** Add an async appender (`AsyncAppender`) wrapping the console appender.

---

### [Tests] H2 test does not exercise `DoobieSkuStorage`
**File:** `processing-engine-scala/src/test/scala/com/polyglider/DatabaseSpec.scala`

The spec runs raw SQL against H2 instead of exercising `DoobieSkuStorage` directly. It tests H2 SQL, not the real storage class.

**Fix:** Test `DoobieSkuStorage` via a Testcontainers Postgres instance, or use the real implementation against H2 with compatible SQL.

---

### [Tests] No end-to-end integration test
No test exercises the full path: HTTP → RabbitMQ → Scala consumer → Postgres → MCP read-back.

**Fix:** Add a Docker Compose–based E2E test (e.g., using Testcontainers from C# or a separate test script).

---

### [Config] `application.conf` worker and queue-size values silently ignored
See the entry above — operators who change these values will see no effect and receive no warning.

---

### [docker-compose] No resource limits
No CPU or memory limits on RabbitMQ or Postgres containers. One service going haywire can starve the host.

**Fix:** Add `mem_limit` / `cpus` constraints in `docker-compose.yml`.
