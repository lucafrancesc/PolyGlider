# Technical overview

An engineer-facing entry point into the system's current end-to-end shape. This doc doesn't re-derive anything that's already documented well elsewhere — it's the map that tells you which existing doc has the answer to a given question. See [`docs/architecture.md`](architecture.md) for *why* the system spans three languages, and the root [`README.md`](../README.md) for the canonical pipeline diagram, run/test commands, and full config reference.

## 1. System at a glance

A `POST /api/orders` HTTP request becomes a durable Postgres ledger update, with every stage — accept, queue, publish, consume, persist — independently observable and independently resilient to its own dependency going down. The root README's pipeline diagram (top of the file) is the canonical picture; this doc walks the same path with more detail on what happens at each hop and where to go to understand any single piece more deeply.

## 2. Request lifecycle end-to-end

```
HTTP client
  → nginx                     (containerized mode only — load-balances across gateway replicas)
  → ApiKeyFilter               (opt-in X-Api-Key check, IEndpointFilter)
  → RedisRateLimitFilter       (global per-IP rate limit, Redis-backed, fails open on Redis outage)
  → POST /api/orders handler   (validates sku/quantity, builds OrderPlacedEvent)
       ⤷ [Jaeger span starts: "POST /api/orders", auto-instrumented]
  → Channel<OrderPlacedEvent>  (bounded in-process buffer, 10k cap, drops on full)
  → RabbitMqPublisherWorker    (BackgroundService, drains the channel)
       ⤷ [Jaeger span: "publish orders.placed", parented onto the request span
           captured at enqueue time — by publish time the original request has
           already completed]
       ⤷ injects W3C traceparent header into the RabbitMQ message
  → RabbitMQ: orders.exchange (topic) → queue orders.placed
  → RabbitConsumer (Scala, 4 worker fibers)
       ⤷ [Jaeger span: "process orders.placed", parented onto the traceparent
           header the gateway injected]
       ⤷ dedup check (processed_events) + upsert ledger (qty, order_count) in one
         transaction, wrapped in a circuit breaker around the Postgres write
  → ack / nack
       ⤷ nack routes to dlx.orders.exchange → DlqReprocessor → needs-attention.orders.placed
         on exhausted/permanent failure (see docs/resilience-design-doc.md)
```

Each filter, span, and failure path above is real, shipped code — not aspirational. Where to read more about any single piece:

| Stage | Code | Deeper rationale |
|---|---|---|
| Rate limiting | `gateway-api-cs/Services/RedisRateLimitFilter.cs` | Root README → Security |
| API key auth | `gateway-api-cs/Services/ApiKeyFilter.cs` | Root README → Security |
| In-process buffer | `gateway-api-cs/Program.cs` (`Channel<OrderPlacedEvent>`) | `docs/adr/` (Channel buffer ADR) |
| RabbitMQ publish/reconnect | `gateway-api-cs/Services/RabbitMqPublisherWorker.cs` | `docs/adr/` (RabbitMQ vs Kafka) |
| Dedup + upsert | `processing-engine-scala/.../storage/DoobieSkuStorage.scala` | `docs/adr/` (dedup strategy) |
| Failure classification | `processing-engine-scala/.../consumer/ProcessingFailure.scala` | `docs/resilience-design-doc.md` |
| Circuit breaker | `processing-engine-scala/.../resilience/CircuitBreaker.scala` | `docs/resilience-design-doc.md`, `docs/adr/` (thresholds), `docs/postmortems/` (measured tripping latency) |
| DLQ reprocessing | `processing-engine-scala/.../reprocessor/DlqReprocessor.scala` | `docs/resilience-design-doc.md` |
| Distributed tracing | `gateway-api-cs/Services/GatewayTracing.cs`, `processing-engine-scala/.../tracing/Tracing.scala` | Root README → Observability → Distributed tracing |

## 3. Run modes

Two ways to run the whole stack, and they're not meant to overlap:

- **Host-based** (`./run-all.sh`) — one gateway instance on `:5187`, no nginx, no Redis fan-out. Fast iteration loop: no Docker image build between editing code and seeing it run.
- **Containerized** (`docker compose --profile containerized up -d --build --scale gateway=N`) — nginx in front of N gateway replicas at `:80`, Redis-backed rate limiting enforced globally across them. This is the mode that actually exercises multi-replica behavior.

`docker-compose.yml`'s `containerized` Compose profile is what keeps these from colliding: `gateway`, `engine`, and `nginx` sit behind that profile, so a plain `docker compose up -d` (what `run-all.sh` and the host-based flow use to bring up shared infra) never starts the containerized app services alongside the host-based ones. Shared infra (RabbitMQ, Postgres, Redis, Prometheus, Grafana, Jaeger) is **not** behind the profile — both run modes need it. See root README → Run for full commands.

## 4. Observability reference

| What | Where | Detail |
|---|---|---|
| Gateway metrics | `:5187/metrics` | `gateway_orders_received_total`, `gateway_orders_rejected_total{reason}`, `gateway_order_buffer_used`, `gateway_rabbitmq_connected`, + HTTP latency histograms |
| Engine metrics | `:9100/metrics` | messages processed, transient/permanent failures, retries, circuit breaker state, DLQ depths |
| MCP server metrics | `:9101/metrics` | `mcp_tool_calls_total{tool,outcome}`, `mcp_tool_call_duration_seconds{tool}` |
| Prometheus | `:9090` | scrapes all three; alert rules `DlqDepthHigh`, `CircuitBreakerOpenTooLong` |
| Grafana | `:3000` | "PolyGlider Resilience" dashboard, pre-provisioned |
| Jaeger | `:16686` UI, `:4317` OTLP | full trace: request span → publish span → consumer span, see §2 above |

Root README → Observability has the full detail (alert thresholds, dashboard panel list, default credentials).

## 5. Configuration reference

Every env var, with defaults, lives in [`.env.example`](../.env.example) and the root README's Configuration table — not duplicated here. The one thing worth calling out at this level: the C# gateway and Scala engine use different env var naming conventions for the same kind of setting (`RABBITMQ__HOST` vs `RABBIT_HOST`, `OTEL__EXPORTERENDPOINT` vs `OTEL_EXPORTER_OTLP_ENDPOINT`) — a side effect of following each language's own idiomatic config-binding convention (ASP.NET Core's double-underscore hierarchy separator vs. a flat env-var-per-setting Scala/Typesafe Config style) rather than inventing a shared one.

## 6. Where to go deeper

- **Why three languages, and what each one teaches** — [`docs/architecture.md`](architecture.md)
- **Why each resilience decision was made the way it was** — [`docs/adr/`](adr/) (one ADR per decision: RabbitMQ vs Kafka, dedup strategy, Channel buffer, failure classification, circuit breaker thresholds)
- **The resilience system as a whole, with trade-offs** — [`docs/resilience-design-doc.md`](resilience-design-doc.md)
- **What actually happened when we broke things on purpose** — [`docs/postmortems/`](postmortems/) (chaos-testing results that informed several of the ADRs above)
- **What to do when something's actually wrong in a running system** — [`docs/runbook/`](runbook/) (Symptoms/Diagnosis/Remediation/Verification for DLQ depth, circuit breaker open, broker unreachable, malformed payloads, duplicate storms)
- **Known gaps and tracked follow-ups** — see open GitHub issues, e.g. [#113](https://github.com/lucafrancesc/PolyGlider/issues/113) (load tester can't simulate multiple client IPs against the rate limiter) and [#114](https://github.com/lucafrancesc/PolyGlider/issues/114) (MCP server's `place_order` doesn't forward an API key)
