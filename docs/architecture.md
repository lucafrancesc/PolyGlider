# Architecture: why three languages?

PolyGlider intentionally spans C#/.NET, Scala/cats-effect, and Python. This is not how a real production system should be built — nobody should casually mix three language runtimes in one pipeline just because each one is individually nice. It's a teaching choice: each language's role was picked because it's a good vehicle for a specific concept, and the point of the repo is to let you read (and run) working code for all of them side by side, with no cloud dependency.

If you're here to understand the system rather than rebuild it from scratch, this doc is the "why," not the "how" — see the root [`README.md`](../README.md) for the quickstart and the per-component READMEs (`gateway-api-cs/`, `processing-engine-scala/`, `tools/mcp-server/`) for implementation detail.

## What each language is here to teach

**C# / ASP.NET Core minimal APIs — backpressure-aware ingestion at the edge.** The gateway's job is to accept a burst of HTTP requests without falling over, even if the thing downstream (RabbitMQ) is slow or briefly unavailable. The `Channel<OrderPlacedEvent>` (`gateway-api-cs/Program.cs`) is the teaching point: a bounded in-process buffer with an explicit high-water mark (`PublishOutcome.NearCapacity` → HTTP 429 before the buffer is actually full, not after) and a drop policy (`BoundedChannelFullMode.DropWrite`) once it is. This is the simplest possible illustration of "decouple accepting work from doing work" — no separate message broker needed to demonstrate the pattern, just a bounded queue and a `BackgroundService` (`RabbitMqPublisherWorker`) draining it.

**Scala / cats-effect — structured concurrency and resource safety.** The engine's job is the opposite problem: consume continuously, and never leak a connection or leave a fiber running past the point where its caller stopped caring. `Resource[IO, Unit]` (`OrderProcessor.process`, `RabbitConsumer.start`) is the teaching point — every acquired resource (the Postgres transactor, the RabbitMQ connection, the metrics HTTP server, the worker fibers) has a paired release, composed via `for`-comprehension so that cancelling the outer `IO` (a SIGTERM, in `Main.scala`'s `IOApp.Simple`) unwinds every layer in reverse order automatically. Contrast this with the C# side's `IAsyncDisposable`/`using` pattern: same goal (deterministic cleanup), different mechanism (effect-system composition vs. language-level disposal), worth seeing both.

**Python / MCP — tool-based LLM integration.** `tools/mcp-server/` isn't part of the order pipeline at all; it's a separate read path that exposes the same Postgres ledger (and the gateway's write path) as four MCP tools (`list_inventory`, `get_sku_quantity`, `list_recent_events`, `place_order`). The teaching point is the Model Context Protocol's tool-calling shape itself — a thin, typed wrapper around existing infrastructure (direct Postgres queries, an HTTP call to the gateway) is enough to make a system inspectable and actionable by an LLM-based assistant, without rebuilding any of the underlying logic.

## Everything else is supporting infrastructure, not a fourth lesson

RabbitMQ, Postgres, Redis, nginx, Prometheus, Grafana, and Jaeger all exist to make the three core lessons above runnable and observable locally — they're not additional "concepts" the way the three languages are:

- **RabbitMQ** — the broker the gateway and engine actually communicate through (see ADR-001 for why RabbitMQ over Kafka).
- **Postgres** — the durable ledger the engine writes to.
- **Redis** — backs the gateway's distributed rate limiter (`RedisRateLimiter`), so the rate limit is enforced globally across however many gateway replicas exist, not per-instance.
- **nginx** — load-balances across gateway replicas in the containerized run mode (see below); the only externally-exposed entry point in that mode.
- **Prometheus + Grafana** — scrape and visualize the metrics each of the three services exposes natively (`/metrics` on the gateway and engine, a separate port on the MCP server).
- **Jaeger** — collects the distributed trace that ties one HTTP request to its RabbitMQ publish to its Scala-side processing, across the C#/Scala language boundary, via the W3C `traceparent` header.

## Two ways to run it

Both run modes are documented with commands in the root README's [Run](../README.md#run) section; the short version of *why* there are two:

- **`./run-all.sh`** (host-based) starts the Scala engine via `sbt run` and the gateway via `dotnet run` directly on the host, with exactly one gateway instance on `:5187`, no nginx, no Redis fan-out. This is the fast-iteration loop — no Docker image build step between editing code and seeing it run.
- **`docker compose --profile containerized up -d --build --scale gateway=3`** builds and runs everything as containers, with nginx in front of as many gateway replicas as you ask for at `:80`. This is the "what would this look like with more than one instance of the stateless service" mode — it's where the Redis-backed rate limiter and nginx's hostname-re-resolution-based replica discovery actually matter, neither of which has anything to demonstrate with a single instance.

The two modes are deliberately not meant to run side by side (see `docker-compose.yml`'s `containerized` Compose profile, which keeps the containerized `gateway`/`engine`/`nginx` out of a plain `docker compose up -d`).
