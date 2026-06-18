# Chaos tooling

Injects one of five failure scenarios into a running PolyGlider stack:

| Scenario | What it does | Exercises |
|---|---|---|
| `postgres-kill` | Stops `polyglider_db` for N seconds, then restarts it | Circuit breaker open → half-open → closed; consumer's transient-failure retry path |
| `broker-delay` | Pauses `polyglider_broker` for N seconds, then unpauses | Gateway reconnect/buffer behaviour; consumer redelivery on resume |
| `malformed-payload` | Publishes an unparseable body straight to `orders.exchange`, bypassing the gateway's input validation | Permanent-failure classification → DLX routing |
| `duplicate-message` | Publishes the same `eventId` twice straight to `orders.exchange` | `processed_events` dedup → second copy permanent-fails → DLX |
| `dlq-reprocessor-retry` | Stops `polyglider_db`, publishes a message pre-tagged to land straight in `dlx.orders.placed` with a *transient* reason, then restarts `polyglider_db` after N seconds | `DlqReprocessor`'s own bounded retry-before-escalate path (separate budget from the main consumer's), not just its immediate-escalate path for permanent failures |

## Setup

```bash
cd tools/chaos
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
./chaos.sh postgres-kill 20      # scenario + duration in seconds (default 20)
./chaos.sh broker-delay 15
./chaos.sh malformed-payload
./chaos.sh duplicate-message
./chaos.sh dlq-reprocessor-retry 8   # < 10s: succeeds on the reprocessor's first retry tier
./chaos.sh dlq-reprocessor-retry 75  # >= ~70s: exhausts the reprocessor's 3-tier budget, escalates
./chaos.sh random                # picks one of the original four at random
```

Requires `docker-compose up -d` already running and the gateway/Scala engine started (see the repo root README). Watch `:9090` (Prometheus) or `:3000` (Grafana) for the effect, and the Scala engine's stdout for per-message logs.
