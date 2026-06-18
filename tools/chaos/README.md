# Chaos tooling

Injects one of four failure scenarios into a running PolyGlider stack:

| Scenario | What it does | Exercises |
|---|---|---|
| `postgres-kill` | Stops `polyglider_db` for N seconds, then restarts it | Circuit breaker open → half-open → closed; consumer's transient-failure retry path |
| `broker-delay` | Pauses `polyglider_broker` for N seconds, then unpauses | Gateway reconnect/buffer behaviour; consumer redelivery on resume |
| `malformed-payload` | Publishes an unparseable body straight to `orders.exchange`, bypassing the gateway's input validation | Permanent-failure classification → DLX routing |
| `duplicate-message` | Publishes the same `eventId` twice straight to `orders.exchange` | `processed_events` dedup → second copy permanent-fails → DLX |

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
./chaos.sh random                # picks one of the four at random
```

Requires `docker-compose up -d` already running and the gateway/Scala engine started (see the repo root README). Watch `:9090` (Prometheus) or `:3000` (Grafana) for the effect, and the Scala engine's stdout for per-message logs.
