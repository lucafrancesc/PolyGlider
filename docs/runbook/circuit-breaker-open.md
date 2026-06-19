# Runbook: Circuit breaker open (`postgres-write`)

## Symptoms

- Prometheus alert `CircuitBreakerOpenTooLong` fires: `polyglider_circuit_breaker_state{name="postgres-write"} == 2` for over 60s.
- Grafana "PolyGlider Resilience" dashboard, **Circuit breaker state (postgres-write)** panel shows `2` (Open). `0` = Closed, `1` = Half-Open, `2` = Open.
- `polyglider_transient_failures_total` rate increasing, paired with log lines `Circuit breaker 'postgres-write' tripped open after 5 consecutive failures`.
- Order throughput visibly drops (see **Messages processed (rate)** panel) but no orders are lost — see ADR-005 and the Postgres-outage postmortem for why a real outage might only produce ~1 failure every 30s rather than a fast trip, depending on HikariCP timeout configuration.

## Diagnosis steps

1. Confirm Postgres is actually the cause (the breaker only guards the Postgres write path, but confirm rather than assume):
   ```bash
   docker exec -it <postgres-container> pg_isready -U postgres
   psql -h localhost -p 5432 -U postgres -d polyglider_inventory -c "select 1;"
   ```
2. If Postgres responds fine, check for a slow query or connection pool exhaustion rather than a hard outage — HikariCP logs `Connection is not available, request timed out after 30000ms` on pool exhaustion, which looks identical to a real outage from the breaker's point of view:
   ```bash
   docker logs <scala-container> 2>&1 | grep -i hikari | tail -20
   ```
3. Check how long the breaker has been open:
   ```bash
   curl -s http://localhost:9100/metrics | grep polyglider_circuit_breaker_state
   ```

## Remediation

- **Postgres genuinely down:** restart/recover Postgres (`docker compose up -d postgres`, or whatever the underlying infra issue is). No action needed on the Scala side — the breaker will move to Half-Open on its own once `reset-timeout-ms` (30s) elapses since it tripped, and a single trial call decides whether to close or reopen.
- **Postgres up but pool exhausted:** check whether load is unusually high (Locust load test left running against a low `HikariCP` max pool size is a common cause in this repo's dev setup) and either reduce load or raise the pool size in `application.conf`.
- Do **not** manually restart the Scala consumer process to "reset" the breaker — `CircuitBreaker.create` initializes to Closed(0) on construction, but a restart while Postgres is still down just trips it open again on the first call. Wait for the underlying outage to clear instead.

## Verification

- `polyglider_circuit_breaker_state{name="postgres-write"}` returns to `0` (Closed) — confirmed by the log line `Circuit breaker 'postgres-write' closed after a successful call`.
- `CircuitBreakerOpenTooLong` alert clears in Prometheus/Alertmanager.
- `polyglider_messages_processed_total`'s rate returns to baseline, and `polyglider_dlq_depth{queue="dlx.orders.placed"}` is not climbing (confirms the outage didn't outlast the retry budget and push messages to the DLQ — see the DLQ-depth runbook entry if it did).
