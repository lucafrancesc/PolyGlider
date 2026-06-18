package com.polyglider.metrics

import cats.effect.{IO, Resource}
import io.prometheus.client.{Counter, Gauge}
import io.prometheus.client.exporter.HTTPServer

/** Prometheus metrics for the failure-handling paths built in Phases 1-4 (failure
  * classification, retry, DLQ reprocessor, circuit breaker). Counters/gauges register
  * against the default collector registry, which `startServer` exposes over HTTP for
  * Prometheus to scrape.
  */
object Metrics {
  val messagesProcessed: Counter = Counter.build()
    .name("polyglider_messages_processed_total")
    .help("Order messages successfully processed and stored to the ledger")
    .register()

  val transientFailures: Counter = Counter.build()
    .name("polyglider_transient_failures_total")
    .help("Messages classified as transient failures, scheduled for backoff retry")
    .register()

  val permanentFailures: Counter = Counter.build()
    .name("polyglider_permanent_failures_total")
    .help("Messages classified as permanent failures, routed to the DLX")
    .register()

  val retries: Counter = Counter.build()
    .name("polyglider_retries_total")
    .help("Backoff retries scheduled by the main consumer")
    .register()

  val circuitBreakerState: Gauge = Gauge.build()
    .name("polyglider_circuit_breaker_state")
    .help("Circuit breaker state: 0=closed, 1=half-open, 2=open")
    .labelNames("name")
    .register()

  val circuitBreakerStateChanges: Counter = Counter.build()
    .name("polyglider_circuit_breaker_state_changes_total")
    .help("Circuit breaker state transitions")
    .labelNames("name", "state")
    .register()

  val dlqDepth: Gauge = Gauge.build()
    .name("polyglider_dlq_depth")
    .help("Number of messages currently sitting in a dead-letter queue")
    .labelNames("queue")
    .register()

  private def stateValue(state: String): Double = state match {
    case "closed"    => 0
    case "half-open" => 1
    case "open"       => 2
    case _            => -1
  }

  /** Callback handed to `CircuitBreaker.create` so the breaker doesn't need to know about
    * Prometheus directly; it just reports its state transitions by name.
    */
  def onCircuitBreakerStateChange(name: String)(state: String): IO[Unit] =
    IO.delay {
      circuitBreakerState.labels(name).set(stateValue(state))
      circuitBreakerStateChanges.labels(name, state).inc()
    }

  def startServer(port: Int): Resource[IO, HTTPServer] =
    Resource.make(IO.blocking(new HTTPServer.Builder().withPort(port).build()))(s => IO.blocking(s.close()))
}
