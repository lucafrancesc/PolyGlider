package com.polyglider.metrics

import cats.effect.{IO, Resource}
import io.prometheus.client.{Counter, Gauge, Histogram}
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

  // Measured from the gateway's `timestamp` field (set when the order was accepted) to this
  // consumer successfully acking the message -- not a true distributed span duration, but
  // good enough to back an SLO without requiring clock-synced span export into Prometheus.
  // Relies on the gateway and engine clocks being reasonably in sync (true for this project's
  // single-host/Docker-Compose deployment; would need real span-duration export instead in a
  // multi-host deployment where that assumption doesn't hold).
  val orderProcessingDuration: Histogram = Histogram.build()
    .name("polyglider_order_processing_duration_seconds")
    .help("Time from the gateway's recorded order timestamp to this consumer successfully acking the message")
    .buckets(0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300)
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
