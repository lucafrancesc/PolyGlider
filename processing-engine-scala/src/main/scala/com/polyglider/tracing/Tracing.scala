package com.polyglider.tracing

import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.context.Context
import io.opentelemetry.context.propagation.{TextMapGetter, TextMapPropagator}
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk

import java.lang.{Iterable as JIterable}
import scala.jdk.CollectionConverters.*

/** Wraps the OpenTelemetry SDK's autoconfigure module, which reads OTEL_EXPORTER_OTLP_ENDPOINT
  * and OTEL_SERVICE_NAME from the environment and wires up the OTLP exporter via ServiceLoader
  * (see opentelemetry-exporter-otlp on the classpath). Defaults to exporting to
  * http://localhost:4317 if no endpoint is configured -- consistent with this app's general
  * fail-open posture: a missing/unreachable collector just means spans are dropped, not that
  * the consumer fails to start.
  */
object Tracing {
  // AutoConfiguredOpenTelemetrySdk falls back to "unknown_service:java" with no OTEL_SERVICE_NAME
  // set; only override via system property (lower precedence than the env var) so an explicit
  // OTEL_SERVICE_NAME still wins.
  if (!sys.env.contains("OTEL_SERVICE_NAME"))
    System.setProperty("otel.service.name", "processing-engine-scala")

  private val sdk = AutoConfiguredOpenTelemetrySdk.initialize().getOpenTelemetrySdk

  val tracer: Tracer = sdk.getTracer("processing-engine-scala")

  private val propagator: TextMapPropagator = sdk.getPropagators.getTextMapPropagator

  private val headersGetter: TextMapGetter[java.util.Map[String, AnyRef]] =
    new TextMapGetter[java.util.Map[String, AnyRef]] {
      override def keys(carrier: java.util.Map[String, AnyRef]): JIterable[String] = carrier.keySet()
      override def get(carrier: java.util.Map[String, AnyRef], key: String): String =
        Option(carrier).flatMap(c => Option(c.get(key))).map(_.toString).orNull
    }

  /** Extracts a W3C `traceparent`/`tracestate` context from RabbitMQ message headers (as set by
    * the C# gateway's RabbitMqPublisherWorker), or Context.root() if absent/malformed -- a
    * message with no/garbled trace headers just starts an unparented span rather than failing.
    */
  def extract(headers: java.util.Map[String, AnyRef]): Context =
    propagator.extract(Context.root(), Option(headers).getOrElse(new java.util.HashMap()), headersGetter)
}
