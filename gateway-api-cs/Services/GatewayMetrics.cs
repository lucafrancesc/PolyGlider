using Prometheus;

/// <summary>
/// Custom Prometheus metrics for the gateway. app.UseHttpMetrics() (wired in Program.cs)
/// separately provides http_request_duration_seconds with latency histograms for every route
/// for free -- these cover the gateway-specific signals that generic HTTP metrics can't.
/// </summary>
public static class GatewayMetrics
{
    public static readonly Counter OrdersReceived = Metrics.CreateCounter(
        "gateway_orders_received_total",
        "Orders accepted and queued via POST /api/orders");

    public static readonly Counter OrdersRejected = Metrics.CreateCounter(
        "gateway_orders_rejected_total",
        "Orders rejected by the gateway",
        new CounterConfiguration { LabelNames = ["reason"] });

    public static readonly Gauge BufferUsed = Metrics.CreateGauge(
        "gateway_order_buffer_used",
        "Current number of orders sitting in the in-process Channel<T> buffer, waiting to be published to RabbitMQ");

    public static readonly Gauge RabbitMqConnected = Metrics.CreateGauge(
        "gateway_rabbitmq_connected",
        "Whether the gateway's RabbitMQ publish connection is currently up (1) or down (0)");
}
