using System.Threading.Channels;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.HttpOverrides;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Prometheus;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

// AddAspNetCoreInstrumentation gives us a span per HTTP request for free; the OTLP endpoint
// defaults to the Jaeger collector wired up in docker-compose.yml / .env.example.
builder.Services.AddOpenTelemetry()
    .ConfigureResource(r => r.AddService("gateway-api-cs"))
    .WithTracing(t => t
        .AddAspNetCoreInstrumentation()
        .AddSource(GatewayTracing.SourceName)
        .AddOtlpExporter(o => o.Endpoint = new Uri(builder.Configuration["Otel:ExporterEndpoint"] ?? "http://localhost:4317")));

builder.Services.AddSingleton(Channel.CreateBounded<OrderPlacedEvent>(new BoundedChannelOptions(10_000)
{
    FullMode = BoundedChannelFullMode.DropWrite
}));
builder.Services.AddSingleton<IOrderPublisher, ChannelOrderPublisher>();
builder.Services.AddHostedService<RabbitMqPublisherWorker>();
builder.Services.AddSingleton<RabbitMqConnectionStatus>();
builder.Services.AddSingleton<IRabbitMqConnectionStatus>(sp => sp.GetRequiredService<RabbitMqConnectionStatus>());
builder.Services.AddHealthChecks().AddCheck<RabbitMqHealthCheck>("rabbitmq");

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddSingleton<IConnectionMultiplexer>(_ =>
{
    var options = ConfigurationOptions.Parse(builder.Configuration["Redis:ConnectionString"] ?? "localhost:6379");
    // Don't block forever if Redis is down at startup; the rate limiter fails open per-request
    // on top of this (see RedisRateLimiter), so a slow/missing Redis shouldn't take the gateway
    // down with it.
    options.AbortOnConnectFail = false;
    options.ConnectTimeout = builder.Configuration.GetValue("Redis:ConnectTimeoutMs", 1000);
    return ConnectionMultiplexer.Connect(options);
});
builder.Services.AddSingleton<IDistributedRateLimiter, RedisRateLimiter>();

var app = builder.Build();

// The gateway has no host-exposed port (see docker-compose.yml) -- nginx is the only way in,
// so trusting forwarded headers from any peer is safe here rather than enumerating the
// compose network's subnet, which Docker assigns dynamically. Without this, every request
// appears to originate from nginx's container IP, which would break per-client behavior
// (e.g. the planned Redis rate limiter keying on client IP).
var forwardedHeadersOptions = new ForwardedHeadersOptions
{
    ForwardedHeaders = ForwardedHeaders.XForwardedFor | ForwardedHeaders.XForwardedProto,
    // nginx's `proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for` appends its own
    // remote address to whatever X-Forwarded-For value (if any) the original request already
    // carried, rather than overwriting it. ForwardLimit's default of 1 only consumes that
    // outermost (nginx-added) hop, discarding anything the original caller set -- every
    // request collapses onto nginx's own address regardless of what the caller sent. 2 walks
    // one hop further left to the caller-supplied value; a request with no pre-existing header
    // (the common case) still resolves correctly since the middleware stops once the header
    // runs out of entries.
    ForwardLimit = 2
};
forwardedHeadersOptions.KnownNetworks.Clear();
forwardedHeadersOptions.KnownProxies.Clear();
app.UseForwardedHeaders(forwardedHeadersOptions);

app.UseHttpMetrics();
app.MapMetrics();

app.UseSwagger();
app.UseSwaggerUI();

app.MapHealthChecks("/health", new HealthCheckOptions
{
    ResponseWriter = async (ctx, report) =>
    {
        ctx.Response.ContentType = "application/json";
        var entry = report.Entries.GetValueOrDefault("rabbitmq");
        await ctx.Response.WriteAsJsonAsync(new
        {
            status = report.Status == HealthStatus.Healthy ? "healthy" : "unhealthy",
            rabbitmq = entry.Status == HealthStatus.Healthy ? "ok" : "unreachable",
            bufferUsed = entry.Data?.GetValueOrDefault("bufferUsed")
        });
    }
})
.WithOpenApi();

app.MapPost("/api/orders", async (OrderRequest request, IOrderPublisher publisher, ILogger<Program> logger, HttpContext context) =>
{
    if (string.IsNullOrWhiteSpace(request.Sku))
    {
        GatewayMetrics.OrdersRejected.WithLabels("validation").Inc();
        return Results.BadRequest(new { error = "sku is required" });
    }
    if (request.Sku.Length > 100)
    {
        GatewayMetrics.OrdersRejected.WithLabels("validation").Inc();
        return Results.BadRequest(new { error = "sku must be at most 100 characters" });
    }
    if (request.Quantity <= 0)
    {
        GatewayMetrics.OrdersRejected.WithLabels("validation").Inc();
        return Results.BadRequest(new { error = "quantity must be positive" });
    }

    // Captured now, while the AspNetCore-instrumented request span is still current -- by the
    // time RabbitMqPublisherWorker dequeues this event the request has already completed and
    // Activity.Current would be null/unrelated, so the W3C context has to travel with the event.
    var orderEvent = new OrderPlacedEvent(
        EventId: Guid.NewGuid(),
        Sku: request.Sku,
        Quantity: request.Quantity,
        CustomerId: request.CustomerId,
        Timestamp: DateTime.UtcNow
    )
    {
        TraceParent = System.Diagnostics.Activity.Current?.Id,
        TraceState = System.Diagnostics.Activity.Current?.TraceStateString
    };

    logger.LogInformation("Order received: sku={Sku} qty={Quantity} eventId={EventId}", request.Sku, request.Quantity, orderEvent.EventId);

    var outcome = await publisher.PublishAsync(orderEvent);
    if (outcome is PublishOutcome.Full or PublishOutcome.NearCapacity)
    {
        GatewayMetrics.OrdersRejected.WithLabels("buffer_full").Inc();
        context.Response.Headers.RetryAfter = "1";
        return Results.StatusCode(429);
    }

    GatewayMetrics.OrdersReceived.Inc();
    return Results.Accepted($"/api/orders/{orderEvent.EventId}", new OrderResponse("Order queued successfully", orderEvent.EventId));
})
.AddEndpointFilter<RedisRateLimitFilter>()
.AddEndpointFilter<ApiKeyFilter>()
.WithOpenApi()
.Produces<OrderResponse>(202)
.Produces<ErrorResponse>(400)
.ProducesProblem(429);

app.Run();

public record OrderRequest(string Sku, int Quantity, Guid CustomerId);

public record OrderPlacedEvent(Guid EventId, string Sku, int Quantity, Guid CustomerId, DateTime Timestamp)
{
    // Schema version of this envelope shape -- see ADR-006. The gateway only ever emits the
    // current version; older consumers (or a version bump on the Scala side) read it to decide
    // whether they understand this payload at all, rather than guessing from field presence.
    public string Version { get; init; } = "1";

    // Carries the W3C trace context from the HTTP request to RabbitMqPublisherWorker so it can
    // be injected into the RabbitMQ message headers. [JsonIgnore] keeps it out of the on-wire
    // JSON body -- the 6-field event schema is a pact-verified contract with the Scala consumer.
    [System.Text.Json.Serialization.JsonIgnore]
    public string? TraceParent { get; init; }
    [System.Text.Json.Serialization.JsonIgnore]
    public string? TraceState { get; init; }
}
public record OrderResponse(string Message, Guid EventId);
public record ErrorResponse(string Error);
public partial class Program { }
