using System.Threading.Channels;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.HttpOverrides;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Prometheus;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

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
    ForwardedHeaders = ForwardedHeaders.XForwardedFor | ForwardedHeaders.XForwardedProto
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

    var orderEvent = new OrderPlacedEvent(
        EventId: Guid.NewGuid(),
        Sku: request.Sku,
        Quantity: request.Quantity,
        CustomerId: request.CustomerId,
        Timestamp: DateTime.UtcNow
    );

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
public record OrderPlacedEvent(Guid EventId, string Sku, int Quantity, Guid CustomerId, DateTime Timestamp);
public record OrderResponse(string Message, Guid EventId);
public record ErrorResponse(string Error);
public partial class Program { }
