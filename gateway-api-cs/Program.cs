using System.Threading.Channels;
using System.Threading.RateLimiting;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.RateLimiting;
using Microsoft.Extensions.Diagnostics.HealthChecks;

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

builder.Services.AddRateLimiter(options =>
{
    options.AddFixedWindowLimiter("orders", o =>
    {
        o.Window = TimeSpan.FromMinutes(1);
        o.PermitLimit = builder.Configuration.GetValue("Gateway:RateLimitPerMinute", 100);
        o.QueueLimit = 0;
        o.QueueProcessingOrder = QueueProcessingOrder.OldestFirst;
    });
    options.RejectionStatusCode = 429;
});

var app = builder.Build();

app.UseRateLimiter();

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
});

app.MapPost("/api/orders", async (OrderRequest request, IOrderPublisher publisher, ILogger<Program> logger, HttpContext context) =>
{
    if (string.IsNullOrWhiteSpace(request.Sku))
        return Results.BadRequest(new { error = "sku is required" });
    if (request.Sku.Length > 100)
        return Results.BadRequest(new { error = "sku must be at most 100 characters" });
    if (request.Quantity <= 0)
        return Results.BadRequest(new { error = "quantity must be positive" });

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
        context.Response.Headers.RetryAfter = "1";
        return Results.StatusCode(429);
    }

    return Results.Accepted($"/api/orders/{orderEvent.EventId}", new
    {
        message = "Order queued successfully",
        eventId = orderEvent.EventId
    });
})
.AddEndpointFilter<ApiKeyFilter>()
.RequireRateLimiting("orders");

app.Run();

public record OrderRequest(string Sku, int Quantity, Guid CustomerId);
public record OrderPlacedEvent(Guid EventId, string Sku, int Quantity, Guid CustomerId, DateTime Timestamp);
public partial class Program { }
