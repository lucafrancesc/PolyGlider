using System.Threading.Channels;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton(Channel.CreateBounded<OrderPlacedEvent>(new BoundedChannelOptions(10_000)
{
    FullMode = BoundedChannelFullMode.DropWrite
}));
builder.Services.AddSingleton<IOrderPublisher, ChannelOrderPublisher>();
builder.Services.AddHostedService<RabbitMqPublisherWorker>();

var app = builder.Build();

app.MapGet("/health", (Channel<OrderPlacedEvent> ch) => Results.Ok(new
{
    status = "healthy",
    bufferAvailable = ch.Reader.CanCount ? ch.Reader.Count < 10_000 : (bool?)null
}));

app.MapPost("/api/orders", async (OrderRequest request, IOrderPublisher publisher) =>
{
    if (string.IsNullOrWhiteSpace(request.Sku))
        return Results.BadRequest(new { error = "sku is required" });
    if (request.Quantity <= 0)
        return Results.BadRequest(new { error = "quantity must be positive" });

    var orderEvent = new OrderPlacedEvent(
        EventId: Guid.NewGuid(),
        Sku: request.Sku,
        Quantity: request.Quantity,
        CustomerId: request.CustomerId,
        Timestamp: DateTime.UtcNow
    );

    await publisher.PublishAsync(orderEvent);

    return Results.Accepted($"/api/orders/{orderEvent.EventId}", new
    {
        message = "Order queued successfully",
        eventId = orderEvent.EventId
    });
});

app.Run();

public record OrderRequest(string Sku, int Quantity, Guid CustomerId);
public record OrderPlacedEvent(Guid EventId, string Sku, int Quantity, Guid CustomerId, DateTime Timestamp);
public partial class Program { }
