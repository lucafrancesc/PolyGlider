using System.Text;
using System.Text.Json;
using RabbitMQ.Client;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

// 1. Configure the connection factory for RabbitMQ
var factory = new ConnectionFactory 
{ 
    HostName = "localhost",
    UserName = "guest",
    Password = "guest"
};

// 2. Open a persistent connection to the broker
await using var connection = await factory.CreateConnectionAsync();
await using var channel = await connection.CreateChannelAsync();

// 3. Declare the exchange we will publish events to
const string exchangeName = "orders.exchange";
await channel.ExchangeDeclareAsync(
    exchange: exchangeName, 
    type: ExchangeType.Topic, 
    durable: true
);

// 4. Create our high-speed ingestion endpoint
app.MapPost("/api/orders", async (OrderRequest request) =>
{
    // A. Create a structured event payload matching our system specification
    var orderEvent = new OrderPlacedEvent(
        EventId: Guid.NewGuid(),
        Sku: request.Sku,
        Quantity: request.Quantity,
        CustomerId: request.CustomerId,
        Timestamp: DateTime.UtcNow
    );

    // B. Serialize the payload to a standard JSON string
    var jsonPayload = JsonSerializer.Serialize(orderEvent);
    var body = Encoding.UTF8.GetBytes(jsonPayload);

    // C. Publish the message asynchronously to the message broker
    await channel.BasicPublishAsync(
        exchange: exchangeName,
        routingKey: "orders.placed",
        body: body
    );

    // D. Return a non-blocking 202 Accepted response immediately
    return Results.Accepted($"/api/orders/{orderEvent.EventId}", new { 
        message = "Order queued successfully", 
        eventId = orderEvent.EventId 
    });
});

app.Run();

// --- Data Contracts ---
// The payload we expect from the incoming HTTP request
public record OrderRequest(string Sku, int Quantity, Guid CustomerId);

// The canonical event schema shared across our polyglot services
internal record OrderPlacedEvent(Guid EventId, string Sku, int Quantity, Guid CustomerId, DateTime Timestamp);
