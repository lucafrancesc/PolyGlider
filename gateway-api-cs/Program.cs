using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

var builder = WebApplication.CreateBuilder(args);

builder.Configuration.AddEnvironmentVariables();

// Bind configuration to a small settings record
builder.Services.Configure<RabbitSettings>(builder.Configuration.GetSection("Rabbit"));

// Register RabbitMQ connection and channel as singletons
builder.Services.AddSingleton(sp => {
    var settings = sp.GetRequiredService<IOptions<RabbitSettings>>().Value;
    var factory = new ConnectionFactory {
        HostName = settings.Host ?? "localhost",
        Port = settings.Port ?? 5672,
        UserName = settings.User ?? "guest",
        Password = settings.Password ?? "guest"
    };
    return factory;
});

builder.Services.AddSingleton(sp => {
    var factory = sp.GetRequiredService<ConnectionFactory>();
    var conn = factory.CreateConnection();
    // keep channel open for publishing; ensure durable exchange/queue declared on startup
    var ch = conn.CreateModel();
    var exchange = builder.Configuration.GetValue<string>("Rabbit:Exchange") ?? "orders.exchange";
    ch.ExchangeDeclare(exchange: exchange, type: ExchangeType.Topic, durable: true);
    ch.QueueDeclare(queue: "orders.placed", durable: true, exclusive: false, autoDelete: false, arguments: null);
    ch.QueueBind("orders.placed", exchange, "orders.placed");
    ch.BasicQos(0, 1, false);
    return (IConnection: conn, IModel: ch, Exchange: exchange);
});

var app = builder.Build();

app.Lifetime.ApplicationStopping.Register(() => {
    // dispose RabbitMQ resources gracefully
    try {
        var tuple = app.Services.GetService<(IConnection IConnection, IModel IModel, string Exchange)?>();
        if (tuple is not null) {
            tuple.Value.IModel.Close();
            tuple.Value.IConnection.Close();
        }
    } catch { /* swallow on shutdown */ }
});

const string defaultRoutingKey = "orders.placed";

app.MapPost("/api/orders", (OrderRequest request) => {
    var tuple = app.Services.GetRequiredService<(IConnection IConnection, IModel IModel, string Exchange)>();
    var ch = tuple.IModel;
    var exchange = tuple.Exchange;

    var orderEvent = new OrderPlacedEvent(Guid.NewGuid(), request.Sku, request.Quantity, request.CustomerId, DateTime.UtcNow);
    var jsonPayload = JsonSerializer.Serialize(orderEvent);
    var body = Encoding.UTF8.GetBytes(jsonPayload);

    var props = ch.CreateBasicProperties();
    props.ContentType = "application/json";
    props.DeliveryMode = 2; // persistent

    ch.BasicPublish(exchange: exchange, routingKey: defaultRoutingKey, basicProperties: props, body: body);

    return Results.Accepted($"/api/orders/{orderEvent.EventId}", new { message = "Order queued", eventId = orderEvent.EventId });
});

app.Run();

public record RabbitSettings(string? Host, int? Port, string? User, string? Password);
public record OrderRequest(string Sku, int Quantity, Guid CustomerId);
public record OrderPlacedEvent(Guid EventId, string Sku, int Quantity, Guid CustomerId, DateTime Timestamp);