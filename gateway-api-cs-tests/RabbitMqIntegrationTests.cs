using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc.Testing;
using RabbitMQ.Client;
using Testcontainers.RabbitMq;
using Xunit;

namespace gateway_api_cs_tests;

[Trait("Category", "Integration")]
public class RabbitMqIntegrationTests : IAsyncLifetime
{
    private readonly RabbitMqContainer _rabbitMq = new RabbitMqBuilder().Build();

    public Task InitializeAsync() => _rabbitMq.StartAsync();
    public Task DisposeAsync() => _rabbitMq.DisposeAsync().AsTask();

    [Fact]
    public async Task PostOrder_PublishesMessageToQueue_WithCamelCaseJson()
    {
        await using var factory = new WebApplicationFactory<Program>()
            .WithWebHostBuilder(builder =>
            {
                builder.UseSetting("RabbitMQ:Host", _rabbitMq.Hostname);
                builder.UseSetting("RabbitMQ:Port", _rabbitMq.GetMappedPublicPort(5672).ToString());
                builder.UseSetting("RabbitMQ:User", "guest");
                builder.UseSetting("RabbitMQ:Password", "guest");
            });

        var client = factory.CreateClient();

        // Wait for the background worker to connect and declare the exchange
        await Task.Delay(TimeSpan.FromSeconds(2));

        // Bind a test queue before posting so we don't miss the message
        var connFactory = new ConnectionFactory
        {
            HostName = _rabbitMq.Hostname,
            Port = _rabbitMq.GetMappedPublicPort(5672),
        };
        await using var conn = await connFactory.CreateConnectionAsync();
        await using var ch = await conn.CreateChannelAsync();
        await ch.ExchangeDeclareAsync("orders.exchange", ExchangeType.Topic, durable: true);
        await ch.QueueDeclareAsync("test.orders", durable: false, exclusive: false, autoDelete: true, arguments: null);
        await ch.QueueBindAsync("test.orders", "orders.exchange", "orders.placed");

        // Post an order
        var customerId = "22222222-2222-4222-8222-222222222222";
        var response = await client.PostAsJsonAsync("/api/orders", new
        {
            sku = "INTEGRATION-SKU",
            quantity = 7,
            customerId
        });
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

        // Retry consuming until the message arrives (worker may take a moment)
        BasicGetResult? result = null;
        for (int i = 0; i < 20 && result is null; i++)
        {
            result = await ch.BasicGetAsync("test.orders", autoAck: true);
            if (result is null) await Task.Delay(500);
        }

        Assert.NotNull(result);
        var json = JsonDocument.Parse(result.Body.ToArray()).RootElement;

        // Verify camelCase keys
        Assert.Equal("INTEGRATION-SKU", json.GetProperty("sku").GetString());
        Assert.Equal(7, json.GetProperty("quantity").GetInt32());
        Assert.Equal(customerId, json.GetProperty("customerId").GetString());
        Assert.True(Guid.TryParse(json.GetProperty("eventId").GetString(), out _));

        // Verify timestamp is ISO-8601 UTC
        var ts = json.GetProperty("timestamp").GetString();
        Assert.True(DateTimeOffset.TryParse(ts, out _), $"timestamp '{ts}' is not a valid ISO-8601 date");
    }
}
