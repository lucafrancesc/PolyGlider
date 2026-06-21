using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Mvc.Testing;
using RabbitMQ.Client;
using Testcontainers.RabbitMq;
using Xunit;

namespace gateway_api_cs_tests;

[Trait("Category", "Integration")]
public class RabbitMqIntegrationTests : IAsyncLifetime
{
    private const string RabbitUser = "guest";
    private const string RabbitPass = "guest";

    private readonly RabbitMqContainer _rabbitMq = new RabbitMqBuilder()
        .WithUsername(RabbitUser)
        .WithPassword(RabbitPass)
        .Build();

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
                builder.UseSetting("RabbitMQ:User", RabbitUser);
                builder.UseSetting("RabbitMQ:Password", RabbitPass);
            });

        var client = factory.CreateClient();

        // Wait for the background worker to connect and declare the exchange
        await Task.Delay(TimeSpan.FromSeconds(2));

        // Bind a test queue before posting so we don't miss the message
        var connFactory = new ConnectionFactory
        {
            HostName = _rabbitMq.Hostname,
            Port = _rabbitMq.GetMappedPublicPort(5672),
            UserName = RabbitUser,
            Password = RabbitPass,
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

    [Fact]
    public async Task PostOrder_InjectsW3CTraceparentHeader()
    {
        await using var factory = new WebApplicationFactory<Program>()
            .WithWebHostBuilder(builder =>
            {
                builder.UseSetting("RabbitMQ:Host", _rabbitMq.Hostname);
                builder.UseSetting("RabbitMQ:Port", _rabbitMq.GetMappedPublicPort(5672).ToString());
                builder.UseSetting("RabbitMQ:User", RabbitUser);
                builder.UseSetting("RabbitMQ:Password", RabbitPass);
            });

        var client = factory.CreateClient();

        // Wait for the background worker to connect and declare the exchange
        await Task.Delay(TimeSpan.FromSeconds(2));

        var connFactory = new ConnectionFactory
        {
            HostName = _rabbitMq.Hostname,
            Port = _rabbitMq.GetMappedPublicPort(5672),
            UserName = RabbitUser,
            Password = RabbitPass,
        };
        await using var conn = await connFactory.CreateConnectionAsync();
        await using var ch = await conn.CreateChannelAsync();
        await ch.ExchangeDeclareAsync("orders.exchange", ExchangeType.Topic, durable: true);
        await ch.QueueDeclareAsync("test.traceparent", durable: false, exclusive: false, autoDelete: true, arguments: null);
        await ch.QueueBindAsync("test.traceparent", "orders.exchange", "orders.placed");

        var response = await client.PostAsJsonAsync("/api/orders", new
        {
            sku = "TRACE-SKU",
            quantity = 1,
            customerId = "33333333-3333-4333-8333-333333333333"
        });
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);

        BasicGetResult? result = null;
        for (int i = 0; i < 20 && result is null; i++)
        {
            result = await ch.BasicGetAsync("test.traceparent", autoAck: true);
            if (result is null) await Task.Delay(500);
        }

        Assert.NotNull(result);
        Assert.NotNull(result!.BasicProperties.Headers);
        Assert.True(result.BasicProperties.Headers!.TryGetValue("traceparent", out var rawTraceparent),
            "published message is missing a 'traceparent' header");

        var traceparent = rawTraceparent switch
        {
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            string s => s,
            _ => throw new Xunit.Sdk.XunitException($"unexpected traceparent header type: {rawTraceparent?.GetType()}")
        };

        Assert.Matches(new Regex("^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$"), traceparent);
    }
}
