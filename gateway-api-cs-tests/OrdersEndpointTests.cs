using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace gateway_api_cs_tests;

public class FakeOrderPublisher : IOrderPublisher
{
    public List<OrderPlacedEvent> Published { get; } = [];
    public ValueTask PublishAsync(OrderPlacedEvent orderEvent)
    {
        Published.Add(orderEvent);
        return ValueTask.CompletedTask;
    }
}

public class OrdersEndpointTests
{
    private WebApplicationFactory<Program> BuildFactory(FakeOrderPublisher fake) =>
        new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
            builder.ConfigureServices(services =>
            {
                services.AddSingleton<IOrderPublisher>(fake);
                var workerDesc = services.FirstOrDefault(d => d.ImplementationType == typeof(RabbitMqPublisherWorker));
                if (workerDesc is not null) services.Remove(workerDesc);
            }));

    [Fact]
    public async Task PostOrder_ValidRequest_Returns202WithEventId()
    {
        var fake = new FakeOrderPublisher();
        await using var factory = BuildFactory(fake);
        var client = factory.CreateClient();

        var response = await client.PostAsJsonAsync("/api/orders", new
        {
            sku = "TEST-001",
            quantity = 3,
            customerId = "22222222-2222-4222-8222-222222222222"
        });

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("Order queued successfully", body.GetProperty("message").GetString());
        Assert.True(Guid.TryParse(body.GetProperty("eventId").GetString(), out _));
    }

    [Fact]
    public async Task PostOrder_ValidRequest_PublishesEventWithCorrectFields()
    {
        var fake = new FakeOrderPublisher();
        await using var factory = BuildFactory(fake);
        var client = factory.CreateClient();

        var customerId = Guid.Parse("22222222-2222-4222-8222-222222222222");
        await client.PostAsJsonAsync("/api/orders", new
        {
            sku = "SKU-XYZ",
            quantity = 5,
            customerId = customerId.ToString()
        });

        var ev = Assert.Single(fake.Published);
        Assert.Equal("SKU-XYZ", ev.Sku);
        Assert.Equal(5, ev.Quantity);
        Assert.Equal(customerId, ev.CustomerId);
        Assert.NotEqual(Guid.Empty, ev.EventId);
    }

    [Fact]
    public async Task PostOrder_MissingSku_Returns400()
    {
        var fake = new FakeOrderPublisher();
        await using var factory = BuildFactory(fake);
        var client = factory.CreateClient();

        var response = await client.PostAsJsonAsync("/api/orders", new
        {
            sku = "",
            quantity = 1,
            customerId = "22222222-2222-4222-8222-222222222222"
        });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Empty(fake.Published);
    }

    [Fact]
    public async Task PostOrder_ZeroQuantity_Returns400()
    {
        var fake = new FakeOrderPublisher();
        await using var factory = BuildFactory(fake);
        var client = factory.CreateClient();

        var response = await client.PostAsJsonAsync("/api/orders", new
        {
            sku = "TEST-001",
            quantity = 0,
            customerId = "22222222-2222-4222-8222-222222222222"
        });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Empty(fake.Published);
    }

    [Fact]
    public async Task PostOrder_NegativeQuantity_Returns400()
    {
        var fake = new FakeOrderPublisher();
        await using var factory = BuildFactory(fake);
        var client = factory.CreateClient();

        var response = await client.PostAsJsonAsync("/api/orders", new
        {
            sku = "TEST-001",
            quantity = -1,
            customerId = "22222222-2222-4222-8222-222222222222"
        });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }
}
