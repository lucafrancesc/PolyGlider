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
    public bool AcceptsWrites { get; set; } = true;
    public bool NearCapacity { get; set; }

    public ValueTask<PublishOutcome> PublishAsync(OrderPlacedEvent orderEvent)
    {
        if (!AcceptsWrites) return ValueTask.FromResult(PublishOutcome.Full);
        Published.Add(orderEvent);
        return ValueTask.FromResult(NearCapacity ? PublishOutcome.NearCapacity : PublishOutcome.Accepted);
    }
}

public class OrdersEndpointTests
{
    private WebApplicationFactory<Program> BuildFactory(FakeOrderPublisher fake, string? apiKey = null) =>
        new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
        {
            if (apiKey is not null)
                builder.UseSetting("Gateway:ApiKey", apiKey);
            builder.ConfigureServices(services =>
            {
                services.AddSingleton<IOrderPublisher>(fake);
                var workerDesc = services.FirstOrDefault(d => d.ImplementationType == typeof(RabbitMqPublisherWorker));
                if (workerDesc is not null) services.Remove(workerDesc);
            });
        });

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
    public async Task PostOrder_SkuTooLong_Returns400()
    {
        var fake = new FakeOrderPublisher();
        await using var factory = BuildFactory(fake);
        var client = factory.CreateClient();

        var response = await client.PostAsJsonAsync("/api/orders", new
        {
            sku = new string('A', 101),
            quantity = 1,
            customerId = "22222222-2222-4222-8222-222222222222"
        });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Empty(fake.Published);
    }

    [Fact]
    public async Task PostOrder_SkuAtMaxLength_Succeeds()
    {
        var fake = new FakeOrderPublisher();
        await using var factory = BuildFactory(fake);
        var client = factory.CreateClient();

        var response = await client.PostAsJsonAsync("/api/orders", new
        {
            sku = new string('A', 100),
            quantity = 1,
            customerId = "22222222-2222-4222-8222-222222222222"
        });

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        Assert.Single(fake.Published);
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

    [Fact]
    public async Task PostOrder_BufferFull_Returns429WithRetryAfterHeader()
    {
        var fake = new FakeOrderPublisher { AcceptsWrites = false };
        await using var factory = BuildFactory(fake);
        var client = factory.CreateClient();

        var response = await client.PostAsJsonAsync("/api/orders", new
        {
            sku = "TEST-001",
            quantity = 1,
            customerId = "22222222-2222-4222-8222-222222222222"
        });

        Assert.Equal(HttpStatusCode.TooManyRequests, response.StatusCode);
        Assert.True(response.Headers.Contains("Retry-After"), "Retry-After header must be present");
        Assert.Empty(fake.Published);
    }

    [Fact]
    public async Task PostOrder_BufferNearCapacity_Returns429WithRetryAfterHeader()
    {
        var fake = new FakeOrderPublisher { NearCapacity = true };
        await using var factory = BuildFactory(fake);
        var client = factory.CreateClient();

        var response = await client.PostAsJsonAsync("/api/orders", new
        {
            sku = "TEST-001",
            quantity = 1,
            customerId = "22222222-2222-4222-8222-222222222222"
        });

        Assert.Equal(HttpStatusCode.TooManyRequests, response.StatusCode);
        Assert.True(response.Headers.Contains("Retry-After"), "Retry-After header must be present");
        // Near-capacity is a warning, not a rejection: the order was still enqueued.
        Assert.Single(fake.Published);
    }

    [Fact]
    public async Task PostOrder_NoApiKeyConfigured_Returns202()
    {
        // Auth is disabled when Gateway:ApiKey is not set — requests go through without a header
        var fake = new FakeOrderPublisher();
        await using var factory = BuildFactory(fake);
        var client = factory.CreateClient();

        var response = await client.PostAsJsonAsync("/api/orders", new
        {
            sku = "TEST-001",
            quantity = 1,
            customerId = "22222222-2222-4222-8222-222222222222"
        });

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
    }

    [Fact]
    public async Task PostOrder_MissingApiKey_Returns401()
    {
        var fake = new FakeOrderPublisher();
        await using var factory = BuildFactory(fake, apiKey: "secret");
        var client = factory.CreateClient();

        var response = await client.PostAsJsonAsync("/api/orders", new
        {
            sku = "TEST-001",
            quantity = 1,
            customerId = "22222222-2222-4222-8222-222222222222"
        });

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
        Assert.Empty(fake.Published);
    }

    [Fact]
    public async Task PostOrder_WrongApiKey_Returns401()
    {
        var fake = new FakeOrderPublisher();
        await using var factory = BuildFactory(fake, apiKey: "secret");
        var client = factory.CreateClient();
        client.DefaultRequestHeaders.Add("X-Api-Key", "wrong");

        var response = await client.PostAsJsonAsync("/api/orders", new
        {
            sku = "TEST-001",
            quantity = 1,
            customerId = "22222222-2222-4222-8222-222222222222"
        });

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
        Assert.Empty(fake.Published);
    }

    [Fact]
    public async Task PostOrder_CorrectApiKey_Returns202()
    {
        var fake = new FakeOrderPublisher();
        await using var factory = BuildFactory(fake, apiKey: "secret");
        var client = factory.CreateClient();
        client.DefaultRequestHeaders.Add("X-Api-Key", "secret");

        var response = await client.PostAsJsonAsync("/api/orders", new
        {
            sku = "TEST-001",
            quantity = 1,
            customerId = "22222222-2222-4222-8222-222222222222"
        });

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        Assert.Single(fake.Published);
    }
}
