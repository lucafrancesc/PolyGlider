using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace gateway_api_cs_tests;

public class FakeDistributedRateLimiter : IDistributedRateLimiter
{
    public bool Allow { get; set; } = true;
    public string? LastKey { get; private set; }

    public Task<bool> TryAcquireAsync(string key, int permitLimit, TimeSpan window)
    {
        LastKey = key;
        return Task.FromResult(Allow);
    }
}

public class RateLimitFilterTests
{
    private (WebApplicationFactory<Program> Factory, FakeDistributedRateLimiter Limiter) BuildFactory()
    {
        var limiter = new FakeDistributedRateLimiter();
        var factory = new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
        {
            builder.ConfigureServices(services =>
            {
                services.AddSingleton<IOrderPublisher>(new FakeOrderPublisher());
                services.AddSingleton<IDistributedRateLimiter>(limiter);
                var workerDesc = services.FirstOrDefault(d => d.ImplementationType == typeof(RabbitMqPublisherWorker));
                if (workerDesc is not null) services.Remove(workerDesc);
            });
        });
        return (factory, limiter);
    }

    [Fact]
    public async Task PostOrder_RateLimiterDenies_Returns429WithRetryAfterHeader()
    {
        var (factory, limiter) = BuildFactory();
        await using var _ = factory;
        limiter.Allow = false;
        var client = factory.CreateClient();

        var response = await client.PostAsJsonAsync("/api/orders", new
        {
            sku = "TEST-001",
            quantity = 1,
            customerId = "22222222-2222-4222-8222-222222222222"
        });

        Assert.Equal(HttpStatusCode.TooManyRequests, response.StatusCode);
        Assert.True(response.Headers.Contains("Retry-After"), "Retry-After header must be present");
    }

    [Fact]
    public async Task PostOrder_RateLimiterAllows_Returns202()
    {
        var (factory, limiter) = BuildFactory();
        await using var _ = factory;
        limiter.Allow = true;
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
    public async Task PostOrder_KeysRateLimitByClientIp()
    {
        var (factory, limiter) = BuildFactory();
        await using var _ = factory;
        var client = factory.CreateClient();

        await client.PostAsJsonAsync("/api/orders", new
        {
            sku = "TEST-001",
            quantity = 1,
            customerId = "22222222-2222-4222-8222-222222222222"
        });

        Assert.NotNull(limiter.LastKey);
        Assert.StartsWith("ratelimit:orders:", limiter.LastKey);
    }
}
