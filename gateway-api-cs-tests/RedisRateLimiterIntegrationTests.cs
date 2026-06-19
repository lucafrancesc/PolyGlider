using Microsoft.Extensions.Logging.Abstractions;
using StackExchange.Redis;
using Testcontainers.Redis;
using Xunit;

namespace gateway_api_cs_tests;

[Trait("Category", "Integration")]
public class RedisRateLimiterIntegrationTests : IAsyncLifetime
{
    private readonly RedisContainer _redis = new RedisBuilder().Build();
    private IConnectionMultiplexer _multiplexer = null!;

    public async Task InitializeAsync()
    {
        await _redis.StartAsync();
        _multiplexer = await ConnectionMultiplexer.ConnectAsync(_redis.GetConnectionString());
    }

    public async Task DisposeAsync()
    {
        await _multiplexer.DisposeAsync();
        await _redis.DisposeAsync();
    }

    [Fact]
    public async Task TryAcquireAsync_WithinLimit_ReturnsTrue()
    {
        var limiter = new RedisRateLimiter(_multiplexer, NullLogger<RedisRateLimiter>.Instance);

        var result = await limiter.TryAcquireAsync(Guid.NewGuid().ToString(), permitLimit: 3, TimeSpan.FromMinutes(1));

        Assert.True(result);
    }

    [Fact]
    public async Task TryAcquireAsync_OverLimit_ReturnsFalse()
    {
        var limiter = new RedisRateLimiter(_multiplexer, NullLogger<RedisRateLimiter>.Instance);
        var key = Guid.NewGuid().ToString();

        for (var i = 0; i < 3; i++)
            await limiter.TryAcquireAsync(key, permitLimit: 3, TimeSpan.FromMinutes(1));

        var result = await limiter.TryAcquireAsync(key, permitLimit: 3, TimeSpan.FromMinutes(1));

        Assert.False(result);
    }

    [Fact]
    public async Task TryAcquireAsync_DifferentKeys_AreIndependentlyLimited()
    {
        var limiter = new RedisRateLimiter(_multiplexer, NullLogger<RedisRateLimiter>.Instance);
        var keyA = Guid.NewGuid().ToString();
        var keyB = Guid.NewGuid().ToString();

        for (var i = 0; i < 3; i++)
            await limiter.TryAcquireAsync(keyA, permitLimit: 3, TimeSpan.FromMinutes(1));

        var resultA = await limiter.TryAcquireAsync(keyA, permitLimit: 3, TimeSpan.FromMinutes(1));
        var resultB = await limiter.TryAcquireAsync(keyB, permitLimit: 3, TimeSpan.FromMinutes(1));

        Assert.False(resultA);
        Assert.True(resultB);
    }

    [Fact]
    public async Task TryAcquireAsync_WindowExpires_ResetsCount()
    {
        var limiter = new RedisRateLimiter(_multiplexer, NullLogger<RedisRateLimiter>.Instance);
        var key = Guid.NewGuid().ToString();
        var window = TimeSpan.FromSeconds(1);

        for (var i = 0; i < 2; i++)
            await limiter.TryAcquireAsync(key, permitLimit: 2, window);
        Assert.False(await limiter.TryAcquireAsync(key, permitLimit: 2, window));

        await Task.Delay(TimeSpan.FromSeconds(2));

        Assert.True(await limiter.TryAcquireAsync(key, permitLimit: 2, window));
    }

    [Fact]
    public async Task TryAcquireAsync_RedisUnreachable_FailsOpen()
    {
        // A multiplexer pointed at a closed port never connects; ConnectAsync with
        // AbortOnConnectFail=false returns a (disconnected) handle instead of throwing.
        var options = ConfigurationOptions.Parse("127.0.0.1:1");
        options.AbortOnConnectFail = false;
        options.ConnectTimeout = 200;
        options.ConnectRetry = 0;
        var brokenMultiplexer = await ConnectionMultiplexer.ConnectAsync(options);
        var limiter = new RedisRateLimiter(brokenMultiplexer, NullLogger<RedisRateLimiter>.Instance);

        var result = await limiter.TryAcquireAsync("any-key", permitLimit: 1, TimeSpan.FromMinutes(1));

        Assert.True(result);
        await brokenMultiplexer.DisposeAsync();
    }
}
