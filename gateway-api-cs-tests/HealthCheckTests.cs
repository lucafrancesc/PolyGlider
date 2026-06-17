using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace gateway_api_cs_tests;

public class FakeRabbitMqProbe(bool isReachable) : IRabbitMqProbe
{
    public Task<bool> IsReachableAsync(CancellationToken ct = default) => Task.FromResult(isReachable);
}

public class HealthCheckTests
{
    private WebApplicationFactory<Program> BuildFactory(bool rabbitReachable, string? apiKey = null) =>
        new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
        {
            if (apiKey is not null)
                builder.UseSetting("Gateway:ApiKey", apiKey);
            builder.ConfigureServices(services =>
            {
                services.AddSingleton<IRabbitMqProbe>(new FakeRabbitMqProbe(rabbitReachable));
                var workerDesc = services.FirstOrDefault(d => d.ImplementationType == typeof(RabbitMqPublisherWorker));
                if (workerDesc is not null) services.Remove(workerDesc);
            });
        });

    [Fact]
    public async Task Health_WhenRabbitReachable_Returns200WithHealthyStatus()
    {
        await using var factory = BuildFactory(rabbitReachable: true);
        var response = await factory.CreateClient().GetAsync("/health");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("healthy", body.GetProperty("status").GetString());
        Assert.Equal("ok", body.GetProperty("rabbitmq").GetString());
    }

    [Fact]
    public async Task Health_WhenRabbitUnreachable_Returns503WithUnhealthyStatus()
    {
        await using var factory = BuildFactory(rabbitReachable: false);
        var response = await factory.CreateClient().GetAsync("/health");

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("unhealthy", body.GetProperty("status").GetString());
        Assert.Equal("unreachable", body.GetProperty("rabbitmq").GetString());
    }

    [Fact]
    public async Task Health_WhenRabbitReachable_IncludesBufferUsed()
    {
        await using var factory = BuildFactory(rabbitReachable: true);
        var response = await factory.CreateClient().GetAsync("/health");

        var body = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.True(body.TryGetProperty("bufferUsed", out _), "bufferUsed field must be present");
    }

    [Fact]
    public async Task Health_IsNotGatedByApiKey()
    {
        await using var factory = BuildFactory(rabbitReachable: true, apiKey: "secret");
        // No X-Api-Key header — health must still respond (it's outside the auth filter)
        var response = await factory.CreateClient().GetAsync("/health");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }
}
