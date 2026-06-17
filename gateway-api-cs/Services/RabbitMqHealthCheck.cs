using Microsoft.Extensions.Diagnostics.HealthChecks;
using RabbitMQ.Client;
using System.Threading.Channels;

public interface IRabbitMqProbe
{
    Task<bool> IsReachableAsync(CancellationToken ct = default);
}

public class RabbitMqProbe(IConfiguration configuration) : IRabbitMqProbe
{
    public async Task<bool> IsReachableAsync(CancellationToken ct = default)
    {
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeSpan.FromSeconds(3));
            var factory = new ConnectionFactory
            {
                HostName = configuration["RabbitMQ:Host"] ?? "localhost",
                Port = int.Parse(configuration["RabbitMQ:Port"] ?? "5672"),
                UserName = configuration["RabbitMQ:User"] ?? "guest",
                Password = configuration["RabbitMQ:Password"] ?? "guest",
            };
            await using var conn = await factory.CreateConnectionAsync(cts.Token);
            return true;
        }
        catch
        {
            return false;
        }
    }
}

public class RabbitMqHealthCheck(IRabbitMqProbe probe, Channel<OrderPlacedEvent> ch) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken ct = default)
    {
        var bufferUsed = ch.Reader.CanCount ? ch.Reader.Count : 0;
        var data = new Dictionary<string, object> { ["bufferUsed"] = bufferUsed };

        if (await probe.IsReachableAsync(ct))
            return HealthCheckResult.Healthy("ok", data);

        return HealthCheckResult.Unhealthy("unreachable", data: data);
    }
}
