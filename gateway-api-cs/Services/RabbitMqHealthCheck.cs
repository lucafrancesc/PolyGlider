using Microsoft.Extensions.Diagnostics.HealthChecks;
using System.Threading.Channels;

/// <summary>Read-only view of whether RabbitMqPublisherWorker's actual publish connection is up.</summary>
public interface IRabbitMqConnectionStatus
{
    bool IsConnected { get; }
}

/// <summary>
/// Set by RabbitMqPublisherWorker as its connection comes up/down. A plain volatile bool is enough —
/// there's a single writer (the worker's reconnect loop) and any number of readers (health checks).
/// </summary>
public class RabbitMqConnectionStatus : IRabbitMqConnectionStatus
{
    private volatile bool _isConnected;
    public bool IsConnected => _isConnected;
    public void SetConnected(bool connected) => _isConnected = connected;
}

public class RabbitMqHealthCheck(IRabbitMqConnectionStatus status, Channel<OrderPlacedEvent> ch) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken ct = default)
    {
        var bufferUsed = ch.Reader.CanCount ? ch.Reader.Count : 0;
        var data = new Dictionary<string, object> { ["bufferUsed"] = bufferUsed };

        var result = status.IsConnected
            ? HealthCheckResult.Healthy("ok", data)
            : HealthCheckResult.Unhealthy("unreachable", data: data);

        return Task.FromResult(result);
    }
}
