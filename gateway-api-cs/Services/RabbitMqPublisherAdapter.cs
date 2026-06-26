using System.Diagnostics;
using System.Net.Security;
using RabbitMQ.Client;

public class RabbitMqPublisherAdapter(
    IConfiguration configuration,
    RabbitMqConnectionStatus connectionStatus,
    ILogger<RabbitMqPublisherAdapter> logger) : IRabbitMqPublisherAdapter
{
    private static readonly TimeSpan BaseDelay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxDelay = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan MaxJitter = TimeSpan.FromSeconds(1);
    private const double BackoffMultiplier = 2.0;
    private const string ExchangeName = "orders.exchange";
    private const string RoutingKey = "orders.placed";

    private IConnection? _connection;
    private IChannel? _channel;
    private bool _connected;
    private int _attempt;

    public async Task PublishAsync(byte[] body, string? traceParent, string? traceState, CancellationToken ct)
    {
        if (!_connected)
            await ConnectAsync(ct);

        // Re-parent onto the original HTTP request's trace (captured at enqueue time, see
        // Program.cs) rather than this adapter's own ambient context, so the gateway's publish
        // span shows up as a child of the request that created the order.
        using var activity = traceParent is not null
            ? GatewayTracing.Source.StartActivity("publish orders.placed", ActivityKind.Producer,
                parentContext: ActivityContext.TryParse(traceParent, traceState, out var ctx) ? ctx : default)
            : GatewayTracing.Source.StartActivity("publish orders.placed", ActivityKind.Producer);

        var props = new BasicProperties();
        if (activity?.Id is not null)
            props.Headers = new Dictionary<string, object?> { ["traceparent"] = activity.Id };

        try
        {
            // Awaits the broker's confirm (publisherConfirmationsEnabled in ConnectAsync) before
            // returning, so this only completes once RabbitMQ has actually accepted the message.
            // CancellationToken.None: StopAsync drains the channel rather than cancelling this
            // token, so we must not interrupt an in-progress publish and abandon an already-202'd order.
            await _channel!.BasicPublishAsync(ExchangeName, RoutingKey, mandatory: false, props, body, CancellationToken.None);
        }
        catch (Exception) when (ct.IsCancellationRequested)
        {
            throw new OperationCanceledException(ct);
        }
        catch (Exception)
        {
            _connected = false;
            connectionStatus.SetConnected(false);
            GatewayMetrics.RabbitMqConnected.Set(0);
            throw;
        }
    }

    private async Task ConnectAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await DisposeConnectionAsync();

                var host = configuration["RabbitMQ:Host"] ?? "localhost";
                var ssl  = bool.TryParse(configuration["RabbitMQ:Ssl"], out var s) && s;

                var factory = new ConnectionFactory
                {
                    HostName = host,
                    Port     = ssl ? 5671 : int.Parse(configuration["RabbitMQ:Port"] ?? "5672"),
                    UserName = configuration["RabbitMQ:User"] ?? "guest",
                    Password = configuration["RabbitMQ:Password"] ?? "guest",
                    Ssl      = new SslOption
                    {
                        Enabled    = ssl,
                        ServerName = host,
                        // Allow self-signed certs in dev; remove in production and provide a trusted CA
                        AcceptablePolicyErrors = ssl
                            ? SslPolicyErrors.RemoteCertificateNameMismatch | SslPolicyErrors.RemoteCertificateChainErrors
                            : SslPolicyErrors.None
                    }
                };

                _connection = await factory.CreateConnectionAsync(ct);
                // Publisher confirms: without these, BasicPublishAsync only confirms the bytes were
                // handed to the local TCP stack, not that RabbitMQ received or routed them.
                var channelOptions = new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true);
                _channel = await _connection.CreateChannelAsync(channelOptions, cancellationToken: ct);
                await _channel.ExchangeDeclareAsync(ExchangeName, ExchangeType.Topic, durable: true, cancellationToken: ct);

                // Detect the broker going unresponsive without waiting for the next BasicPublishAsync
                // to fail — the health check reads connectionStatus directly.
                _connection.ConnectionShutdownAsync += (_, _) =>
                {
                    _connected = false;
                    connectionStatus.SetConnected(false);
                    GatewayMetrics.RabbitMqConnected.Set(0);
                    return Task.CompletedTask;
                };
                _connection.RecoverySucceededAsync += (_, _) =>
                {
                    _connected = true;
                    connectionStatus.SetConnected(true);
                    GatewayMetrics.RabbitMqConnected.Set(1);
                    return Task.CompletedTask;
                };

                logger.LogInformation("Connected to RabbitMQ at {Host}:{Port} (ssl={Ssl})", factory.HostName, factory.Port, ssl);
                connectionStatus.SetConnected(true);
                GatewayMetrics.RabbitMqConnected.Set(1);
                _connected = true;
                // Reset the backoff counter once we're actually connected, so a transient blip
                // after a long healthy run doesn't inherit a stale, maxed-out attempt count.
                _attempt = 0;
                return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _connected = false;
                connectionStatus.SetConnected(false);
                GatewayMetrics.RabbitMqConnected.Set(0);
                var delay = ReconnectBackoff.Delay(_attempt, BaseDelay, BackoffMultiplier, MaxDelay, MaxJitter, Random.Shared);
                _attempt++;
                logger.LogError(ex, "RabbitMQ connection lost, retrying in {Delay}", delay);
                await Task.Delay(delay, ct);
            }
        }
    }

    private async Task DisposeConnectionAsync()
    {
        if (_channel is not null)
        {
            await _channel.DisposeAsync();
            _channel = null;
        }
        if (_connection is not null)
        {
            await _connection.DisposeAsync();
            _connection = null;
        }
    }

    public async ValueTask DisposeAsync()
    {
        _connected = false;
        await DisposeConnectionAsync();
    }
}
