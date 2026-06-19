using System.Net.Security;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using RabbitMQ.Client;

public class RabbitMqPublisherWorker(
    Channel<OrderPlacedEvent> channel,
    IConfiguration configuration,
    RabbitMqConnectionStatus connectionStatus,
    ILogger<RabbitMqPublisherWorker> logger) : BackgroundService
{
    private static readonly JsonSerializerOptions CamelCase = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
    private const string ExchangeName = "orders.exchange";
    private const string RoutingKey = "orders.placed";

    private static readonly TimeSpan BaseDelay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxDelay = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan MaxJitter = TimeSpan.FromSeconds(1);
    private const double BackoffMultiplier = 2.0;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var attempt = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Reset the backoff once we're actually connected, so a transient blip after a
                // long healthy run doesn't inherit a stale, maxed-out attempt count.
                await ConnectAndPublishAsync(stoppingToken, () => attempt = 0);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                connectionStatus.SetConnected(false);
                GatewayMetrics.RabbitMqConnected.Set(0);
                var delay = ReconnectBackoff.Delay(attempt, BaseDelay, BackoffMultiplier, MaxDelay, MaxJitter, Random.Shared);
                attempt++;
                logger.LogError(ex, "RabbitMQ connection lost, retrying in {Delay}", delay);
                await Task.Delay(delay, stoppingToken);
            }
        }
    }

    private async Task ConnectAndPublishAsync(CancellationToken ct, Action onConnected)
    {
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

        await using var connection = await factory.CreateConnectionAsync(ct);
        await using var rabbitChannel = await connection.CreateChannelAsync(cancellationToken: ct);
        await rabbitChannel.ExchangeDeclareAsync(ExchangeName, ExchangeType.Topic, durable: true, cancellationToken: ct);

        // Detect the broker going unresponsive (e.g. missed heartbeats) without waiting for the
        // next BasicPublishAsync to fail — the health check reads connectionStatus directly
        // rather than opening its own probe connection. AutomaticRecoveryEnabled (on by default)
        // means the same IConnection instance can silently reconnect after a shutdown without
        // ever going through ExecuteAsync's outer catch block, so RecoverySucceededAsync is the
        // only signal that flips the status back to healthy in that case.
        connection.ConnectionShutdownAsync += (_, _) =>
        {
            connectionStatus.SetConnected(false);
            GatewayMetrics.RabbitMqConnected.Set(0);
            return Task.CompletedTask;
        };
        connection.RecoverySucceededAsync += (_, _) =>
        {
            connectionStatus.SetConnected(true);
            GatewayMetrics.RabbitMqConnected.Set(1);
            return Task.CompletedTask;
        };

        logger.LogInformation("Connected to RabbitMQ at {Host}:{Port} (ssl={Ssl})", factory.HostName, factory.Port, ssl);
        connectionStatus.SetConnected(true);
        GatewayMetrics.RabbitMqConnected.Set(1);
        onConnected();

        // Deliberately CancellationToken.None, not ct: StopAsync marks the channel writer
        // complete instead of cancelling this token, so ReadAllAsync drains whatever was
        // already buffered and ends normally once empty, rather than throwing immediately and
        // abandoning orders that were already accepted (202'd to the client) but not yet
        // published. New writes stop being accepted as soon as TryComplete() runs — Writer.TryWrite
        // on a completed channel returns false, which ChannelOrderPublisher already turns into a 503.
        await foreach (var orderEvent in channel.Reader.ReadAllAsync(CancellationToken.None))
        {
            var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(orderEvent, CamelCase));
            await rabbitChannel.BasicPublishAsync(ExchangeName, RoutingKey, body, CancellationToken.None);
            if (channel.Reader.CanCount)
                GatewayMetrics.BufferUsed.Set(channel.Reader.Count);
            logger.LogInformation("Published to RabbitMQ: eventId={EventId} sku={Sku} qty={Quantity}", orderEvent.EventId, orderEvent.Sku, orderEvent.Quantity);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        var bufferedCount = channel.Reader.CanCount ? channel.Reader.Count : -1;
        logger.LogInformation("Shutdown requested: draining {BufferedCount} buffered order(s) before exit", bufferedCount);
        channel.Writer.TryComplete();
        await base.StopAsync(cancellationToken);
        logger.LogInformation("Shutdown complete: buffer drained");
    }
}
