using System.Net.Security;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using RabbitMQ.Client;

public class RabbitMqPublisherWorker(
    Channel<OrderPlacedEvent> channel,
    IConfiguration configuration,
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

        logger.LogInformation("Connected to RabbitMQ at {Host}:{Port} (ssl={Ssl})", factory.HostName, factory.Port, ssl);
        onConnected();

        await foreach (var orderEvent in channel.Reader.ReadAllAsync(ct))
        {
            var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(orderEvent, CamelCase));
            await rabbitChannel.BasicPublishAsync(ExchangeName, RoutingKey, body, ct);
            logger.LogInformation("Published to RabbitMQ: eventId={EventId} sku={Sku} qty={Quantity}", orderEvent.EventId, orderEvent.Sku, orderEvent.Quantity);
        }
    }
}
