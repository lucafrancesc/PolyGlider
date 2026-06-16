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

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ConnectAndPublishAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "RabbitMQ connection lost, retrying in 5s");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }

    private async Task ConnectAndPublishAsync(CancellationToken ct)
    {
        var factory = new ConnectionFactory
        {
            HostName = configuration["RabbitMQ:Host"] ?? "localhost",
            Port = int.Parse(configuration["RabbitMQ:Port"] ?? "5672"),
            UserName = configuration["RabbitMQ:User"] ?? "guest",
            Password = configuration["RabbitMQ:Password"] ?? "guest",
        };

        await using var connection = await factory.CreateConnectionAsync(ct);
        await using var rabbitChannel = await connection.CreateChannelAsync(cancellationToken: ct);
        await rabbitChannel.ExchangeDeclareAsync(ExchangeName, ExchangeType.Topic, durable: true, cancellationToken: ct);

        logger.LogInformation("Connected to RabbitMQ at {Host}:{Port}", factory.HostName, factory.Port);

        await foreach (var orderEvent in channel.Reader.ReadAllAsync(ct))
        {
            var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(orderEvent, CamelCase));
            await rabbitChannel.BasicPublishAsync(ExchangeName, RoutingKey, body, ct);
            logger.LogInformation("Published to RabbitMQ: eventId={EventId} sku={Sku} qty={Quantity}", orderEvent.EventId, orderEvent.Sku, orderEvent.Quantity);
        }
    }
}
