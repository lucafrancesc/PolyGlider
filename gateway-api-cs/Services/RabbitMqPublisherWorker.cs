using System.Text;
using System.Text.Json;
using System.Threading.Channels;

public class RabbitMqPublisherWorker(
    Channel<OrderPlacedEvent> channel,
    IRabbitMqPublisherAdapter adapter,
    ILogger<RabbitMqPublisherWorker> logger) : BackgroundService
{
    private static readonly JsonSerializerOptions CamelCase = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Eagerly connect so the health check can report the real connection state before the
        // first publish arrives. Without this the health check stays "unreachable" forever
        // because the adapter only connects lazily inside PublishAsync.
        await adapter.EnsureConnectedAsync(stoppingToken);

        // Deliberately CancellationToken.None, not stoppingToken: StopAsync marks the channel
        // writer complete instead of cancelling this token, so ReadAllAsync drains whatever was
        // already buffered and ends normally once empty, rather than throwing immediately and
        // abandoning orders that were already accepted (202'd to the client) but not yet published.
        await foreach (var orderEvent in channel.Reader.ReadAllAsync(CancellationToken.None))
        {
            var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(orderEvent, CamelCase));
            try
            {
                await adapter.PublishAsync(body, orderEvent.TraceParent, orderEvent.TraceState, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                // A nack or unconfirmed publish is a permanent loss for this message (pre-existing
                // limitation, see #42). The adapter has already marked itself disconnected so the
                // next PublishAsync call will reconnect before attempting to publish.
                logger.LogError(ex, "Publish not confirmed by RabbitMQ: eventId={EventId} sku={Sku} qty={Quantity}", orderEvent.EventId, orderEvent.Sku, orderEvent.Quantity);
                continue;
            }
            if (channel.Reader.CanCount)
                GatewayMetrics.BufferUsed.Set(channel.Reader.Count);
            logger.LogInformation("Published to RabbitMQ: eventId={EventId} sku={Sku} qty={Quantity}", orderEvent.EventId, orderEvent.Sku, orderEvent.Quantity);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        var bufferedCount = channel.Reader.CanCount ? channel.Reader.Count : -1;
        logger.LogInformation("Shutdown requested: draining {BufferedCount} buffered order(s) before exit", bufferedCount);
        // Domain: stop accepting new work. The adapter's connection is closed by DI disposal
        // after ExecuteAsync finishes draining, preserving domain-stops-before-infra ordering.
        channel.Writer.TryComplete();
        await base.StopAsync(cancellationToken);
        logger.LogInformation("Shutdown complete: buffer drained");
    }
}
