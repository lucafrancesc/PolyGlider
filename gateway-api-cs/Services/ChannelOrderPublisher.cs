using System.Threading.Channels;

public class ChannelOrderPublisher(Channel<OrderPlacedEvent> channel, IConfiguration configuration, ILogger<ChannelOrderPublisher> logger) : IOrderPublisher
{
    // 80% of the channel's 10k capacity by default -- a buffer this full means the
    // RabbitMQ publisher worker is falling behind, so callers should back off before
    // TryWrite actually starts failing rather than finding out via a hard drop.
    private readonly int _highWaterMark = configuration.GetValue("Gateway:ChannelHighWaterMark", 8_000);

    public ValueTask<PublishOutcome> PublishAsync(OrderPlacedEvent orderEvent)
    {
        if (!channel.Writer.TryWrite(orderEvent))
        {
            logger.LogWarning("Order buffer full; rejecting order for sku={Sku}", orderEvent.Sku);
            return ValueTask.FromResult(PublishOutcome.Full);
        }

        if (channel.Reader.CanCount && channel.Reader.Count >= _highWaterMark)
        {
            logger.LogWarning("Order buffer near capacity ({Count} >= {HighWaterMark}); signaling backpressure for sku={Sku}", channel.Reader.Count, _highWaterMark, orderEvent.Sku);
            return ValueTask.FromResult(PublishOutcome.NearCapacity);
        }

        if (channel.Reader.CanCount)
            GatewayMetrics.BufferUsed.Set(channel.Reader.Count);

        logger.LogDebug("Order enqueued in buffer: eventId={EventId} sku={Sku}", orderEvent.EventId, orderEvent.Sku);
        return ValueTask.FromResult(PublishOutcome.Accepted);
    }
}
