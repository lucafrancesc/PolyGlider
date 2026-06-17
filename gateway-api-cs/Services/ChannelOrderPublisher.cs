using System.Threading.Channels;

public class ChannelOrderPublisher(Channel<OrderPlacedEvent> channel, ILogger<ChannelOrderPublisher> logger) : IOrderPublisher
{
    public ValueTask<bool> PublishAsync(OrderPlacedEvent orderEvent)
    {
        if (!channel.Writer.TryWrite(orderEvent))
        {
            logger.LogWarning("Order buffer full; rejecting order for sku={Sku}", orderEvent.Sku);
            return ValueTask.FromResult(false);
        }

        logger.LogDebug("Order enqueued in buffer: eventId={EventId} sku={Sku}", orderEvent.EventId, orderEvent.Sku);
        return ValueTask.FromResult(true);
    }
}
