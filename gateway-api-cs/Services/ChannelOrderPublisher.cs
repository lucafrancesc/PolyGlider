using System.Threading.Channels;

public class ChannelOrderPublisher(Channel<OrderPlacedEvent> channel, ILogger<ChannelOrderPublisher> logger) : IOrderPublisher
{
    public ValueTask PublishAsync(OrderPlacedEvent orderEvent)
    {
        if (!channel.Writer.TryWrite(orderEvent))
            logger.LogWarning("Order buffer full; dropping message for sku={Sku}", orderEvent.Sku);
        return ValueTask.CompletedTask;
    }
}
