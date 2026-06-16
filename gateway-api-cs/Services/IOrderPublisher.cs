public interface IOrderPublisher
{
    ValueTask PublishAsync(OrderPlacedEvent orderEvent);
}
