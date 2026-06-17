public interface IOrderPublisher
{
    /// <summary>Returns true if the event was queued, false if the buffer is full.</summary>
    ValueTask<bool> PublishAsync(OrderPlacedEvent orderEvent);
}
