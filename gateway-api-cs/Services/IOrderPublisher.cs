public enum PublishOutcome
{
    Accepted,
    NearCapacity,
    Full
}

public interface IOrderPublisher
{
    /// <summary>Queues the event for publishing. See <see cref="PublishOutcome"/> for the possible results.</summary>
    ValueTask<PublishOutcome> PublishAsync(OrderPlacedEvent orderEvent);
}
