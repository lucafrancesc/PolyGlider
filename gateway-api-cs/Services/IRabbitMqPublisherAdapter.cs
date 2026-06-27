public interface IRabbitMqPublisherAdapter : IAsyncDisposable
{
    /// <summary>
    /// Establishes (or re-establishes) the RabbitMQ connection, retrying with backoff until
    /// the connection is up or <paramref name="ct"/> is cancelled. Call this eagerly at startup
    /// so the health check can report the real connection state before the first publish.
    /// </summary>
    Task EnsureConnectedAsync(CancellationToken ct);

    /// <summary>
    /// Publishes <paramref name="body"/> to RabbitMQ with publisher confirms and W3C
    /// trace-header injection. Reconnects with backoff if the connection is not established.
    /// Throws on publish failure (nack / unconfirmed) so the caller can log and skip the message.
    /// </summary>
    Task PublishAsync(byte[] body, string? traceParent, string? traceState, CancellationToken ct);
}
