public interface IRabbitMqPublisherAdapter : IAsyncDisposable
{
    /// <summary>
    /// Publishes <paramref name="body"/> to RabbitMQ with publisher confirms and W3C
    /// trace-header injection. Reconnects with backoff if the connection is not established.
    /// Throws on publish failure (nack / unconfirmed) so the caller can log and skip the message.
    /// </summary>
    Task PublishAsync(byte[] body, string? traceParent, string? traceState, CancellationToken ct);
}
