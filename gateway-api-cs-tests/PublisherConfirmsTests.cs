using RabbitMQ.Client;
using Testcontainers.RabbitMq;
using Xunit;

namespace gateway_api_cs_tests;

[Trait("Category", "Integration")]
public class PublisherConfirmsTests : IAsyncLifetime
{
    private readonly RabbitMqContainer _rabbitMq = new RabbitMqBuilder().Build();

    public Task InitializeAsync() => _rabbitMq.StartAsync();
    public Task DisposeAsync() => _rabbitMq.DisposeAsync().AsTask();

    // Reproduces the bug fixed in RabbitMqPublisherWorker: without publisher confirms enabled,
    // BasicPublishAsync only confirms the bytes reached the local socket buffer, not that
    // RabbitMQ actually accepted the message. A queue with x-max-length=0 and
    // x-overflow=reject-publish nacks every publish at the broker — the same observable
    // behaviour as a paused/unresponsive broker from the gateway's point of view. With
    // publisher confirmations enabled and tracked, BasicPublishAsync must surface that nack by
    // throwing instead of completing as if the broker accepted it.
    [Fact]
    public async Task BasicPublishAsync_WithConfirmsEnabled_ThrowsWhenBrokerRejectsPublish()
    {
        var connFactory = new ConnectionFactory
        {
            HostName = _rabbitMq.Hostname,
            Port = _rabbitMq.GetMappedPublicPort(5672),
        };
        await using var conn = await connFactory.CreateConnectionAsync();

        var channelOptions = new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true);
        await using var ch = await conn.CreateChannelAsync(channelOptions);

        const string queue = "confirms-test-reject";
        await ch.QueueDeclareAsync(
            queue,
            durable: false,
            exclusive: false,
            autoDelete: true,
            arguments: new Dictionary<string, object?>
            {
                ["x-max-length"] = 0,
                ["x-overflow"] = "reject-publish",
            });

        await Assert.ThrowsAnyAsync<Exception>(() =>
            ch.BasicPublishAsync("", queue, "test"u8.ToArray(), CancellationToken.None).AsTask());
    }
}
