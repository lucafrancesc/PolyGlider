using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace gateway_api_cs_tests;

/// <summary>
/// Exercises RabbitMqPublisherWorker against a fake adapter — proves the seam is real
/// and the domain logic (serialize, drain) is testable without a live RabbitMQ connection.
/// </summary>
public class PublisherWorkerTests
{
    private sealed class FakeAdapter : IRabbitMqPublisherAdapter
    {
        public List<(byte[] Body, string? TraceParent, string? TraceState)> Published { get; } = [];

        public Task PublishAsync(byte[] body, string? traceParent, string? traceState, CancellationToken ct)
        {
            Published.Add((body, traceParent, traceState));
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    [Fact]
    public async Task ExecuteAsync_SerializesEventToCamelCaseJsonAndPassesTraceContextToAdapter()
    {
        var ch = Channel.CreateBounded<OrderPlacedEvent>(10);
        var adapter = new FakeAdapter();
        var worker = new RabbitMqPublisherWorker(ch, adapter, NullLogger<RabbitMqPublisherWorker>.Instance);

        var eventId = Guid.NewGuid();
        var customerId = Guid.NewGuid();
        var orderEvent = new OrderPlacedEvent(eventId, "SKU-X", 5, customerId, DateTime.UtcNow)
        {
            TraceParent = "00-aabbccdd11223344aabbccdd11223344-aabbccdd11223344-01",
            TraceState = null
        };
        await ch.Writer.WriteAsync(orderEvent);
        ch.Writer.Complete();

        await worker.StartAsync(CancellationToken.None);
        await worker.StopAsync(CancellationToken.None);

        Assert.Single(adapter.Published);
        var (body, traceParent, traceState) = adapter.Published[0];

        var json = JsonDocument.Parse(Encoding.UTF8.GetString(body)).RootElement;
        Assert.Equal("SKU-X", json.GetProperty("sku").GetString());
        Assert.Equal(5, json.GetProperty("quantity").GetInt32());
        Assert.Equal(eventId.ToString(), json.GetProperty("eventId").GetString());
        Assert.Equal(customerId.ToString(), json.GetProperty("customerId").GetString());
        Assert.Equal("00-aabbccdd11223344aabbccdd11223344-aabbccdd11223344-01", traceParent);
        Assert.Null(traceState);
    }

    [Fact]
    public async Task StopAsync_CompletesChannelBeforeDrainingRemainingEvents()
    {
        var ch = Channel.CreateBounded<OrderPlacedEvent>(10);
        var adapter = new FakeAdapter();
        var worker = new RabbitMqPublisherWorker(ch, adapter, NullLogger<RabbitMqPublisherWorker>.Instance);

        await worker.StartAsync(CancellationToken.None);
        await ch.Writer.WriteAsync(new OrderPlacedEvent(Guid.NewGuid(), "SKU-Y", 1, Guid.NewGuid(), DateTime.UtcNow));
        await worker.StopAsync(CancellationToken.None);

        Assert.Single(adapter.Published);
        Assert.False(ch.Writer.TryWrite(new OrderPlacedEvent(Guid.NewGuid(), "SKU-Z", 1, Guid.NewGuid(), DateTime.UtcNow)),
            "channel writer must be complete after StopAsync");
    }

    [Fact]
    public async Task ExecuteAsync_ContinuesDrainingAfterPublishFailure()
    {
        var ch = Channel.CreateBounded<OrderPlacedEvent>(10);
        var failOnFirst = false;
        var publishedCount = 0;
        var faultyAdapter = new FaultyOnFirstAdapter(() => { failOnFirst = true; }, () => publishedCount++);
        var worker = new RabbitMqPublisherWorker(ch, faultyAdapter, NullLogger<RabbitMqPublisherWorker>.Instance);

        await ch.Writer.WriteAsync(new OrderPlacedEvent(Guid.NewGuid(), "FAIL", 1, Guid.NewGuid(), DateTime.UtcNow));
        await ch.Writer.WriteAsync(new OrderPlacedEvent(Guid.NewGuid(), "OK", 1, Guid.NewGuid(), DateTime.UtcNow));
        ch.Writer.Complete();

        await worker.StartAsync(CancellationToken.None);
        await worker.StopAsync(CancellationToken.None);

        Assert.True(failOnFirst, "first publish must have been attempted");
        Assert.Equal(1, publishedCount);
    }

    private sealed class FaultyOnFirstAdapter(Action onFirstAttempt, Action onSuccess) : IRabbitMqPublisherAdapter
    {
        private bool _firstAttemptDone;

        public Task PublishAsync(byte[] body, string? traceParent, string? traceState, CancellationToken ct)
        {
            if (!_firstAttemptDone)
            {
                _firstAttemptDone = true;
                onFirstAttempt();
                throw new InvalidOperationException("simulated publish failure");
            }
            onSuccess();
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
