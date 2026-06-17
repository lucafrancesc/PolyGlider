using Xunit;

namespace gateway_api_cs_tests;

public class ReconnectBackoffTests
{
    private static readonly TimeSpan BaseDelay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxDelay = TimeSpan.FromSeconds(30);

    [Fact]
    public void Delay_GrowsExponentiallyWithAttempt()
    {
        var noJitter = TimeSpan.Zero;
        var random = new Random(0);

        var attempt0 = ReconnectBackoff.Delay(0, BaseDelay, 2.0, MaxDelay, noJitter, random);
        var attempt1 = ReconnectBackoff.Delay(1, BaseDelay, 2.0, MaxDelay, noJitter, random);
        var attempt2 = ReconnectBackoff.Delay(2, BaseDelay, 2.0, MaxDelay, noJitter, random);

        Assert.Equal(TimeSpan.FromSeconds(1), attempt0);
        Assert.Equal(TimeSpan.FromSeconds(2), attempt1);
        Assert.Equal(TimeSpan.FromSeconds(4), attempt2);
    }

    [Fact]
    public void Delay_IsCappedAtMaxDelay()
    {
        var noJitter = TimeSpan.Zero;
        var random = new Random(0);

        var farOutAttempt = ReconnectBackoff.Delay(20, BaseDelay, 2.0, MaxDelay, noJitter, random);

        Assert.Equal(MaxDelay, farOutAttempt);
    }

    [Fact]
    public void Delay_AddsJitterWithinBounds()
    {
        var maxJitter = TimeSpan.FromSeconds(1);
        var random = new Random(42);

        for (var i = 0; i < 100; i++)
        {
            var delay = ReconnectBackoff.Delay(0, BaseDelay, 2.0, MaxDelay, maxJitter, random);
            Assert.InRange(delay.TotalMilliseconds, BaseDelay.TotalMilliseconds, BaseDelay.TotalMilliseconds + maxJitter.TotalMilliseconds);
        }
    }

    [Fact]
    public void Delay_DifferentCallersDontAlwaysProduceTheSameJitter()
    {
        var maxJitter = TimeSpan.FromSeconds(1);
        var random = new Random(7);

        var delays = Enumerable.Range(0, 20)
            .Select(_ => ReconnectBackoff.Delay(0, BaseDelay, 2.0, MaxDelay, maxJitter, random))
            .Distinct()
            .Count();

        Assert.True(delays > 1, "jitter should produce varying delays across calls");
    }
}
