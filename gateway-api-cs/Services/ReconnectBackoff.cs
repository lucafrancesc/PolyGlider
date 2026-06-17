/// <summary>
/// Pure delay calculation for RabbitMQ reconnect attempts: exponential backoff capped at
/// <paramref name="maxDelay"/>, plus a random jitter so multiple gateway instances reconnecting
/// after a broker restart don't all retry in lockstep (thundering herd).
/// </summary>
public static class ReconnectBackoff
{
    public static TimeSpan Delay(int attempt, TimeSpan baseDelay, double multiplier, TimeSpan maxDelay, TimeSpan maxJitter, Random random)
    {
        var exponentialMs = baseDelay.TotalMilliseconds * Math.Pow(multiplier, attempt);
        var cappedMs = Math.Min(exponentialMs, maxDelay.TotalMilliseconds);
        var jitterMs = random.NextDouble() * maxJitter.TotalMilliseconds;
        return TimeSpan.FromMilliseconds(cappedMs + jitterMs);
    }
}
