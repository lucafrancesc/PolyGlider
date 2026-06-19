using StackExchange.Redis;

/// <summary>
/// Fixed-window limiter shared across all gateway replicas, backed by Redis. Hand-rolled via a
/// Lua EVAL (atomic INCR + conditional EXPIRE) rather than a framework like AspNetCoreRateLimit,
/// matching this project's style of implementing its own resilience primitives elsewhere (the
/// Scala engine's circuit breaker and retry policy) instead of hiding the mechanism behind one.
/// </summary>
public class RedisRateLimiter(IConnectionMultiplexer redis, ILogger<RedisRateLimiter> logger) : IDistributedRateLimiter
{
    // INCR returns the post-increment count; EXPIRE is only set on the first increment in a
    // window so later calls don't keep pushing the TTL out. Both run atomically in one round trip.
    private const string Script = """
        local current = redis.call('INCR', KEYS[1])
        if current == 1 then
            redis.call('EXPIRE', KEYS[1], ARGV[1])
        end
        return current
        """;

    public async Task<bool> TryAcquireAsync(string key, int permitLimit, TimeSpan window)
    {
        try
        {
            var db = redis.GetDatabase();
            var current = (long)await db.ScriptEvaluateAsync(Script, [key], [(long)window.TotalSeconds]);
            return current <= permitLimit;
        }
        catch (Exception ex)
        {
            // Fail open: a rate-limiter outage shouldn't take down the whole API.
            logger.LogWarning(ex, "Redis rate limiter unavailable, allowing request through: key={Key}", key);
            return true;
        }
    }
}
