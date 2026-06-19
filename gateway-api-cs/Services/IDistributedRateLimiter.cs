public interface IDistributedRateLimiter
{
    /// <summary>True if the request under <paramref name="key"/> is within <paramref name="permitLimit"/> for the current window.</summary>
    Task<bool> TryAcquireAsync(string key, int permitLimit, TimeSpan window);
}
