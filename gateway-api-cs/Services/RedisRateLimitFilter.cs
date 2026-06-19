public class RedisRateLimitFilter(IDistributedRateLimiter limiter, IConfiguration config) : IEndpointFilter
{
    private static readonly TimeSpan Window = TimeSpan.FromMinutes(1);

    public async ValueTask<object?> InvokeAsync(EndpointFilterInvocationContext ctx, EndpointFilterDelegate next)
    {
        var permitLimit = config.GetValue("Gateway:RateLimitPerMinute", 100);

        // Keyed by client IP (set by ForwardedHeaders middleware from nginx's X-Forwarded-For)
        // rather than per-replica, so the limit is global across however many gateway
        // replicas exist instead of multiplying by replica count.
        var clientId = ctx.HttpContext.Connection.RemoteIpAddress?.ToString() ?? "unknown";
        var allowed = await limiter.TryAcquireAsync($"ratelimit:orders:{clientId}", permitLimit, Window);
        if (!allowed)
        {
            GatewayMetrics.OrdersRejected.WithLabels("rate_limited").Inc();
            ctx.HttpContext.Response.Headers.RetryAfter = ((int)Window.TotalSeconds).ToString();
            return Results.StatusCode(429);
        }

        return await next(ctx);
    }
}
