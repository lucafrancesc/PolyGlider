public class ApiKeyFilter(IConfiguration config) : IEndpointFilter
{
    private const string HeaderName = "X-Api-Key";

    public async ValueTask<object?> InvokeAsync(EndpointFilterInvocationContext ctx, EndpointFilterDelegate next)
    {
        var configuredKey = config["Gateway:ApiKey"];

        // Auth is disabled when no key is configured (dev / test default)
        if (string.IsNullOrEmpty(configuredKey))
            return await next(ctx);

        if (!ctx.HttpContext.Request.Headers.TryGetValue(HeaderName, out var provided) || provided != configuredKey)
            return Results.Unauthorized();

        return await next(ctx);
    }
}
