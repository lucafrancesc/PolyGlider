using System.Diagnostics;

/// <summary>
/// ActivitySource for manual spans outside what OpenTelemetry.Instrumentation.AspNetCore
/// covers automatically (the request span). Must be registered via AddSource(Name) in
/// Program.cs's TracerProviderBuilder, otherwise activities created here are dropped before
/// export.
/// </summary>
public static class GatewayTracing
{
    public const string SourceName = "PolyGlider.Gateway";
    public static readonly ActivitySource Source = new(SourceName);
}
