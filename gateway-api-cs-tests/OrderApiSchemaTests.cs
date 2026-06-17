using System.Text.Json.Nodes;
using Json.Schema;
using Xunit;

namespace gateway_api_cs_tests;

/// <summary>
/// Validates the HTTP API request and response shapes against the JSON Schema
/// defined in contracts/order-api.json.
/// </summary>
public class OrderApiSchemaTests
{
    private static readonly string SchemaFilePath = Path.GetFullPath(Path.Combine(
        AppContext.BaseDirectory,
        "..", "..", "..", "..",
        "contracts", "order-api.json"));

    private static readonly EvaluationOptions Options = new()
    {
        OutputFormat = OutputFormat.List
    };

    // Extract a named definition from the schema file as a standalone schema.
    private static JsonSchema GetDefinition(string name)
    {
        var raw = File.ReadAllText(SchemaFilePath);
        var doc = System.Text.Json.JsonDocument.Parse(raw).RootElement;
        var defJson = doc.GetProperty("$defs").GetProperty(name).GetRawText();
        return JsonSchema.FromText(defJson);
    }

    private static readonly JsonSchema RequestSchema = GetDefinition("OrderRequest");
    private static readonly JsonSchema ResponseSchema = GetDefinition("OrderResponse");

    // ── OrderRequest ──────────────────────────────────────────────────────────

    [Fact]
    [Trait("Category", "Contract")]
    public void ValidOrderRequest_PassesSchema()
    {
        var body = JsonNode.Parse("""
            {
              "sku": "TEST-001",
              "quantity": 3,
              "customerId": "22222222-2222-4222-8222-222222222222"
            }
            """)!;

        var result = RequestSchema.Evaluate(body, Options);
        Assert.True(result.IsValid, Describe(result));
    }

    [Fact]
    [Trait("Category", "Contract")]
    public void OrderRequest_EmptySku_FailsSchema()
    {
        var body = JsonNode.Parse("""
            {
              "sku": "",
              "quantity": 3,
              "customerId": "22222222-2222-4222-8222-222222222222"
            }
            """)!;

        var result = RequestSchema.Evaluate(body, Options);
        Assert.False(result.IsValid, "Expected schema to reject empty sku");
    }

    [Fact]
    [Trait("Category", "Contract")]
    public void OrderRequest_SkuTooLong_FailsSchema()
    {
        var body = new JsonObject
        {
            ["sku"] = new string('A', 101),
            ["quantity"] = 3,
            ["customerId"] = "22222222-2222-4222-8222-222222222222"
        };

        var result = RequestSchema.Evaluate(body, Options);
        Assert.False(result.IsValid, "Expected schema to reject sku longer than 100 characters");
    }

    [Fact]
    [Trait("Category", "Contract")]
    public void OrderRequest_SkuAtMaxLength_PassesSchema()
    {
        var body = new JsonObject
        {
            ["sku"] = new string('A', 100),
            ["quantity"] = 3,
            ["customerId"] = "22222222-2222-4222-8222-222222222222"
        };

        var result = RequestSchema.Evaluate(body, Options);
        Assert.True(result.IsValid, Describe(result));
    }

    [Fact]
    [Trait("Category", "Contract")]
    public void OrderRequest_ZeroQuantity_FailsSchema()
    {
        var body = JsonNode.Parse("""
            {
              "sku": "TEST-001",
              "quantity": 0,
              "customerId": "22222222-2222-4222-8222-222222222222"
            }
            """)!;

        var result = RequestSchema.Evaluate(body, Options);
        Assert.False(result.IsValid, "Expected schema to reject zero quantity");
    }

    [Fact]
    [Trait("Category", "Contract")]
    public void OrderRequest_NegativeQuantity_FailsSchema()
    {
        var body = JsonNode.Parse("""
            {
              "sku": "TEST-001",
              "quantity": -1,
              "customerId": "22222222-2222-4222-8222-222222222222"
            }
            """)!;

        var result = RequestSchema.Evaluate(body, Options);
        Assert.False(result.IsValid, "Expected schema to reject negative quantity");
    }

    [Fact]
    [Trait("Category", "Contract")]
    public void OrderRequest_MissingSku_FailsSchema()
    {
        var body = JsonNode.Parse("""
            { "quantity": 1, "customerId": "22222222-2222-4222-8222-222222222222" }
            """)!;

        var result = RequestSchema.Evaluate(body, Options);
        Assert.False(result.IsValid, "Expected schema to reject missing sku");
    }

    // ── OrderResponse ─────────────────────────────────────────────────────────

    [Fact]
    [Trait("Category", "Contract")]
    public void ValidOrderResponse_PassesSchema()
    {
        var body = JsonNode.Parse("""
            {
              "message": "Order queued successfully",
              "eventId": "11111111-1111-4111-8111-111111111111"
            }
            """)!;

        var result = ResponseSchema.Evaluate(body, Options);
        Assert.True(result.IsValid, Describe(result));
    }

    [Fact]
    [Trait("Category", "Contract")]
    public void OrderResponse_MissingEventId_FailsSchema()
    {
        var body = JsonNode.Parse("""{ "message": "Order queued successfully" }""")!;

        var result = ResponseSchema.Evaluate(body, Options);
        Assert.False(result.IsValid, "Expected schema to reject missing eventId");
    }

    [Fact]
    [Trait("Category", "Contract")]
    public void OrderResponse_MissingMessage_FailsSchema()
    {
        var body = JsonNode.Parse("""{ "eventId": "11111111-1111-4111-8111-111111111111" }""")!;

        var result = ResponseSchema.Evaluate(body, Options);
        Assert.False(result.IsValid, "Expected schema to reject missing message");
    }

    private static string Describe(EvaluationResults result) =>
        string.Join("; ", result.Details
            .Where(d => !d.IsValid && d.Errors is not null)
            .SelectMany(d => d.Errors!.Select(e => $"{d.InstanceLocation}: {e.Key}={e.Value}")));
}
