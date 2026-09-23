using System;
using System.IO;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Merger.Tests;

public sealed partial class OpenApiMergerTests
{
    [Test]
    public async Task Duplicate_source_operation_ids_are_disambiguated_unless_a_link_is_ambiguous(CancellationToken token)
    {
        JsonObject source = JsonNode.Parse(Minimal)!.AsObject();
        source["paths"]!["/other"] = source["paths"]!["/items"]!.DeepClone();
        JsonObject merged = await MergeJson(token, ("api", source.ToJsonString()));
        string[] ids = merged["paths"]!.AsObject().Select(p => p.Value!["get"]!["operationId"]!.GetValue<string>()).ToArray();
        await Assert.That(ids.Distinct().Count()).IsEqualTo(2);
        source["paths"]!["/items"]!["get"]!["responses"]!["200"]!["links"] = JsonNode.Parse("""{"next":{"operationId":"list"}}""");
        await Reject(token, ("api", source.ToJsonString()));
    }

    [Test]
    public async Task Merges_component_only_documents_and_converted_yaml_references(CancellationToken token)
    {
        string directory = Path.Combine(Path.GetTempPath(), "merger-converted-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            JsonObject source = JsonNode.Parse(Minimal)!.AsObject();
            source["paths"]!["/items"]!["get"]!["responses"]!["200"]!["content"] =
                JsonNode.Parse("""{"application/json":{"schema":{"$ref":"numbers.yml#/components/schemas/Number"}}}""");
            await File.WriteAllTextAsync(Path.Combine(directory, "api.json"), source.ToJsonString(), token);
            await File.WriteAllTextAsync(Path.Combine(directory, "numbers.json"), """
                {"openapi":"3.0.3","info":{"title":"Shared","version":"1"},"components":{"schemas":{"Number":{"type":"string","pattern":"^[0-9]+$"}}}}
                """, token);
            JsonNode merged = JsonNode.Parse(_util.ToJson(await _util.MergeDirectory(directory, token)))!;
            await Assert.That(merged["paths"]!.AsObject().Count).IsEqualTo(1);
            await Assert.That(merged["components"]!["schemas"]!["Number"]!["pattern"]!.GetValue<string>()).IsEqualTo("^[0-9]+$");
            await Assert.That(merged["paths"]!["/api/items"]!["get"]!["responses"]!["200"]!["content"]!["application/json"]!["schema"]!["$ref"]!.GetValue<string>())
                .IsEqualTo("#/components/schemas/Number");
        }
        finally { Directory.Delete(directory, true); }
    }
}
