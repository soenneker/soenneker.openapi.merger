using Soenneker.Utils.File.Abstract;
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
    [Arguments(false)]
    [Arguments(true)]
    public async Task Merges_Algolia_single_item_schema_arrays(bool mixedVersions, CancellationToken token)
    {
        const string source = """
            {"openapi":"3.0.2","info":{"title":"Crawler API","version":"1"},"paths":{
              "/1/crawlers/{id}/delete_runs":{"post":{"operationId":"deleteCrawlRun",
                "parameters":[{"name":"id","in":"path","required":true,"schema":{"type":"string"}}],
                "requestBody":{"content":{"application/json":{"schema":{"type":"array","items":[{"$ref":"#/components/schemas/CrawlerLogID"}]},"example":{"items":[{"type":"literal"}]}}}},
                "responses":{"200":{"description":"OK","content":{"application/json":{"schema":{"type":"array","items":[{"$ref":"#/components/schemas/CrawlerLogID"}]}}}}}
              }}},"components":{"schemas":{"CrawlerLogID":{"type":"string","pattern":"^[0-9]+$"}}}}
            """;
        JsonObject merged = mixedVersions
            ? await MergeJson(token, ("crawler", source), ("other", """{"openapi":"3.1.0","info":{"title":"Other","version":"1"},"paths":{}}"""))
            : await MergeJson(token, ("crawler", source));
        JsonNode operation = merged["paths"]!["/crawler/1/crawlers/{id}/delete_runs"]!["post"]!;
        JsonNode request = operation["requestBody"]!["content"]!["application/json"]!;
        JsonNode response = operation["responses"]!["200"]!["content"]!["application/json"]!;
        foreach (JsonNode media in new[] { request, response })
        {
            await Assert.That(media["schema"]!["items"] is JsonObject).IsTrue();
            await Assert.That(media["schema"]!["items"]!["$ref"]!.GetValue<string>()).IsEqualTo("#/components/schemas/CrawlerLogID");
        }
        await Assert.That(merged["components"]!["schemas"]!["CrawlerLogID"]!["pattern"]!.GetValue<string>()).IsEqualTo("^[0-9]+$");
        await Assert.That(request["example"]!["items"] is JsonArray).IsTrue();
        await Reject(token, ("crawler", source.Replace("[{\"$ref\":\"#/components/schemas/CrawlerLogID\"}]", "[{\"type\":\"string\"},{\"type\":\"integer\"}]", StringComparison.Ordinal)));
    }

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
            await _fileUtil.Write(Path.Combine(directory, "api.json"), source.ToJsonString(), cancellationToken: token);
            await _fileUtil.Write(Path.Combine(directory, "numbers.json"), """
                {"openapi":"3.0.3","info":{"title":"Shared","version":"1"},"components":{"schemas":{"Number":{"type":"string","pattern":"^[0-9]+$"}}}}
                """, cancellationToken: token);
            JsonNode merged = JsonNode.Parse(_util.ToJson(await _util.MergeDirectory(directory, token)))!;
            await Assert.That(merged["paths"]!.AsObject().Count).IsEqualTo(1);
            await Assert.That(merged["components"]!["schemas"]!["Number"]!["pattern"]!.GetValue<string>()).IsEqualTo("^[0-9]+$");
            await Assert.That(merged["paths"]!["/api/items"]!["get"]!["responses"]!["200"]!["content"]!["application/json"]!["schema"]!["$ref"]!.GetValue<string>())
                .IsEqualTo("#/components/schemas/Number");
        }
        finally { Directory.Delete(directory, true); }
    }
}
