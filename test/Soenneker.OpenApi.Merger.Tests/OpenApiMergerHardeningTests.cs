using System;
using System.IO;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.OpenApi;

namespace Soenneker.OpenApi.Merger.Tests;

public sealed partial class OpenApiMergerTests
{
    private const string Minimal = """
        {"openapi":"3.0.3","info":{"title":"API","version":"1"},"paths":{
          "/items":{"get":{"operationId":"list","responses":{"200":{"description":"OK"}}}}
        }}
        """;

    private async Task<JsonObject> MergeJson(CancellationToken token, params (string Prefix, string Json)[] inputs)
    {
        string directory = Path.Combine(Path.GetTempPath(), "openapi-merger-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            var files = new (string, string)[inputs.Length];
            for (int i = 0; i < inputs.Length; i++)
            {
                string path = Path.Combine(directory, i + ".json");
                await File.WriteAllTextAsync(path, inputs[i].Json, token);
                files[i] = (inputs[i].Prefix, path);
            }
            return JsonNode.Parse(_util.ToJson(await _util.MergeOpenApis(files, token)))!.AsObject();
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private async Task Reject(CancellationToken token, params (string Prefix, string Json)[] inputs)
    {
        bool rejected = false;
        try { await MergeJson(token, inputs); }
        catch (InvalidOperationException) { rejected = true; }
        await Assert.That(rejected).IsTrue();
    }

    [Test]
    public async Task Preserves_inherited_security_servers_and_explicit_anonymous_access(CancellationToken token)
    {
        JsonObject first = JsonNode.Parse(Minimal)!.AsObject();
        first["security"] = JsonNode.Parse("""[{"key":[]}]""");
        first["servers"] = JsonNode.Parse("""[{"url":"https://first.example/v1"}]""");
        first["components"] = JsonNode.Parse("""{"securitySchemes":{"key":{"type":"apiKey","in":"header","name":"X-Key"}}}""");
        first["paths"]!["/public"] = JsonNode.Parse("""{"get":{"security":[],"responses":{"200":{"description":"OK"}}}}""");
        JsonObject second = (JsonObject)first.DeepClone();
        second["servers"] = JsonNode.Parse("""[{"url":"https://second.example/v2"}]""");
        JsonObject merged = await MergeJson(token, ("first", first.ToJsonString()), ("second", second.ToJsonString()));
        await Assert.That(merged["paths"]!["/first/items"]!["get"]!["security"]![0]!["key"] is JsonArray).IsTrue();
        await Assert.That(merged["paths"]!["/second/items"]!["get"]!["security"]![0]!["second_key"] is JsonArray).IsTrue();
        await Assert.That(merged["paths"]!["/first/public"]!["get"]!["security"]!.AsArray().Count).IsEqualTo(0);
        await Assert.That(merged["paths"]!["/second/items"]!["get"]!["servers"]![0]!["url"]!.GetValue<string>()).IsEqualTo("https://second.example/v2");
    }

    [Test]
    [Arguments("security")]
    [Arguments("servers")]
    [Arguments("parameters")]
    public async Task Rejects_conflicting_inherited_contracts(string field, CancellationToken token)
    {
        JsonObject first = JsonNode.Parse(Minimal)!.AsObject();
        JsonObject second = (JsonObject)first.DeepClone();
        if (field == "security")
        {
            second["security"] = JsonNode.Parse("""[{"key":[]}]""");
            second["components"] = JsonNode.Parse("""{"securitySchemes":{"key":{"type":"apiKey","in":"header","name":"X-Key"}}}""");
        }
        else if (field == "servers")
            second["servers"] = JsonNode.Parse("""[{"url":"https://second.example"}]""");
        else
            second["paths"]!["/items"]!["parameters"] = JsonNode.Parse("""[{"name":"limit","in":"query","schema":{"type":"integer"}}]""");
        await Reject(token, ("api", first.ToJsonString()), ("api", second.ToJsonString()));
    }

    [Test]
    public async Task Preserves_method_specific_parameters_when_combining_paths(CancellationToken token)
    {
        JsonObject first = JsonNode.Parse(Minimal)!.AsObject();
        JsonObject second = (JsonObject)first.DeepClone();
        first["paths"]!["/items"]!["parameters"] = JsonNode.Parse("""[{"name":"limit","in":"query","schema":{"type":"integer"}}]""");
        second["paths"]!["/items"]!["post"] = second["paths"]!["/items"]!["get"]!.DeepClone();
        second["paths"]!["/items"]!.AsObject().Remove("get");
        second["paths"]!["/items"]!["parameters"] = JsonNode.Parse("""[{"name":"token","in":"header","schema":{"type":"string"}}]""");
        JsonObject merged = await MergeJson(token, ("api", first.ToJsonString()), ("api", second.ToJsonString()));
        await Assert.That(merged["paths"]!["/api/items"]!["get"]!["parameters"]![0]!["name"]!.GetValue<string>()).IsEqualTo("limit");
        await Assert.That(merged["paths"]!["/api/items"]!["post"]!["parameters"]![0]!["name"]!.GetValue<string>()).IsEqualTo("token");
    }

    [Test]
    [Arguments("description")]
    [Arguments("summary")]
    [Arguments("example")]
    [Arguments("examples")]
    public async Task Schema_properties_named_like_documentation_are_part_of_the_contract(string name, CancellationToken token)
    {
        JsonObject first = JsonNode.Parse(Minimal)!.AsObject();
        first["paths"]!["/items"]!["get"]!["responses"]!["200"]!["content"] = JsonNode.Parse("""{"application/json":{"schema":{"type":"object","properties":{}}}}""");
        JsonObject properties = first["paths"]!["/items"]!["get"]!["responses"]!["200"]!["content"]!["application/json"]!["schema"]!["properties"]!.AsObject();
        properties[name] = JsonNode.Parse("""{"type":"string"}""");
        JsonObject second = (JsonObject)first.DeepClone();
        second["paths"]!["/items"]!["get"]!["responses"]!["200"]!["content"]!["application/json"]!["schema"]!["properties"]![name]!["type"] = "integer";
        await Reject(token, ("api", first.ToJsonString()), ("api", second.ToJsonString()));
    }

    [Test]
    public async Task Does_not_rewrite_payloads_or_extensions(CancellationToken token)
    {
        JsonObject source = JsonNode.Parse(Minimal)!.AsObject();
        const string payload = """{"$ref":"#/components/schemas/Missing","security":[{"notAScheme":[]}],"operationId":"literal","discriminator":{"mapping":{"value":"missing"}}}""";
        source["paths"]!["/items"]!["get"]!["responses"]!["200"]!["content"] = JsonNode.Parse("""{"application/json":{"schema":{"type":"object"},"example":{}}}""");
        source["paths"]!["/items"]!["get"]!["responses"]!["200"]!["content"]!["application/json"]!["example"] = JsonNode.Parse(payload);
        source["x-data"] = JsonNode.Parse(payload);
        JsonObject merged = await MergeJson(token, ("api", source.ToJsonString()));
        await Assert.That(JsonNode.DeepEquals(merged["x-data"], JsonNode.Parse(payload))).IsTrue();
        await Assert.That(JsonNode.DeepEquals(merged["paths"]!["/api/items"]!["get"]!["responses"]!["200"]!["content"]!["application/json"]!["example"], JsonNode.Parse(payload))).IsTrue();
    }

    [Test]
    public async Task Rewrites_operation_links_and_resolves_relative_components(CancellationToken token)
    {
        JsonObject first = JsonNode.Parse(Minimal)!.AsObject();
        first["paths"]!["/items"]!["get"]!["responses"]!["200"]!["links"] = JsonNode.Parse("""{"next":{"operationId":"list"},"self":{"operationRef":"#/paths/~1items/get"}}""");
        first["paths"]!["/items"]!["get"]!["responses"]!["200"]!["content"] = JsonNode.Parse("""{"application/json":{"schema":{"$ref":"1.json#/components/schemas/Result"}}}""");
        JsonObject second = JsonNode.Parse(Minimal)!.AsObject();
        second["components"] = JsonNode.Parse("""{"schemas":{"Result":{"type":"string"}}}""");
        JsonObject merged = await MergeJson(token, ("first", first.ToJsonString()), ("second", second.ToJsonString()));
        JsonNode response = merged["paths"]!["/first/items"]!["get"]!["responses"]!["200"]!;
        await Assert.That(response["links"]!["next"]!["operationRef"]!.GetValue<string>()).IsEqualTo("#/paths/~1first~1items/get");
        await Assert.That(response["links"]!["self"]!["operationRef"]!.GetValue<string>()).IsEqualTo("#/paths/~1first~1items/get");
        await Assert.That(response["content"]!["application/json"]!["schema"]!["$ref"]!.GetValue<string>()).IsEqualTo("#/components/schemas/Result");
    }

    [Test]
    [Arguments("#/components/schemas/Result/properties/missing")]
    [Arguments("missing.json#/components/schemas/Result")]
    [Arguments("https://example.com/openapi.json#/components/schemas/Result")]
    public async Task Rejects_unresolved_and_unbundled_references(string reference, CancellationToken token)
    {
        JsonObject source = JsonNode.Parse(Minimal)!.AsObject();
        source["components"] = JsonNode.Parse("""{"schemas":{"Result":{"type":"object","properties":{"id":{"type":"string"}}},"Other":{}}}""");
        source["components"]!["schemas"]!["Other"]!["$ref"] = reference;
        await Reject(token, ("api", source.ToJsonString()));
    }

    [Test]
    public async Task Preserves_OpenApi31_webhooks_boolean_schemas_and_null_unions(CancellationToken token)
    {
        JsonObject source = JsonNode.Parse(Minimal)!.AsObject();
        source["openapi"] = "3.1.1";
        source["components"] = JsonNode.Parse("""{"schemas":{"Choice":{"type":["string","null"],"const":"fixed"},"Never":false},"pathItems":{"Shared":{"get":{"responses":{"200":{"description":"OK"}}}}}}""");
        source["webhooks"] = JsonNode.Parse("""{"event":{"post":{"responses":{"200":{"description":"OK"}}}}}""");
        source["paths"]!["/shared"] = JsonNode.Parse("""{"$ref":"#/components/pathItems/Shared"}""");
        JsonObject merged = await MergeJson(token, ("api", source.ToJsonString()));
        await Assert.That(merged["openapi"]!.GetValue<string>().StartsWith("3.1.", StringComparison.Ordinal)).IsTrue();
        await Assert.That(merged["components"]!["schemas"]!["Choice"]!["type"]!.AsArray().Count).IsEqualTo(2);
        await Assert.That(merged["components"]!["schemas"]!["Never"]!["not"] is JsonObject).IsTrue();
        await Assert.That(merged["components"]!["schemas"]!["Choice"]!["const"]!.GetValue<string>()).IsEqualTo("fixed");
        await Assert.That(merged["webhooks"]!["api_event"]!["post"] != null).IsTrue();
        await Assert.That(merged["paths"]!["/api/shared"]!["get"] != null).IsTrue();
    }

    [Test]
    public async Task Mixed_versions_upgrade_nullable_without_dropping_constraints(CancellationToken token)
    {
        JsonObject first = JsonNode.Parse(Minimal)!.AsObject();
        first["components"] = JsonNode.Parse("""{"schemas":{"Value":{"type":"string","nullable":true,"minLength":3}}}""");
        JsonObject second = JsonNode.Parse(Minimal)!.AsObject();
        second["openapi"] = "3.1.1";
        JsonObject merged = await MergeJson(token, ("first", first.ToJsonString()), ("second", second.ToJsonString()));
        await Assert.That(merged["components"]!["schemas"]!["Value"]!["type"]!.AsArray().Any(node => node!.GetValue<string>() == "null")).IsTrue();
        await Assert.That(merged["components"]!["schemas"]!["Value"]!["minLength"]!.GetValue<int>()).IsEqualTo(3);
    }

    [Test]
    public async Task Preserves_implicit_discriminator_values_after_schema_collision(CancellationToken token)
    {
        JsonObject source = JsonNode.Parse(Minimal)!.AsObject();
        source["components"] = JsonNode.Parse("""{"schemas":{"Animal":{"oneOf":[{"$ref":"#/components/schemas/Dog"}],"discriminator":{"propertyName":"kind"}},"Dog":{"type":"object","required":["kind"],"properties":{"kind":{"type":"string"}}}}}""");
        JsonObject merged = await MergeJson(token, ("first", source.ToJsonString()), ("second", source.ToJsonString()));
        await Assert.That(merged["components"]!["schemas"]!["second_Animal"]!["discriminator"]!["mapping"]!["Dog"]!.GetValue<string>()).IsEqualTo("#/components/schemas/second_Dog");
    }

    [Test]
    public async Task Rejects_invalid_path_templates_and_missing_responses(CancellationToken token)
    {
        await Reject(token, ("api", Minimal.Replace("/items", "/items/{id}", StringComparison.Ordinal)));
        JsonObject source = JsonNode.Parse(Minimal)!.AsObject();
        source["paths"]!["/items"]!["get"]!.AsObject().Remove("responses");
        await Reject(token, ("api", source.ToJsonString()));
    }

    [Test]
    public async Task Rejects_duplicate_json_keys(CancellationToken token)
    {
        await Reject(token, ("api", Minimal.Replace("\"operationId\":\"list\"", "\"operationId\":\"list\",\"operationId\":\"other\"", StringComparison.Ordinal)));
    }

    [Test]
    public async Task Directory_skips_unrelated_json_but_rejects_broken_OpenApi(CancellationToken token)
    {
        string directory = Path.Combine(Path.GetTempPath(), "openapi-merger-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            await File.WriteAllTextAsync(Path.Combine(directory, "api.json"), Minimal, token);
            await File.WriteAllTextAsync(Path.Combine(directory, "package.json"), "{\"name\":\"package\"}", token);
            OpenApiDocument merged = await _util.MergeDirectory(directory, token);
            await Assert.That(merged.Paths.Count).IsEqualTo(1);
            await File.WriteAllTextAsync(Path.Combine(directory, "broken.json"), "{\"openapi\":\"3.0.3\",", token);
            bool rejected = false;
            try { await _util.MergeDirectory(directory, token); }
            catch (InvalidOperationException) { rejected = true; }
            await Assert.That(rejected).IsTrue();
        }
        finally { Directory.Delete(directory, recursive: true); }
    }
}
