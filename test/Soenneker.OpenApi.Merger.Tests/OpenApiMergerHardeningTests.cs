using Soenneker.Utils.File.Abstract;
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
                await _fileUtil.Write(path, inputs[i].Json, cancellationToken: token);
                files[i] = (inputs[i].Prefix, path);
            }
            return JsonNode.Parse(_util.ToJson(await _util.MergeOpenApis(files, token)))!.AsObject();
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Test]
    public async Task Duplicate_operations_keep_response_examples_and_fill_missing_media_schemas(CancellationToken token)
    {
        const string first = """
            {"openapi":"3.0.3","info":{"title":"API","version":"1"},"paths":{"/items":{"get":{"responses":{"200":{"description":"OK","content":{"application/json":{"example":{"at":1}}}}}}}}}
            """;
        const string second = """
            {"openapi":"3.0.3","info":{"title":"API","version":"1"},"paths":{"/items":{"get":{"responses":{"200":{"description":"OK","content":{"application/json":{"schema":{"type":"object","properties":{"at":{"type":"integer","format":"int64"}}},"examples":{"example":{"value":{"at":1760631246905}}}}}}}}}}}
            """;
        foreach (bool reverse in new[] { false, true })
        {
            JsonObject result = await MergeJson(token, ("api", reverse ? second : first), ("api", reverse ? first : second));
            JsonNode media = result["paths"]!["/api/items"]!["get"]!["responses"]!["200"]!["content"]!["application/json"]!;
            await Assert.That(media["schema"]!["properties"]!["at"]!["format"]!.ToString()).IsEqualTo("int64");
            await Assert.That(media["examples"]!.AsObject().Count).IsEqualTo(2);
            await Assert.That(media["example"]).IsNull();
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
    public async Task Equivalent_referenced_responses_keep_examples_from_both_components(CancellationToken token)
    {
        const string spec = """
            {"openapi":"3.0.3","info":{"title":"API","version":"1"},"paths":{"/items":{"get":{"responses":{"200":{"$ref":"#/components/responses/Result"}}}}},"components":{"responses":{"Result":{"description":"OK","content":{"application/json":{"schema":{"type":"object","properties":{"at":{"type":"integer","format":"int64"}}},"examples":{"saved":{"value":{"at":1}}}}}}}}}
            """;
        JsonObject second = JsonNode.Parse(spec)!.AsObject();
        second["components"]!["responses"]!["Result"]!["content"]!["application/json"]!["examples"]!["saved"]!["value"]!["at"] = 1760631246905L;
        JsonObject merged = await MergeJson(token, ("api", spec), ("api", second.ToJsonString()));
        string reference = merged["paths"]!["/api/items"]!["get"]!["responses"]!["200"]!["$ref"]!.ToString();
        JsonNode target = merged;
        foreach (string segment in reference[2..].Split('/')) target = target[segment]!;
        await Assert.That(target["content"]!["application/json"]!["examples"]!.AsObject().Count).IsEqualTo(2);
    }

    [Test]
    public async Task Preserves_samples_and_tag_groups_scoped_to_source_documents(CancellationToken token)
    {
        JsonObject first = JsonNode.Parse(Minimal)!.AsObject();
        first["x-samples"] = JsonNode.Parse("""[{"$ref":"literal-sample","name":"first"}]""");
        first["x-tagGroups"] = JsonNode.Parse("""[{"name":"First","tags":["items"]}]""");
        JsonObject second = (JsonObject)first.DeepClone();
        second["x-samples"]![0]!["name"] = "second";
        second["x-tagGroups"]![0]!["name"] = "Second";
        JsonObject merged = await MergeJson(token, ("first", first.ToJsonString()), ("second", second.ToJsonString()));
        JsonArray metadata = merged["x-merged-document-metadata"]!.AsArray();
        await Assert.That(metadata.Count).IsEqualTo(2);
        for (int i = 0; i < 2; i++)
        {
            JsonObject original = i == 0 ? first : second;
            await Assert.That(metadata[i]!["prefix"]!.GetValue<string>()).IsEqualTo(i == 0 ? "first" : "second");
            foreach (string key in new[] { "x-samples", "x-tagGroups" })
                await Assert.That(JsonNode.DeepEquals(metadata[i]!["metadata"]![key], original[key])).IsTrue();
        }
        await Assert.That(merged["x-samples"]).IsNull();
        JsonObject single = await MergeJson(token, ("first", first.ToJsonString()));
        await Assert.That(JsonNode.DeepEquals(single["x-samples"], first["x-samples"])).IsTrue();
        await Assert.That(single["x-merged-document-metadata"]).IsNull();
    }

    [Test]
    public async Task Preserves_Postman_metadata_scoped_to_each_collection(CancellationToken token)
    {
        JsonObject first = JsonNode.Parse(Minimal)!.AsObject();
        first["x-postman-warnings"] = new JsonArray("First warning");
        first["x-postman-variables"] = JsonNode.Parse("""[{"key":"baseUrl","value":"https://first.example"}]""");
        first["x-postman-events"] = JsonNode.Parse("""[{"listen":"prerequest","script":{"exec":["first()"]}}]""");
        first["x-postman-unmapped-requests"] = JsonNode.Parse("""[{"reason":"missing-url","item":{"name":"Missing"}}]""");
        JsonObject second = (JsonObject)first.DeepClone();
        second["x-postman-warnings"] = new JsonArray("Second warning");
        second["x-postman-variables"]![0]!["value"] = "https://second.example";
        second["x-postman-events"]![0]!["script"]!["exec"]![0] = "second()";
        JsonObject merged = await MergeJson(token, ("first", first.ToJsonString()), ("second", second.ToJsonString()));
        JsonArray collections = merged["x-merged-postman-collections"]!.AsArray();
        await Assert.That(collections.Count).IsEqualTo(2);
        for (int i = 0; i < 2; i++)
        {
            JsonObject original = i == 0 ? first : second;
            await Assert.That(collections[i]!["prefix"]!.GetValue<string>()).IsEqualTo(i == 0 ? "first" : "second");
            foreach (string key in original.Select(pair => pair.Key).Where(key => key.StartsWith("x-postman-", StringComparison.Ordinal)))
            {
                await Assert.That(merged.ContainsKey(key)).IsFalse();
                await Assert.That(JsonNode.DeepEquals(collections[i]!["metadata"]![key], original[key])).IsTrue();
            }
        }
        JsonObject single = await MergeJson(token, ("first", first.ToJsonString()));
        await Assert.That(JsonNode.DeepEquals(single["x-postman-variables"], first["x-postman-variables"])).IsTrue();
        await Assert.That(single.ContainsKey("x-merged-postman-collections")).IsFalse();
    }

    [Test]
    public async Task Still_rejects_conflicting_unknown_extensions(CancellationToken token)
    {
        JsonObject first = JsonNode.Parse(Minimal)!.AsObject();
        first["x-custom"] = "first";
        JsonObject second = (JsonObject)first.DeepClone();
        second["x-custom"] = "second";
        await Reject(token, ("first", first.ToJsonString()), ("second", second.ToJsonString()));
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
            await _fileUtil.Write(Path.Combine(directory, "api.json"), Minimal, cancellationToken: token);
            await _fileUtil.Write(Path.Combine(directory, "package.json"), "{\"name\":\"package\"}", cancellationToken: token);
            OpenApiDocument merged = await _util.MergeDirectory(directory, token);
            await Assert.That(merged.Paths.Count).IsEqualTo(1);
            await _fileUtil.Write(Path.Combine(directory, "broken.json"), "{\"openapi\":\"3.0.3\",", cancellationToken: token);
            bool rejected = false;
            try { await _util.MergeDirectory(directory, token); }
            catch (InvalidOperationException) { rejected = true; }
            await Assert.That(rejected).IsTrue();
        }
        finally { Directory.Delete(directory, recursive: true); }
    }
}
