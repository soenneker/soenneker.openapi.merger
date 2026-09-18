using System;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Merger.Tests;

public sealed partial class OpenApiMergerTests
{
    [Test]
    [Arguments("3.0.3", false)]
    [Arguments("3.0.3", true)]
    [Arguments("3.1.1", false)]
    [Arguments("3.1.1", true)]
    public async Task Preserves_LinkedIn_path_template_names_and_links(string version, bool referencedParameter, CancellationToken token)
    {
        const string path = "/posts/{encoded ugcPostUrn|shareUrn}";
        const string name = "encoded ugcPostUrn|shareUrn";
        JsonObject source = JsonNode.Parse(Minimal)!.AsObject();
        source["openapi"] = version;
        JsonObject pathItem = (JsonObject)source["paths"]!["/items"]!.DeepClone();
        source["paths"] = new JsonObject { [path] = pathItem };
        JsonNode parameter = JsonNode.Parse("""{"name":"encoded ugcPostUrn|shareUrn","in":"path","required":true,"schema":{"type":"string"}}""")!;
        if (referencedParameter)
        {
            source["components"] = new JsonObject { ["parameters"] = new JsonObject { ["PostUrn"] = parameter } };
            pathItem["parameters"] = JsonNode.Parse("""[{"$ref":"#/components/parameters/PostUrn"}]""");
        }
        else
            pathItem["get"]!["parameters"] = new JsonArray(parameter);

        string target = "#/paths/" + Uri.EscapeDataString(path.Replace("/", "~1", StringComparison.Ordinal)) + "/get";
        pathItem["get"]!["responses"]!["200"]!["links"] = new JsonObject
        {
            ["self"] = new JsonObject { ["operationRef"] = target }
        };

        JsonObject merged = await MergeJson(token, ("linkedin", source.ToJsonString()));
        JsonNode operation = merged["paths"]!["/linkedin" + path]!["get"]!;
        JsonNode mergedParameter = referencedParameter
            ? merged["components"]!["parameters"]!["PostUrn"]!
            : operation["parameters"]![0]!;
        await Assert.That(mergedParameter["name"]!.GetValue<string>()).IsEqualTo(name);
        await Assert.That(mergedParameter["required"]!.GetValue<bool>()).IsTrue();
        string operationRef = operation["responses"]!["200"]!["links"]!["self"]!["operationRef"]!.GetValue<string>();
        await Assert.That(Uri.UnescapeDataString(operationRef)).IsEqualTo("#/paths/~1linkedin~1posts~1{" + name + "}/get");
    }

    [Test]
    [Arguments("/bad path/{id}")]
    [Arguments("/posts/{id}/bad path")]
    [Arguments("/posts/{id}?query=value")]
    [Arguments("/posts/{id}#fragment")]
    [Arguments("/posts/{id")]
    [Arguments("/posts/{}")]
    [Arguments("/posts/{nested{id}}")]
    public async Task Rejects_invalid_literal_paths_and_malformed_templates(string path, CancellationToken token)
    {
        JsonObject source = JsonNode.Parse(Minimal)!.AsObject();
        JsonObject pathItem = (JsonObject)source["paths"]!["/items"]!.DeepClone();
        pathItem["get"]!["parameters"] = JsonNode.Parse("""[{"name":"id","in":"path","required":true,"schema":{"type":"string"}}]""");
        source["paths"] = new JsonObject { [path] = pathItem };
        await Reject(token, ("api", source.ToJsonString()));
    }

    [Test]
    public async Task Collision_renames_are_atomic_for_security_and_component_references(CancellationToken token)
    {
        JsonObject first = JsonNode.Parse(Minimal)!.AsObject();
        first["components"] = JsonNode.Parse("""{"schemas":{"Model":{"type":"string"}},"securitySchemes":{"key":{"type":"apiKey","in":"header","name":"X-Key"}}}""");
        JsonObject second = JsonNode.Parse(Minimal)!.AsObject();
        second["components"] = JsonNode.Parse("""{"schemas":{"Model":{"type":"integer"},"second_Model":{"type":"boolean"}},"securitySchemes":{"key":{"type":"apiKey","in":"header","name":"X-Other"},"second_key":{"type":"http","scheme":"bearer"}}}""");
        second["security"] = JsonNode.Parse("""[{"key":[],"second_key":[]}]""");
        second["paths"]!["/items"]!["get"]!["responses"]!["200"]!["content"] = JsonNode.Parse("""{"application/json":{"schema":{"oneOf":[{"$ref":"#/components/schemas/Model"},{"$ref":"#/components/schemas/second_Model"}]}}}""");
        JsonObject merged = await MergeJson(token, ("first", first.ToJsonString()), ("second", second.ToJsonString()));
        JsonObject security = merged["paths"]!["/second/items"]!["get"]!["security"]![0]!.AsObject();
        await Assert.That(security.Count).IsEqualTo(2);
        foreach ((string key, _) in security)
            await Assert.That(merged["components"]!["securitySchemes"]![key] != null).IsTrue();
        JsonArray refs = merged["paths"]!["/second/items"]!["get"]!["responses"]!["200"]!["content"]!["application/json"]!["schema"]!["oneOf"]!.AsArray();
        await Assert.That(refs[0]!["$ref"]!.GetValue<string>() != refs[1]!["$ref"]!.GetValue<string>()).IsTrue();
    }

    [Test]
    public async Task References_use_uri_decoding_and_json_pointer_escaping(CancellationToken token)
    {
        JsonObject source = JsonNode.Parse(Minimal)!.AsObject();
        source["components"] = JsonNode.Parse("""{"schemas":{"odd/name~value":{"type":"object","properties":{"a/b~c":{"type":"string"}}},"Alias":{"$ref":"#/components/schemas/odd~1name~0value/properties/a~1b~0c"},"percent%2F":{"type":"integer"},"PercentAlias":{"$ref":"#/components/schemas/percent%252F"}}}""");
        JsonObject merged = await MergeJson(token, ("api", source.ToJsonString()));
        string promoted = merged["components"]!["schemas"]!["Alias"]!["$ref"]!.GetValue<string>().Split('/').Last();
        await Assert.That(merged["components"]!["schemas"]![promoted]!["type"]!.GetValue<string>()).IsEqualTo("string");
        await Assert.That(merged["components"]!["schemas"]!["odd_name_value"]!["properties"]!["a/b~c"] != null).IsTrue();
        await Assert.That(merged["components"]!["schemas"]!["PercentAlias"]!["$ref"]!.GetValue<string>()).IsEqualTo("#/components/schemas/percent_2F");
    }

    [Test]
    public async Task Prefix_matching_is_case_sensitive(CancellationToken token)
    {
        JsonObject merged = await MergeJson(token, ("api", Minimal.Replace("/items", "/API/items", StringComparison.Ordinal)));
        await Assert.That(merged["paths"]!["/api/API/items"] != null).IsTrue();
    }

    [Test]
    public async Task Callback_operation_ids_and_links_are_namespaced(CancellationToken token)
    {
        JsonObject source = JsonNode.Parse(Minimal)!.AsObject();
        source["paths"]!["/items"]!["get"]!["callbacks"] = JsonNode.Parse("""{"onEvent":{"{$request.query.callbackUrl}":{"post":{"operationId":"event","responses":{"200":{"description":"OK","links":{"list":{"operationId":"list"}}}}}}}}""");
        JsonObject merged = await MergeJson(token, ("one", source.ToJsonString()), ("two", source.ToJsonString()));
        JsonNode callback = merged["paths"]!["/two/items"]!["get"]!["callbacks"]!["onEvent"]!["{$request.query.callbackUrl}"]!["post"]!;
        await Assert.That(callback["operationId"]!.GetValue<string>()).IsEqualTo("two_event");
        await Assert.That(callback["responses"]!["200"]!["links"]!["list"]!["operationRef"]!.GetValue<string>()).IsEqualTo("#/paths/~1two~1items/get");
    }

    [Test]
    public async Task Operation_parameters_override_path_parameters(CancellationToken token)
    {
        JsonObject source = JsonNode.Parse(Minimal)!.AsObject();
        source["paths"]!["/items"]!["parameters"] = JsonNode.Parse("""[{"in":"query","name":"limit","schema":{"type":"integer","maximum":100}}]""");
        source["paths"]!["/items"]!["get"]!["parameters"] = JsonNode.Parse("""[{"in":"query","name":"limit","schema":{"type":"integer","maximum":10}}]""");
        JsonObject merged = await MergeJson(token, ("api", source.ToJsonString()));
        JsonArray parameters = merged["paths"]!["/api/items"]!["get"]!["parameters"]!.AsArray();
        await Assert.That(parameters.Count).IsEqualTo(1);
        await Assert.That(parameters[0]!["schema"]!["maximum"]!.GetValue<int>()).IsEqualTo(10);
    }

    [Test]
    public async Task AllOf_discriminator_values_and_schema_anchors_survive_collisions(CancellationToken token)
    {
        JsonObject source = JsonNode.Parse(Minimal)!.AsObject();
        source["openapi"] = "3.1.1";
        source["components"] = JsonNode.Parse("""{"schemas":{"Animal":{"type":"object","required":["kind"],"properties":{"kind":{"type":"string"}},"discriminator":{"propertyName":"kind"}},"Dog":{"allOf":[{"$ref":"#/components/schemas/Animal"}]},"Item":{"$anchor":"item","type":"string"},"Alias":{"$ref":"#item"}}}""");
        JsonObject merged = await MergeJson(token, ("first", source.ToJsonString()), ("second", source.ToJsonString()));
        await Assert.That(merged["components"]!["schemas"]!["second_Animal"]!["discriminator"]!["mapping"]!["Dog"]!.GetValue<string>()).IsEqualTo("#/components/schemas/second_Dog");
        await Assert.That(merged["components"]!["schemas"]!["second_Alias"]!["$ref"]!.GetValue<string>()).IsEqualTo("#/components/schemas/second_Item");
    }

    [Test]
    public async Task Equivalent_path_templates_with_different_variable_names_are_rejected(CancellationToken token)
    {
        JsonObject source = JsonNode.Parse(Minimal)!.AsObject();
        source["paths"]!["/items/{id}"] = JsonNode.Parse("""{"get":{"parameters":[{"in":"path","name":"id","required":true,"schema":{"type":"string"}}],"responses":{"200":{"description":"OK"}}}}""");
        source["paths"]!["/items/{name}"] = JsonNode.Parse("""{"get":{"parameters":[{"in":"path","name":"name","required":true,"schema":{"type":"string"}}],"responses":{"200":{"description":"OK"}}}}""");
        await Reject(token, ("api", source.ToJsonString()));
    }

    [Test]
    public async Task Missing_security_cannot_bind_to_a_different_source(CancellationToken token)
    {
        JsonObject first = JsonNode.Parse(Minimal)!.AsObject();
        first["components"] = JsonNode.Parse("""{"securitySchemes":{"key":{"type":"apiKey","in":"header","name":"X-Key"}}}""");
        JsonObject second = JsonNode.Parse(Minimal)!.AsObject();
        second["security"] = JsonNode.Parse("""[{"key":[]}]""");
        await Reject(token, ("one", first.ToJsonString()), ("two", second.ToJsonString()));
    }

    [Test]
    public async Task Preserves_advanced_31_schema_keywords_and_validates_nested_refs(CancellationToken token)
    {
        JsonObject source = JsonNode.Parse(Minimal)!.AsObject();
        source["openapi"] = "3.1.1";
        source["components"] = JsonNode.Parse("""{"schemas":{"Advanced":{"type":"object","$defs":{"Value":{"type":"integer","minimum":3}},"properties":{"value":{"$ref":"#/components/schemas/Advanced/$defs/Value"}},"dependentRequired":{"value":["other"]},"unevaluatedProperties":false,"if":{"required":["value"]},"then":{"properties":{"other":{"const":"yes"}}}},"Tuple":{"type":"array","prefixItems":[{"type":"string"},false],"items":false}}}""");
        JsonObject merged = await MergeJson(token, ("api", source.ToJsonString()));
        JsonNode schema = merged["components"]!["schemas"]!["Advanced"]!;
        await Assert.That(schema["dependentRequired"]!["value"]![0]!.GetValue<string>()).IsEqualTo("other");
        await Assert.That(schema["unevaluatedProperties"]?["not"] is JsonObject).IsTrue();
        await Assert.That(schema["then"]!["properties"]!["other"]!["const"]!.GetValue<string>()).IsEqualTo("yes");
        await Assert.That(merged["components"]!["schemas"]!["Tuple"]!["prefixItems"]!.AsArray().Count).IsEqualTo(2);
    }

    [Test]
    public async Task Non_string_constants_and_reference_siblings_preserve_constraints(CancellationToken token)
    {
        JsonObject source = JsonNode.Parse(Minimal)!.AsObject();
        source["openapi"] = "3.1.1";
        source["components"] = JsonNode.Parse("""{"schemas":{"Base":{"type":"string"},"Constrained":{"$ref":"#/components/schemas/Base","minLength":5},"Numeric":{"const":42},"Object":{"const":{"enabled":true}},"Null":{"const":null}}}""");
        JsonObject merged = await MergeJson(token, ("api", source.ToJsonString()));
        JsonNode schemas = merged["components"]!["schemas"]!;
        await Assert.That(schemas["Numeric"]!["enum"]![0]!.GetValue<int>()).IsEqualTo(42);
        await Assert.That(schemas["Object"]!["enum"]![0]!["enabled"]!.GetValue<bool>()).IsTrue();
        await Assert.That(schemas["Null"]!["enum"]!.AsArray().Count).IsEqualTo(1);
        await Assert.That(schemas["Null"]!["enum"]![0] == null).IsTrue();
        await Assert.That(schemas["Constrained"]!["minLength"]!.GetValue<int>()).IsEqualTo(5);
        await Assert.That(schemas["Constrained"]!["allOf"]![0]!["$ref"]!.GetValue<string>()).IsEqualTo("#/components/schemas/Base");
    }

    [Test]
    public async Task Rejects_reference_targets_of_the_wrong_OpenApi_type(CancellationToken token)
    {
        JsonObject source = JsonNode.Parse(Minimal)!.AsObject();
        source["components"] = JsonNode.Parse("""{"schemas":{"Invalid":{"$ref":"#/paths/~1items/get/responses/200"}}}""");
        await Reject(token, ("api", source.ToJsonString()));
    }

    [Test]
    [Arguments("{\"prefixItems\":[42]}")]
    [Arguments("{\"anyOf\":[]}")]
    [Arguments("{\"properties\":{\"bad\":null}}")]
    public async Task Rejects_invalid_schema_keyword_shapes(string schema, CancellationToken token)
    {
        JsonObject source = JsonNode.Parse(Minimal)!.AsObject();
        source["openapi"] = "3.1.1";
        source["components"] = new JsonObject { ["schemas"] = new JsonObject { ["Invalid"] = JsonNode.Parse(schema) } };
        await Reject(token, ("api", source.ToJsonString()));
    }

    [Test]
    public async Task Cancellation_is_not_swallowed(CancellationToken token)
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
        cancellation.Cancel();
        bool canceled = false;
        try { await _util.MergeOpenApis([("api", "unused.json")], cancellation.Token); }
        catch (OperationCanceledException) { canceled = true; }
        await Assert.That(canceled).IsTrue();
    }
}
