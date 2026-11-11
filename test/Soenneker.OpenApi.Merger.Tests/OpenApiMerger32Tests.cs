using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Merger.Tests;

public sealed partial class OpenApiMergerTests
{
    [Test]
    public async ValueTask Merge32_preserves_query_streaming_and_renames_references(CancellationToken cancellationToken)
    {
        const string source = """
            {"openapi":"3.2.0","info":{"title":"Events","version":"1"},"paths":{
              "/events":{"query":{"operationId":"search","responses":{"200":{"description":"OK","content":{
                "text/event-stream":{"itemSchema":{"$ref":"#/components/schemas/Event"}}
              }}}},"additionalOperations":{"CUSTOM":{"operationId":"custom","responses":{"200":{"description":"OK"}}}}}
            },"components":{"schemas":{"Event":{"type":"string"}}}}
            """;
        var result = await MergeJson(cancellationToken, ("one", source), ("two", source));
        await Assert.That(result["openapi"]!.GetValue<string>()).IsEqualTo("3.2.0");
        await Assert.That(result["paths"]!["/two/events"]!["query"]!["responses"]!["200"]!["content"]!["text/event-stream"]!["itemSchema"]!["$ref"]!.GetValue<string>()).IsEqualTo("#/components/schemas/two_Event");
        await Assert.That(result["paths"]!["/two/events"]!["additionalOperations"]!["CUSTOM"]!["operationId"]!.GetValue<string>()).IsEqualTo("two_custom");
    }

    [Test]
    public async ValueTask Merge32_upgrades_older_nullable_schemas(CancellationToken cancellationToken)
    {
        const string older = """
            {"openapi":"3.0.3","info":{"title":"Old","version":"1"},"paths":{},"components":{"schemas":{"Name":{"type":"string","nullable":true}}}}
            """;
        const string newer = """
            {"openapi":"3.2.0","info":{"title":"New","version":"1"},"webhooks":{"changed":{"post":{"responses":{"200":{"description":"OK"}}}}}}
            """;
        var result = await MergeJson(cancellationToken, ("old", older), ("new", newer));
        await Assert.That(result["openapi"]!.GetValue<string>()).IsEqualTo("3.2.0");
        await Assert.That(result["components"]!["schemas"]!["Name"]!.ToJsonString().Contains("null")).IsTrue();
        await Assert.That(result["webhooks"]).IsNotNull();
    }
}
