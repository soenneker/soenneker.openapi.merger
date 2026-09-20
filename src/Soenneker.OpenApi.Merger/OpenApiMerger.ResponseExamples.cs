using System;
using System.Linq;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Merger;

public sealed partial class OpenApiMerger
{
    private static void FillMissingResponseSchemas(JsonObject left, JsonObject right)
    {
        if (left["responses"] is not JsonObject responses || right["responses"] is not JsonObject otherResponses) return;
        foreach ((string status, JsonNode? response) in responses)
        {
            if (response is not JsonObject first || first["content"] is not JsonObject content ||
                otherResponses[status] is not JsonObject second || second["content"] is not JsonObject otherContent) continue;
            foreach ((string type, JsonNode? media) in content)
            {
                if (media is not JsonObject firstMedia || otherContent[type] is not JsonObject secondMedia) continue;
                // Missing schema means unspecified payload; explicit {} and boolean schemas are intentional.
                if (!firstMedia.ContainsKey("schema") && secondMedia.ContainsKey("schema"))
                    firstMedia["schema"] = secondMedia["schema"]?.DeepClone();
                else if (!secondMedia.ContainsKey("schema") && firstMedia.ContainsKey("schema"))
                    secondMedia["schema"] = firstMedia["schema"]?.DeepClone();
            }
        }
    }

    private static void MergeDocumentationExamples(JsonObject target, JsonObject source, ObjectKind kind)
    {
        if (ReferenceEquals(target, source)) return;
        if (kind == ObjectKind.Schema)
        {
            // JSON Schema uses an array of payloads; OpenAPI media/parameter examples use named Example Objects.
            if (source["examples"] is JsonArray incoming)
            {
                JsonArray values = (target["examples"] ??= new JsonArray()).AsArray();
                foreach (JsonNode? value in incoming.ToArray())
                    if (!values.Any(existing => JsonNode.DeepEquals(existing, value))) values.Add(value?.DeepClone());
            }
            if (!target.ContainsKey("example") && source.ContainsKey("example")) target["example"] = source["example"]?.DeepClone();
            return;
        }
        if (!source.ContainsKey("example") && !source.ContainsKey("examples")) return;
        var examples = target["examples"] as JsonObject ?? new JsonObject();
        void Add(string name, JsonNode? value)
        {
            if (examples.Any(pair => JsonNode.DeepEquals(pair.Value, value))) return;
            string unique = name;
            for (int suffix = 2; examples.ContainsKey(unique); suffix++) unique = name + "_" + suffix;
            examples[unique] = value?.DeepClone();
        }
        if (target.ContainsKey("example"))
        {
            Add("example", new JsonObject { ["value"] = target["example"]?.DeepClone() });
            target.Remove("example");
        }
        if (source.ContainsKey("example")) Add("example", new JsonObject { ["value"] = source["example"]?.DeepClone() });
        if (source["examples"] is JsonObject incomingExamples)
            foreach ((string name, JsonNode? value) in incomingExamples.ToArray()) Add(name, value);
        if (target["examples"] == null) target["examples"] = examples;
    }
}
