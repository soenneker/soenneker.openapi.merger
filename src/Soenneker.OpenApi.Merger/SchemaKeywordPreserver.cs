using Microsoft.OpenApi;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Merger;

// Microsoft.OpenApi writes unknown JSON Schema keywords inside an "unrecognizedKeywords" object.
// Its extension writer can preserve those keywords at their actual schema locations instead.
internal sealed class SchemaKeywordPreserver(bool is31) : OpenApiVisitorBase
{
    private readonly HashSet<OpenApiSchema> _visited = new(ReferenceEqualityComparer.Instance);

    public override void Visit(IOpenApiSchema schema)
    {
        if (schema is not OpenApiSchema concrete || !_visited.Add(concrete))
            return;
        if (concrete.UnrecognizedKeywords is { Count: > 0 } keywords)
        {
            if (!is31)
                throw new InvalidOperationException($"Unsupported OpenAPI 3.0 schema keywords at {PathString}: {string.Join(", ", keywords.Keys)}.");
            concrete.Extensions ??= new Dictionary<string, IOpenApiExtension>(StringComparer.Ordinal);
            foreach ((string key, JsonNode value) in keywords)
                concrete.Extensions[key] = new JsonNodeExtension(value.DeepClone());
            concrete.UnrecognizedKeywords = null;
        }

        // The library walker does not currently visit every 2020-12 schema-valued keyword.
        IEnumerable<IOpenApiSchema?> children = new IOpenApiSchema?[]
        {
            concrete.Items, concrete.AdditionalProperties, concrete.Not, concrete.If, concrete.Then, concrete.Else,
            concrete.Contains, concrete.ContentSchema, concrete.PropertyNames, concrete.UnevaluatedPropertiesSchema
        };
        children = children.Concat(concrete.Properties?.Values ?? []).Concat(concrete.Definitions?.Values ?? [])
            .Concat(concrete.PatternProperties?.Values ?? []).Concat(concrete.DependentSchemas?.Values ?? [])
            .Concat(concrete.AllOf ?? []).Concat(concrete.OneOf ?? []).Concat(concrete.AnyOf ?? []);
        foreach (IOpenApiSchema? child in children)
            if (child != null)
                Visit(child);
    }
}
