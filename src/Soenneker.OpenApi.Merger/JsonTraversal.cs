using System;
using System.Linq;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Merger;

internal enum ObjectKind
{
    Document, PathItem, Operation, Schema, Parameter, Header, Response, RequestBody, MediaType,
    Encoding, SecurityScheme, Link, Callback, Example, Server, Tag
}

// Follow OpenAPI fields, never arbitrary user data (examples, defaults, extensions or property names).
internal static class JsonTraversal
{
    internal static void Visit(JsonNode? node, ObjectKind kind, Action<JsonObject, ObjectKind, string> action, string pointer = "#")
    {
        if (pointer.Count(static ch => ch == '/') > 128)
            throw new InvalidOperationException("OpenAPI structure exceeds the supported nesting depth.");
        // Microsoft.OpenApi drops boolean component schemas. Equivalent object schemas survive its round trip.
        if (kind == ObjectKind.Schema && node is JsonValue scalar && scalar.TryGetValue(out bool boolean))
        {
            JsonObject replacement = boolean ? new JsonObject() : new JsonObject { ["not"] = new JsonObject() };
            if (node.Parent is JsonObject parentObject)
                parentObject[node.GetPropertyName()] = replacement;
            else if (node.Parent is JsonArray parentArray)
                parentArray[parentArray.IndexOf(node)] = replacement;
            node = replacement;
        }
        if (node is not JsonObject obj)
            throw new InvalidOperationException($"Expected a {kind} object at {pointer}.");

        // Some publishers (including Algolia) wrap a homogeneous item schema in an array.
        // Only unwrap the unambiguous single-object form; leave payloads and tuples alone.
        if (kind == ObjectKind.Schema && obj["items"] is JsonArray { Count: 1 } items && items[0] is JsonObject item)
            obj["items"] = item.DeepClone();

        action(obj, kind, pointer);

        void Child(string key, ObjectKind childKind)
        {
            if (obj.TryGetPropertyValue(key, out JsonNode? child))
                Visit(child, childKind, action, pointer + "/" + Escape(key));
        }
        void Map(string key, ObjectKind childKind)
        {
            if (!obj.ContainsKey(key))
                return;
            if (obj[key] is not JsonObject map)
                throw new InvalidOperationException($"Expected an object at {pointer}/{key}.");
            foreach ((string name, JsonNode? value) in map.ToArray())
                if (!name.StartsWith("x-", StringComparison.Ordinal) || key is not ("paths" or "responses"))
                    Visit(value, childKind, action, pointer + "/" + Escape(key) + "/" + Escape(name));
        }
        void Array(string key, ObjectKind childKind)
        {
            if (!obj.ContainsKey(key))
                return;
            if (obj[key] is not JsonArray array || (kind == ObjectKind.Schema && array.Count == 0))
                throw new InvalidOperationException($"Invalid array at {pointer}/{key}.");
            for (int i = 0; i < array.Count; i++)
                Visit(array[i], childKind, action, pointer + "/" + Escape(key) + "/" + i);
        }

        switch (kind)
        {
            case ObjectKind.Document:
                if (obj.ContainsKey("components") && obj["components"] is not JsonObject)
                    throw new InvalidOperationException($"Expected a components object at {pointer}.");
                Map("paths", ObjectKind.PathItem);
                Map("webhooks", ObjectKind.PathItem);
                Array("servers", ObjectKind.Server);
                Array("tags", ObjectKind.Tag);
                if (obj["components"] is JsonObject components)
                    foreach ((string section, ObjectKind componentKind) in ComponentSections)
                        if (components[section] is JsonObject entries)
                            foreach ((string name, JsonNode? value) in entries.ToArray())
                                Visit(value, componentKind, action, pointer + "/components/" + section + "/" + Escape(name));
                break;
            case ObjectKind.PathItem:
                foreach ((string name, _) in obj.ToArray())
                    if (IsHttpMethod(name))
                        Child(name, ObjectKind.Operation);
                Array("parameters", ObjectKind.Parameter);
                Array("servers", ObjectKind.Server);
                break;
            case ObjectKind.Operation:
                Array("parameters", ObjectKind.Parameter);
                Array("servers", ObjectKind.Server);
                Child("requestBody", ObjectKind.RequestBody);
                Map("responses", ObjectKind.Response);
                Map("callbacks", ObjectKind.Callback);
                break;
            case ObjectKind.Schema:
                foreach (string name in new[] { "properties", "patternProperties", "$defs", "definitions", "dependentSchemas" })
                    Map(name, ObjectKind.Schema);
                foreach (string name in new[] { "allOf", "anyOf", "oneOf", "prefixItems" })
                    Array(name, ObjectKind.Schema);
                foreach (string name in new[] { "items", "additionalProperties", "unevaluatedProperties", "unevaluatedItems", "contains", "not", "if", "then", "else", "propertyNames", "contentSchema" })
                    Child(name, ObjectKind.Schema);
                break;
            case ObjectKind.Parameter:
            case ObjectKind.Header:
                Child("schema", ObjectKind.Schema);
                Map("content", ObjectKind.MediaType);
                Map("examples", ObjectKind.Example);
                break;
            case ObjectKind.Response:
                Map("headers", ObjectKind.Header);
                Map("links", ObjectKind.Link);
                Map("content", ObjectKind.MediaType);
                break;
            case ObjectKind.RequestBody:
                Map("content", ObjectKind.MediaType);
                break;
            case ObjectKind.MediaType:
                Child("schema", ObjectKind.Schema);
                Map("examples", ObjectKind.Example);
                Map("encoding", ObjectKind.Encoding);
                break;
            case ObjectKind.Encoding:
                Map("headers", ObjectKind.Header);
                break;
            case ObjectKind.Link:
                Child("server", ObjectKind.Server);
                break;
            case ObjectKind.Callback:
                foreach ((string name, JsonNode? value) in obj.ToArray())
                    if (name != "$ref" && !name.StartsWith("x-", StringComparison.Ordinal))
                        Visit(value, ObjectKind.PathItem, action, pointer + "/" + Escape(name));
                break;
        }
    }

    internal static readonly (string Section, ObjectKind Kind)[] ComponentSections =
    [
        ("schemas", ObjectKind.Schema), ("parameters", ObjectKind.Parameter), ("responses", ObjectKind.Response),
        ("requestBodies", ObjectKind.RequestBody), ("headers", ObjectKind.Header), ("securitySchemes", ObjectKind.SecurityScheme),
        ("links", ObjectKind.Link), ("callbacks", ObjectKind.Callback), ("examples", ObjectKind.Example), ("pathItems", ObjectKind.PathItem)
    ];

    internal static bool IsHttpMethod(string method) => method is "get" or "put" or "post" or "delete" or "options" or "head" or "patch" or "trace";
    internal static string Escape(string value) => value.Replace("~", "~0", StringComparison.Ordinal).Replace("/", "~1", StringComparison.Ordinal);
    internal static string Unescape(string value) => value.Replace("~1", "/", StringComparison.Ordinal).Replace("~0", "~", StringComparison.Ordinal);
}
