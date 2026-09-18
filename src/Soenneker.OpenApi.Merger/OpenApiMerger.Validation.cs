using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading;

namespace Soenneker.OpenApi.Merger;

public sealed partial class OpenApiMerger
{
    private static void PromoteNestedReferenceTargets(JsonObject root)
    {
        // Microsoft.OpenApi's typed references only round-trip direct component targets reliably.
        // Hoist nested targets instead of flattening their schemas or discarding pointer suffixes.
        var promoted = new Dictionary<string, string>(StringComparer.Ordinal);
        var pending = new Queue<(JsonNode Node, ObjectKind Kind)>();
        pending.Enqueue((root, ObjectKind.Document));
        while (pending.TryDequeue(out (JsonNode Node, ObjectKind Kind) entry))
        {
            JsonTraversal.Visit(entry.Node, entry.Kind, (obj, kind, _) =>
            {
                if (StringValue(obj["$ref"]) is not string reference)
                    return;
                string decoded = Uri.UnescapeDataString(reference);
                string[] segments = decoded.Split('/');
                if (segments.Length == 4 && segments[1] == "components")
                    return;
                if (promoted.TryGetValue(decoded, out string? existing))
                {
                    obj["$ref"] = existing;
                    return;
                }

                string? section = JsonTraversal.ComponentSections.FirstOrDefault(item => item.Kind == kind).Section;
                if (section == null || !TryResolvePointer(root, reference, out JsonNode? target))
                    throw new InvalidOperationException($"Unresolved reference '{reference}'.");
                JsonObject components = root["components"]!.AsObject();
                JsonObject entries = (components[section] ??= new JsonObject()).AsObject();
                string name = "merged_" + kind.ToString().ToLowerInvariant() + "_" + (promoted.Count + 1);
                while (entries.ContainsKey(name))
                    name = "_" + name;
                string replacement = "#/components/" + section + "/" + name;
                promoted.Add(decoded, replacement);
                JsonNode copy = target!.DeepClone();
                entries[name] = copy;
                obj["$ref"] = replacement;
                pending.Enqueue((copy, kind));
            });
        }
    }

    private static bool TryResolvePointer(JsonNode root, string reference, out JsonNode? result, bool uriEncoded = true)
    {
        result = root;
        if (reference == "#")
            return true;
        if (!reference.StartsWith("#/", StringComparison.Ordinal))
            return false;
        string pointer = uriEncoded ? Uri.UnescapeDataString(reference[2..]) : reference[2..];
        foreach (string encoded in pointer.Split('/'))
        {
            if (Regex.IsMatch(encoded, "~(?![01])"))
                return false;
            string segment = JsonTraversal.Unescape(encoded);
            if (result is JsonObject obj && obj.TryGetPropertyValue(segment, out JsonNode? child))
                result = child;
            else if (result is JsonArray array && (segment == "0" || !segment.StartsWith('0')) &&
                     int.TryParse(segment, NumberStyles.None, CultureInfo.InvariantCulture, out int index) &&
                     index < array.Count)
                result = array[index];
            else
                return false;
        }

        return result != null;
    }

    private static JsonObject ResolveObject(JsonObject root, JsonObject obj)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        while (StringValue(obj["$ref"]) is string reference)
        {
            if (!seen.Add(reference))
                throw new InvalidOperationException($"Circular non-schema reference '{reference}'.");
            if (!TryResolvePointer(root, reference, out JsonNode? target) || target is not JsonObject resolved)
                throw new InvalidOperationException($"Unresolved object reference '{reference}'.");
            obj = resolved;
        }

        return obj;
    }

    private static void ValidateContract(JsonObject root, CancellationToken cancellationToken)
    {
        var operationPointers = new HashSet<string>(StringComparer.Ordinal);
        var schemaPointers = new HashSet<string>(StringComparer.Ordinal);
        var operationIds = new HashSet<string>(StringComparer.Ordinal);
        var objectKinds = new Dictionary<string, ObjectKind>(StringComparer.Ordinal);
        JsonTraversal.Visit(root, ObjectKind.Document, (obj, kind, pointer) =>
        {
            objectKinds[pointer] = kind;
            if (kind == ObjectKind.Operation)
                operationPointers.Add(pointer);
            if (kind == ObjectKind.Schema)
                schemaPointers.Add(pointer);
        });

        JsonTraversal.Visit(root, ObjectKind.Document, (obj, kind, pointer) =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (obj.ContainsKey("$ref"))
            {
                string reference = StringValue(obj["$ref"]) ??
                                   throw new InvalidOperationException($"Invalid $ref at {pointer}.");
                if (!TryResolvePointer(root, reference, out _) ||
                    !objectKinds.TryGetValue(Uri.UnescapeDataString(reference), out ObjectKind targetKind) ||
                    targetKind != kind)
                    throw new InvalidOperationException($"Unresolved reference '{reference}' at {pointer}.");
            }

            if (kind == ObjectKind.Schema && obj["discriminator"]?["mapping"] is JsonObject mapping)
                foreach ((string key, JsonNode? value) in mapping)
                {
                    string? reference = StringValue(value);
                    if (reference == null || !TryResolvePointer(root, reference, out _) ||
                        !schemaPointers.Contains(Uri.UnescapeDataString(reference)))
                        throw new InvalidOperationException(
                            $"Discriminator mapping '{key}' at {pointer} must reference a schema: '{reference}'.");
                }

            if (kind is ObjectKind.Document or ObjectKind.Operation)
                ValidateSecurity(root, obj, pointer);
            if (kind == ObjectKind.Operation)
            {
                string? id = StringValue(obj["operationId"]);
                if (string.IsNullOrWhiteSpace(id) || !operationIds.Add(id))
                    throw new InvalidOperationException($"Missing or duplicate operationId '{id}' at {pointer}.");
                if (obj["responses"] is not JsonObject responses || !responses.Any(static entry =>
                        !entry.Key.StartsWith("x-", StringComparison.Ordinal)))
                    throw new InvalidOperationException($"Operation at {pointer} requires at least one response.");
            }

            if (kind == ObjectKind.Link && !obj.ContainsKey("$ref"))
            {
                string? reference = StringValue(obj["operationRef"]);
                if (reference == null || !operationPointers.Contains(Uri.UnescapeDataString(reference)))
                    throw new InvalidOperationException(
                        $"Link at {pointer} must reference an existing operation: '{reference}'.");
            }

            if (kind == ObjectKind.PathItem)
                ValidatePathParameters(root, obj, pointer);
        });

        if (root["paths"] is JsonObject paths)
        {
            var shapes = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach ((string path, _) in paths)
            {
                if (path.StartsWith("x-", StringComparison.Ordinal))
                    continue;
                string shape = Regex.Replace(path, "\\{[^{}]+\\}", "{}");
                if (shapes.TryGetValue(shape, out string? previous) && previous != path)
                    throw new InvalidOperationException(
                        $"Paths '{previous}' and '{path}' have the same template and cannot coexist.");
                shapes[shape] = path;
            }
        }
    }

    private static void ValidateSecurity(JsonObject root, JsonObject obj, string pointer)
    {
        if (!obj.ContainsKey("security"))
            return;
        if (obj["security"] is not JsonArray requirements)
            throw new InvalidOperationException($"Security at {pointer} must be an array.");
        foreach (JsonNode? node in requirements)
        {
            if (node is not JsonObject requirement)
                throw new InvalidOperationException($"Invalid security requirement at {pointer}.");
            foreach ((string name, JsonNode? scopes) in requirement)
            {
                if (root["components"]?["securitySchemes"]?[name] is not JsonObject scheme)
                    throw new InvalidOperationException($"Missing security scheme '{name}' at {pointer}.");
                scheme = ResolveObject(root, scheme);
                if (scopes is not JsonArray scopeArray || scopeArray.Any(static scope => StringValue(scope) == null))
                    throw new InvalidOperationException(
                        $"Security scopes for '{name}' at {pointer} must be an array of strings.");
                if (StringValue(scheme["type"]) == "oauth2")
                {
                    HashSet<string> advertised = (scheme["flows"] as JsonObject)
                                                 ?.SelectMany(static flow =>
                                                     (flow.Value?["scopes"] as JsonObject)?.Select(static entry => entry.Key) ?? [])
                                                 .ToHashSet(StringComparer.Ordinal) ?? [];
                    foreach (JsonNode? scope in scopeArray)
                        if (!advertised.Contains(StringValue(scope)!))
                            throw new InvalidOperationException(
                                $"Unknown OAuth scope '{StringValue(scope)}' for '{name}' at {pointer}.");
                }
            }
        }
    }

    private static void ValidatePathParameters(JsonObject root, JsonObject pathItem, string pointer)
    {
        bool regularPath = pointer.StartsWith("#/paths/", StringComparison.Ordinal) &&
                           pointer["#/paths/".Length..].IndexOf('/') < 0;
        string path = regularPath ? JsonTraversal.Unescape(pointer["#/paths/".Length..]) : "";
        HashSet<string> variables = Regex.Matches(path, "\\{([^{}]+)\\}").Select(static match => match.Groups[1].Value)
                                         .ToHashSet(StringComparer.Ordinal);
        if (regularPath && Regex.Replace(path, "\\{[^{}]+\\}", "").IndexOfAny(['{', '}']) >= 0)
            throw new InvalidOperationException($"Malformed path template '{path}'.");
        foreach ((string method, JsonNode? value) in pathItem)
        {
            if (!JsonTraversal.IsHttpMethod(method) || value is not JsonObject operation)
                continue;
            var names = new HashSet<(string, string)>();
            var pathNames = new HashSet<string>(StringComparer.Ordinal);
            if (operation["parameters"] is JsonArray parameters)
                foreach (JsonNode? node in parameters)
                {
                    if (node is not JsonObject parameter)
                        throw new InvalidOperationException($"Invalid parameter for '{method}' at {pointer}.");
                    parameter = ResolveObject(root, parameter);
                    string name = StringValue(parameter["name"]) ?? "";
                    string location = StringValue(parameter["in"]) ?? "";
                    if (name.Length == 0 || location is not ("path" or "query" or "header" or "cookie") ||
                        !names.Add((name, location)))
                        throw new InvalidOperationException(
                            $"Invalid or duplicate parameter '{name}' in '{location}' for '{method}' at {pointer}.");
                    if (location == "path")
                    {
                        if (parameter["required"] is not JsonValue required || !required.TryGetValue(out bool flag) ||
                            !flag)
                            throw new InvalidOperationException(
                                $"Path parameter '{name}' must be required for '{method}' at {pointer}.");
                        pathNames.Add(name);
                    }
                }

            if (regularPath && !variables.SetEquals(pathNames))
                throw new InvalidOperationException(
                    $"Path parameters for '{method} {path}' must match its template variables.");
        }
    }
}