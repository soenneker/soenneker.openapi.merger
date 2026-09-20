using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using Soenneker.OpenApi.Merger.Dtos;

namespace Soenneker.OpenApi.Merger;

public sealed partial class OpenApiMerger
{
    private static void NormalizeInheritedSettings(JsonObject root, JsonObject merged,
        List<(SourceDocument Source, JsonObject Root)> documents)
    {
        if (root.ContainsKey("servers") && root["servers"] is not JsonArray)
            throw new InvalidOperationException("Root servers must be an array.");

        JsonNode Resolve(string reference)
        {
            if (TryResolvePointer(merged, reference, out JsonNode? component))
                return component!;
            foreach ((_, JsonObject document) in documents)
                if (TryResolvePointer(document, reference, out JsonNode? target))
                    return target!;
            throw new InvalidOperationException($"Unresolved reference '{reference}'.");
        }

        JsonObject Dereference(JsonObject obj, HashSet<string> visited)
        {
            if (StringValue(obj["$ref"]) is not string reference)
                return obj;
            if (!visited.Add(reference))
                throw new InvalidOperationException($"Circular path-item or parameter reference '{reference}'.");
            if (Resolve(reference) is not JsonObject target)
                throw new InvalidOperationException($"Reference '{reference}' must target an object.");
            JsonObject result = (JsonObject)Dereference(target, visited).DeepClone();
            foreach ((string key, JsonNode? value) in obj.Where(static property => property.Key != "$ref"))
            {
                if (result.ContainsKey(key) && key is not "summary" and not "description" &&
                    !JsonNode.DeepEquals(result[key], value))
                    throw new InvalidOperationException($"Conflicting sibling '{key}' on reference '{reference}'.");
                result[key] = value?.DeepClone();
            }

            return result;
        }

        List<(string Name, string Location, JsonNode Node)> Parameters(JsonNode? node, string pointer)
        {
            var result = new List<(string Name, string Location, JsonNode Node)>();
            if (node == null)
                return result;
            if (node is not JsonArray array)
                throw new InvalidOperationException($"Parameters at {pointer} must be an array.");
            var names = new HashSet<(string, string)>();
            foreach (JsonNode? item in array)
            {
                if (item is not JsonObject parameter)
                    throw new InvalidOperationException($"Invalid parameter at {pointer}.");
                JsonObject resolved = Dereference(parameter, new HashSet<string>(StringComparer.Ordinal));
                string name = StringValue(resolved["name"]) ??
                              throw new InvalidOperationException($"Parameter at {pointer} has no name.");
                string location = StringValue(resolved["in"]) ??
                                  throw new InvalidOperationException(
                                      $"Parameter '{name}' at {pointer} has no location.");
                if (!names.Add((name, location)))
                    throw new InvalidOperationException($"Duplicate parameter '{name}' in '{location}' at {pointer}.");
                result.Add((name, location, parameter));
            }

            return result;
        }

        JsonTraversal.Visit(root, ObjectKind.Document, (obj, kind, pointer) =>
        {
            if (kind != ObjectKind.PathItem)
                return;
            if (obj.ContainsKey("servers") && obj["servers"] is not JsonArray)
                throw new InvalidOperationException($"Servers at {pointer} must be an array.");
            if (obj.ContainsKey("$ref"))
            {
                JsonObject resolved = Dereference(obj, new HashSet<string>(StringComparer.Ordinal));
                obj.Clear();
                foreach ((string key, JsonNode? value) in resolved)
                    obj[key] = value?.DeepClone();
            }

            List<(string Name, string Location, JsonNode Node)> inheritedParameters =
                Parameters(obj["parameters"], pointer);
            JsonNode servers = obj["servers"] is JsonArray pathServers && pathServers.Count > 0 ? pathServers :
                root["servers"] is JsonArray rootServers && rootServers.Count > 0 ? rootServers :
                new JsonArray(new JsonObject { ["url"] = "/" });
            foreach ((string method, JsonNode? node) in obj.ToArray())
            {
                if (!JsonTraversal.IsHttpMethod(method) || node is not JsonObject operation)
                    continue;
                if (operation.ContainsKey("servers") && operation["servers"] is not JsonArray)
                    throw new InvalidOperationException($"Servers at {pointer}/{method} must be an array.");
                List<(string Name, string Location, JsonNode Node)> localParameters =
                    Parameters(operation["parameters"], pointer + "/" + method);
                (string Name, string Location, JsonNode Node)[] effective = inheritedParameters
                                                                            .Where(inherited =>
                                                                                !localParameters.Any(local =>
                                                                                    local.Name == inherited.Name &&
                                                                                    local.Location ==
                                                                                    inherited.Location))
                                                                            .Concat(localParameters)
                                                                            .OrderBy(
                                                                                static parameter => parameter.Location,
                                                                                StringComparer.Ordinal)
                                                                            .ThenBy(static parameter => parameter.Name,
                                                                                StringComparer.Ordinal).ToArray();
                if (effective.Length > 0)
                    operation["parameters"] =
                        new JsonArray(effective.Select(static parameter => parameter.Node.DeepClone()).ToArray());
                if (!operation.ContainsKey("security"))
                    operation["security"] = root["security"]?.DeepClone() ?? new JsonArray();
                if (operation["servers"] is not JsonArray operationServers || operationServers.Count == 0)
                    operation["servers"] = servers.DeepClone();
            }

            // Settings are now operation-local, so merging another method cannot change its inheritance.
            obj.Remove("parameters");
            obj.Remove("servers");
        });
    }

    private static void MergePathMap(JsonObject merged, JsonObject source, string section)
    {
        if (source[section] is not JsonObject paths)
            return;
        JsonObject destination = (merged[section] ??= new JsonObject()).AsObject();
        foreach ((string path, JsonNode? node) in paths)
        {
            if (section == "paths" && path.StartsWith("x-", StringComparison.Ordinal))
            {
                MergeMetadata(destination, path, node, section);
                continue;
            }

            if (node is not JsonObject candidate)
                throw new InvalidOperationException($"Path '{path}' must contain a path item.");
            if (destination[path] is not JsonObject existing)
            {
                destination[path] = candidate.DeepClone();
                continue;
            }

            foreach ((string field, JsonNode? value) in candidate)
            {
                if (!JsonTraversal.IsHttpMethod(field))
                {
                    MergeMetadata(existing, field, value, path);
                    continue;
                }

                if (value is not JsonObject operation)
                    throw new InvalidOperationException($"Operation '{field} {path}' must be an object.");
                if (existing[field] is not JsonObject previous)
                {
                    existing[field] = operation.DeepClone();
                    continue;
                }

                JsonObject enrichedPrevious = (JsonObject)previous.DeepClone();
                JsonObject enrichedOperation = (JsonObject)operation.DeepClone();
                FillMissingResponseSchemas(enrichedPrevious, enrichedOperation);
                if (!JsonNode.DeepEquals(OperationSignature(enrichedPrevious, merged), OperationSignature(enrichedOperation, merged)))
                    throw new InvalidOperationException(
                        $"Multiple source documents produced different operations for '{field} {path}'.");
                existing[field] = enrichedPrevious;
                MergeDocumentation(enrichedPrevious, enrichedOperation, merged);
            }
        }
    }

    private static JsonObject OperationSignature(JsonObject operation, JsonObject merged)
    {
        JsonObject copy = (JsonObject)SortObject(operation);
        var components = new JsonObject();
        var names = new Dictionary<string, string>(StringComparer.Ordinal);
        int referenceDepth = 0;

        string AddReference(string reference, ObjectKind kind)
        {
            string decoded = Uri.UnescapeDataString(reference);
            if (!decoded.StartsWith("#/components/", StringComparison.Ordinal))
                return reference;
            if (names.TryGetValue(decoded, out string? assigned))
                return assigned;
            if (!TryResolvePointer(merged, reference, out JsonNode? target))
                throw new InvalidOperationException($"Unresolved reference '{reference}' while comparing operations.");
            string name = "component" + names.Count;
            string canonical = "#/components/" + name;
            names.Add(decoded, canonical);
            JsonNode component = SortObject(target!);
            components[name] = component;
            if (++referenceDepth > 128)
                throw new InvalidOperationException(
                    "Component reference graph exceeds the supported comparison depth.");
            try
            {
                Normalize(component, kind);
            }
            finally
            {
                referenceDepth--;
            }

            return canonical;
        }

        void Normalize(JsonNode node, ObjectKind initialKind)
        {
            JsonTraversal.Visit(node, initialKind, (obj, kind, _) =>
            {
                obj.Remove("summary");
                obj.Remove("description");
                obj.Remove("externalDocs");
                if (kind is ObjectKind.Schema or ObjectKind.Parameter or ObjectKind.Header or ObjectKind.MediaType)
                {
                    obj.Remove("example");
                    obj.Remove("examples");
                }

                if (kind == ObjectKind.Operation)
                {
                    obj.Remove("operationId");
                    obj.Remove("tags");
                    if (obj["security"] is JsonArray security)
                    {
                        foreach (JsonObject requirement in security.OfType<JsonObject>())
                        {
                            var renamed = new JsonObject();
                            foreach ((string scheme, JsonNode? scopes) in requirement)
                            {
                                string schemeReference = "#/components/securitySchemes/" + JsonTraversal.Escape(scheme);
                                string name = AddReference(schemeReference, ObjectKind.SecurityScheme);
                                renamed[name] = scopes?.DeepClone();
                                // Unused OAuth scopes and scope descriptions do not change this operation's contract.
                                string componentName = name["#/components/".Length..];
                                if (components[componentName]?["flows"] is JsonObject flows)
                                {
                                    HashSet<string?> required = (scopes as JsonArray)?.Select(StringValue)
                                        .ToHashSet(StringComparer.Ordinal) ?? [];
                                    foreach (JsonObject flow in flows.Select(static entry => entry.Value)
                                                                     .OfType<JsonObject>())
                                        if (flow["scopes"] is JsonObject advertised)
                                            foreach (string scope in advertised.Select(static entry => entry.Key)
                                                                               .ToArray())
                                                if (!required.Contains(scope))
                                                    advertised.Remove(scope);
                                                else
                                                    advertised[scope] = "";
                                }
                            }

                            requirement.Clear();
                            foreach ((string key, JsonNode? value) in renamed)
                                requirement[key] = value?.DeepClone();
                        }
                    }
                }

                if (StringValue(obj["$ref"]) is string reference)
                    obj["$ref"] = AddReference(reference, kind);
                if (kind == ObjectKind.Schema && obj["discriminator"]?["mapping"] is JsonObject mapping)
                    foreach ((string key, JsonNode? value) in mapping.ToArray())
                        if (StringValue(value) is string mappingRef)
                            mapping[key] = AddReference(mappingRef, ObjectKind.Schema);
            });
        }

        Normalize(copy, ObjectKind.Operation);
        return new JsonObject { ["operation"] = copy, ["components"] = components };
    }

    private static JsonNode SortObject(JsonNode node)
    {
        if (node is JsonObject obj)
        {
            var result = new JsonObject();
            foreach ((string key, JsonNode? value) in obj.OrderBy(static entry => entry.Key, StringComparer.Ordinal))
                result[key] = value == null ? null : SortObject(value);
            return result;
        }

        if (node is JsonArray array)
            return new JsonArray(array.Select(static value => value == null ? null : SortObject(value)).ToArray());
        return node.DeepClone();
    }

    private static void MergeDocumentation(JsonObject target, JsonObject candidate, JsonObject merged,
        ObjectKind initialKind = ObjectKind.Operation, HashSet<(JsonObject, JsonObject)>? visited = null)
    {
        visited ??= [];
        if (!visited.Add((target, candidate))) return;
        JsonTraversal.Visit(target, initialKind, (obj, kind, pointer) =>
        {
            if (!TryResolvePointer(candidate, pointer, out JsonNode? matching, uriEncoded: false) ||
                matching is not JsonObject other)
                return;
            foreach (string key in new[] { "summary", "description" })
                if (StringValue(other[key]) is string text && text.Length > (StringValue(obj[key])?.Length ?? 0))
                    obj[key] = text;
            if (kind is ObjectKind.Schema or ObjectKind.MediaType or ObjectKind.Parameter or ObjectKind.Header)
                MergeDocumentationExamples(obj, other, kind);
            if (StringValue(obj["$ref"]) is string left && StringValue(other["$ref"]) is string right &&
                TryResolvePointer(merged, left, out JsonNode? leftNode) && leftNode is JsonObject leftObject &&
                TryResolvePointer(merged, right, out JsonNode? rightNode) && rightNode is JsonObject rightObject)
                MergeDocumentation(leftObject, rightObject, merged, kind, visited);
        });
        if (candidate["tags"] is JsonArray tags)
        {
            JsonArray targetTags = (target["tags"] ??= new JsonArray()).AsArray();
            foreach (JsonNode? tag in tags)
                if (!targetTags.Any(existing => JsonNode.DeepEquals(existing, tag)))
                    targetTags.Add(tag?.DeepClone());
        }
    }

    private static void MergeTags(JsonObject merged, JsonObject source)
    {
        if (source["tags"] is not JsonArray tags)
            return;
        JsonArray destination = (merged["tags"] ??= new JsonArray()).AsArray();
        foreach (JsonObject tag in tags.OfType<JsonObject>())
        {
            JsonObject? existing = destination.OfType<JsonObject>()
                                              .FirstOrDefault(item =>
                                                  StringValue(item["name"]) == StringValue(tag["name"]));
            if (existing == null)
                destination.Add(tag.DeepClone());
            else
                foreach ((string key, JsonNode? value) in tag)
                    MergeMetadata(existing, key, value, "tag " + StringValue(tag["name"]));
        }
    }

    private static void MergeMetadata(JsonObject target, string key, JsonNode? value, string context)
    {
        if (!target.ContainsKey(key))
            target[key] = value?.DeepClone();
        else if (key is "description" or "summary")
        {
            if ((StringValue(value)?.Length ?? 0) > (StringValue(target[key])?.Length ?? 0))
                target[key] = value?.DeepClone();
        }
        else if (!JsonNode.DeepEquals(target[key], value))
            throw new InvalidOperationException($"Conflicting '{key}' metadata at {context}.");
    }

    private static void EnsureUniqueOperationIds(JsonObject root)
    {
        var ids = new HashSet<string>(StringComparer.Ordinal);
        JsonTraversal.Visit(root, ObjectKind.Document, (obj, kind, pointer) =>
        {
            if (kind != ObjectKind.Operation)
                return;
            string baseId = StringValue(obj["operationId"]) ?? ToSafeId(pointer);
            string id = baseId;
            for (int suffix = 2; !ids.Add(id); suffix++)
                id = baseId + "_" + suffix;
            obj["operationId"] = id;
        });
    }
}
