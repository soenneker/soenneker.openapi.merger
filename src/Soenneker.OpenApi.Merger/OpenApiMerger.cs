using Microsoft.Extensions.Logging;
using Microsoft.OpenApi;
using Microsoft.OpenApi.Reader;
using Soenneker.Git.Util.Abstract;
using Soenneker.OpenApi.Merger.Abstract;
using Soenneker.OpenApi.Merger.Dtos;
using Soenneker.Utils.Directory.Abstract;
using Soenneker.Utils.File.Abstract;
using Soenneker.Utils.MemoryStream.Abstract;
using Soenneker.Utils.Yaml.Abstract;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Merger;

public sealed partial class OpenApiMerger : IOpenApiMerger
{
    private readonly ILogger<OpenApiMerger> _logger;
    private readonly IGitUtil _gitUtil;
    private readonly IDirectoryUtil _directoryUtil;
    private readonly IFileUtil _fileUtil;
    private readonly IMemoryStreamUtil _memoryStreamUtil;
    private readonly IYamlUtil _yamlUtil;
    private sealed record OutputVersion(bool Is31);
    private static readonly ConditionalWeakTable<OpenApiDocument, OutputVersion> _versions = new();
    private static readonly StringComparer _filePathComparer = OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;

    public OpenApiMerger(ILogger<OpenApiMerger> logger, IGitUtil gitUtil, IDirectoryUtil directoryUtil, IFileUtil fileUtil,
        IMemoryStreamUtil memoryStreamUtil, IYamlUtil yamlUtil)
    {
        _logger = logger;
        _gitUtil = gitUtil;
        _directoryUtil = directoryUtil;
        _fileUtil = fileUtil;
        _memoryStreamUtil = memoryStreamUtil;
        _yamlUtil = yamlUtil;
    }

    public async ValueTask<OpenApiDocument> MergeOpenApis(IEnumerable<(string prefix, string filePath)> inputs, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(inputs);
        var sources = new List<SourceDocument>();
        foreach ((string prefix, string filePath) in inputs)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ValidatePrefix(prefix);
            ArgumentException.ThrowIfNullOrWhiteSpace(filePath);
            sources.Add(await LoadSource(prefix, filePath, false, cancellationToken).ConfigureAwait(false)
                ?? throw new InvalidOperationException($"'{filePath}' is not an OpenAPI document."));
        }
        return MergeSources(sources, cancellationToken);
    }

    public async ValueTask<OpenApiDocument> MergeDirectory(string directoryPath, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(directoryPath);
        cancellationToken.ThrowIfCancellationRequested();
        if (!await _directoryUtil.Exists(directoryPath, cancellationToken).ConfigureAwait(false))
            throw new DirectoryNotFoundException($"OpenAPI directory was not found: {directoryPath}");

        var sources = new List<SourceDocument>();
        foreach (string file in Directory.EnumerateFiles(directoryPath, "*", new EnumerationOptions
                 { RecurseSubdirectories = true, IgnoreInaccessible = false, AttributesToSkip = FileAttributes.ReparsePoint })
                     .Where(IsSupportedFile).OrderBy(static file => file, StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            SourceDocument? source = await LoadSource(Path.GetFileNameWithoutExtension(file), file, true, cancellationToken).ConfigureAwait(false);
            if (source != null)
                sources.Add(source);
        }
        return MergeSources(sources, cancellationToken);
    }

    public async ValueTask<OpenApiDocument> MergeGitUrl(string gitUrl, string? repositorySubdirectory = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(gitUrl);
        string repositoryDirectory = await _gitUtil.CloneToTempDirectory(gitUrl, cancellationToken: cancellationToken).ConfigureAwait(false);
        string repositoryRoot = Path.TrimEndingDirectorySeparator(Path.GetFullPath(repositoryDirectory));
        string targetDirectory = Path.GetFullPath(Path.Combine(repositoryRoot, repositorySubdirectory ?? ""));
        string relative = Path.GetRelativePath(repositoryRoot, targetDirectory);
        if (Path.IsPathRooted(relative) || relative == ".." || relative.StartsWith(".." + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            throw new InvalidOperationException("The repository subdirectory must remain inside the cloned repository.");
        for (DirectoryInfo? directory = new(targetDirectory); directory != null && !_filePathComparer.Equals(directory.FullName, repositoryRoot); directory = directory.Parent)
            if (directory.Exists && (directory.Attributes & FileAttributes.ReparsePoint) != 0)
                throw new InvalidOperationException("The repository subdirectory must not traverse symbolic links or junctions.");
        return await MergeDirectory(targetDirectory, cancellationToken).ConfigureAwait(false);
    }

    public string ToJson(OpenApiDocument document)
    {
        ArgumentNullException.ThrowIfNull(document);
        using var text = new StringWriter(new StringBuilder(4096));
        var writer = new OpenApiJsonWriter(text);
        if (_versions.TryGetValue(document, out OutputVersion? version) && version.Is31)
            document.SerializeAsV31(writer);
        else
            document.SerializeAsV3(writer);
        return EscapeUnescapedJsonControlCharacters(text.ToString());
    }

    private async ValueTask<SourceDocument?> LoadSource(string prefix, string path, bool discovery, CancellationToken cancellationToken)
    {
        string fullPath = Path.GetFullPath(path);
        if (!await _fileUtil.Exists(fullPath, cancellationToken).ConfigureAwait(false))
            throw new FileNotFoundException($"OpenAPI file was not found: {fullPath}", fullPath);
        string content = await _fileUtil.Read(fullPath, log: false, cancellationToken).ConfigureAwait(false);
        JsonObject? root;
        try
        {
            string json = IsYamlFile(fullPath)
                ? _yamlUtil.YamlToJson(content) ?? throw new InvalidOperationException($"Cannot convert YAML '{fullPath}'.")
                : EscapeUnescapedJsonControlCharacters(content);
            root = JsonNode.Parse(json, documentOptions: new JsonDocumentOptions { AllowDuplicateProperties = false }) as JsonObject;
        }
        catch (Exception ex) when (ex is not OperationCanceledException && discovery && !LooksLikeOpenApi(content))
        {
            _logger.LogDebug(ex, "Skipping non-OpenAPI file {FilePath}.", fullPath);
            return null;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw new InvalidOperationException($"Invalid OpenAPI JSON or YAML in '{fullPath}': {ex.Message}", ex);
        }

        if (root == null || (!root.ContainsKey("openapi") && !root.ContainsKey("swagger")))
        {
            if (discovery)
                return null;
            throw new InvalidOperationException($"'{fullPath}' has no OpenAPI version declaration.");
        }
        string? version = StringValue(root["openapi"]);
        if (version == null || !System.Text.RegularExpressions.Regex.IsMatch(version, @"^3\.[01]\.\d+$"))
            throw new InvalidOperationException($"'{fullPath}' must use OpenAPI 3.0 or 3.1; unsupported versions cannot be merged without losing semantics.");
        if (root["info"] is not JsonObject info || string.IsNullOrWhiteSpace(StringValue(info["title"])) || string.IsNullOrWhiteSpace(StringValue(info["version"])))
            throw new InvalidOperationException($"'{fullPath}' requires info.title and info.version.");
        // Shared component documents are common inputs to multi-file specifications.
        if (!root.ContainsKey("paths") && root["components"] is JsonObject)
            root["paths"] = new JsonObject();
        if (root["paths"] is not JsonObject && !(version.StartsWith("3.1.", StringComparison.Ordinal) && root["webhooks"] is JsonObject))
            throw new InvalidOperationException($"'{fullPath}' requires a paths object (or webhooks for OpenAPI 3.1).");
        ValidatePrefix(prefix);
        if (version.StartsWith("3.0.", StringComparison.Ordinal) && (root.ContainsKey("webhooks") || root.ContainsKey("jsonSchemaDialect") || root["components"]?["pathItems"] != null))
            throw new InvalidOperationException($"'{fullPath}' uses OpenAPI 3.1 fields with an OpenAPI 3.0 version declaration.");
        return new SourceDocument(fullPath, prefix.Trim('/'), root);
    }

    private OpenApiDocument MergeSources(List<SourceDocument> sources, CancellationToken cancellationToken)
    {
        if (sources.Count == 0)
            throw new InvalidOperationException("No readable OpenAPI documents were found in the provided inputs.");
        var lookup = new Dictionary<string, SourceDocument>(_filePathComparer);
        foreach (SourceDocument source in sources)
            if (!lookup.TryAdd(source.FilePath, source))
                throw new InvalidOperationException($"OpenAPI file '{source.FilePath}' was supplied more than once; reference targets would be ambiguous.");

        bool is31 = sources.Any(static source => StringValue(source.Document["openapi"])!.StartsWith("3.1.", StringComparison.Ordinal));
        Dictionary<string, HashSet<string>> reserved = JsonTraversal.ComponentSections.ToDictionary(static section => section.Section, static _ => new HashSet<string>(StringComparer.Ordinal));
        var operationIds = new HashSet<string>(StringComparer.Ordinal);
        foreach (SourceDocument source in sources)
        {
            cancellationToken.ThrowIfCancellationRequested();
            // The OpenAPI model performs the 3.0 nullable/exclusive-bound conversion when mixing versions.
            if (is31 && StringValue(source.Document["openapi"])!.StartsWith("3.0.", StringComparison.Ordinal))
                source.Document = UpgradeTo31(source.Document, source.FilePath);
            AllocateNames(source, reserved);
            JsonTraversal.Visit(source.Document, ObjectKind.Document, (obj, kind, pointer) =>
            {
                if (kind == ObjectKind.Operation && StringValue(obj["operationId"]) is string id && !source.OperationPointers.TryAdd(id, pointer))
                    source.AmbiguousOperationIds.Add(id);
                if (kind == ObjectKind.Schema)
                {
                    if (obj.ContainsKey("$id") || obj.ContainsKey("$dynamicRef") || obj.ContainsKey("$dynamicAnchor"))
                        throw new InvalidOperationException($"Schema resource scopes ($id/$dynamicRef/$dynamicAnchor) cannot be safely relocated: '{source.FilePath}' at {pointer}.");
                    if (StringValue(obj["$anchor"]) is string anchor && !source.Anchors.TryAdd(anchor, pointer))
                        throw new InvalidOperationException($"Duplicate schema anchor '{anchor}' in '{source.FilePath}'.");
                }
            });
        }

        // Keep originals intact until every cross-document pointer has been resolved.
        var transformed = new List<(SourceDocument Source, JsonObject Root)>();
        foreach (SourceDocument source in sources)
        {
            cancellationToken.ThrowIfCancellationRequested();
            JsonObject root = (JsonObject)source.Document.DeepClone();
            JsonTraversal.Visit(root, ObjectKind.Document, (obj, kind, pointer) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                // Introduced allOf children must still contain source references when traversal visits them.
                if (kind == ObjectKind.Schema && is31)
                    PreserveSchemaConstraints(obj);
                if (StringValue(obj["$ref"]) is string reference)
                    obj["$ref"] = RewriteReference(reference, source, lookup);
                if (kind == ObjectKind.Schema)
                {
                    obj.Remove("$anchor"); // All anchors are rewritten to unambiguous JSON pointers.
                    RewriteDiscriminator(obj, source, lookup, pointer);
                }
                if (kind is ObjectKind.Operation or ObjectKind.Document)
                    RewriteSecurity(obj, source);
                if (kind == ObjectKind.Operation)
                {
                    string baseId = ToSafeId(source.Prefix) + "_" + ToSafeId(StringValue(obj["operationId"]) ?? pointer);
                    string id = baseId;
                    for (int suffix = 2; !operationIds.Add(id); suffix++)
                        id = baseId + "_" + suffix;
                    obj["operationId"] = id;
                }
                if (kind == ObjectKind.Link)
                {
                    if (obj.ContainsKey("operationId") && obj.ContainsKey("operationRef"))
                        throw new InvalidOperationException($"Link at {pointer} cannot specify both operationId and operationRef.");
                    if (StringValue(obj["operationId"]) is string targetId)
                    {
                        if (source.AmbiguousOperationIds.Contains(targetId))
                            throw new InvalidOperationException($"Link at {pointer} references ambiguous operationId '{targetId}' in '{source.FilePath}'.");
                        if (!source.OperationPointers.TryGetValue(targetId, out string? targetPointer))
                            throw new InvalidOperationException($"Link at {pointer} references missing operationId '{targetId}' in '{source.FilePath}'.");
                        obj.Remove("operationId");
                        obj["operationRef"] = RewriteReference(targetPointer, source, lookup);
                    }
                    else if (StringValue(obj["operationRef"]) is string operationRef)
                        obj["operationRef"] = RewriteReference(operationRef, source, lookup);
                }
            });
            RenameKeys(root["components"] as JsonObject, source.ComponentRenameMaps);
            RenamePaths(root, source.Prefix);
            transformed.Add((source, root));
        }

        var merged = new JsonObject
        {
            ["openapi"] = is31 ? "3.1.1" : "3.0.3",
            ["info"] = new JsonObject { ["title"] = "Merged OpenAPI", ["version"] = "1.0.0" },
            ["paths"] = new JsonObject(), ["components"] = new JsonObject()
        };
        // Components must all be available before comparing operations that use cross-file references.
        foreach ((_, JsonObject root) in transformed)
            MergeComponents(merged, root);

        foreach ((_, JsonObject root) in transformed)
        {
            cancellationToken.ThrowIfCancellationRequested();
            NormalizeInheritedSettings(root, merged, transformed);
        }
        merged["components"] = new JsonObject();
        foreach ((_, JsonObject root) in transformed)
            MergeComponents(merged, root);

        var postmanCollections = new JsonArray();
        var documentMetadata = new JsonArray();
        foreach ((SourceDocument source, JsonObject root) in transformed)
        {
            cancellationToken.ThrowIfCancellationRequested();
            MergePathMap(merged, root, "paths");
            MergePathMap(merged, root, "webhooks");
            MergeTags(merged, root);
            var postmanMetadata = new JsonObject();
            var scopedMetadata = new JsonObject();
            foreach ((string key, JsonNode? value) in root.Where(static entry => entry.Key.StartsWith("x-", StringComparison.Ordinal) || entry.Key == "jsonSchemaDialect"))
            {
                // These converter payloads describe the source collection, not the merged API.
                // In particular, collection variables and scripts must keep their original scope.
                if (sources.Count > 1 && key is "x-postman-warnings" or "x-postman-variables" or "x-postman-events" or "x-postman-unmapped-requests")
                    postmanMetadata[key] = value?.DeepClone();
                // Samples and tag groups describe their source document, not the combined API.
                else if (sources.Count > 1 && key is "x-samples" or "x-tagGroups")
                    scopedMetadata[key] = value?.DeepClone();
                else
                    MergeMetadata(merged, key, value, "document");
            }
            if (postmanMetadata.Count > 0)
                postmanCollections.Add((JsonNode?)new JsonObject { ["prefix"] = source.Prefix, ["metadata"] = postmanMetadata });
            if (scopedMetadata.Count > 0)
                documentMetadata.Add((JsonNode?)new JsonObject { ["prefix"] = source.Prefix, ["metadata"] = scopedMetadata });
        }
        if (postmanCollections.Count > 0)
            MergeMetadata(merged, "x-merged-postman-collections", postmanCollections, "document");
        if (documentMetadata.Count > 0)
            MergeMetadata(merged, "x-merged-document-metadata", documentMetadata, "document");
        // References to reusable path items/callbacks are also part of the resulting document.
        EnsureUniqueOperationIds(merged);
        ValidateContract(merged, cancellationToken);
        PromoteNestedReferenceTargets(merged);
        EnsureUniqueOperationIds(merged);
        ValidateContract(merged, cancellationToken);
        OpenApiDocument result = ReadModel(merged, "merged output", validate: true);
        new OpenApiWalker(new SchemaKeywordPreserver(is31)).Walk(result);
        _versions.Add(result, new OutputVersion(is31));
        // Verify the actual consumer-facing serialization, including model reference resolution.
        JsonObject emitted = JsonNode.Parse(ToJson(result))!.AsObject();
        ValidateContract(emitted, cancellationToken);
        return result;
    }

    private static void AllocateNames(SourceDocument source, Dictionary<string, HashSet<string>> reserved)
    {
        foreach ((string section, _) in JsonTraversal.ComponentSections)
        {
            var names = new Dictionary<string, string>(StringComparer.Ordinal);
            source.ComponentRenameMaps[section] = names;
            if (source.Document["components"]?[section] is not JsonObject entries)
                continue;
            foreach ((string name, _) in entries)
            {
                string candidate = IsComponentName(name) ? name : ToSafeId(name);
                if (reserved[section].Contains(candidate))
                    candidate = ToSafeId(source.Prefix) + "_" + candidate;
                string baseName = candidate;
                for (int suffix = 2; !reserved[section].Add(candidate); suffix++)
                    candidate = baseName + "_" + suffix;
                names.Add(name, candidate);
            }
        }
    }

    private static string RewriteReference(string reference, SourceDocument source, Dictionary<string, SourceDocument> lookup)
    {
        int hash = reference.IndexOf('#');
        string location = hash < 0 ? reference : reference[..hash];
        string fragment = hash < 0 ? "#" : reference[hash..];
        SourceDocument target = source;
        if (location.Length > 0)
        {
            if (Uri.TryCreate(location, UriKind.Absolute, out Uri? absolute) && !absolute.IsFile)
                throw new InvalidOperationException($"External reference '{reference}' in '{source.FilePath}' is not an included local document.");
            string targetPath = absolute?.IsFile == true ? absolute.LocalPath
                : Path.GetFullPath(Path.Combine(Path.GetDirectoryName(source.FilePath)!, Uri.UnescapeDataString(location)));
            if (!lookup.TryGetValue(targetPath, out target!))
            {
                // Runners can convert YAML files to JSON without rewriting their relative references.
                string extension = Path.GetExtension(targetPath);
                if (!(extension.Equals(".yaml", StringComparison.OrdinalIgnoreCase) || extension.Equals(".yml", StringComparison.OrdinalIgnoreCase)) ||
                    !lookup.TryGetValue(Path.ChangeExtension(targetPath, ".json"), out target!))
                    throw new InvalidOperationException($"Reference '{reference}' in '{source.FilePath}' targets a file that was not included in the merge.");
            }
        }
        fragment = Uri.UnescapeDataString(fragment);
        if (!fragment.StartsWith("#/", StringComparison.Ordinal))
        {
            if (!target.Anchors.TryGetValue(fragment.TrimStart('#'), out string? anchorPointer))
                throw new InvalidOperationException($"Unsupported or unresolved reference '{reference}' in '{source.FilePath}'.");
            fragment = anchorPointer;
        }
        if (!TryResolvePointer(target.Document, fragment, out _, uriEncoded: false))
            throw new InvalidOperationException($"Unresolved reference '{reference}' in '{source.FilePath}'.");
        string[] segments = fragment[2..].Split('/').Select(JsonTraversal.Unescape).ToArray();
        if (segments.Length >= 3 && segments[0] == "components" && target.ComponentRenameMaps.TryGetValue(segments[1], out Dictionary<string, string>? names)
            && names.TryGetValue(segments[2], out string? name))
            segments[2] = name;
        else if (segments.Length >= 2 && segments[0] == "paths")
            segments[1] = PrefixPath(target.Prefix, segments[1]);
        else if (segments.Length >= 2 && segments[0] == "webhooks")
            segments[1] = ToSafeId(target.Prefix) + "_" + segments[1];
        else
            throw new InvalidOperationException($"Reference '{reference}' targets a document location that is not retained by the merge.");
        return "#/" + string.Join('/', segments.Select(static segment => Uri.EscapeDataString(JsonTraversal.Escape(segment))));
    }

    private static void RewriteDiscriminator(JsonObject schema, SourceDocument source, Dictionary<string, SourceDocument> lookup, string pointer)
    {
        if (schema["discriminator"] is not JsonObject discriminator)
            return;
        if (discriminator["mapping"] is not JsonObject mapping)
        {
            mapping = new JsonObject();
            discriminator["mapping"] = mapping;
        }
        // Explicitly retain implicit discriminator values when a component gets renamed.
        foreach (string keyword in new[] { "oneOf", "anyOf" })
            if (schema[keyword] is JsonArray alternatives)
                foreach (JsonObject alternative in alternatives.OfType<JsonObject>())
                    if (StringValue(alternative["$ref"]) is string reference && reference.StartsWith("#/components/schemas/", StringComparison.Ordinal))
                    {
                        string name = JsonTraversal.Unescape(Uri.UnescapeDataString(reference["#/components/schemas/".Length..]));
                        if (!name.Contains('/') && !mapping.ContainsKey(name))
                            mapping[name] = reference;
                    }
        // allOf inheritance can use an implicit discriminator without listing descendants on the base schema.
        if (source.Document["components"]?["schemas"] is JsonObject schemas)
        {
            var descendants = new HashSet<string>(StringComparer.Ordinal) { pointer };
            bool added;
            do
            {
                added = false;
                foreach ((string name, JsonNode? child) in schemas)
                {
                    string childPointer = "#/components/schemas/" + JsonTraversal.Escape(name);
                    if (descendants.Contains(childPointer) || child is not JsonObject childSchema || childSchema["allOf"] is not JsonArray parents)
                        continue;
                    if (!parents.OfType<JsonObject>().Any(parent => StringValue(parent["$ref"]) is string parentRef && descendants.Contains(Uri.UnescapeDataString(parentRef))))
                        continue;
                    descendants.Add(childPointer);
                    if (!mapping.ContainsKey(name))
                        mapping[name] = childPointer;
                    added = true;
                }
            } while (added);
        }
        foreach ((string key, JsonNode? value) in mapping.ToArray())
        {
            string reference = StringValue(value) ?? throw new InvalidOperationException($"Invalid discriminator mapping '{key}' in '{source.FilePath}'.");
            if (source.ComponentRenameMaps["schemas"].ContainsKey(reference))
                reference = "#/components/schemas/" + JsonTraversal.Escape(reference);
            mapping[key] = RewriteReference(reference, source, lookup);
        }
    }

    private static void RewriteSecurity(JsonObject obj, SourceDocument source)
    {
        if (!obj.ContainsKey("security"))
            return;
        if (obj["security"] is not JsonArray requirements)
            throw new InvalidOperationException($"Security must be an array in '{source.FilePath}'.");
        foreach (JsonNode? requirement in requirements)
        {
            if (requirement is not JsonObject original)
                throw new InvalidOperationException($"Invalid security requirement in '{source.FilePath}'.");
            var replacements = new JsonObject();
            foreach ((string key, JsonNode? value) in original)
            {
                if (!source.ComponentRenameMaps["securitySchemes"].TryGetValue(key, out string? renamed))
                    throw new InvalidOperationException($"Security requirement references missing security scheme '{key}' in '{source.FilePath}'.");
                replacements[renamed] = value?.DeepClone();
            }
            original.Clear();
            foreach ((string key, JsonNode? value) in replacements)
                original[key] = value?.DeepClone();
        }
    }

    private static void RenameKeys(JsonObject? components, Dictionary<string, Dictionary<string, string>> maps)
    {
        if (components == null)
            return;
        foreach ((string section, Dictionary<string, string> names) in maps)
        {
            if (components[section] is not JsonObject entries)
                continue;
            var replacement = new JsonObject();
            foreach ((string key, JsonNode? value) in entries)
                replacement[names[key]] = value?.DeepClone();
            components[section] = replacement;
        }
    }

    private static void RenamePaths(JsonObject root, string prefix)
    {
        foreach (string section in new[] { "paths", "webhooks" })
        {
            if (root[section] is not JsonObject paths)
                continue;
            var replacement = new JsonObject();
            foreach ((string key, JsonNode? value) in paths)
            {
                string newKey = section == "paths" && key.StartsWith("x-", StringComparison.Ordinal) ? key : section == "paths" ? PrefixPath(prefix, key) : ToSafeId(prefix) + "_" + key;
                if (!replacement.TryAdd(newKey, value?.DeepClone()))
                    throw new InvalidOperationException($"Multiple paths become '{newKey}' after applying prefix '{prefix}'.");
            }
            root[section] = replacement;
        }
    }

    private static void MergeComponents(JsonObject merged, JsonObject source)
    {
        if (source["components"] is not JsonObject components)
            return;
        foreach ((string section, JsonNode? entries) in components)
        {
            if (section.StartsWith("x-", StringComparison.Ordinal))
            {
                MergeMetadata(merged["components"]!.AsObject(), section, entries, "components");
                continue;
            }
            if (!JsonTraversal.ComponentSections.Any(item => item.Section == section) || entries is not JsonObject map)
                throw new InvalidOperationException($"Unsupported component section '{section}'.");
            JsonObject destination = (merged["components"]![section] ??= new JsonObject()).AsObject();
            foreach ((string name, JsonNode? value) in map)
                if (!destination.TryAdd(name, value?.DeepClone()))
                    throw new InvalidOperationException($"Component name collision: {section}/{name}.");
        }
    }

    private OpenApiDocument ReadModel(JsonObject root, string context, bool validate)
    {
        using MemoryStream stream = _memoryStreamUtil.GetSync();
        using (var writer = new Utf8JsonWriter(stream))
            root.WriteTo(writer);
        stream.Position = 0;
        ReadResult read = OpenApiDocument.Load(stream, OpenApiConstants.Json, new OpenApiReaderSettings
        {
            LoadExternalRefs = false,
            RuleSet = validate ? ValidationRuleSet.GetDefaultRuleSet() : ValidationRuleSet.GetEmptyRuleSet()
        });
        if (read.Document == null || read.Diagnostic?.Errors.Count > 0)
            throw new InvalidOperationException($"Invalid {context}: " + string.Join("; ", read.Diagnostic?.Errors.Select(static error => error.ToString()) ?? []));
        if (validate)
        {
            OpenApiError[] errors = read.Document.Validate(ValidationRuleSet.GetDefaultRuleSet()).ToArray();
            if (errors.Length > 0)
                throw new InvalidOperationException($"Invalid {context}: " + string.Join("; ", errors.Select(static error => error.ToString())));
        }
        return read.Document;
    }

    private JsonObject UpgradeTo31(JsonObject root, string context)
    {
        // Normalize publisher schema shapes before the model reader sees the 3.0 document.
        JsonTraversal.Visit(root, ObjectKind.Document, static (_, _, _) => { });
        OpenApiDocument document = ReadModel(root, context, validate: false);
        new OpenApiWalker(new SchemaKeywordPreserver(false)).Walk(document);
        using var text = new StringWriter();
        document.SerializeAsV31(new OpenApiJsonWriter(text));
        return JsonNode.Parse(text.ToString())!.AsObject();
    }

    private static void PreserveSchemaConstraints(JsonObject schema)
    {
        // The model exposes const as string; singleton enums preserve non-string JSON values exactly.
        if (schema.TryGetPropertyValue("const", out JsonNode? constant) && StringValue(constant) == null)
        {
            var values = new JsonArray(constant?.DeepClone());
            if (schema.ContainsKey("enum"))
                (schema["allOf"] ??= new JsonArray()).AsArray().Add((JsonNode?)new JsonObject { ["enum"] = values });
            else
                schema["enum"] = values;
            schema.Remove("const");
        }
        // Reference-holder models can omit JSON Schema siblings. allOf preserves the same conjunction.
        if (schema.Count > 1 && StringValue(schema["$ref"]) is string reference)
        {
            schema.Remove("$ref");
            (schema["allOf"] ??= new JsonArray()).AsArray().Insert(0, new JsonObject { ["$ref"] = reference });
        }
    }

    private static void ValidatePrefix(string prefix)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(prefix);
        string trimmed = prefix.Trim('/');
        if (trimmed.Length == 0 || trimmed.Any(static ch => char.IsWhiteSpace(ch) || ch is '?' or '#' or '{' or '}' or '\\') ||
            trimmed.Split('/').Any(static segment => segment is "" or "." or ".."))
            throw new ArgumentException($"Invalid OpenAPI path prefix '{prefix}'.", nameof(prefix));
    }

    private static string PrefixPath(string prefix, string path)
    {
        // Template names identify parameters; they are replaced by values before constructing the request URL.
        string literalPath = System.Text.RegularExpressions.Regex.Replace(path, @"\{[^{}]+\}", "");
        if (!path.StartsWith('/') || literalPath.Contains('?') || literalPath.Contains('#') || literalPath.Any(char.IsWhiteSpace))
            throw new InvalidOperationException($"Invalid OpenAPI path '{path}'.");
        string start = "/" + prefix.Trim('/');
        return path == start || path.StartsWith(start + "/", StringComparison.Ordinal) ? path : start + path;
    }

    private static string ToSafeId(string value)
    {
        var builder = new StringBuilder(value.Length);
        foreach (char ch in value)
        {
            char next = char.IsAsciiLetterOrDigit(ch) || ch == '_' ? ch : '_';
            if (next != '_' || builder.Length == 0 || builder[^1] != '_')
                builder.Append(next);
        }
        string result = builder.ToString().Trim('_');
        return result.Length == 0 ? "default" : result;
    }

    private static bool IsComponentName(string name) => name.Length > 0 && name.All(static ch => char.IsAsciiLetterOrDigit(ch) || ch is '_' or '-' or '.');
    private static string? StringValue(JsonNode? node) => node is JsonValue value && value.TryGetValue(out string? result) ? result : null;
    private static bool IsYamlFile(string path) => Path.GetExtension(path).Equals(".yaml", StringComparison.OrdinalIgnoreCase) || Path.GetExtension(path).Equals(".yml", StringComparison.OrdinalIgnoreCase);
    private static bool IsSupportedFile(string path) => IsYamlFile(path) || Path.GetExtension(path).Equals(".json", StringComparison.OrdinalIgnoreCase);
    private static bool LooksLikeOpenApi(string content) => System.Text.RegularExpressions.Regex.IsMatch(content, "(?m)^\\s*(?:[\"']?(?:openapi|swagger)[\"']?)\\s*:") || content.Contains("\"openapi\"", StringComparison.Ordinal) || content.Contains("\"swagger\"", StringComparison.Ordinal);

    private static string EscapeUnescapedJsonControlCharacters(string json)
    {
        var builder = new StringBuilder(json.Length);
        bool inString = false;
        bool escaped = false;
        foreach (char ch in json)
        {
            if (inString && ch < ' ')
            {
                if (escaped)
                    throw new JsonException("A control character cannot follow an unpaired JSON escape character.");
                builder.Append("\\u").Append(((int)ch).ToString("X4"));
                escaped = false;
                continue;
            }
            builder.Append(ch);
            if (escaped)
                escaped = false;
            else if (inString && ch == '\\')
                escaped = true;
            else if (ch == '"')
                inString = !inString;
        }
        return builder.ToString();
    }
}
