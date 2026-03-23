using Microsoft.Extensions.Logging;
using Soenneker.Git.Util.Abstract;
using Soenneker.OpenApi.Merger.Abstract;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.OpenApi;
using Microsoft.OpenApi.Reader;
using Soenneker.OpenApi.Merger.Dtos;
using Soenneker.Extensions.String;
using Soenneker.Extensions.Task;
using Soenneker.Extensions.ValueTask;

namespace Soenneker.OpenApi.Merger;

/// <inheritdoc cref="IOpenApiMerger"/>
public sealed class OpenApiMerger : IOpenApiMerger
{
    private static readonly string[] _componentSections =
    [
        "schemas",
        "parameters",
        "responses",
        "requestBodies",
        "headers",
        "securitySchemes",
        "links",
        "callbacks",
        "examples"
    ];

    private readonly ILogger<OpenApiMerger> _logger;
    private readonly IGitUtil _gitUtil;

    public OpenApiMerger(ILogger<OpenApiMerger> logger, IGitUtil gitUtil)
    {
        _logger = logger;
        _gitUtil = gitUtil;
    }

    public ValueTask<OpenApiDocument> MergeOpenApis(params (string prefix, string filePath)[] inputs)
    {
        return MergeOpenApis(inputs, CancellationToken.None);
    }

    public async ValueTask<OpenApiDocument> MergeOpenApis(IEnumerable<(string prefix, string filePath)> inputs,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(inputs);

        (string prefix, string filePath)[] sourceInputs = inputs.ToArray();

        if (sourceInputs.Length == 0)
            throw new InvalidOperationException("No OpenAPI inputs were provided.");

        List<SourceDocument> sourceDocuments = await LoadSourceDocuments(sourceInputs, cancellationToken).NoSync();

        if (sourceDocuments.Count == 0)
            throw new InvalidOperationException("No readable OpenAPI documents were found in the provided inputs.");

        return await MergeSourceDocuments(sourceDocuments, cancellationToken).NoSync();
    }

    public async ValueTask<OpenApiDocument> MergeDirectory(string directoryPath, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(directoryPath))
            throw new ArgumentException("Directory path cannot be null or whitespace.", nameof(directoryPath));

        if (!Directory.Exists(directoryPath))
            throw new DirectoryNotFoundException($"OpenAPI directory was not found: {directoryPath}");

        string[] files = Directory.GetFiles(directoryPath, "*.*", SearchOption.AllDirectories)
                                  .Where(IsSupportedOpenApiFile)
                                  .OrderBy(static file => file, StringComparer.OrdinalIgnoreCase)
                                  .ToArray();

        if (files.Length == 0)
            throw new InvalidOperationException($"No OpenAPI files were found beneath '{directoryPath}'.");

        List<SourceDocument> sourceDocuments = await LoadSourceDocuments(files.Select(static file => (Path.GetFileNameWithoutExtension(file), file)),
            cancellationToken).NoSync();

        if (sourceDocuments.Count == 0)
            throw new InvalidOperationException($"No readable OpenAPI documents were found beneath '{directoryPath}'.");

        return await MergeSourceDocuments(sourceDocuments, cancellationToken).NoSync();
    }

    public async ValueTask<OpenApiDocument> MergeGitUrl(string gitUrl, string? repositorySubdirectory = null,
        CancellationToken cancellationToken = default)
    {
        if (gitUrl.IsNullOrWhiteSpace())
            throw new ArgumentException("Git URL cannot be null or whitespace.", nameof(gitUrl));

        string repositoryDirectory = await _gitUtil.CloneToTempDirectory(gitUrl, cancellationToken: cancellationToken).NoSync();

        string targetDirectory = repositoryDirectory;

        if (!string.IsNullOrWhiteSpace(repositorySubdirectory))
        {
            targetDirectory = Path.Combine(repositoryDirectory, repositorySubdirectory);

            if (!Directory.Exists(targetDirectory))
                throw new DirectoryNotFoundException($"Repository subdirectory was not found: {targetDirectory}");
        }

        return await MergeDirectory(targetDirectory, cancellationToken).NoSync();
    }

    public string ToJson(OpenApiDocument document)
    {
        ArgumentNullException.ThrowIfNull(document);

        using var stringWriter = new StringWriter(new StringBuilder(4096));
        var writer = new OpenApiJsonWriter(stringWriter);
        document.SerializeAsV3(writer);

        return stringWriter.ToString();
    }

    private async ValueTask<List<SourceDocument>> LoadSourceDocuments(IEnumerable<(string prefix, string filePath)> inputs,
        CancellationToken cancellationToken)
    {
        var sourceDocuments = new List<SourceDocument>();

        foreach ((string prefix, string filePath) in inputs)
        {
            if (string.IsNullOrWhiteSpace(prefix))
                throw new ArgumentException("OpenAPI input prefix cannot be null or whitespace.", nameof(inputs));

            if (string.IsNullOrWhiteSpace(filePath))
                throw new ArgumentException("OpenAPI input file path cannot be null or whitespace.", nameof(inputs));

            cancellationToken.ThrowIfCancellationRequested();

            string fullPath = Path.GetFullPath(filePath);

            if (!File.Exists(fullPath))
                throw new FileNotFoundException($"OpenAPI file was not found: {fullPath}", fullPath);

            OpenApiDocument? document = await TryLoadDocument(fullPath, cancellationToken).NoSync();

            if (document == null)
                continue;

            sourceDocuments.Add(new SourceDocument(fullPath, prefix, document));
        }

        return sourceDocuments;
    }

    private ValueTask<OpenApiDocument?> TryLoadDocument(string filePath, CancellationToken cancellationToken)
    {
        return TryLoadDocumentInternal(filePath, cancellationToken);
    }

    private async ValueTask<OpenApiDocument?> TryLoadDocumentInternal(string filePath, CancellationToken cancellationToken)
    {
        try
        {
            cancellationToken.ThrowIfCancellationRequested();

            await using FileStream stream = File.OpenRead(filePath);
            ReadResult readResult = await OpenApiDocument.LoadAsync(stream, GetOpenApiFormat(filePath), new OpenApiReaderSettings(), cancellationToken)
                                                         .NoSync();
            OpenApiDocument? document = readResult.Document;
            OpenApiDiagnostic? diagnostic = readResult.Diagnostic;

            if (document == null)
            {
                _logger.LogDebug("Skipping {FilePath} because it did not produce an OpenAPI document.", filePath);
                return null;
            }

            if (diagnostic?.Errors?.Count > 0)
                _logger.LogWarning("OpenAPI read for {FilePath} finished with {ErrorCount} diagnostic errors.", filePath, diagnostic.Errors.Count);

            return document;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Skipping non-OpenAPI file {FilePath}.", filePath);
            return null;
        }
    }

    private async ValueTask<OpenApiDocument> MergeSourceDocuments(List<SourceDocument> sourceDocuments, CancellationToken cancellationToken)
    {
        Dictionary<string, HashSet<string>> reservedComponentNames = CreateReservedComponentNames();

        foreach (SourceDocument sourceDocument in sourceDocuments)
        {
            sourceDocument.ComponentRenameMaps = BuildComponentRenameMaps(sourceDocument.Prefix, sourceDocument.Document.Components, reservedComponentNames);
            AddReservedComponentNames(sourceDocument.ComponentRenameMaps, reservedComponentNames);
        }

        Dictionary<string, SourceDocument> sourceLookup = sourceDocuments.ToDictionary(static source => NormalizePath(source.FilePath), StringComparer.OrdinalIgnoreCase);

        var merged = new OpenApiDocument
        {
            Info = new OpenApiInfo
            {
                Title = "Merged OpenAPI",
                Version = "1.0.0"
            },
            Paths = new OpenApiPaths(),
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>(),
                Parameters = new Dictionary<string, IOpenApiParameter>(),
                Responses = new Dictionary<string, IOpenApiResponse>(),
                RequestBodies = new Dictionary<string, IOpenApiRequestBody>(),
                Headers = new Dictionary<string, IOpenApiHeader>(),
                SecuritySchemes = new Dictionary<string, IOpenApiSecurityScheme>(),
                Links = new Dictionary<string, IOpenApiLink>(),
                Callbacks = new Dictionary<string, IOpenApiCallback>(),
                Examples = new Dictionary<string, IOpenApiExample>()
            },
            Servers = new List<OpenApiServer>()
        };

        foreach (SourceDocument sourceDocument in sourceDocuments)
        {
            cancellationToken.ThrowIfCancellationRequested();

            OpenApiDocument transformed = await TransformDocument(sourceDocument, sourceLookup, cancellationToken).NoSync();
            MergeInto(merged, transformed, sourceDocument.Prefix);
        }

        return merged;
    }

    private ValueTask<OpenApiDocument> TransformDocument(SourceDocument sourceDocument, Dictionary<string, SourceDocument> sourceLookup,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        JsonNode root = JsonNode.Parse(ToJson(sourceDocument.Document))
                        ?? throw new InvalidOperationException($"Failed to parse serialized OpenAPI document '{sourceDocument.FilePath}'.");

        RenameComponentKeys(root, sourceDocument.ComponentRenameMaps);
        RewriteComponentReferences(root, sourceDocument, sourceLookup);

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(root.ToJsonString()));
        ReadResult readResult = OpenApiDocument.Load(stream, OpenApiConstants.Json, new OpenApiReaderSettings());
        OpenApiDocument? transformed = readResult.Document;
        OpenApiDiagnostic? diagnostic = readResult.Diagnostic;

        if (transformed == null)
            throw new InvalidOperationException($"Failed to rehydrate transformed OpenAPI document '{sourceDocument.FilePath}'.");

        if (diagnostic?.Errors?.Count > 0)
            _logger.LogWarning("Transformed OpenAPI document for {FilePath} finished with {ErrorCount} diagnostic errors.", sourceDocument.FilePath,
                diagnostic.Errors.Count);

        return ValueTask.FromResult(transformed);
    }

    private static void MergeInto(OpenApiDocument merged, OpenApiDocument sourceDocument, string prefix)
    {
        IList<OpenApiServer> mergedServers = merged.Servers ??= [];

        foreach (OpenApiServer server in sourceDocument.Servers ?? [])
        {
            if (mergedServers.All(existing => !string.Equals(existing.Url, server.Url, StringComparison.OrdinalIgnoreCase)))
                mergedServers.Add(server);
        }

        foreach ((string pathKey, IOpenApiPathItem pathItem) in sourceDocument.Paths)
        {
            string mergedPath = PrefixPath(prefix, pathKey);
            merged.Paths[mergedPath] = pathItem;
        }

        OpenApiComponents mergedComponents = merged.Components ?? throw new InvalidOperationException("Merged document components were not initialized.");

        CopyComponentSection(sourceDocument.Components?.Schemas, mergedComponents.Schemas);
        CopyComponentSection(sourceDocument.Components?.Parameters, mergedComponents.Parameters);
        CopyComponentSection(sourceDocument.Components?.Responses, mergedComponents.Responses);
        CopyComponentSection(sourceDocument.Components?.RequestBodies, mergedComponents.RequestBodies);
        CopyComponentSection(sourceDocument.Components?.Headers, mergedComponents.Headers);
        CopyComponentSection(sourceDocument.Components?.SecuritySchemes, mergedComponents.SecuritySchemes);
        CopyComponentSection(sourceDocument.Components?.Links, mergedComponents.Links);
        CopyComponentSection(sourceDocument.Components?.Callbacks, mergedComponents.Callbacks);
        CopyComponentSection(sourceDocument.Components?.Examples, mergedComponents.Examples);
    }

    private static void CopyComponentSection<T>(IDictionary<string, T>? source, IDictionary<string, T>? destination)
    {
        if (source == null || destination == null)
            return;

        foreach ((string key, T value) in source)
        {
            destination[key] = value;
        }
    }

    private static Dictionary<string, HashSet<string>> CreateReservedComponentNames()
    {
        var result = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);

        foreach (string section in _componentSections)
        {
            result[section] = new HashSet<string>(StringComparer.Ordinal);
        }

        return result;
    }

    private static Dictionary<string, Dictionary<string, string>> BuildComponentRenameMaps(string prefix, OpenApiComponents? components,
        Dictionary<string, HashSet<string>> reservedComponentNames)
    {
        var renameMaps = new Dictionary<string, Dictionary<string, string>>(StringComparer.Ordinal);

        foreach (string section in _componentSections)
        {
            renameMaps[section] = BuildSectionRenameMap(GetComponentNames(components, section), reservedComponentNames[section], prefix);
        }

        return renameMaps;
    }

    private static Dictionary<string, string> BuildSectionRenameMap(IEnumerable<string> names, HashSet<string> reservedNames, string prefix)
    {
        var renameMap = new Dictionary<string, string>(StringComparer.Ordinal);
        var assignedNames = new HashSet<string>(StringComparer.Ordinal);
        var safePrefix = $"{ToSafeId(prefix)}_";

        foreach (string name in names)
        {
            string targetName = name;

            if (reservedNames.Contains(targetName) || assignedNames.Contains(targetName))
                targetName = safePrefix + name;

            while (reservedNames.Contains(targetName) || assignedNames.Contains(targetName))
            {
                targetName = "_" + targetName;
            }

            renameMap[name] = targetName;
            assignedNames.Add(targetName);
        }

        return renameMap;
    }

    private static IEnumerable<string> GetComponentNames(OpenApiComponents? components, string section)
    {
        return section switch
        {
            "schemas" => components?.Schemas?.Keys ?? [],
            "parameters" => components?.Parameters?.Keys ?? [],
            "responses" => components?.Responses?.Keys ?? [],
            "requestBodies" => components?.RequestBodies?.Keys ?? [],
            "headers" => components?.Headers?.Keys ?? [],
            "securitySchemes" => components?.SecuritySchemes?.Keys ?? [],
            "links" => components?.Links?.Keys ?? [],
            "callbacks" => components?.Callbacks?.Keys ?? [],
            "examples" => components?.Examples?.Keys ?? [],
            _ => []
        };
    }

    private static void AddReservedComponentNames(Dictionary<string, Dictionary<string, string>> renameMaps,
        Dictionary<string, HashSet<string>> reservedComponentNames)
    {
        foreach ((string section, Dictionary<string, string> renameMap) in renameMaps)
        {
            foreach (string renamedValue in renameMap.Values)
            {
                reservedComponentNames[section].Add(renamedValue);
            }
        }
    }

    private static void RenameComponentKeys(JsonNode root, Dictionary<string, Dictionary<string, string>> renameMaps)
    {
        if (root["components"] is not JsonObject componentsObject)
            return;

        foreach ((string section, Dictionary<string, string> renameMap) in renameMaps)
        {
            if (componentsObject[section] is not JsonObject sectionObject)
                continue;

            var renamedEntries = new Dictionary<string, JsonNode?>(StringComparer.Ordinal);

            foreach ((string originalName, string renamedName) in renameMap)
            {
                if (!sectionObject.TryGetPropertyValue(originalName, out JsonNode? value))
                    continue;

                renamedEntries[renamedName] = value?.DeepClone();
            }

            sectionObject.Clear();

            foreach ((string renamedName, JsonNode? value) in renamedEntries)
            {
                sectionObject[renamedName] = value;
            }
        }
    }

    private static void RewriteComponentReferences(JsonNode node, SourceDocument currentSource, Dictionary<string, SourceDocument> sourceLookup)
    {
        switch (node)
        {
            case JsonObject jsonObject:
            {
                if (jsonObject.TryGetPropertyValue("$ref", out JsonNode? refNode) &&
                    refNode is JsonValue refValue &&
                    refValue.TryGetValue(out string? reference) &&
                    !string.IsNullOrWhiteSpace(reference))
                {
                    jsonObject["$ref"] = RewriteReference(reference, currentSource, sourceLookup);
                }

                foreach ((_, JsonNode? child) in jsonObject.ToList())
                {
                    if (child != null)
                        RewriteComponentReferences(child, currentSource, sourceLookup);
                }

                break;
            }
            case JsonArray jsonArray:
            {
                foreach (JsonNode? child in jsonArray)
                {
                    if (child != null)
                        RewriteComponentReferences(child, currentSource, sourceLookup);
                }

                break;
            }
        }
    }

    private static string RewriteReference(string reference, SourceDocument currentSource, Dictionary<string, SourceDocument> sourceLookup)
    {
        if (TryRewriteLocalComponentReference(reference, currentSource.ComponentRenameMaps, out string rewrittenLocalReference))
            return rewrittenLocalReference;

        int fragmentIndex = reference.IndexOf('#');

        if (fragmentIndex <= 0)
            return reference;

        string relativePath = reference[..fragmentIndex];
        string fragment = reference[fragmentIndex..];

        if (!TryParseComponentReference(fragment, out string section, out string encodedName, out string suffix))
            return reference;

        string resolvedPath = NormalizePath(Path.GetFullPath(Path.Combine(Path.GetDirectoryName(currentSource.FilePath) ?? "", relativePath)));

        if (!sourceLookup.TryGetValue(resolvedPath, out SourceDocument? targetSource))
            return reference;

        if (!targetSource.ComponentRenameMaps.TryGetValue(section, out Dictionary<string, string>? renameMap))
            return reference;

        string originalName = DecodeJsonPointerSegment(encodedName);

        if (!renameMap.TryGetValue(originalName, out string? renamedName))
            return reference;

        return $"#/components/{section}/{EncodeJsonPointerSegment(renamedName)}{suffix}";
    }

    private static bool TryRewriteLocalComponentReference(string reference, Dictionary<string, Dictionary<string, string>> renameMaps, out string rewrittenReference)
    {
        rewrittenReference = string.Empty;

        if (!TryParseComponentReference(reference, out string section, out string encodedName, out string suffix))
            return false;

        if (!renameMaps.TryGetValue(section, out Dictionary<string, string>? renameMap))
            return false;

        string originalName = DecodeJsonPointerSegment(encodedName);

        if (!renameMap.TryGetValue(originalName, out string? renamedName))
            return false;

        rewrittenReference = $"#/components/{section}/{EncodeJsonPointerSegment(renamedName)}{suffix}";
        return true;
    }

    private static bool TryParseComponentReference(string reference, out string section, out string encodedName, out string suffix)
    {
        section = string.Empty;
        encodedName = string.Empty;
        suffix = string.Empty;

        const string componentPrefix = "#/components/";

        if (!reference.StartsWith(componentPrefix, StringComparison.Ordinal))
            return false;

        string remainder = reference[componentPrefix.Length..];
        int sectionSeparatorIndex = remainder.IndexOf('/');

        if (sectionSeparatorIndex <= 0)
            return false;

        section = remainder[..sectionSeparatorIndex];

        string nameAndSuffix = remainder[(sectionSeparatorIndex + 1)..];
        int suffixSeparatorIndex = nameAndSuffix.IndexOf('/');

        if (suffixSeparatorIndex < 0)
        {
            encodedName = nameAndSuffix;
            suffix = "";
            return true;
        }

        encodedName = nameAndSuffix[..suffixSeparatorIndex];
        suffix = nameAndSuffix[suffixSeparatorIndex..];
        return true;
    }

    private static bool IsSupportedOpenApiFile(string filePath)
    {
        string extension = Path.GetExtension(filePath);

        return extension.Equals(".json", StringComparison.OrdinalIgnoreCase) ||
               extension.Equals(".yaml", StringComparison.OrdinalIgnoreCase) ||
               extension.Equals(".yml", StringComparison.OrdinalIgnoreCase);
    }

    private static string PrefixPath(string prefix, string path)
    {
        string trimmedPrefix = prefix.Trim('/');

        if (path.StartsWith('/'))
        {
            if (path.Equals("/" + trimmedPrefix, StringComparison.OrdinalIgnoreCase) ||
                path.StartsWith("/" + trimmedPrefix + "/", StringComparison.OrdinalIgnoreCase))
                return path;

            return "/" + trimmedPrefix + path;
        }

        if (path.Equals(trimmedPrefix, StringComparison.OrdinalIgnoreCase) ||
            path.StartsWith(trimmedPrefix + "/", StringComparison.OrdinalIgnoreCase))
            return "/" + path;

        return "/" + trimmedPrefix + "/" + path;
    }

    private static string ToSafeId(string value)
    {
        return new string(value.Select(static ch => char.IsLetterOrDigit(ch) ? ch : '_').ToArray());
    }

    private static string NormalizePath(string path)
    {
        return Path.GetFullPath(path);
    }

    private static string EncodeJsonPointerSegment(string segment)
    {
        return segment.Replace("~", "~0", StringComparison.Ordinal)
                      .Replace("/", "~1", StringComparison.Ordinal);
    }

    private static string DecodeJsonPointerSegment(string segment)
    {
        return segment.Replace("~1", "/", StringComparison.Ordinal)
                      .Replace("~0", "~", StringComparison.Ordinal);
    }

    private static string GetOpenApiFormat(string filePath)
    {
        string extension = Path.GetExtension(filePath);

        return extension.ToLowerInvariant() switch
        {
            ".json" => OpenApiConstants.Json,
            ".yaml" => OpenApiConstants.Yaml,
            ".yml" => OpenApiConstants.Yml,
            _ => throw new InvalidOperationException($"Unsupported OpenAPI file extension: {extension}")
        };
    }


}
