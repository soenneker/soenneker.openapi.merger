using Microsoft.Extensions.Logging;
using Microsoft.OpenApi;
using Microsoft.OpenApi.Reader;
using Soenneker.Extensions.String;
using Soenneker.Extensions.Task;
using Soenneker.Extensions.ValueTask;
using Soenneker.Git.Util.Abstract;
using Soenneker.OpenApi.Merger.Abstract;
using Soenneker.OpenApi.Merger.Dtos;
using Soenneker.Utils.Directory.Abstract;
using Soenneker.Utils.File.Abstract;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;


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
    private readonly IDirectoryUtil _directoryUtil;
    private readonly IFileUtil _fileUtil;

    public OpenApiMerger(ILogger<OpenApiMerger> logger, IGitUtil gitUtil, IDirectoryUtil directoryUtil, IFileUtil fileUtil)
    {
        _logger = logger;
        _gitUtil = gitUtil;
        _directoryUtil = directoryUtil;
        _fileUtil = fileUtil;
    }

    private async ValueTask<OpenApiDocument> MergeSourceDocuments(List<SourceDocument> sourceDocuments, CancellationToken cancellationToken)
    {
        Dictionary<string, HashSet<string>> reservedComponentNames = CreateReservedComponentNames();

        foreach (SourceDocument sourceDocument in sourceDocuments)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                sourceDocument.ComponentRenameMaps = BuildComponentRenameMaps(sourceDocument.Prefix, sourceDocument.Document.Components, reservedComponentNames);
                AddReservedComponentNames(sourceDocument.ComponentRenameMaps, reservedComponentNames);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "Skipping component rename preparation for '{FilePath}' (prefix '{Prefix}').",
                    sourceDocument.FilePath, sourceDocument.Prefix);

                sourceDocument.ComponentRenameMaps = [];
            }
        }

        Dictionary<string, SourceDocument> sourceLookup = sourceDocuments
            .GroupBy(static source => NormalizePath(source.FilePath), StringComparer.OrdinalIgnoreCase)
            .ToDictionary(static group => group.Key, static group => group.First(), StringComparer.OrdinalIgnoreCase);

        OpenApiDocument merged = CreateMergedDocument();

        foreach (SourceDocument sourceDocument in sourceDocuments)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                OpenApiDocument? transformed = await TransformDocument(sourceDocument, sourceLookup, cancellationToken).NoSync();

                if (transformed == null)
                {
                    _logger.LogDebug("Skipping '{FilePath}' because transform returned null.", sourceDocument.FilePath);
                    continue;
                }

                MergeInto(merged, transformed, sourceDocument.Prefix, _logger);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "Skipping source document '{FilePath}' (prefix '{Prefix}') because merge failed.",
                    sourceDocument.FilePath, sourceDocument.Prefix);
            }
        }

        OpenApiDocument validated;

        try
        {
            validated = ValidateMergedDocument(merged);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "ValidateMergedDocument() failed. Using unvalidated merged document.");
            validated = merged;
        }

        try
        {
            EnsureUniqueOperationIds(validated);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "EnsureUniqueOperationIds() failed. Continuing with current document.");
        }

        try
        {
            EnsureSecuritySchemesResolve(validated);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "EnsureSecuritySchemesResolve() failed. Continuing with current document.");
        }

        try
        {
            EnsureReferencesResolve(validated);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "EnsureReferencesResolve() failed. Continuing with current document.");
        }

        try
        {
            EnsureDiscriminatorMappingsResolve(validated);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "EnsureDiscriminatorMappingsResolve() failed. Continuing with current document.");
        }

        return validated;
    }

    public async ValueTask<OpenApiDocument> MergeOpenApis(IEnumerable<(string prefix, string filePath)> inputs, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(inputs);

        (string prefix, string filePath)[] sourceInputs = inputs.ToArray();

        if (sourceInputs.Length == 0)
            throw new InvalidOperationException("No OpenAPI inputs were provided.");

        List<SourceDocument> sourceDocuments = await LoadSourceDocuments(sourceInputs, strict: true, cancellationToken)
            .NoSync();

        if (sourceDocuments.Count == 0)
            throw new InvalidOperationException("No readable OpenAPI documents were found in the provided inputs.");

        return await MergeSourceDocuments(sourceDocuments, cancellationToken)
            .NoSync();
    }

    public async ValueTask<OpenApiDocument> MergeDirectory(string directoryPath, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(directoryPath))
            throw new ArgumentException("Directory path cannot be null or whitespace.", nameof(directoryPath));

        if (!await _directoryUtil.Exists(directoryPath, cancellationToken)
                                 .NoSync())
            throw new DirectoryNotFoundException($"OpenAPI directory was not found: {directoryPath}");

        string[] files = Directory.GetFiles(directoryPath, "*.*", SearchOption.AllDirectories)
                                  .Where(IsSupportedOpenApiFile)
                                  .OrderBy(static file => file, StringComparer.OrdinalIgnoreCase)
                                  .ToArray();

        if (files.Length == 0)
            throw new InvalidOperationException($"No OpenAPI files were found beneath '{directoryPath}'.");

        List<SourceDocument> sourceDocuments = await LoadSourceDocuments(
                files.Select(static file => (Path.GetFileNameWithoutExtension(file), file)), strict: false, cancellationToken)
            .NoSync();

        if (sourceDocuments.Count == 0)
            throw new InvalidOperationException($"No readable OpenAPI documents were found beneath '{directoryPath}'.");

        return await MergeSourceDocuments(sourceDocuments, cancellationToken)
            .NoSync();
    }

    public async ValueTask<OpenApiDocument> MergeGitUrl(string gitUrl, string? repositorySubdirectory = null, CancellationToken cancellationToken = default)
    {
        if (gitUrl.IsNullOrWhiteSpace())
            throw new ArgumentException("Git URL cannot be null or whitespace.", nameof(gitUrl));

        string repositoryDirectory = await _gitUtil.CloneToTempDirectory(gitUrl, cancellationToken: cancellationToken)
                                                   .NoSync();

        string targetDirectory = repositoryDirectory;

        if (!string.IsNullOrWhiteSpace(repositorySubdirectory))
        {
            targetDirectory = Path.Combine(repositoryDirectory, repositorySubdirectory);

            if (!await _directoryUtil.Exists(targetDirectory, cancellationToken)
                                     .NoSync())
                throw new DirectoryNotFoundException($"Repository subdirectory was not found: {targetDirectory}");
        }

        return await MergeDirectory(targetDirectory, cancellationToken)
            .NoSync();
    }

    public string ToJson(OpenApiDocument document)
    {
        ArgumentNullException.ThrowIfNull(document);

        using var stringWriter = new StringWriter(new StringBuilder(4096));
        var writer = new OpenApiJsonWriter(stringWriter);
        document.SerializeAsV3(writer);

        return stringWriter.ToString();
    }

    private async ValueTask<List<SourceDocument>> LoadSourceDocuments(IEnumerable<(string prefix, string filePath)> inputs, bool strict,
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

            if (!await _fileUtil.Exists(fullPath, cancellationToken)
                                .NoSync())
                throw new FileNotFoundException($"OpenAPI file was not found: {fullPath}", fullPath);

            OpenApiDocument? document = strict
                ? await LoadDocumentStrict(fullPath, cancellationToken)
                    .NoSync()
                : await TryLoadDocumentLenient(fullPath, cancellationToken)
                    .NoSync();

            if (document == null)
                continue;

            sourceDocuments.Add(new SourceDocument(fullPath, prefix, document));
        }

        return sourceDocuments;
    }

    private async ValueTask<OpenApiDocument> LoadDocumentStrict(string filePath, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        await using MemoryStream stream = await _fileUtil.ReadToMemoryStream(filePath, log: false, cancellationToken)
                                                         .NoSync();
        ReadResult readResult = await OpenApiDocument.LoadAsync(stream, GetOpenApiFormat(filePath), new OpenApiReaderSettings(), cancellationToken)
                                                     .NoSync();

        OpenApiDocument? document = readResult.Document;

        if (document == null)
            throw new InvalidOperationException($"Failed to read OpenAPI document '{filePath}'.");

        LogDiagnostics("OpenAPI read", filePath, readResult.Diagnostic);

        return document;
    }

    private async ValueTask<OpenApiDocument?> TryLoadDocumentLenient(string filePath, CancellationToken cancellationToken)
    {
        try
        {
            cancellationToken.ThrowIfCancellationRequested();

            await using MemoryStream stream = await _fileUtil.ReadToMemoryStream(filePath, log: false, cancellationToken)
                                                             .NoSync();
            ReadResult readResult = await OpenApiDocument.LoadAsync(stream, GetOpenApiFormat(filePath), new OpenApiReaderSettings(), cancellationToken)
                                                         .NoSync();

            OpenApiDocument? document = readResult.Document;

            if (document == null)
            {
                _logger.LogDebug("Skipping {FilePath} because it did not produce an OpenAPI document.", filePath);
                return null;
            }

            LogDiagnostics("OpenAPI read", filePath, readResult.Diagnostic);

            return document;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Skipping non-OpenAPI file {FilePath}.", filePath);
            return null;
        }
    }

    private static void MergeInto(OpenApiDocument merged, OpenApiDocument sourceDocument, string prefix, ILogger logger)
    {
        if (merged == null || sourceDocument == null)
            return;

        IList<OpenApiServer> mergedServers = merged.Servers ??= [];

        if (sourceDocument.Servers != null)
        {
            foreach (OpenApiServer? server in sourceDocument.Servers)
            {
                if (server == null || string.IsNullOrWhiteSpace(server.Url))
                    continue;

                if (mergedServers.All(existing => existing == null || !string.Equals(existing.Url, server.Url, StringComparison.OrdinalIgnoreCase)))
                    mergedServers.Add(server);
            }
        }

        ISet<OpenApiTag> mergedTags = merged.Tags ??= new HashSet<OpenApiTag>();

        if (sourceDocument.Tags != null)
        {
            foreach (OpenApiTag? tag in sourceDocument.Tags)
            {
                if (tag == null || string.IsNullOrWhiteSpace(tag.Name))
                    continue;

                if (!mergedTags.Any(existing => existing != null && string.Equals(existing.Name, tag.Name, StringComparison.Ordinal)))
                    mergedTags.Add(tag);
            }
        }

        merged.Paths ??= new OpenApiPaths();

        if (sourceDocument.Paths != null)
        {
            foreach (var kvp in sourceDocument.Paths)
            {
                string pathKey = kvp.Key;
                IOpenApiPathItem? pathItem = kvp.Value;

                if (string.IsNullOrWhiteSpace(pathKey))
                {
                    logger.LogDebug("Skipping path with null/empty key from prefix '{Prefix}'.", prefix);
                    continue;
                }

                if (pathItem == null)
                {
                    logger.LogDebug("Skipping null path item for '{PathKey}' from prefix '{Prefix}'.", pathKey, prefix);
                    continue;
                }

                string mergedPath = PrefixPath(prefix, pathKey);

                if (string.IsNullOrWhiteSpace(mergedPath))
                {
                    logger.LogDebug("Skipping merged path because PrefixPath returned null/empty for '{PathKey}' and prefix '{Prefix}'.", pathKey, prefix);
                    continue;
                }

                if (merged.Paths.ContainsKey(mergedPath))
                {
                    logger.LogWarning("Skipping duplicate merged path '{MergedPath}'.", mergedPath);
                    continue;
                }

                merged.Paths[mergedPath] = pathItem;
            }
        }

        OpenApiComponents mergedComponents = merged.Components ??= new OpenApiComponents
        {
            Schemas = new Dictionary<string, IOpenApiSchema>(StringComparer.Ordinal),
            Parameters = new Dictionary<string, IOpenApiParameter>(StringComparer.Ordinal),
            Responses = new Dictionary<string, IOpenApiResponse>(StringComparer.Ordinal),
            RequestBodies = new Dictionary<string, IOpenApiRequestBody>(StringComparer.Ordinal),
            Headers = new Dictionary<string, IOpenApiHeader>(StringComparer.Ordinal),
            SecuritySchemes = new Dictionary<string, IOpenApiSecurityScheme>(StringComparer.Ordinal),
            Links = new Dictionary<string, IOpenApiLink>(StringComparer.Ordinal),
            Callbacks = new Dictionary<string, IOpenApiCallback>(StringComparer.Ordinal),
            Examples = new Dictionary<string, IOpenApiExample>(StringComparer.Ordinal)
        };

        CopyComponentSection(sourceDocument.Components?.Schemas, mergedComponents.Schemas, "schemas", logger);
        CopyComponentSection(sourceDocument.Components?.Parameters, mergedComponents.Parameters, "parameters", logger);
        CopyComponentSection(sourceDocument.Components?.Responses, mergedComponents.Responses, "responses", logger);
        CopyComponentSection(sourceDocument.Components?.RequestBodies, mergedComponents.RequestBodies, "requestBodies", logger);
        CopyComponentSection(sourceDocument.Components?.Headers, mergedComponents.Headers, "headers", logger);
        CopyComponentSection(sourceDocument.Components?.SecuritySchemes, mergedComponents.SecuritySchemes, "securitySchemes", logger);
        CopyComponentSection(sourceDocument.Components?.Links, mergedComponents.Links, "links", logger);
        CopyComponentSection(sourceDocument.Components?.Callbacks, mergedComponents.Callbacks, "callbacks", logger);
        CopyComponentSection(sourceDocument.Components?.Examples, mergedComponents.Examples, "examples", logger);
    }

    private static OpenApiDocument CreateMergedDocument()
    {
        return new OpenApiDocument
        {
            Info = new OpenApiInfo
            {
                Title = "Merged OpenAPI",
                Version = "1.0.0"
            },
            Paths = new OpenApiPaths(),
            Components = new OpenApiComponents
            {
                Schemas = new Dictionary<string, IOpenApiSchema>(StringComparer.Ordinal),
                Parameters = new Dictionary<string, IOpenApiParameter>(StringComparer.Ordinal),
                Responses = new Dictionary<string, IOpenApiResponse>(StringComparer.Ordinal),
                RequestBodies = new Dictionary<string, IOpenApiRequestBody>(StringComparer.Ordinal),
                Headers = new Dictionary<string, IOpenApiHeader>(StringComparer.Ordinal),
                SecuritySchemes = new Dictionary<string, IOpenApiSecurityScheme>(StringComparer.Ordinal),
                Links = new Dictionary<string, IOpenApiLink>(StringComparer.Ordinal),
                Callbacks = new Dictionary<string, IOpenApiCallback>(StringComparer.Ordinal),
                Examples = new Dictionary<string, IOpenApiExample>(StringComparer.Ordinal)
            },
            Servers = new List<OpenApiServer>(),
            Tags = new HashSet<OpenApiTag>()
        };
    }

    private ValueTask<OpenApiDocument?> TransformDocument(SourceDocument sourceDocument, Dictionary<string, SourceDocument> sourceLookup,
        CancellationToken cancellationToken)
    {
        try
        {
            cancellationToken.ThrowIfCancellationRequested();

            string json = ToJson(sourceDocument.Document);

            if (string.IsNullOrWhiteSpace(json))
            {
                _logger.LogWarning("Skipping '{FilePath}' because serialized OpenAPI JSON was empty.", sourceDocument.FilePath);
                return ValueTask.FromResult<OpenApiDocument?>(null);
            }

            JsonNode? root = JsonNode.Parse(json);

            if (root == null)
            {
                _logger.LogWarning("Skipping '{FilePath}' because serialized OpenAPI JSON could not be parsed.", sourceDocument.FilePath);
                return ValueTask.FromResult<OpenApiDocument?>(null);
            }

            RenameComponentKeys(root, sourceDocument.ComponentRenameMaps);
            RewriteComponentReferences(root, sourceDocument, sourceLookup);
            RewriteDiscriminatorMappings(root, sourceDocument, sourceLookup);
            RewriteSecurityRequirementNames(root, sourceDocument.ComponentRenameMaps);
            NamespaceOperationIds(root, sourceDocument.Prefix);

            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(root.ToJsonString()));
            ReadResult readResult = OpenApiDocument.Load(stream, OpenApiConstants.Json, new OpenApiReaderSettings());
            OpenApiDocument? transformed = readResult.Document;

            if (transformed == null)
            {
                _logger.LogWarning("Skipping '{FilePath}' because transformed document could not be rehydrated.", sourceDocument.FilePath);
                return ValueTask.FromResult<OpenApiDocument?>(null);
            }

            LogDiagnostics("Transformed OpenAPI document", sourceDocument.FilePath, readResult.Diagnostic);

            return ValueTask.FromResult<OpenApiDocument?>(transformed);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Skipping '{FilePath}' because transformation failed.", sourceDocument.FilePath);
            return ValueTask.FromResult<OpenApiDocument?>(null);
        }
    }

    private static void CopyComponentSection<T>(IDictionary<string, T>? source, IDictionary<string, T>? destination, string sectionName, ILogger logger)
    {
        if (source == null || source.Count == 0 || destination == null)
            return;

        foreach (var kvp in source)
        {
            string key = kvp.Key;
            T value = kvp.Value;

            if (string.IsNullOrWhiteSpace(key))
            {
                logger.LogDebug("Skipping component in '{SectionName}' because key was null/empty.", sectionName);
                continue;
            }

            if (value == null)
            {
                logger.LogDebug("Skipping null component '{Key}' in '{SectionName}'.", key, sectionName);
                continue;
            }

            if (destination.ContainsKey(key))
            {
                logger.LogWarning("Skipping duplicate component key '{Key}' in '{SectionName}'.", key, sectionName);
                continue;
            }

            destination[key] = value;
        }
    }

    private void LogDiagnostics(string stage, string filePath, OpenApiDiagnostic? diagnostic)
    {
        if (diagnostic?.Errors == null || diagnostic.Errors.Count == 0)
            return;

        _logger.LogWarning("{Stage} for {FilePath} finished with {ErrorCount} diagnostic errors.", stage, filePath, diagnostic.Errors.Count);

        foreach (OpenApiError error in diagnostic.Errors)
        {
            _logger.LogWarning("{Stage} diagnostic for {FilePath}: {Message}", stage, filePath, error.Message);
        }
    }

    private OpenApiDocument ValidateMergedDocument(OpenApiDocument merged)
    {
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(ToJson(merged)));
        ReadResult readResult = OpenApiDocument.Load(stream, OpenApiConstants.Json, new OpenApiReaderSettings());
        OpenApiDocument? document = readResult.Document;

        if (document == null)
            throw new InvalidOperationException("Failed to rehydrate merged OpenAPI document.");

        LogDiagnostics("Merged OpenAPI document", "merged", readResult.Diagnostic);

        return document;
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
        string safePrefix = ToSafeId(prefix);

        foreach (string name in names)
        {
            string targetName = name;

            if (reservedNames.Contains(targetName) || assignedNames.Contains(targetName))
                targetName = $"{safePrefix}_{name}";

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
                reservedComponentNames[section]
                    .Add(renamedValue);
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
                if (jsonObject.TryGetPropertyValue("$ref", out JsonNode? refNode) && refNode is JsonValue refValue &&
                    refValue.TryGetValue(out string? reference) && !string.IsNullOrWhiteSpace(reference))
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

    private static void RewriteDiscriminatorMappings(JsonNode node, SourceDocument currentSource, Dictionary<string, SourceDocument> sourceLookup)
    {
        switch (node)
        {
            case JsonObject jsonObject:
            {
                if (jsonObject.TryGetPropertyValue("discriminator", out JsonNode? discriminatorNode) && discriminatorNode is JsonObject discriminatorObject &&
                    discriminatorObject["mapping"] is JsonObject mappingObject)
                {
                    foreach ((string mappingKey, JsonNode? mappingValueNode) in mappingObject.ToList())
                    {
                        if (mappingValueNode is JsonValue mappingValue && mappingValue.TryGetValue(out string? mappingValueString) &&
                            !string.IsNullOrWhiteSpace(mappingValueString))
                        {
                            mappingObject[mappingKey] = RewriteDiscriminatorMappingValue(mappingValueString, currentSource, sourceLookup);
                        }
                    }
                }

                foreach ((_, JsonNode? child) in jsonObject.ToList())
                {
                    if (child != null)
                        RewriteDiscriminatorMappings(child, currentSource, sourceLookup);
                }

                break;
            }
            case JsonArray jsonArray:
            {
                foreach (JsonNode? child in jsonArray)
                {
                    if (child != null)
                        RewriteDiscriminatorMappings(child, currentSource, sourceLookup);
                }

                break;
            }
        }
    }

    private static void RewriteSecurityRequirementNames(JsonNode node, Dictionary<string, Dictionary<string, string>> renameMaps)
    {
        if (!renameMaps.TryGetValue("securitySchemes", out Dictionary<string, string>? securityRenameMap) || securityRenameMap.Count == 0)
            return;

        switch (node)
        {
            case JsonObject jsonObject:
            {
                if (jsonObject.TryGetPropertyValue("security", out JsonNode? securityNode) && securityNode is JsonArray securityArray)
                {
                    RewriteSecurityRequirementArray(securityArray, securityRenameMap);
                }

                foreach ((_, JsonNode? child) in jsonObject.ToList())
                {
                    if (child != null)
                        RewriteSecurityRequirementNames(child, renameMaps);
                }

                break;
            }
            case JsonArray jsonArray:
            {
                foreach (JsonNode? child in jsonArray)
                {
                    if (child != null)
                        RewriteSecurityRequirementNames(child, renameMaps);
                }

                break;
            }
        }
    }

    private static void RewriteSecurityRequirementArray(JsonArray securityArray, Dictionary<string, string> securityRenameMap)
    {
        foreach (JsonNode? requirementNode in securityArray)
        {
            if (requirementNode is not JsonObject requirementObject || requirementObject.Count == 0)
                continue;

            var replacements = new List<(string oldKey, string newKey, JsonNode? value)>();

            foreach ((string schemeName, JsonNode? value) in requirementObject.ToList())
            {
                if (!securityRenameMap.TryGetValue(schemeName, out string? renamedScheme) || string.Equals(schemeName, renamedScheme, StringComparison.Ordinal))
                    continue;

                replacements.Add((schemeName, renamedScheme, value?.DeepClone()));
            }

            foreach ((string oldKey, string newKey, JsonNode? value) in replacements)
            {
                requirementObject.Remove(oldKey);
                requirementObject[newKey] = value;
            }
        }
    }

    private static void NamespaceOperationIds(JsonNode root, string prefix)
    {
        if (root["paths"] is not JsonObject pathsObject)
            return;

        string safePrefix = ToSafeId(prefix);

        foreach ((string pathKey, JsonNode? pathNode) in pathsObject)
        {
            if (pathNode is not JsonObject pathObject)
                continue;

            foreach ((string method, JsonNode? operationNode) in pathObject)
            {
                if (!IsHttpMethod(method) || operationNode is not JsonObject operationObject)
                    continue;

                if (operationObject.TryGetPropertyValue("operationId", out JsonNode? operationIdNode) && operationIdNode is JsonValue operationIdValue &&
                    operationIdValue.TryGetValue(out string? operationId) && !string.IsNullOrWhiteSpace(operationId))
                {
                    operationObject["operationId"] = $"{safePrefix}_{ToSafeId(operationId)}";
                }
                else
                {
                    operationObject["operationId"] = BuildGeneratedOperationId(safePrefix, method, pathKey);
                }
            }
        }
    }

    private static bool IsHttpMethod(string method)
    {
        return method.Equals("get", StringComparison.OrdinalIgnoreCase) || method.Equals("put", StringComparison.OrdinalIgnoreCase) ||
               method.Equals("post", StringComparison.OrdinalIgnoreCase) || method.Equals("delete", StringComparison.OrdinalIgnoreCase) ||
               method.Equals("options", StringComparison.OrdinalIgnoreCase) || method.Equals("head", StringComparison.OrdinalIgnoreCase) ||
               method.Equals("patch", StringComparison.OrdinalIgnoreCase) || method.Equals("trace", StringComparison.OrdinalIgnoreCase);
    }

    private static string BuildGeneratedOperationId(string safePrefix, string method, string path)
    {
        string normalizedPath = path.Trim('/');
        string safePath = normalizedPath.Length == 0 ? "root" : ToSafeId(normalizedPath.Replace('/', '_'));
        return $"{safePrefix}_{method.ToLowerInvariant()}_{safePath}";
    }

    private static string RewriteDiscriminatorMappingValue(string value, SourceDocument currentSource, Dictionary<string, SourceDocument> sourceLookup)
    {
        if (value.StartsWith("#/", StringComparison.Ordinal))
            return RewriteReference(value, currentSource, sourceLookup);

        if (value.Contains('#'))
            return RewriteReference(value, currentSource, sourceLookup);

        if (currentSource.ComponentRenameMaps.TryGetValue("schemas", out Dictionary<string, string>? schemaRenameMap) &&
            schemaRenameMap.TryGetValue(value, out string? renamedSchema))
        {
            return $"#/components/schemas/{EncodeJsonPointerSegment(renamedSchema)}";
        }

        return value;
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

    private static bool TryRewriteLocalComponentReference(string reference, Dictionary<string, Dictionary<string, string>> renameMaps,
        out string rewrittenReference)
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

    private void EnsureUniqueOperationIds(OpenApiDocument document)
    {
        if (document?.Paths == null || document.Paths.Count == 0)
            return;

        var seen = new HashSet<string>(StringComparer.Ordinal);

        foreach (var pathKvp in document.Paths)
        {
            string path = pathKvp.Key;

            if (string.IsNullOrWhiteSpace(path))
                continue;

            IOpenApiPathItem? pathItem = pathKvp.Value;

            if (pathItem?.Operations == null || pathItem.Operations.Count == 0)
                continue;

            foreach (var opKvp in pathItem.Operations)
            {
                HttpMethod method = opKvp.Key;
                OpenApiOperation? operation = opKvp.Value;

                if (operation == null)
                {
                    _logger.LogDebug("Skipping null operation at '{Method} {Path}'.", method, path);
                    continue;
                }

                string baseOperationId = operation.OperationId?.Trim() ?? string.Empty;

                if (baseOperationId.Length == 0)
                {
                    baseOperationId = BuildGeneratedOperationId("merged", method.ToString(), path);
                    _logger.LogDebug("Generated operationId '{OperationId}' for '{Method} {Path}'.", baseOperationId, method, path);
                }

                string uniqueOperationId = baseOperationId;
                int suffix = 2;

                while (!seen.Add(uniqueOperationId))
                {
                    uniqueOperationId = $"{baseOperationId}_{suffix}";
                    suffix++;
                }

                if (!string.Equals(operation.OperationId, uniqueOperationId, StringComparison.Ordinal))
                {
                    _logger.LogDebug("Adjusted operationId for '{Method} {Path}' to '{OperationId}'.", method, path, uniqueOperationId);
                }

                operation.OperationId = uniqueOperationId;
            }
        }
    }
    private void EnsureSecuritySchemesResolve(OpenApiDocument document)
    {
        JsonNode root = JsonNode.Parse(ToJson(document)) ??
                        throw new InvalidOperationException("Failed to serialize merged OpenAPI document for security validation.");

        HashSet<string> schemes = GetComponentNamesFromJson(root, "securitySchemes");

        ValidateSecurityArraysRecursive(root, schemes);
    }

    private static void ValidateSecurityArraysRecursive(JsonNode node, HashSet<string> schemes)
    {
        switch (node)
        {
            case JsonObject jsonObject:
            {
                if (jsonObject.TryGetPropertyValue("security", out JsonNode? securityNode) && securityNode is JsonArray securityArray)
                {
                    foreach (JsonNode? requirementNode in securityArray)
                    {
                        if (requirementNode is not JsonObject requirementObject || requirementObject.Count == 0)
                            continue;

                        foreach ((string schemeName, _) in requirementObject)
                        {
                            if (!schemes.Contains(schemeName))
                                throw new InvalidOperationException($"Security requirement references missing security scheme '{schemeName}'.");
                        }
                    }
                }

                foreach ((_, JsonNode? child) in jsonObject)
                {
                    if (child != null)
                        ValidateSecurityArraysRecursive(child, schemes);
                }

                break;
            }
            case JsonArray jsonArray:
            {
                foreach (JsonNode? child in jsonArray)
                {
                    if (child != null)
                        ValidateSecurityArraysRecursive(child, schemes);
                }

                break;
            }
        }
    }

    private void EnsureReferencesResolve(OpenApiDocument document)
    {
        JsonNode root = JsonNode.Parse(ToJson(document)) ??
                        throw new InvalidOperationException("Failed to serialize merged OpenAPI document for reference validation.");

        Dictionary<string, HashSet<string>> componentNames = GetAllComponentNamesFromJson(root);

        ValidateReferencesRecursive(root, componentNames);
    }

    private static void ValidateReferencesRecursive(JsonNode node, Dictionary<string, HashSet<string>> componentNames)
    {
        switch (node)
        {
            case JsonObject jsonObject:
            {
                if (jsonObject.TryGetPropertyValue("$ref", out JsonNode? refNode) && refNode is JsonValue refValue &&
                    refValue.TryGetValue(out string? reference) && !string.IsNullOrWhiteSpace(reference) &&
                    TryParseComponentReference(reference, out string section, out string encodedName, out _))
                {
                    string name = DecodeJsonPointerSegment(encodedName);

                    if (!componentNames.TryGetValue(section, out HashSet<string>? sectionNames) || !sectionNames.Contains(name))
                        throw new InvalidOperationException($"Unresolved component reference '{reference}'.");
                }

                foreach ((_, JsonNode? child) in jsonObject)
                {
                    if (child != null)
                        ValidateReferencesRecursive(child, componentNames);
                }

                break;
            }
            case JsonArray jsonArray:
            {
                foreach (JsonNode? child in jsonArray)
                {
                    if (child != null)
                        ValidateReferencesRecursive(child, componentNames);
                }

                break;
            }
        }
    }

    private void EnsureDiscriminatorMappingsResolve(OpenApiDocument document)
    {
        JsonNode root = JsonNode.Parse(ToJson(document)) ??
                        throw new InvalidOperationException("Failed to serialize merged OpenAPI document for discriminator validation.");

        HashSet<string> schemaNames = GetComponentNamesFromJson(root, "schemas");

        ValidateDiscriminatorMappingsRecursive(root, schemaNames);
    }

    private static void ValidateDiscriminatorMappingsRecursive(JsonNode node, HashSet<string> schemaNames)
    {
        switch (node)
        {
            case JsonObject jsonObject:
            {
                if (jsonObject.TryGetPropertyValue("discriminator", out JsonNode? discriminatorNode) && discriminatorNode is JsonObject discriminatorObject &&
                    discriminatorObject["mapping"] is JsonObject mappingObject)
                {
                    foreach ((string mappingKey, JsonNode? mappingValueNode) in mappingObject)
                    {
                        if (mappingValueNode is not JsonValue mappingValue || !mappingValue.TryGetValue(out string? mappingValueString) ||
                            string.IsNullOrWhiteSpace(mappingValueString))
                        {
                            continue;
                        }

                        if (mappingValueString.StartsWith("#/components/schemas/", StringComparison.Ordinal))
                        {
                            if (!TryParseComponentReference(mappingValueString, out _, out string encodedName, out _))
                                throw new InvalidOperationException($"Invalid discriminator mapping '{mappingKey}' -> '{mappingValueString}'.");

                            string schemaName = DecodeJsonPointerSegment(encodedName);

                            if (!schemaNames.Contains(schemaName))
                                throw new InvalidOperationException($"Discriminator mapping '{mappingKey}' points to missing schema '{mappingValueString}'.");
                        }
                        else if (!mappingValueString.StartsWith("#/", StringComparison.Ordinal))
                        {
                            if (!schemaNames.Contains(mappingValueString))
                                throw new InvalidOperationException(
                                    $"Discriminator mapping '{mappingKey}' points to missing schema name '{mappingValueString}'.");
                        }
                    }
                }

                foreach ((_, JsonNode? child) in jsonObject)
                {
                    if (child != null)
                        ValidateDiscriminatorMappingsRecursive(child, schemaNames);
                }

                break;
            }
            case JsonArray jsonArray:
            {
                foreach (JsonNode? child in jsonArray)
                {
                    if (child != null)
                        ValidateDiscriminatorMappingsRecursive(child, schemaNames);
                }

                break;
            }
        }
    }

    private static Dictionary<string, HashSet<string>> GetAllComponentNamesFromJson(JsonNode root)
    {
        var result = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);

        foreach (string section in _componentSections)
        {
            result[section] = GetComponentNamesFromJson(root, section);
        }

        return result;
    }

    private static HashSet<string> GetComponentNamesFromJson(JsonNode root, string section)
    {
        if (root["components"] is not JsonObject componentsObject)
            return new HashSet<string>(StringComparer.Ordinal);

        if (componentsObject[section] is not JsonObject sectionObject)
            return new HashSet<string>(StringComparer.Ordinal);

        return sectionObject.Select(static kvp => kvp.Key)
                            .ToHashSet(StringComparer.Ordinal);
    }

    private static bool IsSupportedOpenApiFile(string filePath)
    {
        string extension = Path.GetExtension(filePath);

        return extension.Equals(".json", StringComparison.OrdinalIgnoreCase) || extension.Equals(".yaml", StringComparison.OrdinalIgnoreCase) ||
               extension.Equals(".yml", StringComparison.OrdinalIgnoreCase);
    }

    private static string PrefixPath(string prefix, string path)
    {
        if (string.IsNullOrWhiteSpace(prefix))
            return path.StartsWith('/') ? path : "/" + path;

        string trimmedPrefix = prefix.Trim('/');

        if (path.StartsWith('/'))
        {
            if (path.Equals("/" + trimmedPrefix, StringComparison.OrdinalIgnoreCase) ||
                path.StartsWith("/" + trimmedPrefix + "/", StringComparison.OrdinalIgnoreCase))
                return path;

            return "/" + trimmedPrefix + path;
        }

        if (path.Equals(trimmedPrefix, StringComparison.OrdinalIgnoreCase) || path.StartsWith(trimmedPrefix + "/", StringComparison.OrdinalIgnoreCase))
            return "/" + path;

        return "/" + trimmedPrefix + "/" + path;
    }

    private static string ToSafeId(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return "default";

        var sb = new StringBuilder(value.Length);
        bool lastWasUnderscore = false;

        foreach (char ch in value)
        {
            char normalized = char.IsLetterOrDigit(ch) ? ch : '_';

            if (normalized == '_')
            {
                if (lastWasUnderscore)
                    continue;

                lastWasUnderscore = true;
            }
            else
            {
                lastWasUnderscore = false;
            }

            sb.Append(normalized);
        }

        string result = sb.ToString()
                          .Trim('_');
        return result.Length == 0 ? "default" : result;
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