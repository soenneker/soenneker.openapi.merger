using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.OpenApi;

namespace Soenneker.OpenApi.Merger.Abstract;

/// <summary>
/// A utility library to merge OpenApi specs
/// </summary>
public interface IOpenApiMerger
{
    /// <summary>
    /// Merges the provided OpenAPI files into a single document, prefixing paths by the supplied input prefix.
    /// </summary>
    ValueTask<OpenApiDocument> MergeOpenApis(IEnumerable<(string prefix, string filePath)> inputs, CancellationToken cancellationToken = default);

    /// <summary>
    /// Merges every OpenAPI file discovered beneath <paramref name="directoryPath"/> into a single document.
    /// </summary>
    ValueTask<OpenApiDocument> MergeDirectory(string directoryPath, CancellationToken cancellationToken = default);

    /// <summary>
    /// Clones <paramref name="gitUrl"/>, optionally scopes to <paramref name="repositorySubdirectory"/>, and merges the discovered OpenAPI files.
    /// </summary>
    ValueTask<OpenApiDocument> MergeGitUrl(string gitUrl, string? repositorySubdirectory = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Serializes a merged OpenAPI document as v3 JSON.
    /// </summary>
    string ToJson(OpenApiDocument document);
}
