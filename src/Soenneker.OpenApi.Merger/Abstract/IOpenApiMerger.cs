using Microsoft.OpenApi.Models;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Merger.Abstract;

/// <summary>
/// A utility library to merge OpenApi specs
/// </summary>
public interface IOpenApiMerger
{
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
