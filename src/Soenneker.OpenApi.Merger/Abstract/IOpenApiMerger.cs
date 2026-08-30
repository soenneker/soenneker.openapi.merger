using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.OpenApi;

namespace Soenneker.OpenApi.Merger.Abstract;

/// <summary>
/// Merges OpenAPI documents while namespacing paths, components, and operation identifiers.
/// </summary>
public interface IOpenApiMerger
{
    /// <summary>
    /// Merges the provided OpenAPI files into a single document, prefixing paths by the supplied input prefix.
    /// </summary>
    /// <param name="inputs">The prefix and file path for each OpenAPI JSON or YAML document.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>The validated merged document.</returns>
    ValueTask<OpenApiDocument> MergeOpenApis(IEnumerable<(string prefix, string filePath)> inputs, CancellationToken cancellationToken = default);

    /// <summary>
    /// Merges every OpenAPI file discovered beneath <paramref name="directoryPath"/> into a single document.
    /// </summary>
    /// <param name="directoryPath">The root directory to search recursively for JSON and YAML documents.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>The validated merged document.</returns>
    ValueTask<OpenApiDocument> MergeDirectory(string directoryPath, CancellationToken cancellationToken = default);

    /// <summary>
    /// Clones <paramref name="gitUrl"/>, optionally scopes to <paramref name="repositorySubdirectory"/>, and merges the discovered OpenAPI files.
    /// </summary>
    /// <param name="gitUrl">The Git repository URL to clone.</param>
    /// <param name="repositorySubdirectory">An optional directory inside the cloned repository to search.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>The validated merged document.</returns>
    ValueTask<OpenApiDocument> MergeGitUrl(string gitUrl, string? repositorySubdirectory = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Serializes a merged OpenAPI document as v3 JSON.
    /// </summary>
    /// <param name="document">The document to serialize.</param>
    /// <returns>OpenAPI 3 JSON.</returns>
    string ToJson(OpenApiDocument document);
}
