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
    /// <param name="inputs">inputs to read or transform.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>A task whose result is the requested openAPI Document.</returns>
    ValueTask<OpenApiDocument> MergeOpenApis(IEnumerable<(string prefix, string filePath)> inputs, CancellationToken cancellationToken = default);

    /// <summary>
    /// Merges every OpenAPI file discovered beneath <paramref name="directoryPath"/> into a single document.
    /// </summary>
    /// <param name="directoryPath">Root directory whose generated contents should be removed.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>A task whose result is the requested openAPI Document.</returns>
    ValueTask<OpenApiDocument> MergeDirectory(string directoryPath, CancellationToken cancellationToken = default);

    /// <summary>
    /// Clones <paramref name="gitUrl"/>, optionally scopes to <paramref name="repositorySubdirectory"/>, and merges the discovered OpenAPI files.
    /// </summary>
    /// <param name="gitUrl">URL of the git to target.</param>
    /// <param name="repositorySubdirectory">Repository Subdirectory for the merge git url operation.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>A task whose result is the requested openAPI Document.</returns>
    ValueTask<OpenApiDocument> MergeGitUrl(string gitUrl, string? repositorySubdirectory = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Serializes a merged OpenAPI document as v3 JSON.
    /// </summary>
    /// <param name="document">Document to read, persist, or update.</param>
    /// <returns>The text produced by to JSON.</returns>
    string ToJson(OpenApiDocument document);
}
