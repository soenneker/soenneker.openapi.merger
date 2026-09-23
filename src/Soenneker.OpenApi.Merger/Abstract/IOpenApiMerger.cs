using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.OpenApi;

namespace Soenneker.OpenApi.Merger.Abstract;

/// <summary>
/// Merges OpenAPI 3.0 and 3.1 documents while preserving operation contracts and namespacing colliding identifiers.
/// </summary>
public interface IOpenApiMerger
{
    /// <summary>
    /// Merges the provided OpenAPI files into a single document, prefixing paths by the supplied input prefix.
    /// </summary>
    /// <param name="inputs">The prefix and file path for each OpenAPI JSON or YAML document.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>The validated merged document.</returns>
    /// <remarks>
    /// Inherited security, parameters, and servers are made explicit on operations. Equivalent operations on the same
    /// method and prefixed path are combined; conflicting contracts fail. Referenced local documents must be included
    /// in <paramref name="inputs"/>. Broken references are never replaced with unconstrained schemas.
    /// When merging multiple sources, document-level x-samples and x-tagGroups are retained per source prefix
    /// under x-merged-document-metadata, rather than being treated as global metadata.
    /// Component-only documents may omit paths. Relative YAML references also resolve to an included same-directory,
    /// same-stem JSON file when the original filename is absent. Duplicate operation IDs are disambiguated;
    /// links to an ambiguous source operation ID still fail rather than selecting an arbitrary target.
    /// </remarks>
    /// <exception cref="System.InvalidOperationException">An input, reference, merge conflict, or emitted document is invalid or unsupported.</exception>
    ValueTask<OpenApiDocument> MergeOpenApis(IEnumerable<(string prefix, string filePath)> inputs, CancellationToken cancellationToken = default);

    /// <summary>
    /// Merges every OpenAPI file discovered beneath <paramref name="directoryPath"/> into a single document.
    /// </summary>
    /// <param name="directoryPath">The root directory to search recursively for JSON and YAML documents.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>The validated merged document.</returns>
    /// <remarks>Unrelated files are skipped. Files declaring OpenAPI that are malformed or unsupported fail the merge.</remarks>
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
    /// Serializes a merged document as OpenAPI 3.0 JSON, or 3.1 JSON if any merged input used 3.1.
    /// </summary>
    /// <param name="document">The document to serialize.</param>
    /// <returns>OpenAPI JSON with the merged document's schema semantics preserved.</returns>
    /// <remarks>Documents not returned by this merger default to OpenAPI 3.0 serialization.</remarks>
    string ToJson(OpenApiDocument document);
}
