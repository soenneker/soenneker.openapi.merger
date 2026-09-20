[![](https://img.shields.io/nuget/v/soenneker.openapi.merger.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.openapi.merger/)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.openapi.merger/publish-package.yml?style=for-the-badge)](https://github.com/soenneker/soenneker.openapi.merger/actions/workflows/publish-package.yml)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.openapi.merger/codeql.yml?label=codeql&style=for-the-badge)](https://github.com/soenneker/soenneker.openapi.merger/actions/workflows/codeql.yml)
[![](https://img.shields.io/nuget/dt/soenneker.openapi.merger.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.openapi.merger/)

# Soenneker.OpenApi.Merger

Merge OpenAPI JSON or YAML documents while namespacing paths, components, and operation IDs.

## Install

```bash
dotnet add package Soenneker.OpenApi.Merger
```

## Registration

```csharp
using Microsoft.Extensions.DependencyInjection;
using Soenneker.OpenApi.Merger.Registrars;

services.AddOpenApiMergerAsSingleton();
```

Use `AddOpenApiMergerAsScoped()` when the merger should follow a dependency-injection scope.

## Merge selected files

Inject `IOpenApiMerger` and assign a prefix to each source:

```csharp
using Microsoft.OpenApi;
using Soenneker.OpenApi.Merger.Abstract;

OpenApiDocument merged = await merger.MergeOpenApis(
    [
        ("accounts", "accounts.openapi.json"),
        ("billing", "billing.openapi.yaml")
    ],
    cancellationToken);

string json = merger.ToJson(merged);
```

For example, the path `/users` from the `accounts` input becomes `/accounts/users`. If a source path already begins with its prefix, the prefix is not duplicated. Prefix matching is case-sensitive. Component names and operation IDs are made unique, and local or relative references are rewritten to follow renamed components.

Equivalent duplicate operations retain examples from both inputs, including examples on referenced components. Named example collisions receive unique names. If matching response media types have a schema in only one input, the supplied schema fills the missing definition before comparison. Explicit schemas, including unconstrained schemas, must still agree; missing response bodies are not invented.

Path prefixes describe the routes of the merged API. The serving gateway must expose those routes: the merger does not configure routing or make a source server serve a new prefixed URL.

The merger preserves root and path-level security, parameters, and servers on each operation, including explicit anonymous access. Different methods can share a merged path. Equivalent operations on the same method and path are combined with richer documentation; differing contracts fail with the affected method and path. Schema properties named `description`, `example`, or other documentation keywords remain part of the comparison.

`MergeOpenApis` is strict: every listed file must exist and contain one OpenAPI 3.0 or 3.1 document. Invalid reader diagnostics, duplicate JSON/YAML keys, multiple YAML documents, unresolved references, missing security schemes, invalid discriminator mappings, conflicting operations, and ambiguous path templates throw. Missing schemas are never replaced with generic objects. Each file must be listed once so cross-file reference targets remain unambiguous.

References to other local OpenAPI documents require those documents to be included in the inputs. Remote references, standalone schema files, Swagger 2.0, OpenAPI 3.2, and JSON Schema resource scopes using `$id`, `$dynamicRef`, or `$dynamicAnchor` are not supported and fail explicitly. Local `$anchor` references are converted to JSON pointers. Nested reference targets are promoted to named components when needed for reliable serialization. Links using `operationId` are converted to `operationRef` so they survive ID collisions and operation deduplication.

## Merge a directory

```csharp
OpenApiDocument merged = await merger.MergeDirectory(
    "contracts",
    cancellationToken);
```

The directory is searched recursively for `.json`, `.yaml`, and `.yml` files in deterministic order, without following symbolic links or junctions. Unrelated files are ignored; files declaring OpenAPI that are malformed or unsupported fail the merge. Each included document uses its filename without the extension as its prefix. At least one readable document is required.

## Merge from Git

```csharp
OpenApiDocument merged = await merger.MergeGitUrl(
    "https://github.com/example/api-contracts.git",
    "openapi",
    cancellationToken);
```

The optional subdirectory must resolve inside the cloned repository. The same recursive discovery and merge rules as `MergeDirectory` apply.

## Output

When combining multiple documents from the Postman converter, collection-level `x-postman-warnings`, `x-postman-variables`, `x-postman-events`, and `x-postman-unmapped-requests` are retained under `x-merged-postman-collections`. Each entry contains its input `prefix` and original `metadata`, keeping collection variables and scripts separate. A single input retains these extensions at the root. Conflicting unknown extensions still fail explicitly.

`ToJson` emits OpenAPI 3.0 when all inputs use 3.0, or OpenAPI 3.1 when any input uses 3.1. Mixed inputs upgrade 3.0 nullable schemas to 3.1 semantics. Webhooks, reusable path items, recursive schemas, discriminator mappings, and callback operations are retained. Examples, defaults, and extension payloads are not traversed as OpenAPI objects.

The output is checked both before and after serialization. Boolean schemas and non-string `const` values may be expressed using equivalent object schemas or singleton enums to avoid limitations of the underlying OpenAPI model. JSON Schema keywords such as `prefixItems` remain at their actual schema locations.

The merger returns an in-memory `OpenApiDocument`; it does not write an output file. Use `ToJson` on the returned instance to select the correct output version. Direct callers of Microsoft's serialization API must select `SerializeAsV31` for a 3.1 result.
