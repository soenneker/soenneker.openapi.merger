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

For example, the path `/users` from the `accounts` input becomes `/accounts/users`. If a source path already begins with its prefix, the prefix is not duplicated. Component names and operation IDs are namespaced as needed, and local or relative component references are rewritten to follow renamed components.

`MergeOpenApis` is strict: every listed file must exist and produce a document. A duplicate merged path, duplicate component after renaming, unresolved component reference, missing security scheme, invalid discriminator mapping, or transformation failure throws instead of returning an incomplete document.

## Merge a directory

```csharp
OpenApiDocument merged = await merger.MergeDirectory(
    "contracts",
    cancellationToken);
```

The directory is searched recursively for `.json`, `.yaml`, and `.yml` files. Files that do not parse as OpenAPI documents are ignored; each included document uses its filename without the extension as its prefix. At least one readable document is required.

## Merge from Git

```csharp
OpenApiDocument merged = await merger.MergeGitUrl(
    "https://github.com/example/api-contracts.git",
    "openapi",
    cancellationToken);
```

The optional subdirectory must resolve inside the cloned repository. The same recursive discovery and merge rules as `MergeDirectory` apply.

## Output

`ToJson` serializes the merged document as OpenAPI 3 JSON. The merger returns an in-memory `OpenApiDocument`; it does not write an output file.
