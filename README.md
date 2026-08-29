[![](https://img.shields.io/nuget/v/soenneker.openapi.merger.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.openapi.merger/)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.openapi.merger/publish-package.yml?style=for-the-badge)](https://github.com/soenneker/soenneker.openapi.merger/actions/workflows/publish-package.yml)
[![](https://img.shields.io/nuget/dt/soenneker.openapi.merger.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.openapi.merger/)

# Soenneker.OpenApi.Merger

A utility library to merge OpenApi specs.

## Install

```bash
dotnet add package Soenneker.OpenApi.Merger
```

## Quick start

```csharp
using Soenneker.OpenApi.Merger.Registrars;
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection();
var result = services.AddOpenApiMergerAsSingleton();
```

Adds `IOpenApiMerger` as a singleton service.

## What you get

- `IOpenApiMerger` — A utility library to merge OpenApi specs.
- `OpenApiMergerRegistrar` — A utility library to merge OpenApi specs.

## API at a glance

| API | What it does | Result / important behavior |
| --- | --- | --- |
| `IOpenApiMerger.MergeOpenApis(inputs, cancellationToken)` | Merges the provided OpenAPI files into a single document, prefixing paths by the supplied input prefix. | A task whose result is the requested openAPI Document. |
| `IOpenApiMerger.MergeDirectory(directoryPath, cancellationToken)` | Merges every OpenAPI file discovered beneath `directoryPath` into a single document. | A task whose result is the requested openAPI Document. |
| `IOpenApiMerger.MergeGitUrl(gitUrl, repositorySubdirectory, cancellationToken)` | Clones `gitUrl`, optionally scopes to `repositorySubdirectory`, and merges the discovered OpenAPI files. | A task whose result is the requested openAPI Document. |
| `IOpenApiMerger.ToJson(document)` | Serializes a merged OpenAPI document as v3 JSON. | Returns `string`. |
| `OpenApiMergerRegistrar.AddOpenApiMergerAsSingleton(services)` | Adds `IOpenApiMerger` as a singleton service. | The same service collection, so additional registrations can be chained. |
| `OpenApiMergerRegistrar.AddOpenApiMergerAsScoped(services)` | Adds `IOpenApiMerger` as a scoped service. | The same service collection, so additional registrations can be chained. |

## Practical notes

- Cancellation stops pending work; it does not undo work that has already completed.
