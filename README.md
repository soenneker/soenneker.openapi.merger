[![](https://img.shields.io/nuget/v/soenneker.openapi.merger.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.openapi.merger/)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.openapi.merger/publish-package.yml?style=for-the-badge)](https://github.com/soenneker/soenneker.openapi.merger/actions/workflows/publish-package.yml)
[![](https://img.shields.io/nuget/dt/soenneker.openapi.merger.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.openapi.merger/)

# ![](https://user-images.githubusercontent.com/4441470/224455560-91ed3ee7-f510-4041-a8d2-3fc093025112.png) Soenneker.OpenApi.Merger
### A utility library to merge OpenApi specs

## Installation

```
dotnet add package Soenneker.OpenApi.Merger
```

## Usage

```csharp
using Soenneker.OpenApi.Merger.Abstract;

OpenApiDocument mergedFromFiles = await openApiMerger.MergeOpenApis(
    ("contacts", @"C:\specs\contacts.json"),
    ("locations", @"C:\specs\locations.yaml"));

OpenApiDocument mergedFromDirectory = await openApiMerger.MergeDirectory(@"C:\specs");
string json = openApiMerger.ToJson(mergedFromDirectory);

OpenApiDocument mergedFromGit = await openApiMerger.MergeGitUrl("https://github.com/owner/repo");
OpenApiDocument mergedFromGitSubdirectory = await openApiMerger.MergeGitUrl("https://github.com/owner/repo", "apps");
```

The merger:

- can merge an explicit set of `(prefix, filePath)` OpenAPI inputs
- scans a directory recursively for `.json`, `.yaml`, and `.yml` OpenAPI files
- prefixes paths with the source file name when needed
- renames colliding component names and rewrites component `$ref` values to match
- can clone and merge directly from a Git repository URL
