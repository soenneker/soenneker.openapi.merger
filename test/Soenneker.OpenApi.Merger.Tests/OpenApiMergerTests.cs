using System;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.OpenApi;
using Soenneker.OpenApi.Merger.Abstract;
using Soenneker.Tests.HostedUnit;

namespace Soenneker.OpenApi.Merger.Tests;

[ClassDataSource<Host>(Shared = SharedType.PerTestSession)]
public sealed class OpenApiMergerTests : HostedUnitTest
{
    private readonly IOpenApiMerger _util;

    public OpenApiMergerTests(Host host) : base(host)
    {
        _util = Resolve<IOpenApiMerger>(true);
    }

    [Test]
    [Arguments(false, false)]
    [Arguments(true, false)]
    [Arguments(false, true)]
    public async ValueTask MergeOpenApis_compares_renamed_recursive_schemas_and_security(bool differentSchema, bool differentSecurity, CancellationToken cancellationToken)
    {
        string firstPath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid():N}.json");
        string secondPath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid():N}.json");
        const string document = """
            {
              "openapi": "3.0.3", "info": { "title": "Example", "version": "1" },
              "paths": { "/users": { "get": {
                "operationId": "list", "tags": ["Basic"], "security": [{"oauth": ["read"]}],
                "responses": { "200": { "description": "OK", "content": { "application/json": {
                  "schema": { "$ref": "#/components/schemas/User" }
                } } } }
              } } },
              "components": {
                "schemas": { "User": { "type": "object", "properties": {
                  "value": {"type": "string"}, "next": {"$ref": "#/components/schemas/User"}
                } } },
                "securitySchemes": { "oauth": { "type": "oauth2", "flows": { "clientCredentials": {
                  "tokenUrl": "https://example.com/token", "scopes": {"read": "Read access", "unused": "Other endpoint"}
                } } } }
              }
            }
            """;
        string renamed = document.Replace("User", "User_1", StringComparison.Ordinal)
            .Replace("\"list\"", "\"getPage\"", StringComparison.Ordinal)
            .Replace("\"Basic\"", "\"Details\"", StringComparison.Ordinal)
            .Replace(", \"unused\": \"Other endpoint\"", "", StringComparison.Ordinal);
        if (differentSchema)
            renamed = renamed.Replace("\"type\": \"string\"", "\"type\": \"integer\"", StringComparison.Ordinal);
        if (differentSecurity)
            renamed = renamed.Replace("[\"read\"]", "[\"write\"]", StringComparison.Ordinal);
        try
        {
            await File.WriteAllTextAsync(firstPath, document, cancellationToken);
            await File.WriteAllTextAsync(secondPath, renamed, cancellationToken);
            bool collisionThrown = false;
            try
            {
                OpenApiDocument merged = await _util.MergeOpenApis([("api", firstPath), ("api", secondPath)], cancellationToken);
                await Assert.That(merged.Paths["/api/users"].Operations!.Count).IsEqualTo(1);
            }
            catch (InvalidOperationException)
            {
                collisionThrown = true;
            }
            await Assert.That(collisionThrown).IsEqualTo(differentSchema || differentSecurity);
        }
        finally
        {
            File.Delete(firstPath);
            File.Delete(secondPath);
        }
    }

    [Test]
    public void Default()
    {

    }

    [Test]
    public async ValueTask MergeOpenApis_namespaces_paths_and_deduplicates_equivalent_operations(CancellationToken cancellationToken)
    {
        string firstPath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid():N}.json");
        string secondPath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid():N}.json");

        const string document = """
                                {
                                  "openapi": "3.0.3",
                                  "info": { "title": "Example", "version": "1.0" },
                                  "paths": {
                                    "/users": {
                                      "get": {
                                        "operationId": "listUsers",
                                        "responses": {
                                          "200": {
                                            "description": "OK",
                                            "content": {
                                              "application/json": {
                                                "schema": { "$ref": "#/components/schemas/User" }
                                              }
                                            }
                                          }
                                        }
                                      }
                                    }
                                  },
                                  "components": {
                                    "schemas": {
                                      "User": { "type": "string" }
                                    }
                                  }
                                }
                                """;

        try
        {
            await File.WriteAllTextAsync(firstPath, document);
            string richerDocument = document.Replace("\"description\": \"OK\"", "\"description\": \"A more detailed successful response\"", StringComparison.Ordinal);
            await File.WriteAllTextAsync(secondPath, richerDocument);

            OpenApiDocument merged = await _util.MergeOpenApis([("accounts", firstPath), ("billing", secondPath)], cancellationToken: cancellationToken);

            await Assert.That(merged.Paths.ContainsKey("/accounts/users")).IsTrue();
            await Assert.That(merged.Paths.ContainsKey("/billing/users")).IsTrue();

            OpenApiDocument deduplicated = await _util.MergeOpenApis([("accounts", firstPath), ("accounts", secondPath)], cancellationToken: cancellationToken);
            OpenApiOperation operation = deduplicated.Paths["/accounts/users"]!.Operations![HttpMethod.Get]!;

            await Assert.That(deduplicated.Paths.Count).IsEqualTo(1);
            await Assert.That(operation.Responses!["200"]!.Description).IsEqualTo("A more detailed successful response");
        }
        finally
        {
            File.Delete(firstPath);
            File.Delete(secondPath);
        }
    }

    [Test]
    public async ValueTask MergeOpenApis_rejects_different_operations_on_the_same_method_and_path(CancellationToken cancellationToken)
    {
        string firstPath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid():N}.json");
        string secondPath = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid():N}.json");

        const string document = """
                                {
                                  "openapi": "3.0.3",
                                  "info": { "title": "Example", "version": "1.0" },
                                  "paths": {
                                    "/users": {
                                      "get": {
                                        "operationId": "listUsers",
                                        "responses": {
                                          "200": {
                                            "description": "OK",
                                            "content": {
                                              "application/json": {
                                                "schema": { "$ref": "#/components/schemas/User" }
                                              }
                                            }
                                          }
                                        }
                                      }
                                    }
                                  },
                                  "components": {
                                    "schemas": {
                                      "User": { "type": "string" }
                                    }
                                  }
                                }
                                """;

        try
        {
            await File.WriteAllTextAsync(firstPath, document, cancellationToken);
            await File.WriteAllTextAsync(secondPath, document.Replace("\"type\": \"string\"", "\"type\": \"integer\"", StringComparison.Ordinal),
                cancellationToken);

            bool collisionThrown = false;

            try
            {
                await _util.MergeOpenApis([("accounts", firstPath), ("accounts", secondPath)], cancellationToken: cancellationToken);
            }
            catch (InvalidOperationException)
            {
                collisionThrown = true;
            }

            await Assert.That(collisionThrown).IsTrue();
        }
        finally
        {
            File.Delete(firstPath);
            File.Delete(secondPath);
        }
    }

    [Test]
    public async ValueTask MergeOpenApis_reads_yaml_inputs(CancellationToken cancellationToken)
    {
        string path = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid():N}.yaml");
        const string document = """
                                openapi: 3.0.3
                                info:
                                  title: YAML example
                                  version: '1.0'
                                paths:
                                  /users:
                                    get:
                                      operationId: listUsers
                                      responses:
                                        '200':
                                          description: OK
                                """;

        try
        {
            await File.WriteAllTextAsync(path, document, cancellationToken);
            OpenApiDocument merged = await _util.MergeOpenApis([("accounts", path)], cancellationToken);

            await Assert.That(merged.Paths.ContainsKey("/accounts/users")).IsTrue();
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Test]
    public async ValueTask MergeOpenApis_repairs_unresolved_schema_references(CancellationToken cancellationToken)
    {
        string path = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid():N}.json");
        const string document = """
                                {
                                  "openapi": "3.0.3",
                                  "info": { "title": "Dangling reference", "version": "1.0" },
                                  "paths": {
                                    "/users": {
                                      "get": {
                                        "responses": {
                                          "200": {
                                            "description": "OK",
                                            "content": {
                                              "application/json": {
                                                "schema": { "$ref": "#/components/schemas/Missing" }
                                              }
                                            }
                                          }
                                        }
                                      }
                                    }
                                  }
                                }
                                """;

        try
        {
            await File.WriteAllTextAsync(path, document, cancellationToken);
            OpenApiDocument merged = await _util.MergeOpenApis([("accounts", path)], cancellationToken);

            IOpenApiSchema schema = merged.Paths["/accounts/users"]!.Operations![HttpMethod.Get]!.Responses!["200"]!.Content!["application/json"]!.Schema!;
            await Assert.That(schema.Type).IsEqualTo(JsonSchemaType.Object);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Test]
    public async ValueTask MergeOpenApis_escapes_unescaped_json_control_characters(CancellationToken cancellationToken)
    {
        string path = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid():N}.json");
        string document = """
                          {
                            "openapi": "3.0.3",
                            "info": { "title": "Control character", "version": "1.0" },
                            "paths": {
                              "/users": {
                                "get": {
                                  "description": "before__VT__after",
                                  "responses": { "200": { "description": "OK" } }
                                }
                              }
                            }
                          }
                          """;
        document = document.Replace("__VT__", ((char)0x0B).ToString(), StringComparison.Ordinal);

        try
        {
            await File.WriteAllTextAsync(path, document, cancellationToken);
            OpenApiDocument merged = await _util.MergeOpenApis([("accounts", path)], cancellationToken);

            await Assert.That(merged.Paths.ContainsKey("/accounts/users")).IsTrue();
        }
        finally
        {
            File.Delete(path);
        }
    }

}
