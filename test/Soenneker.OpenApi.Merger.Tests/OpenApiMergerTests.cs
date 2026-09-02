using System;
using System.IO;
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
    public void Default()
    {

    }

    [Test]
    public async ValueTask MergeOpenApis_namespaces_paths_and_rejects_collisions(CancellationToken cancellationToken)
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
                                        "responses": { "200": { "description": "OK" } }
                                      }
                                    }
                                  }
                                }
                                """;

        try
        {
            await File.WriteAllTextAsync(firstPath, document);
            await File.WriteAllTextAsync(secondPath, document);

            OpenApiDocument merged = await _util.MergeOpenApis([("accounts", firstPath), ("billing", secondPath)], cancellationToken: cancellationToken);

            await Assert.That(merged.Paths.ContainsKey("/accounts/users")).IsTrue();
            await Assert.That(merged.Paths.ContainsKey("/billing/users")).IsTrue();

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
}
