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
}
