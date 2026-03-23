using Soenneker.OpenApi.Merger.Abstract;
using Soenneker.Tests.FixturedUnit;
using Xunit;

namespace Soenneker.OpenApi.Merger.Tests;

[Collection("Collection")]
public sealed class OpenApiMergerTests : FixturedUnitTest
{
    private readonly IOpenApiMerger _util;

    public OpenApiMergerTests(Fixture fixture, ITestOutputHelper output) : base(fixture, output)
    {
        _util = Resolve<IOpenApiMerger>(true);
    }

    [Fact]
    public void Default()
    {

    }
}
