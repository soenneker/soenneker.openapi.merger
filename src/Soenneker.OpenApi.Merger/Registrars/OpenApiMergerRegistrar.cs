using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Soenneker.Git.Util.Registrars;
using Soenneker.OpenApi.Merger.Abstract;

namespace Soenneker.OpenApi.Merger.Registrars;

/// <summary>
/// A utility library to merge OpenApi specs
/// </summary>
public static class OpenApiMergerRegistrar
{
    /// <summary>
    /// Adds <see cref="IOpenApiMerger"/> as a singleton service. <para/>
    /// </summary>
    public static IServiceCollection AddOpenApiMergerAsSingleton(this IServiceCollection services)
    {
        services.AddGitUtilAsSingleton();
        services.TryAddSingleton<IOpenApiMerger, OpenApiMerger>();

        return services;
    }

    /// <summary>
    /// Adds <see cref="IOpenApiMerger"/> as a scoped service. <para/>
    /// </summary>
    public static IServiceCollection AddOpenApiMergerAsScoped(this IServiceCollection services)
    {
        services.AddGitUtilAsScoped();
        services.TryAddScoped<IOpenApiMerger, OpenApiMerger>();

        return services;
    }
}
