using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Soenneker.Git.Util.Registrars;
using Soenneker.OpenApi.Merger.Abstract;
using Soenneker.Utils.MemoryStream.Registrars;

namespace Soenneker.OpenApi.Merger.Registrars;

/// <summary>
/// Registers the OpenAPI merger and its file and Git dependencies.
/// </summary>
public static class OpenApiMergerRegistrar
{
    /// <summary>
    /// Adds <see cref="IOpenApiMerger"/> as a singleton service. <para/>
    /// </summary>
    /// <param name="services">Service collection that receives the registration.</param>
    /// <returns>The same service collection, so additional registrations can be chained.</returns>
    public static IServiceCollection AddOpenApiMergerAsSingleton(this IServiceCollection services)
    {
        services.AddGitUtilAsSingleton();
        services.AddMemoryStreamUtilAsSingleton();
        services.TryAddSingleton<IOpenApiMerger, OpenApiMerger>();

        return services;
    }

    /// <summary>
    /// Adds <see cref="IOpenApiMerger"/> as a scoped service. <para/>
    /// </summary>
    /// <param name="services">Service collection that receives the registration.</param>
    /// <returns>The same service collection, so additional registrations can be chained.</returns>
    public static IServiceCollection AddOpenApiMergerAsScoped(this IServiceCollection services)
    {
        services.AddGitUtilAsScoped();
        services.AddMemoryStreamUtilAsScoped();
        services.TryAddScoped<IOpenApiMerger, OpenApiMerger>();

        return services;
    }
}
