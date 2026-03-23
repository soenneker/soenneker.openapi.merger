using Microsoft.OpenApi.Models;
using System;
using System.Collections.Generic;

namespace Soenneker.OpenApi.Merger.Dtos;

internal sealed class SourceDocument
{
    public SourceDocument(string filePath, string prefix, OpenApiDocument document)
    {
        FilePath = filePath;
        Prefix = prefix;
        Document = document;
        ComponentRenameMaps = new Dictionary<string, Dictionary<string, string>>(StringComparer.Ordinal);
    }

    public string FilePath { get; }
    public string Prefix { get; }
    public OpenApiDocument Document { get; }
    public Dictionary<string, Dictionary<string, string>> ComponentRenameMaps { get; set; }
}