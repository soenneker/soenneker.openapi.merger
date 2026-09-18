using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Merger.Dtos;

internal sealed class SourceDocument
{
    public SourceDocument(string filePath, string prefix, JsonObject document)
    {
        FilePath = filePath;
        Prefix = prefix;
        Document = document;
        ComponentRenameMaps = new Dictionary<string, Dictionary<string, string>>(StringComparer.Ordinal);
    }

    public string FilePath { get; }
    public string Prefix { get; }
    public JsonObject Document { get; set; }
    public Dictionary<string, Dictionary<string, string>> ComponentRenameMaps { get; set; }
    public Dictionary<string, string> OperationPointers { get; } = new(StringComparer.Ordinal);
    public Dictionary<string, string> Anchors { get; } = new(StringComparer.Ordinal);
}
