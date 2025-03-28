using System.Diagnostics;
using System.IO.Compression;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

var rootDirectory = @"..\..\..\files";

// + filename
var mergedFilesDirectory = @"..\..\..\mergedFiles\";
var sortedFilesDirectory = @"..\..\..\sortedFiles\";

try
{
    ComposeDuplicateFiles(GroupFilesByName(GetFilePaths(rootDirectory, "*.gz")));

    SortFiles(GetFilePaths(mergedFilesDirectory[..^1], "*.txt"));

    ProcessFiles(GetFilePaths(sortedFilesDirectory[..^1], "*.txt"));
}
catch (Exception ex)
{
    Console.WriteLine($"An error occurred: {ex.Message}");
}


return;

void ComposeDuplicateFiles(List<DateCollection> collections)
{
    foreach (var collection in collections)
    {
        using StreamWriter writer = new StreamWriter(mergedFilesDirectory + collection.Date);
        foreach (string filePath in collection.FilePaths)
        {
            var reader = OpenGzipFile(filePath);
            var fileContents = reader.ReadToEnd();
            writer.Write(fileContents);
        }
    }
}

void SortFiles(string[] filePaths)
{
    foreach (var path in filePaths)
    {
        SortLinesInFile(path);
    }
}

void ProcessFiles(string[] filePaths)
{
    foreach (var path in filePaths)
    {
        RewriteRemoveFirstElement(path);
    }
}


string[] GetFilePaths(string directory, string pattern)
{
    return Directory.GetFiles(directory, pattern, SearchOption.AllDirectories);
}

// Remove first element and count the largest line size
void RewriteRemoveFirstElement(string filePath)
{
    var maxJsonSize = 0;
    var largestLine = "";
    var sb = new StringBuilder();

    using (var reader = new StreamReader(filePath))
    {
        while (reader.ReadLine() is { } line)
        {
            try
            {
                var jsonNode = JsonNode.Parse(line);
                if (jsonNode != null)
                {
                    var lastElementSize = GetLastElementSize(jsonNode);
                    jsonNode.AsArray().RemoveAt(0);
                    var jsonString = jsonNode.ToJsonString();
                    sb.AppendLine(jsonString);

                    if (lastElementSize > maxJsonSize)
                    {
                        maxJsonSize = lastElementSize;
                        largestLine = jsonString;
                    }
                }
                else
                {
                    Console.WriteLine("Line was null");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        Console.WriteLine("Largest json size: " + maxJsonSize);
        Console.WriteLine("Largest json line: " + largestLine);
    }

    using (var writer = new StreamWriter(filePath))
    {
        writer.WriteLine(sb.ToString());
    }
}

StreamReader OpenGzipFile(string filePath)
{
    var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
    var gzipStream = new GZipStream(fileStream, CompressionMode.Decompress);
    return new StreamReader(gzipStream);
}


int GetLastElementSize(JsonNode jsonNode)
{
    if (jsonNode is not JsonArray jsonArray)
    {
        throw new InvalidOperationException("Input is not a JSON array");
    }

    if (jsonArray.Count == 0)
    {
        throw new InvalidOperationException("JSON array is empty");
    }

    var lastArray = jsonArray[^1];

    if (lastArray != null)
    {
        return lastArray.AsObject().Count;
    }
    else
    {
        throw new InvalidOperationException("JSON array is empty");
    }
}

void SortLinesInFile(string inputFilePath)
{
    var sortedLines = File.ReadAllLines(inputFilePath)
        .Select(line =>
        {
            try
            {
                var jsonNode = JsonNode.Parse(line);
                return new
                {
                    OriginalLine = line,
                    FirstTimestamp = jsonNode[0].GetValue<long>()
                };
            }
            catch (JsonException)
            {
                Console.WriteLine($"Could not parse line: {line}");
                return null;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unexpected error parsing line: {line}. Error: {ex.Message}");
                return null;
            }
        })
        .Where(x => x != null)
        .OrderBy(x => x.FirstTimestamp)
        .Select(x => x.OriginalLine)
        .ToArray();

    File.WriteAllLines(sortedFilesDirectory + Path.GetFileName(inputFilePath), sortedLines);
}


List<DateCollection> GroupFilesByName(string[] filePaths)
{
    var groupedFiles = filePaths
        .GroupBy(path => Path.GetFileName(path))
        .Select(g => new DateCollection(
            g.Key,
            g.ToList()
        ));
    return groupedFiles.ToList();
}

internal class DateCollection(string date, List<string> filePaths)
{
    private string _date = date[..^2] + "txt";

    public string Date
    {
        get => _date;
        set => _date = value[..^2] + "txt";
    }

    public List<string> FilePaths { get; set; } = filePaths;
}