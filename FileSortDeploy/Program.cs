using System.Diagnostics;
using System.IO.Compression;
using System.Text.Json.Nodes;

var rootDirectory = @"..\..\..\files";

// + filename
var sortedFilesDirectory = @"..\..\..\sortedFiles\";

try
{
    var gzFiles = GetFilePaths(rootDirectory);

    var collections = GroupFilesByName(gzFiles);

    ComposeDuplicateFiles(collections);
    // ReadFilesRecursively(gzFiles);
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
        using StreamWriter writer = new StreamWriter(sortedFilesDirectory + collection.Date);
        foreach (string filePath in collection.FilePaths)
        {
            var reader = OpenGzipFile(filePath);
            var fileContents = reader.ReadToEnd();
            writer.Write(fileContents);
        }
    }
}

void ReadFilesRecursively(string[] directories)
{
    try
    {
        foreach (string filePath in directories)
        {
            var totalProcessingTimer = new Stopwatch();
            totalProcessingTimer.Start();
            ProcessGzipFile(filePath);

            totalProcessingTimer.Stop();
            Console.WriteLine($"File {filePath} processing time: {totalProcessingTimer.Elapsed}");
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error reading directory {directories}: {ex.Message}");
    }
}


string[] GetFilePaths(string directory)
{
    return Directory.GetFiles(directory, "*.gz", SearchOption.AllDirectories);
}

void ProcessGzipFile(string filePath)
{
    using var reader = OpenGzipFile(filePath);
    var maxJsonSize = 0;
    var largestLine = "";

    while (reader.ReadLine() is { } line)
    {
        try
        {
            var jsonNode = JsonNode.Parse(line);
            if (jsonNode != null)
            {
                var lastElementSize = GetLastElementSize(jsonNode);
                jsonNode.AsArray().RemoveAt(0);
                var jsonString = jsonNode?.ToJsonString();

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