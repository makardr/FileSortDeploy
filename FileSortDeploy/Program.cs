using System.Diagnostics;
using System.IO.Compression;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

var rootDirectory = @"..\..\..\files";

try
{
    using (new StopwatchTimer("Process finished in: "))
    {
        var fileCollections = GroupFilesByName(GetFilePaths(rootDirectory, "*.gz"));

        Parallel.ForEach(fileCollections, new ParallelOptions { MaxDegreeOfParallelism = 3 },
            collection =>
            {
                if (!File.Exists(resultDirectory + collection.Date))
                {
                    using (new StopwatchTimer($"Date {collection.Date} finished processing in: "))
                    {
                        try
                        {
                            Console.WriteLine($"Start Processing file {collection.Date}");
                            WriteFile(ProcessFileJson(SortLines(ConvertComposedFile(ReadComposeFile(collection)))),
                                resultDirectory + collection.Date);
                        }
                        catch (OutOfMemoryException e)
                        {
                            //Attempt to process using slower method reading file line by line
                            WriteArrayFile(ProcessJsonList(SortLines(ReadComposeFileLines(collection))),
                                resultDirectory + collection.Date);
                        }
                        catch (Exception e)
                        {
                            Console.Write($"File {collection.Date} errored out with an exception ");
                            Console.WriteLine(e);
                        }
                    }
                }
                else
                {
                    Console.WriteLine($"File skipped {collection.Date}");
                }
            });

        // var historicalOrderPaths = GetFilePathsHistorically(resultDirectory, "*.txt");
    }
}

catch (Exception ex)
{
    Console.WriteLine($"An error occurred: {ex.Message}");
}

return;

string ReadComposeFile(DateCollection collection)
{
    var mergedFilesBuilder = new StringBuilder();
    foreach (string filePath in collection.FilePaths)
    {
        var reader = OpenGzipFile(filePath);
        mergedFilesBuilder.AppendLine(reader.ReadToEnd());
    }

    return mergedFilesBuilder.ToString();
}

string?[] ReadComposeFileLines(DateCollection collection)
{
    var lines = new List<string?>();

    foreach (string filePath in collection.FilePaths)
    {
        using var reader = OpenGzipFile(filePath);
        while (reader.ReadLine() is { } line)
        {
            lines.Add(line);
        }
    }

    return lines.ToArray();
}

string[] ConvertComposedFile(string file)
{
    List<string> stringList = [];

    using (StringReader reader = new StringReader(file))
    {
        while (reader.ReadLine() is { } line)
        {
            stringList.Add(line);
        }
    }

    return stringList.ToArray();
}

string[] SortLines(string?[] inputFilePath)
{
    return inputFilePath
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
                // Console.WriteLine($"Could not parse line: {line}");
                return null;
            }
            catch (Exception ex)
            {
                // Console.WriteLine($"Unexpected error parsing line: {line}. Error: {ex.Message}");
                return null;
            }
        })
        .Where(x => x != null)
        .OrderBy(x => x.FirstTimestamp)
        .Select(x => x.OriginalLine)
        .ToArray();
}

string ProcessFileJson(string[] lines)
{
    // var maxJsonSize = 0;
    // var largestLine = "";
    var processedFileBuilder = new StringBuilder();

    foreach (var line in lines)
    {
        try
        {
            var jsonNode = JsonNode.Parse(line);
            if (jsonNode != null)
            {
                jsonNode.AsArray().RemoveAt(0);
                var jsonString = jsonNode.ToJsonString();
                processedFileBuilder.AppendLine(jsonString);


                // var lastElementSize = GetLastElementSize(jsonNode);
                // if (lastElementSize > maxJsonSize)
                // {
                //     maxJsonSize = lastElementSize;
                //     largestLine = jsonString;
                // }
            }
            else
            {
                // Console.WriteLine("Line was null");
            }
        }
        catch (Exception ex)
        {
            // Console.WriteLine(ex.Message);
        }
    }

    // Console.WriteLine("Largest json size: " + maxJsonSize);
    // Console.WriteLine("Largest json line: " + largestLine);
    return processedFileBuilder.ToString();
}


string[] ProcessJsonList(string[] lines)
{
    List<string> stringList = [];

    foreach (var line in lines)
    {
        try
        {
            var jsonNode = JsonNode.Parse(line);
            if (jsonNode != null)
            {
                jsonNode.AsArray().RemoveAt(0);
                var jsonString = jsonNode.ToJsonString();
                stringList.Add(jsonString);
            }
            else
            {
                // Console.WriteLine("Line was null");
            }
        }
        catch (Exception ex)
        {
            // Console.WriteLine(ex.Message);
        }
    }

    return stringList.ToArray();
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

string[] GetFilePathsHistorically(string directory, string pattern)
{
    string[] filePaths = Directory.GetFiles(directory, pattern, SearchOption.AllDirectories);

    Array.Sort(filePaths, (a, b) =>
    {
        var fileNameA = Path.GetFileNameWithoutExtension(a);
        var fileNameB = Path.GetFileNameWithoutExtension(b);

        if (DateTime.TryParse(fileNameA, out var dateA) &&
            DateTime.TryParse(fileNameB, out var dateB))
        {
            return DateTime.Compare(dateA, dateB);
        }

        return string.CompareOrdinal(fileNameA, fileNameB);
    });

    return filePaths;
}

string[] GetFilePaths(string directory, string pattern)
{
    return Directory.GetFiles(directory, pattern, SearchOption.AllDirectories);
}

void WriteFile(string file, string path)
{
    Console.WriteLine($"File written {path}");
    File.WriteAllText(path, file);
}

void WriteArrayFile(string[] lines, string filePath)
{
    using (StreamWriter writer = new StreamWriter(filePath))
    {
        foreach (string line in lines)
        {
            writer.WriteLine(line);
        }
    }
}

void FileDelete(string path)
{
    File.Delete(path);
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

internal class StopwatchTimer(string message = "Elapsed time") : IDisposable
{
    private readonly Stopwatch _stopwatch = Stopwatch.StartNew();

    public void Dispose()
    {
        _stopwatch.Stop();
        Console.WriteLine($"{message}: {_stopwatch.ElapsedMilliseconds} ms");
    }
}