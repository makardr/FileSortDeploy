using System.Diagnostics;
using System.IO.Compression;
using System.Text.Json.Nodes;

var rootDirectory = @"..\..\..\files";

try
{
    ReadFilesRecursively(rootDirectory);
}
catch (Exception ex)
{
    Console.WriteLine($"An error occurred: {ex.Message}");
}


return;


void ReadFilesRecursively(string directory)
{
    try
    {
        string[] gzFiles = Directory.GetFiles(directory, "*.gz", SearchOption.AllDirectories);

        foreach (string filePath in gzFiles)
        {
            Stopwatch totalProcessingTimer = new Stopwatch();
            totalProcessingTimer.Start();
            ProcessGzipFile(filePath);

            totalProcessingTimer.Stop();
            Console.WriteLine($"File {filePath} processing time: {totalProcessingTimer.Elapsed}");
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error reading directory {directory}: {ex.Message}");
    }
}

void ProcessGzipFile(string filePath)
{
    using (FileStream fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read))
    using (GZipStream gzipStream = new GZipStream(fileStream, CompressionMode.Decompress))
    using (StreamReader reader = new StreamReader(gzipStream))
    {
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
                    // Console.WriteLine(jsonString);
                    // Console.WriteLine(lastElementSize);
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
                // Skip every json failed line comletely
                Console.WriteLine(ex.Message);
            }
        }

        Console.WriteLine("Largest json size: " + maxJsonSize);
        Console.WriteLine("Largest json line: " + largestLine);
    }
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