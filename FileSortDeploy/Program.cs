using System.Diagnostics;
using System.IO.Compression;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;

var filesDirectory = @"..\..\..\files";
var resultDirectory = @"E:\LogsResult\";

var rootDirectory = @"..\..\..\files";

try
{
    using (new StopwatchTimer("Process finished in: "))
    {
        // Unpack, read and sort files 
        // Read files from local drive
        // var filePaths = GetFilePaths(filesDirectory, "*.gz");
        // var fileCollections = GroupFilesByName(filePaths);

        // Parallel.ForEach(fileCollections, new ParallelOptions { MaxDegreeOfParallelism = 3 },
        //     collection =>
        //     {
        //         if (!File.Exists(resultDirectory + collection.Date))
        //         {
        //             using (new StopwatchTimer($"Date {collection.Date} finished processing in: "))
        //             {
        //                 try
        //                 {
        //                     Console.WriteLine($"Start Processing file {collection.Date}");
        //                     WriteFile(ProcessFileJson(SortLines(ConvertComposedFile(LocalReadComposeFile(collection)))),
        //                         resultDirectory + collection.Date);
        //                 }
        //                 catch (OutOfMemoryException e)
        //                 {
        //                     //Attempt to process using slower method reading file line by line
        //                     Console.WriteLine(
        //                         $"Date {collection.Date} errored out with OutOfMemoryException, attempting to use different method");
        //                     WriteArrayFile(ProcessJsonList(SortLines(LocalReadComposeFileAsLines(collection))),
        //                         resultDirectory + collection.Date);
        //                 }
        //                 catch (Exception e)
        //                 {
        //                     Console.Write($"File {collection.Date} errored out with an exception ");
        //                     Console.WriteLine(e);
        //                 }
        //             }
        //         }
        //         else
        //         {
        //             Console.WriteLine($"File skipped {collection.Date}");
        //         }
        //     });
        //

        // Read files from amazon
         var amazonFiles = await AmazonReadComposePaths();
         var remainingDates = amazonFiles.TakeLast(7).ToList();
        
         foreach (var collection in remainingDates)
         {
             if (!File.Exists(resultDirectory + collection.Date))
             {
                 using (new StopwatchTimer($"Date {collection.Date} finished processing in: "))
                 {
                     try
                     {
                         Console.WriteLine($"Start Processing file {collection.Date}");
                         WriteFile(
                             ProcessFileJson(
                                 SortLines(ConvertComposedFile(await AmazonReadComposeFile(collection)))),
                             resultDirectory + collection.Date);
                     }
                     catch (OutOfMemoryException e)
                     {
                         //Attempt to process using slower method reading file line by line
                         Console.WriteLine(
                             $"Date {collection.Date} errored out with OutOfMemoryException, attempting to use different method");
                         WriteArrayFile(ProcessJsonList(SortLines(await AmazonReadComposeFileAsLines(collection))),
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
         }

            
        


        // Partition files
        // var historicalOrderPaths = GetFilePathsHistorically(resultDirectory, "*.txt");
        // foreach (var path in historicalOrderPaths)
        // {
        //     using (new StopwatchTimer($"Date {path[..^14]} finished processing in: "))
        //     {
        //         PartitionFileWrite(path, partitionResultDirectory);
        //     }
        //     
        //     FileDelete(path);
        // }

        // Test GetFilePathsHistorically paths
        // var partitionedLogsHistoricalOrderPaths = GetFilePathsHistorically(partitionResultDirectory, "*.txt");
        // foreach (var path in partitionedLogsHistoricalOrderPaths)
        // {
        //     Console.WriteLine(path);
        // }
    }
}

catch (Exception ex)
{
    Console.WriteLine($"An error occurred: {ex.Message}");
}

return;

async Task<List<DateCollection>> AmazonReadComposePaths()
{
    var folders = await ListFoldersAsync(bucketName, prefix);
    var sortedPaths = folders.OrderBy(path =>
    {
        var parts = path.Split('/');
        return int.Parse(parts[3]);
    }).ToList();

    var allFilePaths = new List<string>();

    foreach (var folder in sortedPaths)
    {
        var filesList = await ListFilesAsync(bucketName, folder);
        var sortedList = filesList.OrderBy(obj => obj.LastModified)
            .ToList();

        foreach (var fileS3Object in sortedList)
        {
            allFilePaths.Add(fileS3Object.Key);
        }
    }

    return GroupFilesByName(allFilePaths.ToArray());
}

async Task DownloadFileAsync(string key, string localFilePath)
{
    var request = new GetObjectRequest
    {
        BucketName = bucketName,
        Key = key
    };

    var keyPathParts = key.Split('/');

    using var response = await amazonS3Client.GetObjectAsync(request);
    await response.WriteResponseStreamToFileAsync(@$"{localFilePath}\\{keyPathParts[3]}\\{keyPathParts[4]}", false,
        CancellationToken.None);
}

async Task<string> AmazonFileToStringAsync(string key)
{
    var request = new GetObjectRequest
    {
        BucketName = bucketName,
        Key = key
    };

    using var response = await amazonS3Client.GetObjectAsync(request);
    await using var gzipStream = new GZipStream(response.ResponseStream, CompressionMode.Decompress);
    using var reader = new StreamReader(gzipStream, Encoding.UTF8);
    return await reader.ReadToEndAsync();
}

async Task<StreamReader> AmazonFileGetReader(string key)
{
    var request = new GetObjectRequest
    {
        BucketName = bucketName,
        Key = key
    };

    using var response = await amazonS3Client.GetObjectAsync(request);
    await using var gzipStream = new GZipStream(response.ResponseStream, CompressionMode.Decompress);
    return new StreamReader(gzipStream, Encoding.UTF8);
}

Task<AmazonS3Client> ConnectAmazonS3()
{
    var credentials = new BasicAWSCredentials(accessKey, secretKey);
    var config = new AmazonS3Config
    {
        ServiceURL = serviceUrl,
        ForcePathStyle = true
    };

    var client = new AmazonS3Client(credentials, config);
    return Task.FromResult(client);
}

async Task<List<string>> ListFoldersAsync(string bucketName, string prefix = "")
{
    var request = new ListObjectsV2Request
    {
        BucketName = bucketName,
        Prefix = prefix,
        Delimiter = "/"
    };

    var response = await amazonS3Client.ListObjectsV2Async(request);

    return response.CommonPrefixes;
}

async Task<List<S3Object>> ListFilesAsync(string bucketName, string prefix = "")
{
    var request = new ListObjectsV2Request
    {
        BucketName = bucketName,
        Prefix = prefix,
        Delimiter = "/"
    };

    var response = await amazonS3Client.ListObjectsV2Async(request);

    return response.S3Objects;
}

void PartitionFileWrite(string path, string resultPath)
{
    var sb = new StringBuilder();
    var fileCounter = 1;
    var lineCounter = 0;

    var date = Path.GetFileName(path)[..^4];
    var resultDirectoryPath = @$"{resultPath}\{date}";

    Directory.CreateDirectory(resultDirectoryPath);

    using var reader = new StreamReader(path);
    while (reader.ReadLine() is { } line)
    {
        sb.AppendLine(line);
        lineCounter++;

        if (lineCounter >= 500000)
        {
            File.WriteAllText(@$"{resultDirectoryPath}\{date}_P{fileCounter}.txt", sb.ToString());
            fileCounter++;
            sb.Clear();
            lineCounter = 0;
        }
    }

    if (lineCounter > 0)
    {
        File.WriteAllText(@$"{resultDirectoryPath}\{date}_P{fileCounter}.txt", sb.ToString());
    }
}


string LocalReadComposeFile(DateCollection collection)
{
    var mergedFilesBuilder = new StringBuilder();
    foreach (string filePath in collection.FilePaths)
    {
        var reader = OpenGzipFile(filePath);
        mergedFilesBuilder.AppendLine(reader.ReadToEnd());
    }

    return mergedFilesBuilder.ToString();
}


async Task<string> AmazonReadComposeFile(DateCollection collection)
{
    var mergedFilesBuilder = new StringBuilder();
    foreach (var filePath in collection.FilePaths)
    {
        var amazonFile = await AmazonFileToStringAsync(filePath);
        mergedFilesBuilder.AppendLine(amazonFile);
    }

    return mergedFilesBuilder.ToString();
}

string?[] LocalReadComposeFileAsLines(DateCollection collection)
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

async Task<string?[]> AmazonReadComposeFileAsLines(DateCollection collection)
{
    var lines = new List<string?>();

    foreach (var filePath in collection.FilePaths)
    {
        using var reader = await AmazonFileGetReader(filePath);
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
    var filePaths = Directory.GetFiles(directory, pattern, SearchOption.AllDirectories);

    Array.Sort(filePaths, (a, b) =>
    {
        var fileNameA = Path.GetFileNameWithoutExtension(a);
        var fileNameB = Path.GetFileNameWithoutExtension(b);

        var datePart1 = fileNameA.Split('_')[0];
        var datePart2 = fileNameB.Split('_')[0];


        if (DateTime.TryParse(datePart1, out var dateA) &&
            DateTime.TryParse(datePart2, out var dateB))
        {
            var dateComparison = DateTime.Compare(dateA, dateB);
            if (dateComparison != 0)
                return dateComparison;
        }

        if (fileNameA.Contains("_P") && fileNameB.Contains("_P"))
        {
            var partA = fileNameA.Split('_')[1];
            var partB = fileNameB.Split('_')[1];

            if (int.TryParse(partA.Substring(1), out var numA) &&
                int.TryParse(partB.Substring(1), out var numB))
            {
                return numA.CompareTo(numB);
            }
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
    path = path[..^2] + "txt";
    Console.WriteLine($"File written {path}");
    File.WriteAllText(path, file);
}

void WriteArrayFile(string[] lines, string filePath)
{
    Console.WriteLine($"File written {filePath}");
    using var writer = new StreamWriter(filePath);
    foreach (string line in lines)
    {
        writer.WriteLine(line);
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
    private string _date = date;

    public string Date
    {
        get => _date;
        set => _date = value;
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