using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using FileSortDeploy.FileProviders;
using FileSortDeploy.Helpers;
using FileSortDeploy.Values;
using dotenv.net;
using dotenv.net.Utilities;

namespace FileSortDeploy;

public static class Program
{
    private static readonly string PartitionResultDirectory = @"E:\PartitionedLogs\";

    private static readonly LocalProperties LocalProperties = new(
        @"..\..\..\files",
        @"E:\LogsResult\"
    );

    private static AmazonS3Properties? _amazonS3Properties;

    public static async Task Main(string[] args)
    {
        DotEnv.Load(new DotEnvOptions(envFilePaths: new[] { @"..\..\..\.env" }));

        _amazonS3Properties = new AmazonS3Properties(
            EnvReader.GetStringValue("BUCKET_NAME"),
            EnvReader.GetStringValue("PREFIX"),
            EnvReader.GetStringValue("ACCESS_KEY"),
            EnvReader.GetStringValue("SECRET_KEY"),
            EnvReader.GetStringValue("SERVICE_URL")
        );
        try
        {
            using (new StopwatchTimer("Process finished in: "))
            {
                // Download file from amazon storage
                // var fileProvider = new AmazonFileProvider(_amazonS3Properties);
                // await fileProvider.DownloadFile("events.parquet", @"E:\PartitionedLogs");
                Console.WriteLine($"RAM: {MemoryMeter.GetMemoryUsage()}");
                var fileProvider = new LocalFileProvider(LocalProperties);
                var filePaths = await fileProvider.ProvideComposedFilePaths();

                foreach (var collection in filePaths)
                {
                    if (!File.Exists(LocalProperties.ResultPath + collection.Date))
                    {
                        using (new StopwatchTimer($"Date {collection.Date} finished processing in: "))
                        {
                            try
                            {
                                Console.WriteLine($"Start Processing file {collection.Date} ");
                                FileHelper.WriteFile(
                                    ProcessFileJson(
                                        SortLines(await fileProvider.ReadComposedFile(collection))),
                                    LocalProperties.ResultPath + collection.Date);
                            }
                            catch (OutOfMemoryException e)
                            {
                                //Attempt to process using the slower method reading file line by line
                                Console.WriteLine(
                                    $"Date {collection.Date} errored out with OutOfMemoryException, attempting to use different method");
                                FileHelper.WriteArrayFile(
                                    ProcessJsonList(SortLines(await fileProvider.ReadComposedFileAsLines(collection))),
                                    LocalProperties.ResultPath + collection.Date);
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
                var historicalOrderPaths = FileHelper.GetFilePathsHistorically(LocalProperties.ResultPath, "*.txt");
                foreach (var path in historicalOrderPaths)
                {
                    using (new StopwatchTimer($"Date {path[..^14]} finished processing in: "))
                    {
                        FileHelper.PartitionFileWrite(path, PartitionResultDirectory);
                    }

                    FileHelper.FileDelete(path);
                }
            }
        }

        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred: {ex.Message}");
        }
    }

    private static string[] SortLines(string?[] inputFilePath)
    {
        using (new StopwatchTimer("SortLines"))
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
                .OrderBy(x => x!.FirstTimestamp)
                .Select(x => x!.OriginalLine)
                .ToArray();
        }
    }

    private static string ProcessFileJson(string[] lines)
    {
        using (new StopwatchTimer("ProcessFileJson"))
        {
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
    }


    private static string[] ProcessJsonList(string[] lines)
    {
        using (new StopwatchTimer("ProcessJsonList"))
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
    }

    private static int GetLastElementSize(JsonNode jsonNode)
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
}