using System.IO.Compression;
using System.Text;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using FileSortDeploy.Helpers;
using FileSortDeploy.Values;

namespace FileSortDeploy.FileProviders;

public class AmazonFileProvider : IFileProvider
{
    private readonly AmazonS3Properties _properties;
    private readonly AmazonS3Client _amazonS3Client;

    public AmazonFileProvider(AmazonS3Properties properties)
    {
        _properties = properties;
        _amazonS3Client = InitializeAmazonS3();
    }

    public async Task<List<DateCollection>> ProvideComposedFilePaths()
    {
        var folders = await ListFoldersAsync(_properties.BucketName, _properties.Prefix);
        var sortedPaths = folders.OrderBy(path =>
        {
            var parts = path.Split('/');
            return int.Parse(parts[3]);
        }).ToList();

        var allFilePaths = new List<string>();

        foreach (var folder in sortedPaths)
        {
            var filesList = await ListFilesAsync(_properties.BucketName, folder);
            var sortedList = filesList.OrderBy(obj => obj.LastModified)
                .ToList();

            foreach (var fileS3Object in sortedList)
            {
                allFilePaths.Add(fileS3Object.Key);
            }
        }

        return FileHelper.GroupFilesByName(allFilePaths.ToArray());
    }

    public async Task<string[]> ReadComposedFile(DateCollection collection)
    {
        var mergedFilesBuilder = new StringBuilder();
        foreach (var filePath in collection.FilePaths)
        {
            var amazonFile = await FileToStringAsync(filePath);
            mergedFilesBuilder.AppendLine(amazonFile);
        }

        return FileHelper.ConvertComposedFile(mergedFilesBuilder.ToString());
    }

    public async Task<string?[]> ReadComposedFileAsLines(DateCollection collection)
    {
        var lines = new List<string?>();

        foreach (var filePath in collection.FilePaths)
        {
            using var reader = await GetReader(filePath);
            while (await reader.ReadLineAsync() is { } line)
            {
                lines.Add(line);
            }
        }

        return lines.ToArray();
    }

    public async Task DownloadAllFromDrive(string filesDirectory)
    {
        var folders = await ListFoldersAsync(_properties.BucketName, _properties.Prefix);
        var sortedPaths = folders.OrderBy(path =>
        {
            var parts = path.Split('/');
            return int.Parse(parts[3]);
        }).ToList();

        var allFilePaths = new List<string>();

        foreach (var folder in sortedPaths)
        {
            var filesList = await ListFilesAsync(_properties.BucketName, folder);
            var sortedList = filesList.OrderBy(obj => obj.LastModified)
                // .Take(30)
                .ToList();

            foreach (var fileS3Object in sortedList)
            {
                allFilePaths.Add(fileS3Object.Key);
            }
        }

        foreach (var path in allFilePaths)
        {
            Console.WriteLine($"Downloading {path}");
            await DownloadFilePart(path, filesDirectory);
        }
    }

    private async Task<StreamReader> GetReader(string key)
    {
        var request = new GetObjectRequest
        {
            BucketName = _properties.BucketName,
            Key = key
        };

        using var response = await _amazonS3Client.GetObjectAsync(request);
        await using var gzipStream = new GZipStream(response.ResponseStream, CompressionMode.Decompress);
        return new StreamReader(gzipStream, Encoding.UTF8);
    }


    private async Task<string> FileToStringAsync(string key)
    {
        var request = new GetObjectRequest
        {
            BucketName = _properties.BucketName,
            Key = key
        };

        using var response = await _amazonS3Client.GetObjectAsync(request);
        await using var gzipStream = new GZipStream(response.ResponseStream, CompressionMode.Decompress);
        using var reader = new StreamReader(gzipStream, Encoding.UTF8);
        return await reader.ReadToEndAsync();
    }


    private AmazonS3Client InitializeAmazonS3()
    {
        var credentials = new BasicAWSCredentials(_properties.AccessKey, _properties.SecretKey);
        var config = new AmazonS3Config
        {
            ServiceURL = _properties.ServiceUrl,
            ForcePathStyle = true
        };

        var client = new AmazonS3Client(credentials, config);
        return client;
    }

    private async Task<List<string>> ListFoldersAsync(string bucketName, string prefix = "")
    {
        var request = new ListObjectsV2Request
        {
            BucketName = bucketName,
            Prefix = prefix,
            Delimiter = "/"
        };

        var response = await _amazonS3Client.ListObjectsV2Async(request);

        return response.CommonPrefixes;
    }

    private async Task<List<S3Object>> ListFilesAsync(string bucketName, string prefix = "")
    {
        var request = new ListObjectsV2Request
        {
            BucketName = bucketName,
            Prefix = prefix,
            Delimiter = "/"
        };

        var response = await _amazonS3Client.ListObjectsV2Async(request);

        return response.S3Objects;
    }

    private async Task DownloadFilePart(string key, string downloadPath)
    {
        var request = new GetObjectRequest
        {
            BucketName = _properties.BucketName,
            Key = key
        };

        var keyPathParts = key.Split('/');

        using var response = await _amazonS3Client.GetObjectAsync(request);
        await response.WriteResponseStreamToFileAsync(@$"{downloadPath}\\{keyPathParts[3]}\\{keyPathParts[4]}", false,
            CancellationToken.None);
    }

    public async Task DownloadFile(string keyFrom, string toDownloadPath)
    {
        var request = new GetObjectRequest
        {
            BucketName = _properties.BucketName,
            Key = keyFrom
        };
        using var response = await _amazonS3Client.GetObjectAsync(request);
        await response.WriteResponseStreamToFileAsync(@$"{toDownloadPath}\{keyFrom}", false,
            CancellationToken.None);
    }
}