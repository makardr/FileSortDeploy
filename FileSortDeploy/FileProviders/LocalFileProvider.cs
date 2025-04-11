using System.IO.Compression;
using System.Text;
using FileSortDeploy.Helpers;
using FileSortDeploy.Values;

namespace FileSortDeploy.FileProviders;

internal class LocalFileProvider(LocalProperties properties) : IFileProvider
{
    public Task<List<DateCollection>> ProvideComposedFilePaths()
    {
        return Task.FromResult(
            FileHelper.GroupFilesByName(Directory.GetFiles(properties.InputPath, "*.gz", SearchOption.AllDirectories)));
    }

    public Task<string[]> ReadComposedFile(DateCollection collection)
    {
        var mergedFilesBuilder = new StringBuilder();
        foreach (var reader in collection.FilePaths.Select(OpenGzipFile))
        {
            mergedFilesBuilder.AppendLine(reader.ReadToEnd());
        }

        return Task.FromResult(FileHelper.ConvertComposedFile(mergedFilesBuilder.ToString()));
    }

    public Task<string?[]> ReadComposedFileAsLines(DateCollection collection)
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

        return Task.FromResult(lines.ToArray());
    }

    private static StreamReader OpenGzipFile(string filePath)
    {
        var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
        var gzipStream = new GZipStream(fileStream, CompressionMode.Decompress);
        return new StreamReader(gzipStream);
    }
}