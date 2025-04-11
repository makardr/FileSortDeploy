using FileSortDeploy.values;

namespace FileSortDeploy.FileProviders;

public interface IFileProvider
{
    Task<List<DateCollection>> ProvideComposedFilePaths();
    Task<string[]> ReadComposedFile(DateCollection collection);
    Task<string?[]> ReadComposedFileAsLines(DateCollection collection);
}