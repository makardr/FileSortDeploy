namespace FileSortDeploy.Values;

public class DateCollection(string date, List<string> filePaths)
{
    public string Date { get; set; } = date;
    public List<string> FilePaths { get; set; } = filePaths;
}