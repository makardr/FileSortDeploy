namespace FileSortDeploy.Values;

public class LocalProperties(string inputPath, string resultPath)
{
    public string InputPath { get; set; } = inputPath;
    public string ResultPath { get; set; } = resultPath;
}