namespace FileSortDeploy.values;

public class LocalProperties(string inputPath, string resultPath)
{
    public string InputPath { get; set; } = inputPath;
    public string ResultPath { get; set; } = resultPath;
}