using System.Text;
using FileSortDeploy.values;

namespace FileSortDeploy.FileProviders;

public static class FileHelper
{
    public static List<DateCollection> GroupFilesByName(string[] filePaths)
    {
        var groupedFiles = filePaths
            .GroupBy(path => Path.GetFileName(path))
            .Select(g => new DateCollection(
                g.Key,
                g.ToList()
            ));
        return groupedFiles.ToList();
    }

    public static string[] ConvertComposedFile(string file)
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

    public static string[] GetFilePathsHistorically(string directory, string pattern)
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
    
    public static void PartitionFileWrite(string path, string resultPath)
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

    public static void WriteFile(string file, string path)
    {
        path = path[..^2] + "txt";
        Console.WriteLine($"File written {path}");
        File.WriteAllText(path, file);
    }

    public static void WriteArrayFile(string[] lines, string filePath)
    {
        Console.WriteLine($"File written {filePath}");
        using var writer = new StreamWriter(filePath);
        foreach (string line in lines)
        {
            writer.WriteLine(line);
        }
    }

    public static void FileDelete(string path)
    {
        File.Delete(path);
    }
}