using System.Diagnostics;

namespace FileSortDeploy.Helpers;

public class StopwatchTimer(string message = "Elapsed time") : IDisposable
{
    private readonly Stopwatch _stopwatch = Stopwatch.StartNew();

    public void Dispose()
    {
        _stopwatch.Stop();
        Console.WriteLine($"{message}: {_stopwatch.ElapsedMilliseconds} ms");
    }
}