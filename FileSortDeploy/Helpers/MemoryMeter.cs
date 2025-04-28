using System.Diagnostics;

namespace FileSortDeploy.Helpers;

public class MemoryMeter
{
    public static string GetMemoryUsage()
    {
        Process currentProcess = Process.GetCurrentProcess();

        var memoryInBytes = currentProcess.WorkingSet64;
        var memoryInMegabytes = memoryInBytes / (1024.0 * 1024.0);

        return memoryInMegabytes.ToString("0.00");
    }
}