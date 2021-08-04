using BenchmarkDotNet.Running;

namespace LimitedConcurrency.Benchmarks
{
    public static class Program
    {
        public static void Main()
        {
            BenchmarkRunner.Run<TrackableBenchmarks>();
        }
    }
}
