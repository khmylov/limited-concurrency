using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace LimitedConcurrency.Benchmarks
{
    /*
     * 2021-08-04
     * BenchmarkDotNet=v0.13.0, OS=Windows 10.0.19042.1110 (20H2/October2020Update)
       Intel Core i7-8700 CPU 3.20GHz (Coffee Lake), 1 CPU, 12 logical and 6 physical cores
       .NET SDK=5.0.201
         [Host]     : .NET 5.0.4 (5.0.421.11614), X64 RyuJIT
         Job-VYZQXT : .NET 5.0.4 (5.0.421.11614), X64 RyuJIT

       InvocationCount=1  UnrollFactor=1

       |           Method |     Mean |    Error |   StdDev |   Median |
       |----------------- |---------:|---------:|---------:|---------:|
       | ConcurrentAccess | 203.4 ms | 14.45 ms | 41.70 ms | 183.0 ms |
     */
    public class TrackableBenchmarks
    {
        private Trackable<int> _trackable = default!;

        [IterationSetup]
        public void Setup()
        {
            _trackable = new Trackable<int>(5);
        }

        [Benchmark]
        public async Task ConcurrentAccess()
        {
            var sync = new ManualResetEventSlim();
            var tasks = Enumerable.Range(1, 100).Select(_ => Task.Run(() =>
            {
                sync.Wait();
                var counter = 0;
                while (counter++ < 200_000)
                {
                    if (_trackable.TryEnter())
                    {
                        Thread.Yield();
                        _trackable.ExitAndTryCleanup();
                    }

                    Thread.Yield();
                }
            })).ToArray();

            sync.Set();
            await Task.WhenAll(tasks);
        }
    }
}
