using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Shouldly;

namespace LimitedConcurrency.Tests
{
    [TestFixture]
    public class LimitedParallelExecutorTests
    {
        /// <remarks>
        /// Not really sure about this behavior, so decided to just make it explicit in a test.
        /// On the one hand, if we want our executor to be truly parallel,
        /// then we must wrap passed callbacks into Task.Run/ThreadPool.QueueUserWorkItem/etc.,
        /// so that if the callback is actually synchronous it doesn't block execution of other queued callbacks (until we hit a concurrency limit).
        ///
        /// On the other hand, doing so creates an overhead when executor is used as a part of other scheduling configuration, e.g.
        /// LimitedParallelExecutor (global degree of parallelism) -> Partitioner -> LimitedParallelExecutor (ordering within a partition).
        /// </remarks>
        [Test]
        public async Task ShouldRunConcurrentlyEvenIfCallbacksAreSynchronous()
        {
            var task1CanComplete = new ManualResetEventSlim(false);
            var task2CanComplete = new ManualResetEventSlim(false);
            var task3CanComplete = new ManualResetEventSlim(false);
            const int maxConcurrency = 2;

            var runningCounter = 0;
            var runningStates = new ConcurrentBag<int>();

            var startedJobs = new bool[3];
            var completedJobs = new bool[3];

            Task Execute(ManualResetEventSlim canComplete, int jobIndex)
            {
                runningStates.Add(Interlocked.Increment(ref runningCounter));
                startedJobs[jobIndex] = true;
                canComplete.Wait();
                completedJobs[jobIndex] = true;
                runningStates.Add(Interlocked.Decrement(ref runningCounter));
                return Task.CompletedTask;
            }

            var executor = new LimitedParallelExecutor(maxConcurrency);
            executor.Enqueue(() => Execute(task1CanComplete, 0));
            executor.Enqueue(() => Execute(task2CanComplete, 1));
            executor.Enqueue(() => Execute(task3CanComplete, 2));

            // Should now start 2 jobs even though the first one is blocking...
            await SpinWaitFor(() => startedJobs[0] && startedJobs[1]);
            // ... but none jobs should be completed so far ...
            completedJobs.ShouldBe(new[] {false, false, false});
            // ... and Job 3 should still be queued due to max concurrency limit.
            startedJobs.ShouldBe(new[] {true, true, false});

            task1CanComplete.Set();
            // If we allow Job 1 to complete, it should be now possible to start Job 3 (due to max concurrency)
            await SpinWaitFor(() => completedJobs[0] && startedJobs[2]);
            startedJobs.ShouldBe(new[] {true, true, true});
            completedJobs.ShouldBe(new[] {true, false, false});

            task2CanComplete.Set();
            await SpinWaitFor(() => completedJobs[1]);

            task3CanComplete.Set();
            await SpinWaitFor(() => completedJobs.All(flag => flag));

            runningStates.All(x => x <= maxConcurrency)
                .ShouldBe(true, $"runningStates: {string.Join(",", runningStates)}");
        }

        [Test]
        [Repeat(5)]
        public async Task ShouldNotExecuteMoreThanSpecifiedNumberOfConcurrentAsyncJobs()
        {
            /*
             * Given a limit of 2 concurrent tasks, and the following start and completion timeline:
             * --|-execute---------------|----------------------------------------> Task 1
             * ---|-execute--------|----------------------------------------------> Task 2
             * ----|-wait----------|-execute-|------------------------------------> Task 3
             * -----|-wait---------------|-execute-|------------------------------> Task 4
             */

            var task1CanComplete = new ManualResetEventSlim(false);
            var task2CanComplete = new ManualResetEventSlim(false);
            var task3CanComplete = new ManualResetEventSlim(false);
            var task4CanComplete = new ManualResetEventSlim(false);

            const int maxConcurrency = 2;

            var startedJobs = new bool[4];
            var completedJobs = new bool[4];

            var runningCounter = 0;
            var runningStates = new ConcurrentBag<int>();

            async Task Execute(ManualResetEventSlim canComplete, int jobIndex)
            {
                runningStates.Add(Interlocked.Increment(ref runningCounter));
                startedJobs[jobIndex] = true;
                await WaitAsync(canComplete).ConfigureAwait(false);
                completedJobs[jobIndex] = true;
                runningStates.Add(Interlocked.Decrement(ref runningCounter));
            }

            var executor = new LimitedParallelExecutor(maxConcurrency);
            executor.Enqueue(() => Execute(task1CanComplete, 0));
            executor.Enqueue(() => Execute(task2CanComplete, 1));
            executor.Enqueue(() => Execute(task3CanComplete, 2));
            executor.Enqueue(() => Execute(task4CanComplete, 3));

            await SpinWaitFor(() => startedJobs[1]);
            startedJobs.ShouldBe(new[] {true, true, false, false});
            completedJobs.ShouldBe(new[] {false, false, false, false});

            task2CanComplete.Set();
            await SpinWaitFor(() => completedJobs[1] && startedJobs[2]);
            startedJobs.ShouldBe(new[] {true, true, true, false});
            completedJobs.ShouldBe(new[] {false, true, false, false});

            task1CanComplete.Set();
            await SpinWaitFor(() => completedJobs[0] && startedJobs[3]);
            startedJobs.ShouldBe(new[] {true, true, true, true});
            completedJobs.ShouldBe(new[] {true, true, false, false});

            task3CanComplete.Set();
            task4CanComplete.Set();
            await SpinWaitFor(() => startedJobs.All(set => set));
            await SpinWaitFor(() => completedJobs.All(set => set));

            runningStates.All(x => x <= maxConcurrency)
                .ShouldBe(true, $"runningStates: {string.Join(",", runningStates)}");
        }

        [Test]
        [Repeat(5)]
        public async Task ShouldExecuteCallbacksInFirstInFirstOutOrder()
        {
            var lastNumber = 0;
            var executor = new LimitedParallelExecutor(1);
            var tasks = Enumerable
                .Range(1, 10000)
                .Select(index =>
                {
                    return executor.ExecuteAsync(number =>
                    {
                        var wait = new SpinWait();
                        for (var i = 0; i < 5; i++)
                        {
                            wait.SpinOnce();
                        }

                        Interlocked.Increment(ref lastNumber).ShouldBe(number);
                        return Task.CompletedTask;
                    }, index, default);
                })
                .ToArray();

            await Task.WhenAll(tasks);
        }

        [Test]
        public async Task ShouldKeepWorkingWhenCallbackThrowsSynchronously()
        {
            var executor = new LimitedParallelExecutor(1);
            var task1Failed = executor.ExecuteAsync(_ => throw new Exception("Test exception"), 1, default);
            var task1Observed = task1Failed.ContinueWith(_ => { });
            var task2 = executor.ExecuteAsync(async _ => await Task.Delay(30), 2, default);

            await Task.WhenAll(task1Observed, task2);

            task1Failed.IsFaulted.ShouldBe(true);
        }

        private static Task WaitAsync(ManualResetEventSlim handle)
        {
            return Task.Run(() => handle.Wait());
        }

        private static async Task SpinWaitFor(Func<bool> condition)
        {
            using (var cancellationTokenSource = new CancellationTokenSource(Debugger.IsAttached ? 60_000 : 1000))
            {
                var cancellationToken = cancellationTokenSource.Token;
                var winner = await Task.WhenAny(
                    Task.Delay(-1, cancellationToken),
                    Task.Run(() =>
                    {
                        var spinWait = new SpinWait();
                        while (!condition())
                        {
                            cancellationToken.ThrowIfCancellationRequested();
                            spinWait.SpinOnce();
                        }
                    }, cancellationToken)
                );
                cancellationToken.IsCancellationRequested.ShouldBe(false,
                    $"{nameof(SpinWaitFor)} cancelled by a timeout");
                await winner;
            }
        }
    }
}
