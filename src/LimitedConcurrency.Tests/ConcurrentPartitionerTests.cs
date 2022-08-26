using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Shouldly;
using static LimitedConcurrency.Tests.TestUtils;

namespace LimitedConcurrency.Tests
{
    [TestFixture]
    public class ConcurrentPartitionerTests
    {
        [Test]
        [Repeat(10)]
        public async Task ShouldExecuteMessagesOneByOneInTheSameOrderTheyArrived()
        {
            var counter = 0;

            async Task<int> Execute(ManualResetEventSlim sync)
            {
                // If there is a bug in the partitioner,
                // which doesn't force sequential execution...
                await WaitAsync(sync).ConfigureAwait(false);

                // ... then the value of the counter here will not correspond
                // to the task ID, and the final assert will fail.
                var newCounter = Interlocked.Increment(ref counter);
                return newCounter;
            }

            var partitioner = CreatePartitioner();

            var sync1 = new ManualResetEventSlim(false);
            var sync2 = new ManualResetEventSlim(false);

            var task1 = partitioner.ExecuteAsync("Key1", () => Execute(sync1));
            var task2 = partitioner.ExecuteAsync("Key1", () => Execute(sync2));

            // First, allow the second task to start
            sync2.Set();
            await Task.Delay(1);

            // Then, allow the first task to start
            sync1.Set();

            await Task.WhenAll(task1, task2);

            var result1 = await task1;
            var result2 = await task2;

            Assert.That(result1, Is.EqualTo(1));
            Assert.That(result2, Is.EqualTo(2));

            Assert.That(partitioner.CurrentPartitionCount, Is.EqualTo(0));
        }

        [Test]
        [Repeat(10)]
        public async Task ShouldNotExecuteMoreThanOneMessagePerPartitionAtTheSameTime()
        {
            const int taskCount = 1000;
            const int iterationsPerTask = 1;
            const int partitionsCount = 40;

            var completedCount = 0;
            var startSync = new ManualResetEventSlim();

            var processingState = Enumerable.Repeat(0, partitionsCount).ToArray();
            var partitioner = CreatePartitioner();

            var tasks = Enumerable.Range(1, taskCount)
                .Select(taskIndex => SimulateParallelism(async () =>
                {
                    var partitionKey = taskIndex % partitionsCount;
                    startSync.Wait();
                    for (var iterationIndex = 0; iterationIndex < iterationsPerTask; iterationIndex++)
                    {
                        await partitioner.ExecuteAsync<object?>(partitionKey.ToString(), async () =>
                        {
                            var value1 = Interlocked.Increment(ref processingState[partitionKey]);
                            EnsureConcurrentPartitionCount(partitioner, partitionsCount);
                            Assert.AreEqual(
                                1, value1,
                                "If partitioner works correctly, then there should never be more than 1 message per partition processed at the same time");
                            await Task.Delay(1);
                            EnsureConcurrentPartitionCount(partitioner, partitionsCount);

                            var value2 = Interlocked.Decrement(ref processingState[partitionKey]);
                            Assert.AreEqual(
                                0, value2,
                                "If partitioner works correctly, no other message in that partition should be able to interfere and change the counter value");
                            Interlocked.Increment(ref completedCount);
                            return null;
                        });
                    }
                }))
                .ToArray();

            startSync.Set();
            await Task.WhenAll(tasks);
            Assert.AreEqual(taskCount * iterationsPerTask, completedCount);
        }

        [Test]
        [Repeat(10)]
        public async Task ShouldNotExceedCustomMaxConcurrency()
        {
            const int taskCount = 1000;
            const int iterationsPerTask = 1;
            const int partitionsCount = 40;
            const int maxConcurrencyPerPartition = 4;

            var completedCount = 0;
            var startSync = new ManualResetEventSlim();

            var processingState = Enumerable.Repeat(0, partitionsCount).ToArray();
            var partitioner = CreatePartitioner(maxConcurrencyPerPartition);

            var tasks = Enumerable.Range(1, taskCount)
                .Select(taskIndex => SimulateParallelism(async () =>
                {
                    var partitionKey = taskIndex % partitionsCount;
                    startSync.Wait();
                    for (var iterationIndex = 0; iterationIndex < iterationsPerTask; iterationIndex++)
                    {
                        await partitioner.ExecuteAsync<object?>(partitionKey.ToString(), async () =>
                        {
                            var value1 = Interlocked.Increment(ref processingState[partitionKey]);
                            EnsureConcurrentPartitionCount(partitioner, partitionsCount);
                            Assert.That(value1, Is.LessThanOrEqualTo(maxConcurrencyPerPartition));
                            await Task.Delay(1);
                            EnsureConcurrentPartitionCount(partitioner, partitionsCount);

                            var value2 = Interlocked.Decrement(ref processingState[partitionKey]);
                            Assert.That(value2, Is.LessThanOrEqualTo(maxConcurrencyPerPartition));
                            Interlocked.Increment(ref completedCount);
                            return null;
                        });
                    }
                }))
                .ToArray();

            startSync.Set();
            await Task.WhenAll(tasks);
            Assert.AreEqual(taskCount * iterationsPerTask, completedCount);
        }

        [Test]
        [Repeat(10)]
        public async Task ShouldNeverExecuteMoreThanOneMessagePerPartitionAtTheSameTime(
            [Values(5, 10, 100, 1000)] int taskCount,
            [Values(1, 4, 10)] int partitionCount)
        {
            var completedCount = 0;
            var startSync = new ManualResetEventSlim();

            var processingState = Enumerable.Repeat(0, partitionCount).ToArray();

            var partitioner = CreatePartitioner();

            var messages = Enumerable.Range(1, taskCount)
                .Select(taskNumber => (TaskId: taskNumber, PartitionKey: taskNumber % partitionCount))
                .ToArray();

            var tasks = messages
                .Select<(int TaskId, int PartitionKey), Task>(x => SimulateParallelism(() => partitioner.ExecuteAsync<object?>(
                    x.PartitionKey.ToString(),
                    async () =>
                    {
                        EnsureConcurrentPartitionCount(partitioner, partitionCount);
                        var value1 = Interlocked.Increment(ref processingState[x.PartitionKey]);
                        Assert.AreEqual(
                            1, value1,
                            "If partitioner works correctly, then there should never be more than 1 message per partition processed at the same time");
                        await Task.Yield();
                        EnsureConcurrentPartitionCount(partitioner, partitionCount);

                        var value2 = Interlocked.Decrement(ref processingState[x.PartitionKey]);
                        Assert.AreEqual(
                            0, value2,
                            "If partitioner works correctly, no other message in that partition should be able to interfere and change the counter value");
                        Interlocked.Increment(ref completedCount);
                        return null;
                    })))
                .ToArray();
            startSync.Set();
            await Task.WhenAll(tasks);

            Assert.AreEqual(taskCount, completedCount);
        }

        [Test]
        public async Task OneFailedMessageShouldNotBlockOrFailFollowingMessagesInPartition()
        {
            var partitioner = CreatePartitioner();

            Task<object?> Execute(int taskId)
            {
                return partitioner.ExecuteAsync<object?>("Default", async () =>
                {
                    if (taskId == 0)
                    {
                        throw new Exception("Task 0 exception");
                    }

                    await Task.Yield();

                    if (taskId == 1)
                    {
                        throw new Exception("Task 1 exception");
                    }

                    return null;
                });
            }

            var exception1 = await Should.ThrowAsync<Exception>(() => Execute(0));
            Assert.That(exception1.Message, Is.EqualTo("Task 0 exception"));

            var exception2 = await Should.ThrowAsync<Exception>(() => Execute(1));
            Assert.That(exception2.Message, Is.EqualTo("Task 1 exception"));

            Should.NotThrow(() => Execute(2));
        }

        [Test]
        [Repeat(10)]
        public async Task ShouldRunDifferentPartitionsInParallel()
        {
            const int count = 100;
            var startedFlags = Enumerable.Range(1, count).Select(_ => false).ToArray();
            var completedFlags = Enumerable.Range(1, count).Select(_ => false).ToArray();
            var startSync = new ManualResetEventSlim();
            var canContinueInner =
                new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);

            var partitioner = CreatePartitioner();

            Task<object?> Execute(int number)
            {
                return partitioner.ExecuteAsync<object?>(number.ToString(), async () =>
                {
                    startSync.Wait();
                    startedFlags[number] = true;
                    await canContinueInner.Task;
                    completedFlags[number] = true;
                    return null;
                });
            }

            var tasks = Enumerable
                .Range(0, count)
                .Select(number => SimulateParallelism(() => Execute(number)))
                .ToArray();

            startSync.Set();
            await SpinWaitFor(() => startedFlags.All(x => x));

            Assert.AreEqual(
                Enumerable.Range(1, count).Select(_ => true).ToArray(),
                startedFlags,
                "All computations should be started");

            Assert.AreEqual(
                Enumerable.Range(1, count).Select(_ => false).ToArray(),
                completedFlags,
                "None of computations should be completed yet");

            canContinueInner.SetResult(null);
            await Task.WhenAll(tasks);
            Assert.AreEqual(
                Enumerable.Range(1, count).Select(_ => true).ToArray(),
                completedFlags,
                "All computations should be finished");
        }

        [Test]
        [Repeat(10)]
        public async Task ShouldCorrectlyCleanUpPartitions()
        {
            const int count = 100;
            const int partitionCount = 3;
            var startSync = new ManualResetEventSlim();
            var startedFlags = Enumerable.Range(1, count).Select(_ => false).ToArray();
            var completedFlags = Enumerable.Range(1, count).Select(_ => false).ToArray();

            var partitioner = CreatePartitioner();

            var tasks = Enumerable
                .Range(0, count)
                .Select(number => SimulateParallelism(() => partitioner.ExecuteAsync<object?>(
                    (number % partitionCount).ToString(),
                    async () =>
                    {
                        startSync.Wait();
                        EnsureConcurrentPartitionCount(partitioner, partitionCount);
                        startedFlags[number] = true;
                        await Task.Delay(1).ConfigureAwait(false);
                        EnsureConcurrentPartitionCount(partitioner, partitionCount);
                        completedFlags[number] = true;
                        return null;
                    })))
                .ToArray();

            startSync.Set();

            await Task.WhenAll(tasks);

            Assert.That(partitioner.CurrentPartitionCount, Is.EqualTo(0));

            Assert.AreEqual(
                Enumerable.Range(1, count).Select(_ => true).ToArray(),
                completedFlags,
                "All computations should be finished");
        }

        private static ConcurrentPartitioner CreatePartitioner(int? maxConcurrency = null)
        {
            if (maxConcurrency.HasValue)
            {
                return new ConcurrentPartitioner(maxConcurrency.Value);
            }

            return new ConcurrentPartitioner();
        }

        private static Task WaitAsync(ManualResetEventSlim sync)
        {
            return Task.Run(sync.Wait);
        }

        private static void EnsureConcurrentPartitionCount(ConcurrentPartitioner partitioner, int expectedMax)
        {
            Assert.That(
                partitioner.CurrentPartitionCount,
                Is.LessThanOrEqualTo(expectedMax));
        }

    }
}
