using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Shouldly;

namespace LimitedConcurrency.Tests
{
    [TestFixture]
    public class ConcurrentPartitionerTests
    {
        [Test]
        public async Task ShouldExecuteMessagesOneByOneInTheSameOrderTheyArrived()
        {
            var counter = 0;

            async Task<int> Execute(ManualResetEventSlim sync, int taskId)
            {
                Console.WriteLine($"Starting task {taskId}");

                // If there is a bug in the partitioner,
                // which doesn't force sequential execution...
                await WaitAsync(sync).ConfigureAwait(false);

                // ... then the value of the counter here will not correspond
                // to the task ID, and the final assert will fail.
                var newCounter = Interlocked.Increment(ref counter);
                Console.WriteLine($"Completed task {taskId}");
                return newCounter;
            }

            var partitioner = new ConcurrentPartitioner<int>();

            var sync1 = new ManualResetEventSlim(false);
            var sync2 = new ManualResetEventSlim(false);

            var task1 = partitioner.ExecuteAsync("Key1", () => Execute(sync1, 1));
            var task2 = partitioner.ExecuteAsync("Key1", () => Execute(sync2, 2));

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
        }

        [Test]
        public async Task ShouldNeverExecuteMoreThanOneMessagePerPartitionAtTheSameTime(
            [Values(5, 10, 100, 1000)] int taskCount,
            [Values(1, 4, 10)] int partitionCount)
        {
            var completedCount = 0;

            var processingState = Enumerable.Repeat(0, partitionCount).ToArray();

            async Task<object?> Execute(int taskId, int partitionKey)
            {
                Console.WriteLine($"Starting task {taskId} in partition {partitionKey}");

                var value1 = Interlocked.Increment(ref processingState![partitionKey]);
                Assert.AreEqual(
                    1, value1,
                    "If partitioner works correctly, then there should never be more than 1 message per partition processed at the same time");
                await Task.Yield();

                var value2 = Interlocked.Decrement(ref processingState[partitionKey]);
                Assert.AreEqual(
                    0, value2,
                    "If partitioner works correctly, no other message in that partition should be able to interfere and change the counter value");
                Interlocked.Increment(ref completedCount);
                return null;
            }

            var partitioner = new ConcurrentPartitioner<object?>();

            var messages = Enumerable.Range(1, taskCount)
                .Select(taskNumber => (TaskId: taskNumber, PartitionKey: taskNumber % partitionCount))
                .ToArray();

            var tasks = messages
                // It's important to use Task.Run here to simulate multithreaded environment
                .Select<(int TaskId, int PartitionKey), Task>(x => Task.Run(() =>
                    partitioner.ExecuteAsync(x.PartitionKey.ToString(),
                        () => Execute(x.TaskId, x.PartitionKey))))
                .ToArray();
            await Task.WhenAll(tasks);

            Assert.AreEqual(taskCount, completedCount);
        }

        [Test]
        public async Task OneFailedMessageShouldNotBlockOrFailFollowingMessagesInPartition()
        {
            var partitioner = new ConcurrentPartitioner<object?>();

            Task<object?> Execute(int taskId)
            {
                return partitioner.ExecuteAsync("Default", async () =>
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
        public async Task ShouldRunDifferentPartitionsInParallel()
        {
            var count = 100;
            var startedFlags = Enumerable.Range(1, count).Select(_ => false).ToArray();
            var completedFlags = Enumerable.Range(1, count).Select(_ => false).ToArray();
            var sync = new TaskCompletionSource<object?>();

            var partitioner = new ConcurrentPartitioner<object?>();

            Task<object?> Execute(int number)
            {
                return partitioner.ExecuteAsync(number.ToString(), async () =>
                {
                    startedFlags[number] = true;
                    await sync.Task;
                    completedFlags[number] = true;
                    return null;
                });
            }

            var tasks = Enumerable
                .Range(0, count)
                .Select(number => Task.Run(() => Execute(number)))
                .ToArray();

            await Task.Delay(10);

            Assert.AreEqual(
                Enumerable.Range(1, count).Select(x => true).ToArray(),
                startedFlags,
                "All computations should be started");

            Assert.AreEqual(
                Enumerable.Range(1, count).Select(x => false).ToArray(),
                completedFlags,
                "None of computations should be completed yet");

            sync.SetResult(null);
            await Task.WhenAll(tasks);
            Assert.AreEqual(
                Enumerable.Range(1, count).Select(x => true).ToArray(),
                completedFlags,
                "All computations should be finished");
        }

        [Test]
        public async Task ShouldCorrectlyCleanUpPartitions()
        {
            var count = 100;
            var startedFlags = Enumerable.Range(1, count).Select(x => false).ToArray();
            var completedFlags = Enumerable.Range(1, count).Select(x => false).ToArray();

            var partitioner = new ConcurrentPartitioner<object?>();

            Task<object?> Execute(int number)
            {
                return partitioner.ExecuteAsync(
                    (number % 3).ToString(),
                    async () =>
                    {
                        startedFlags[number] = true;
                        await Task.Delay(number % 3 + 1);
                        completedFlags[number] = true;
                        return null;
                    });
            }

            var tasks = Enumerable
                .Range(0, count)
                .Select(number => Task.Run(() => Execute(number)))
                .ToArray();

            await Task.WhenAll(tasks);

            // To ensure that cleanup computation is called
            await Task.Delay(10);

            Assert.That(partitioner.CurrentPartitionCount, Is.EqualTo(0));

            Assert.AreEqual(
                Enumerable.Range(1, count).Select(x => true).ToArray(),
                completedFlags,
                "All computations should be finished");
        }

        private static Task WaitAsync(ManualResetEventSlim sync)
        {
            return Task.Run(sync.Wait);
        }
    }
}
