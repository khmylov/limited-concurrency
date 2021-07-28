using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

namespace LimitedConcurrency
{
    /// <summary>
    /// Partitions jobs by some key to allow concurrent execution of jobs with the different keys,
    /// while jobs with the same key are executed sequentially.
    /// </summary>
    /// <typeparam name="TResult">Type of value returned by enqueued jobs.</typeparam>
    [SuppressMessage("ReSharper", "UnusedType.Global")]
    public class ConcurrentPartitioner<TResult>
    {
        private readonly ConcurrentDictionary<string, LimitedParallelExecutor> _dictionary = new();

        private readonly object _cleanUpSync = new();

        private volatile int _currentPartitionCount;

        /// <summary>
        /// Returns the number of different partition keys across all enqueued jobs.
        /// </summary>
        [SuppressMessage("ReSharper", "UnusedMember.Global")]
        public int CurrentPartitionCount => _currentPartitionCount;

        private volatile int _totalQueueSize;

        /// <summary>
        /// Returns the total number of enqueued jobs across all partitions.
        /// </summary>
        [SuppressMessage("ReSharper", "UnusedMember.Global")]
        public int TotalQueueSize => _totalQueueSize;

        /// <summary>
        /// Invoked when a job is dequeued and is ready to be processed.
        /// Allows consumers to track how long the job spent in the queue.
        /// </summary>
        [SuppressMessage("ReSharper", "EventNeverSubscribedTo.Global")]
        public event Action<TimeSpan>? OnQueueWaitCompleted;

        /// <summary>
        /// Adds a job to the end of the queue with a specified partition key.
        /// </summary>
        /// <returns>Task which allows consumers to wait for a job to be dequeued and completed.</returns>
        /// <exception cref="ArgumentNullException">If <paramref name="partitionKey"/> or <paramref name="job"/> is null.</exception>
        [SuppressMessage("ReSharper", "UnusedMember.Global")]
        public Task<TResult> ExecuteAsync(
            string partitionKey,
            Func<Task<TResult>> job)
        {
            if (partitionKey is null) throw new ArgumentNullException(nameof(partitionKey));
            if (job is null) throw new ArgumentNullException(nameof(job));

            var tcs = new TaskCompletionSource<TResult>();
            var request = new PartitionRequest(job, tcs);
            LimitedParallelExecutor executor;

            lock (_cleanUpSync)
            {
                executor = _dictionary.GetOrAdd(
                    partitionKey,
                    _ =>
                    {
                        Interlocked.Increment(ref _currentPartitionCount);
                        return new LimitedParallelExecutor(1);
                    });
            }

            Interlocked.Increment(ref _totalQueueSize);

            executor.Enqueue(async () =>
            {
                request.CreatedAt.Stop();
                var onQueueWaitCompleted = OnQueueWaitCompleted;
                if (onQueueWaitCompleted != null)
                {
                    try
                    {
                        onQueueWaitCompleted(request.CreatedAt.Elapsed);
                    }
                    catch
                    {
                        // ignore, assuming it's the client's fault
                    }
                }

                await ExecuteImpl(request).ConfigureAwait(false);

                Interlocked.Decrement(ref _totalQueueSize);
                lock (_cleanUpSync)
                {
                    if (executor.QueueLength == 0)
                    {
                        Interlocked.Decrement(ref _currentPartitionCount);
                        _dictionary.TryRemove(partitionKey, out _);
                    }
                }
            });

            return tcs.Task;
        }

        /// <remarks>
        /// This method should not throw exceptions
        /// </remarks>
        private static async Task ExecuteImpl(PartitionRequest request)
        {
            TResult result;
            try
            {
                result = await request.Action().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                request.CompletionSource.TrySetException(ex);
                return;
            }

            request.CompletionSource.TrySetResult(result);
        }

        private readonly struct PartitionRequest
        {
            public readonly Func<Task<TResult>> Action;
            public readonly TaskCompletionSource<TResult> CompletionSource;
            public readonly Stopwatch CreatedAt;

            public PartitionRequest(
                Func<Task<TResult>> action,
                TaskCompletionSource<TResult> completionSource)
            {
                Action = action;
                CompletionSource = completionSource;
                CreatedAt = Stopwatch.StartNew();
            }
        }
    }
}
