using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

namespace LimitedConcurrency
{
    /// <summary>
    /// Partitions jobs by the specified key to allow concurrent execution of jobs with the different keys,
    /// while jobs with the same key are executed with the specified max concurrency level (1 by default, i.e. sequentially).
    /// </summary>
    /// <remarks>
    /// Like with other data structures, to ensure FIFO order of the jobs with the same partition key,
    /// multi-threaded clients must serialize invocation of synchronous part of <see cref="ExecuteAsync"/>,
    /// i.e. retrieval of the returned <see cref="Task"/>.
    /// Awaiting the returned task does not need any synchronization.
    /// </remarks>
    /// <typeparam name="TResult">Type of value returned by enqueued jobs.</typeparam>
    [SuppressMessage("ReSharper", "UnusedType.Global")]
    public class ConcurrentPartitioner<TResult>
    {
        private readonly int _partitionConcurrency;
        private readonly ConcurrentDictionary<string, Trackable<LimitedParallelExecutor>> _dictionary = new();

        public ConcurrentPartitioner()
            : this(1)
        {
        }

        /// <param name="partitionConcurrency">The max number of tasks to execute concurrently per partition.</param>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="partitionConcurrency"/> is less than 1.</exception>
        public ConcurrentPartitioner(int partitionConcurrency)
        {
            _partitionConcurrency = partitionConcurrency < 1
                ? throw new ArgumentOutOfRangeException(nameof(partitionConcurrency), 1,
                    "Max partition concurrency should be a positive number")
                : partitionConcurrency;
        }

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
        /// <remarks>
        /// To ensure correct enqueueing order, clients must synchronize execution of synchronous part of this method.
        /// </remarks>
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
            Trackable<LimitedParallelExecutor> entry;

            while (true)
            {
                entry = _dictionary.GetOrAdd(
                    partitionKey,
                    _ => new Trackable<LimitedParallelExecutor>(new LimitedParallelExecutor(_partitionConcurrency)));

                if (entry.GetIsNewOnce())
                {
                    Interlocked.Increment(ref _currentPartitionCount);
                }

                if (entry.TryEnter())
                {
                    break;
                }
            }

            Interlocked.Increment(ref _totalQueueSize);

            entry.Value.Enqueue(async () =>
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

                await ExecuteImpl(request, () =>
                {
                    Interlocked.Decrement(ref _totalQueueSize);
                    if (entry.ExitAndTryCleanup())
                    {
                        Interlocked.Decrement(ref _currentPartitionCount);
                        _dictionary.TryRemove(partitionKey, out _);
                    }
                }).ConfigureAwait(false);
            });

            return tcs.Task;
        }

        /// <remarks>
        /// This method should not throw exceptions
        /// </remarks>
        private static async Task ExecuteImpl(PartitionRequest request, Action beforeResolveSafe)
        {
            TResult result;
            try
            {
                result = await request.Action().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                beforeResolveSafe();
                request.CompletionSource.TrySetException(ex);
                return;
            }

            beforeResolveSafe();
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
