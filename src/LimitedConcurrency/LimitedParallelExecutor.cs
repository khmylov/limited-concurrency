using System;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

namespace LimitedConcurrency
{
    /// <summary>
    /// Allows to run <see cref="Task"/>s with a limited degree of parallelism.
    /// </summary>
    [SuppressMessage("ReSharper", "ClassNeverInstantiated.Global")]
    public class LimitedParallelExecutor
    {
        private readonly int _degreeOfParallelism;

        private readonly ConcurrentQueue<Func<Task>> _queue;

        private volatile int _queueLength;

        private int _activeJobCount;

        private readonly Action<object?> _scheduleNextCached;

        /// <param name="degreeOfParallelism">Controls how many Tasks can be run in parallel at any given moment.</param>
        /// <exception cref="ArgumentOutOfRangeException">If degree of parallelism is non-positive.</exception>
        public LimitedParallelExecutor(
            int degreeOfParallelism)
        {
            if (degreeOfParallelism < 1) throw new ArgumentOutOfRangeException(nameof(degreeOfParallelism));
            _degreeOfParallelism = degreeOfParallelism;

            _queue = new ConcurrentQueue<Func<Task>>();
            Action processNextCached = ProcessNextItem;
            _scheduleNextCached = _ => Task.Run(processNextCached);
        }

        /// <summary>
        /// Adds the specified job to the queue to be executed when there is an idle execution slot.
        /// </summary>
        /// <exception cref="NullReferenceException">If <paramref name="item"/> is null.</exception>
        public void Enqueue(Func<Task> item)
        {
            if (item is null) throw new ArgumentNullException(nameof(item));

            lock (_queue)
            {
                _queue.Enqueue(item);
                Interlocked.Increment(ref _queueLength);

                if (_activeJobCount < _degreeOfParallelism)
                {
                    _activeJobCount++;
                    _scheduleNextCached(null);
                }
            }
        }

        /// <summary>
        /// Returns a number of enqueued jobs. Does not include currently running jobs.
        /// </summary>
        [SuppressMessage("ReSharper", "UnusedMember.Global")]
        public int QueueLength => _queueLength;

        /// <summary>
        /// Returns a number of jobs currently being executed.
        /// </summary>
        [SuppressMessage("ReSharper", "UnusedMember.Global")]
        public int CurrentlyRunningCount => Volatile.Read(ref _activeJobCount);

        private void ProcessNextItem()
        {
            var scheduled = false;

            try
            {
                // ReSharper disable once InconsistentlySynchronizedField
                if (_queue.TryDequeue(out var nextItem))
                {
                    Interlocked.Decrement(ref _queueLength);
                    var task = nextItem();
                    task.ContinueWith(_scheduleNextCached);
                    scheduled = true;
                }
            }
            finally
            {
                if (!scheduled)
                {
                    lock (_queue)
                    {
                        if (!_queue.IsEmpty)
                        {
                            _scheduleNextCached(null);
                        }
                        else
                        {
                            _activeJobCount--;
                        }
                    }
                }
            }
        }
    }
}
