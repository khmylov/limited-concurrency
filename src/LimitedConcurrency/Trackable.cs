using System;
using System.Collections.Concurrent;
using System.Threading;

namespace LimitedConcurrency
{
    /// <remarks>
    /// There is a common task to keep a dictionary of some objects which must be cleaned up or released after usage.
    /// For example, keep a <see cref="ConcurrentDictionary{TKey,TValue}"/> of semaphores/limiters/TaskSchedulers/etc.
    /// While there are active tasks per a single dictionary key, they should be passed through an object stored in a dictionary.
    /// When all tasks for that key are completed, we want to remove the object from a dictionary to avoid memory leaks.
    ///
    /// This class is a value wrapper which can be stored in such dictionary.
    /// It provides a lock-free mechanism to count the usages and go to special "closed" state once there are no more active usages.
    /// </remarks>
    /// <remarks>
    /// Avoiding locks on highly contented scenarios provides 10x boost
    ///
    /// |                 Method |        Mean |     Error |   StdDev |      Median |
    /// |----------------------- |------------:|----------:|---------:|------------:|
    /// |             BTrackable |    99.99 ms |  7.510 ms | 21.67 ms |    89.95 ms |
    /// |         BTrackableLock | 1,292.30 ms | 21.691 ms | 20.29 ms | 1,302.19 ms |
    /// </remarks>
    public class Trackable<T>
    {
        public readonly T Value;
        private int _currentCount;
        private int _isNewCheck;

        public Trackable(T value)
        {
            Value = value;
        }

        /// <remarks>
        /// It's expected that this wrapper is placed into <see cref="ConcurrentDictionary{TKey,TValue}"/>,
        /// whose <see cref="ConcurrentDictionary{TKey,TValue}.GetOrAdd(TKey,Func{TKey,TValue})"/> method
        /// may execute the passed constructor multiple times (but only the one returned value is actually used).
        ///
        /// This method returns `true` for its first execution, and always returns `false` afterwards,
        /// which allows only one of the multiple concurrent `GetOrAdd` call results to understand whether its value was actually elected for storage.
        /// </remarks>>
        public bool GetIsNewOnce() => Interlocked.CompareExchange(ref _isNewCheck, 1, 0) is 0;

        /// <summary>
        /// Increments the current usage count if the wrapper is not in a closed state.
        /// </summary>
        /// <returns>
        /// False if wrapper is in closed state, True otherwise.
        /// </returns>
        public bool TryEnter()
        {
            int value;

            do
            {
                value = Volatile.Read(ref _currentCount);

                if (value == -1)
                {
                    return false;
                }
            } while (Interlocked.CompareExchange(ref _currentCount, value + 1, value) != value);

            return true;
        }

        /// <summary>
        /// Decrements the current usage count. If it drops to 0, moves the wrapper into a closed state.
        /// </summary>
        /// <returns>True if wrapper was moved to closed state or had no usage yet, False otherwise (meaning there are still active usages).</returns>
        public bool ExitAndTryCleanup()
        {
            while (true)
            {
                var value = Volatile.Read(ref _currentCount);

                // Note that if you want to make it reusable or expose via library,
                // you should also check for `value < 1`, e.g. when ExitAndTryCleanup is called without preceding TryEnter.
                // For now, all usages inside internal code are safe.

                if (value == 1)
                {
                    if (Interlocked.CompareExchange(ref _currentCount, -1, 1) == 1)
                    {
                        return true;
                    }

                    continue;
                }

                if (Interlocked.CompareExchange(ref _currentCount, value - 1, value) == value)
                {
                    return false;
                }
            }
        }
    }
}
