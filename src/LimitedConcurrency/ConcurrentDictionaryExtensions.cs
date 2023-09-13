#if NETSTANDARD2_0
using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace LimitedConcurrency
{
    internal static class ConcurrentDictionaryExtensions
    {
        public static TValue GetOrAdd<TKey, TValue, TArg>(
            this ConcurrentDictionary<TKey, TValue> concurrentDictionary, TKey key,
            Func<TKey, TArg, TValue> valueFactory, TArg arg)
        {
            if (concurrentDictionary.TryGetValue(key, out var value))
            {
                return value;
            }

            return GetOrAddFallback(concurrentDictionary, key, valueFactory, arg);

            // Using a separate static method allows us to avoid allocating a closure object containing `value` in the parent method on a happy path.
            [MethodImpl(MethodImplOptions.NoInlining)]
            static TValue GetOrAddFallback(ConcurrentDictionary<TKey, TValue> concurrentDictionary, TKey key,
                Func<TKey, TArg, TValue> valueFactory, TArg arg)
            {
                var value = valueFactory(key, arg);

                return concurrentDictionary.TryAdd(key, value)
                    ? value
                    : concurrentDictionary.GetOrAdd(key, _ => value);
            }
        }
    }
}
#endif

