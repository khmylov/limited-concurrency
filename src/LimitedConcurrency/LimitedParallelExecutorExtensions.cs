using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;

namespace LimitedConcurrency
{
    [SuppressMessage("ReSharper", "UnusedType.Global")]
    public static class LimitedParallelExecutorExtensions
    {
        [SuppressMessage("ReSharper", "UnusedMember.Global")]
        public static Task<TResult> ExecuteAsync<T, TResult>(
            this LimitedParallelExecutor executor,
            Func<T, Task<TResult>> handler,
            T input,
            CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return Task.FromCanceled<TResult>(cancellationToken);
            }

            var tcs = new TaskCompletionSource<TResult>();
            executor.Enqueue(async () =>
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    tcs.TrySetCanceled(cancellationToken);
                    return;
                }

                TResult result;
                try
                {
                    result = await handler(input).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    tcs.TrySetException(ex);
                    return;
                }

                tcs.TrySetResult(result);
            });

            return tcs.Task;
        }

        [SuppressMessage("ReSharper", "UnusedMember.Global")]
        public static Task ExecuteAsync<T>(
            this LimitedParallelExecutor executor,
            Func<T, Task> handler,
            T input,
            CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return Task.FromCanceled(cancellationToken);
            }

            var tcs = new TaskCompletionSource<object?>();
            executor.Enqueue(async () =>
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    tcs.TrySetCanceled(cancellationToken);
                    return;
                }

                try
                {
                    await handler(input).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    tcs.TrySetException(ex);
                    return;
                }

                tcs.TrySetResult(null);
            });

            return tcs.Task;
        }
    }
}
