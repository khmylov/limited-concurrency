using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Shouldly;

namespace LimitedConcurrency.Tests;

internal static class TestUtils
{
    public static async Task SpinWaitFor(Func<bool> condition)
    {
        var satisfied = await Task
            .Run(() => SpinWait.SpinUntil(condition, Debugger.IsAttached ? 60_000 : 1000))
            .ConfigureAwait(false);
        satisfied.ShouldBe(true);
    }

    /// <summary>
    /// For some tests it's important to simulate truly concurrent activation of some async action,
    /// therefore it's better to explicitly schedule action execution on thread pool.
    /// This method looks technically redundant but I prefer to explicitly state the intention via method name instead of duplicating comments.
    /// </summary>
    public static Task<TResult> SimulateParallelism<TResult>(Func<Task<TResult>> action)
    {
        return Task.Run(action);
    }

    /// <summary>
    /// For some tests it's important to simulate truly concurrent activation of some async action,
    /// therefore it's better to explicitly schedule action execution on thread pool.
    /// This method looks technically redundant but I prefer to explicitly state the intention via method name instead of duplicating comments.
    /// </summary>
    public static Task SimulateParallelism(Func<Task> action)
    {
        return Task.Run(action);
    }
}
