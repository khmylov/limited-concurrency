using System;
using System.Threading;

namespace LimitedConcurrency.Tests;

public class Disposable<TArg> : IDisposable
{
    private TArg? _arg;
    private Action<TArg>? _action;

    // ReSharper disable once ConvertToPrimaryConstructor
    public Disposable(Action<TArg> action, TArg arg)
    {
        _action = action;
        _arg = arg;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _action, null) is { } action)
        {
            var arg = _arg!;
            _arg = default;
            action(arg);
        }
    }
}

public static class Disposable
{
    public static Disposable<TArg> Create<TArg>(Action<TArg> action, TArg arg) => new(action, arg);
    public static Disposable<Action> Create(Action action) => new(static a => a(), action);
}
