.NET utilities for concurrent message processing with configurable degree of parallelism, and per-partition ordering.

```
dotnet add package LimitedConcurrency
```

# LimitedParallelExecutor

`LimitedParallelExecutor` allows to run `Task`s with a limited degree of parallelism (i.e. not more than N tasks are run in parallel at any given moment).
Unlike various similar custom `TaskScheduler` implementations, it maintains the limited degree of parallelism not only for the *synchronous* part of the task execution, but for entire asynchronous operation.

```csharp
async Task Job(int delay, string message)
{
    Console.WriteLine($"{message} started");
    await Task.Delay(delay).ConfigureAwait(false);
    Console.WriteLine($"{message} finished");
}

var executor = new LimitedParallelExecutor(degreeOfParallelism: 2);
executor.Enqueue(() => Job(2000, "Job A"));
executor.Enqueue(() => Job(1000, "Job B"));
executor.Enqueue(() => Job(500, "Job C"));
```

Output:
```
Job A started
Job B started
Job B finished
Job C started
Job C finished
Job A finished
```

## Notes

- Executor maintains FIFO order, Tasks are started in the order they were enqueued
    - FIFO order is guaranteed only when clients synchronize `Enqueue` call order.
- Executor schedules Tasks via `Task.Run`, i.e. on default thread pool scheduler, to ensure that executed is truly parallel even if passed `Func<Task>` implementations are synchronous and blocking.

# ConcurrentPartitioner

Another common requirement in concurrent message processing is "partitioning":
- Every message belongs to exactly one partition key, for example customer name in multi-tenant environment
- Messages with different partition keys may be processed in parallel
- Messages within the same partition key must be processed sequentially

`ConcurrentPartitioner` provides this exact behavior

```csharp
async Task<int> Job(int delay, string message)
{
    Console.WriteLine($"{message} started");
    await Task.Delay(delay).ConfigureAwait(false);
    Console.WriteLine($"{message} finished");
    return 0;
}

var partitioner = new ConcurrentPartitioner<int>();

partitioner.ExecuteAsync("partition A", () => Job(100, "Job A1"));
partitioner.ExecuteAsync("partition B", () => Job(100, "Job B1"));
partitioner.ExecuteAsync("partition A", () => Job(100, "Job A2"));
partitioner.ExecuteAsync("partition B", () => Job(100, "Job B2"));
```

Example output:
```
Job B1 started
Job A1 started
Job B1 finished
Job A1 finished
Job A2 started
Job B2 started
Job A2 finished
Job B2 finished
```

## Notes
- Unlike `LimitedParallelExecutor`, this partitioner does guarantee FIFO order **across multiple partitions** (note that B1 may be started before A1)
    - However, FIFO order is guaranteed within a single partition key
    - FIFO order is guaranteed when the clients synchronize access to the _synchronous_ part of `ExecuteAsync`
- You can specify custom per partition concurrency limit via `ConcurrentPartitioner`'s constructor.
    - Just like with `LimitedParallelExecutor`, FIFO task start order is maintained if clients synchronize calls to `ExecuteAsync`.
- You can wrap `ConcurrentPartitioner` into another `LimitedParallelExecutor` to enforce a global degree of parallelism across all partitions.
