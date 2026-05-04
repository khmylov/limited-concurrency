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

- Executor follows the order of the enqueued jobs, subject to the configured `degreeOfParallelism`:
  - If `degreeOfParallelism = 1`, then the execution is strictly sequential and FIFO.
  - If `degreeOfParallelism = N (N > 1)`, then up to `N` jobs may start concurrently in non-deterministic order.
    ```
    var executor = new LimitedParallelExecutor(degreeOfParallelism: 2);
    executor.Enqueue(A); executor.Enqueue(B); executor.Enqueue(C);
    // Job start order may be [A, B, C] or [B, A, C]; but C may never start before A or B.
    ```
- Executor schedules Tasks via `Task.Run`, i.e. on default thread pool scheduler, to ensure that execution is truly parallel even if passed `Func<Task>` implementations are synchronous and blocking.
- While the executor itself is thread-safe, multi-threaded concurrent clients may need synchronization to ensure correct enqueuing order.
- `void Enqueue(...)` method is fire-and-forget, but `Task<TResult> ExecuteAsync<TResult>(...)` is also provided if you need to await the job completion.

# ConcurrentPartitioner

Another common requirement in concurrent message processing is "partitioning":
- Every message belongs to exactly one partition key, for example customer name in multi-tenant environment
- Messages with different partition keys may be processed in parallel
- Messages within the same partition key must be processed sequentially (or with a limited max concurrency level)

`ConcurrentPartitioner` provides such behavior.

```csharp
async Task<int> Job(int delay, string message)
{
    Console.WriteLine($"{message} started");
    await Task.Delay(delay).ConfigureAwait(false);
    Console.WriteLine($"{message} finished");
    return 0;
}

var partitioner = new ConcurrentPartitioner();

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
- Unlike `LimitedParallelExecutor`, this partitioner does not guarantee FIFO order **across multiple partitions** (note that B1 may be started before A1)
    - However, FIFO order is guaranteed within a single partition key
    - Just like with `LimitedParallelExecutor`, FIFO order is guaranteed when the clients synchronize access to the _synchronous_ part of `ExecuteAsync`
- You can specify custom per partition concurrency limit via `ConcurrentPartitioner`'s constructor.
    - This allows to implemented "keyed limiter" scenarios, e.g. executing not more than N concurrent jobs per partition at the same time.
- You can wrap `ConcurrentPartitioner` into another `LimitedParallelExecutor` to enforce a global degree of parallelism across all partitions.
- `ConcurrentPartitioner` is designed to automatically clean up its internal storage for unused partitions, so you don't have to worry about memory leaks if you generate a huge number of different partition keys over a long period of time.
