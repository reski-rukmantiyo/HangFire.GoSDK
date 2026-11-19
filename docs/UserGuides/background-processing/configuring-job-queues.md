# Configuring job queues

Queues let you split jobs between servers and prevent slow jobs from delaying urgent jobs. A producer puts each job in a queue, and each server lists the queues it fetches from. This page shows both sides, the naming rule, and the order in which SQL Server fetches queues.

## Put a job in a queue

Pass `hangfire.WithQueue` when you create the job. Without it, the job goes to `default`.

```go
client := sqlserver.NewClient(storage)
inv := hangfire.Invocation{
	Type:           "MyApp.Jobs.EmailJob, MyApp",
	Method:         "Send",
	ParameterTypes: []string{"System.String"},
	Args:           []any{"ops@example.com"},
}
if _, err := client.Enqueue(ctx, inv, hangfire.WithQueue("critical")); err != nil {
	log.Fatal(err)
}
if _, err := client.Schedule(ctx, inv, time.Hour, hangfire.WithQueue("critical")); err != nil {
	log.Fatal(err)
}
```

The client stores the queue in the job's payload. The queue applies in these cases:

- The client enqueues the job at once.
- A scheduled job reaches its scheduled time.
- A continuation starts.
- A retry runs.
- Someone requeues the job from the Dashboard.

Recurring jobs take their queue from `hangfire.RecurringOptions.Queue`.

## Choose the queues a server fetches

Set `Options.Queues`. Without it, a server fetches only `default`.

```go
srv, err := server.New(storage, server.Options{
	Queues: []string{"critical", "default"},
})
if err != nil {
	log.Fatal(err)
}
```

A handler does not belong to a queue. A server runs a job from any of its queues if it has a handler for the job. Inside the handler, `server.FromContext(ctx).Queue` tells you which queue the job came from.

## Name queues with lowercase letters, digits, `_`, and `-`

A queue name must match `^[a-z0-9_-]+$`, the same rule that .NET applies. `server.New`, `Enqueue`, and the other create calls reject other names with an error that wraps `hangfire.ErrInvalidQueue`:

```go
_, err := server.New(storage, server.Options{Queues: []string{"Critical"}})
if errors.Is(err, hangfire.ErrInvalidQueue) {
	log.Print("queue names must be lower case")
}
```

## Know the fetch order on SQL Server

On SQL Server, a worker fetches from its queues in alphanumeric order of the queue names. The order of `Options.Queues` does not matter. This is how Hangfire.SqlServer behaves in .NET too.

With `[]string{"default", "critical"}`, `critical` comes first because it sorts first. With `[]string{"default", "urgent"}`, `default` comes first. To give a queue priority, choose a name that sorts before the others, such as `a-urgent` or `0-urgent`.

The order is a preference, not a guarantee. A worker takes the first visible row it finds, and workers on other servers may fetch the same queues in a different mix. To guarantee capacity for a queue, give it its own server with its own workers. See [Configuring the degree of parallelism](configuring-degree-of-parallelism.md#use-separate-servers-for-separate-pools).

## Differences from .NET Hangfire

- .NET picks a queue with the `[Queue]` attribute on the method. Go producers pass `hangfire.WithQueue` on each call. The `[Queue]` attribute has no effect on Go handlers.
- In a fleet with Go and .NET servers, do not let Go servers and .NET servers serve the same queue. See [Placing processing into another process](placing-processing-into-another-process.md#split-queues-between-go-and-net).

## Related pages

- [Processing background jobs](processing-background-jobs.md)
- [Concurrency and rate limiting](throttling.md)
