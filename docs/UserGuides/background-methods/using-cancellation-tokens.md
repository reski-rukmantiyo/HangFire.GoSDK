# Using cancellation tokens

A long job should stop when its server shuts down or when someone deletes or requeues it. In .NET the job takes a `CancellationToken`. In Go the handler's `context.Context` plays that role. This page shows how a Go handler notices cancellation, what the server does with the job afterwards, and how to call a .NET method that takes a token.

## Watch the handler's context

Every handler gets a `context.Context` as its first parameter. Pass it to every call that accepts one, and check it in loops that do not:

```go
srv.Handle("MyApp.Jobs.IImportJobs, MyApp.Contracts", "ImportAll",
	server.Handler1(func(ctx context.Context, ids []int64) error {
		for _, id := range ids {
			if err := ctx.Err(); err != nil {
				return err
			}
			log.Printf("importing %d", id)
		}
		return nil
	}))
```

`ctx.Err()` reads a field. It makes no storage request, so it is safe in a tight loop.

## Cancellation on shutdown

When the server shuts down, it stops fetching new jobs and cancels running handlers' contexts after `StopTimeout`, 500 ms by default. A handler that then returns `context.Canceled` or `context.DeadlineExceeded` is not marked as failed. The server puts the job back on its queue, and a server runs it again after a restart.

The server waits up to `ShutdownTimeout`, 15 seconds by default, for handlers to return. A handler that ignores its context past that point is abandoned, and its job is requeued. Either way the job runs again, so make it idempotent.

## Cancellation when the job is aborted

If someone deletes or requeues a running job, from the Dashboard or with `client.Delete` or `client.Requeue`, the job leaves the `Processing` state. Every server runs a cancellation watcher that checks its running jobs every `CancellationCheckInterval`, 5 seconds by default. When it sees that a job is no longer `Processing` on this worker, it cancels that handler's context. Whatever the handler returns afterwards is ignored, because the job's new state is already set.

```go
srv, err := server.New(storage, server.Options{
	Queues:                    []string{"imports"},
	CancellationCheckInterval: time.Second,
})
if err != nil {
	log.Fatal(err)
}
```

`DisableCancellationWatcher` turns the watcher off. Handlers then see cancellation only on shutdown.

## Call a .NET method that takes a token

When a Go producer enqueues a .NET method with a `CancellationToken` parameter, list the parameter type and pass `nil`. The .NET server replaces the `null` with a real token when the job runs:

```go
id, err := client.Enqueue(ctx, hangfire.Invocation{
	Type:   "MyApp.Jobs.IImportJobs, MyApp.Contracts",
	Method: "ImportAll",
	ParameterTypes: []string{
		"System.Int64[]",
		"System.Threading.CancellationToken",
	},
	Args: []any{[]int64{1, 2, 3}, nil},
})
if err != nil {
	log.Fatal(err)
}
log.Printf("enqueued job %s", id)
```

The same job can run on a Go server. The handler above declares only `ids`, because the server drops `CancellationToken`, `IJobCancellationToken`, and `PerformContext` arguments before it decodes the rest.

## Differences from .NET Hangfire

- There is no `IJobCancellationToken` and no `ThrowIfCancellationRequested`. Return `ctx.Err()` instead.
- The context is always there. You do not add a parameter to get it.
- .NET puts a shutdown-cancelled job back at the start of its queue. A Go server requeues it the same way, by making its queue row visible again.

## Related pages

- [Using IoC containers](using-ioc-containers.md)
- [Calling methods in background](calling-methods-in-background.md)
