# Processing background jobs

A Go process runs Hangfire jobs only while it runs a `server.Server`. This page shows how to create a server, what it runs, how it stops, and what happens to jobs when a process dies.

## Create and start a server

`server.New` binds a server to a storage. Register one handler for each job method the server runs, then start it.

```go
srv, err := server.New(storage, server.Options{Queues: []string{"default"}})
if err != nil {
	log.Fatal(err)
}
srv.Handle("MyApp.Jobs.EmailJob, MyApp", "Send",
	server.Handler2(func(ctx context.Context, to string, attempt int) error {
		return sendEmail(ctx, to, attempt)
	}))

if err := srv.Start(ctx); err != nil {
	log.Fatal(err)
}
defer srv.Shutdown(context.Background())
```

The server matches each job by the exact `Type` and `Method` strings in its payload. `Handler0` to `Handler4` decode the job's arguments from JSON into the Go parameter types. The first parameter is always a `context.Context`.

Some rules apply to the lifecycle:

- `Handle` panics if you call it after `Start`. Register every handler first.
- `Start` registers the server in the `Server` table and starts the workers and background processes. It returns once they run. Its `ctx` bounds only the registration.
- A second `Start` returns an error. A server runs once.
- `Run(ctx)` calls `Start`, blocks until `ctx` is done, and then calls `Shutdown`. Use it when the server is the whole program. See [Processing jobs in a console application](processing-jobs-in-console-app.md).

To prevent a process from running jobs, do not create a server in it. A process that only creates jobs needs a `sqlserver.Client` and nothing else. See [Placing processing into another process](placing-processing-into-another-process.md).

## What a server runs

A server runs `WorkerCount` workers. Each worker is a goroutine that fetches one job at a time from the server's queues, runs its handler, and records the outcome. When no job is visible, a worker waits `QueuePollInterval` (200 ms) and fetches again.

The server also runs the background processes of a .NET `BackgroundJobServer`. Each process that needs a distributed lock takes the same SQL Server application lock as its .NET counterpart. Only one server at a time does a pass, whether it is a Go server or a .NET server.

| Process | Interval | Default | Option | Lock |
|---|---|---|---|---|
| Server heartbeat | `HeartbeatInterval`, 30 s | always on | none | none |
| Server watchdog | `ServerCheckInterval`, 5 min | on | `DisableServerWatchdog` | none |
| Cancellation watcher | `CancellationCheckInterval`, 5 s | on | `DisableCancellationWatcher` | none |
| Delayed job scheduler | `SchedulePollingInterval`, 15 s | on | `DisableDelayedScheduler` | `HangFire:locks:schedulepoller` |
| Recurring job scheduler | `SchedulePollingInterval`, 15 s | off | `EnableRecurringScheduler` | `HangFire:recurring-jobs:lock` |
| Expiration manager | `JobExpirationCheckInterval`, 30 min | off | `EnableExpirationManager` | `HangFire:locks:expirationmanager` |
| Counters aggregator | `CountersAggregateInterval`, 5 min | off | `EnableCountersAggregator` | none, it skips locked rows with `READPAST` |

The lock names use the schema name, `HangFire` by default.

The last three processes are off by default, because the .NET servers in a mixed fleet usually run them. In a fleet with no .NET server, enable them in at least one Go server. Without the recurring scheduler, recurring jobs never fire. Without the expiration manager, finished jobs never leave the database.

```go
srv, err := server.New(storage, server.Options{
	EnableRecurringScheduler: true,
	EnableExpirationManager:  true,
	EnableCountersAggregator: true,
})
if err != nil {
	log.Fatal(err)
}
```

Running these processes next to .NET servers is safe. The locks prevent two servers from doing the same pass. A scheduler that cannot take its lock within one minute skips that pass and tries again at the next interval. The expiration manager waits up to five minutes for its lock.

A failed pass does not stop the server. The server logs `hangfire: background process failed` with the error and runs the pass again at the next interval.

## How a job runs

For each job, a worker does these steps:

1. Fetches a row from the `JobQueue` table and sets its `FetchedAt` time. Other workers skip the row while `FetchedAt` is newer than `InvisibilityTimeout` (5 min).
2. Refreshes `FetchedAt` every fifth of `InvisibilityTimeout` (every minute by default) while the job runs. The row stays invisible to other workers for as long as the job runs.
3. Moves the job to `Processing`, under the job's state lock.
4. Calls the handler with a context that carries a `server.JobContext`.
5. Moves the job to `Succeeded`, or to `Failed` under the retry policy, and deletes the queue row in the same transaction.

If the job is no longer `Enqueued`, `Scheduled`, or `Processing` when the worker reaches step 3, the worker deletes the queue row and does not run the handler. A job deleted from the Dashboard before it runs ends this way.

## Stop a server gracefully

`Shutdown(ctx)` stops the server in this order:

1. Workers stop fetching new jobs. Background processes stop.
2. After `StopTimeout` (500 ms), the server cancels the contexts of running handlers.
3. The server waits for handlers to return, up to `ShutdownTimeout` (15 s) from the start of `Shutdown`, or until `ctx` is done.
4. The server requeues each job whose handler is still running, by setting `FetchedAt` back to null.
5. The server deletes its row from the `Server` table.

Shutdown cancels the context of a running handler. If the handler then returns `ctx.Err()`, or an error that wraps it, the server requeues the job instead of failing it. Another worker fetches the job again. Any other error counts as a failure and goes through the retry policy.

```go
srv.Handle("MyApp.Jobs.ImportJob, MyApp", "Run",
	server.Handler1(func(ctx context.Context, fileID int64) error {
		for row := range rows(fileID) {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := importRow(ctx, row); err != nil {
				return fmt.Errorf("import row %d: %w", row, err)
			}
		}
		return nil
	}))
```

Go cannot stop a goroutine from outside. A handler that ignores its context keeps running after `Shutdown` returns. The server already requeued its job at that point, so another worker can run the job again while the old goroutine still runs. When the old goroutine returns, the server discards its result. Check `ctx` in every loop and pass it to every call that blocks.

`Shutdown` is safe to call more than once. Later calls return the first call's result.

## Crashes and at-least-once delivery

A process can die without calling `Shutdown`: a kill signal, a panic outside a handler, or a lost machine. No job is lost when that happens:

- The queue row of each running job keeps its last `FetchedAt` value. Once `InvisibilityTimeout` passes, any worker can fetch the row again.
- The job is still in the `Processing` state. Workers accept `Processing` jobs, so the next worker runs the job again.
- The dead server's row stays in the `Server` table until a watchdog deletes it. See [Running multiple server instances](running-multiple-server-instances.md).

A job can also run twice without a crash. If the worker cannot write the final state, for example because the database connection drops, it logs `hangfire: recording job outcome failed; requeued` and requeues the job. If the requeue fails too, the job runs again after `InvisibilityTimeout`.

Jobs therefore run at least once. Make each handler safe to run twice. One way is to key side effects on the job id:

```go
srv.Handle("MyApp.Jobs.BillingJob, MyApp", "Charge",
	server.Handler1(func(ctx context.Context, invoiceID int64) error {
		job := server.FromContext(ctx)
		return chargeOnce(ctx, invoiceID, string(job.JobID))
	}))
```

## Differences from .NET Hangfire

- A .NET server starts with `new BackgroundJobServer()`. A Go server starts with `server.New`, `Handle` for each job method, and then `Start` or `Run`.
- .NET activates job classes with a `JobActivator`. Go has no reflection-based activation, so you register each `(Type, Method)` pair with `Handle`.
- .NET aborts a worker thread's job when shutdown times out. Go cannot stop a goroutine, so a handler that ignores its context keeps running after the server requeues its job.
- The recurring scheduler, the expiration manager, and the counters aggregator are off by default in Go. They are on by default in .NET.
- The SDK supports only the sliding invisibility fetch. It is the default for Hangfire.SqlServer 1.8.

## Related pages

- [Dealing with exceptions](dealing-with-exceptions.md)
- [Configuring the degree of parallelism](configuring-degree-of-parallelism.md)
- [Configuring job queues](configuring-job-queues.md)
