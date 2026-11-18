# Concurrency and rate limiting

In .NET, Hangfire.Ace's throttling package limits how many jobs run at once and how often they run, across the whole fleet. The Go SDK does not support Hangfire.Ace. This page shows what it offers instead, and what each option does and does not limit.

## Hangfire.Ace is not supported

The Go server does not read or write Hangfire.Ace's mutexes, semaphores, or window counters. Ace attributes on a C# interface have no effect on Go handlers. A .NET server's Ace limits do not count jobs that Go servers run, and Go servers do not count jobs that .NET servers run.

## Limit one handler in one server

`server.WithMaxConcurrency(n)` caps how many jobs of one handler run at once in one server.

```go
srv.Handle("MyApp.Jobs.ReportJob, MyApp", "Build", server.Handler1(buildReport),
	server.WithMaxConcurrency(2))
```

The limit is local to the `*server.Server` in this process. It is not shared:

- Not across processes. Three worker processes with `WithMaxConcurrency(2)` run up to six report jobs at once.
- Not across servers in one process. Each server keeps its own count.
- Not with .NET servers, and not with Hangfire.Ace.

A value of zero or less means no limit.

A job that waits for a slot keeps its worker busy. The worker has already fetched the job, and it holds the job's queue row until a slot frees. If a queue fills with jobs of a capped handler, they can occupy every worker while only `n` of them run. Jobs from other queues then wait too. To avoid that, put the capped jobs in their own queue on their own server, or use the worker count of that server as the cap:

```go
reports, err := server.New(storage, server.Options{
	Queues:      []string{"reports"},
	WorkerCount: 2,
})
if err != nil {
	log.Fatal(err)
}
reports.Handle("MyApp.Jobs.ReportJob, MyApp", "Build", server.Handler1(buildReport))
```

A waiting job stays in the `Enqueued` state. If the server shuts down, the job goes back to its queue.

## Limit a resource across the fleet

To let only one job at a time use a resource across every Go server, take a SQL Server application lock in the handler. `storage.Session` opens a dedicated connection, and `Session.Lock` takes a lock that lasts until `Close`.

```go
srv.Handle("MyApp.Jobs.ReportJob, MyApp", "Build",
	server.Handler1(func(ctx context.Context, tenant string) error {
		se, err := storage.Session(ctx)
		if err != nil {
			return err
		}
		defer se.Close()
		if err := se.Lock(ctx, "myapp:report:"+tenant, time.Minute); err != nil {
			return err
		}
		return buildReport(ctx, tenant)
	}))
```

The lock name gets the schema prefix, so the lock above is `HangFire:myapp:report:<tenant>`. If another job holds the lock for longer than the timeout, `Lock` returns an error that wraps `hangfire.ErrLockTimeout`. The job then fails and goes through the retry policy, so each wait costs a retry attempt. The connection stays open for as long as the handler runs.

This is a mutex for Go handlers only. .NET jobs do not take the same lock unless your .NET code takes it too.

## Limit the rate in one process

For a rate limit, such as calls per second to an external API, use a limiter shared by the handlers in the process. `golang.org/x/time/rate` waits until a token is free:

```go
limiter := rate.NewLimiter(rate.Limit(10), 1)

srv.Handle("MyApp.Jobs.WebhookJob, MyApp", "Deliver",
	server.Handler1(func(ctx context.Context, url string) error {
		if err := limiter.Wait(ctx); err != nil {
			return err
		}
		return deliver(ctx, url)
	}))
```

This limit is per process. With several worker processes, divide the rate between them, or keep the count in a shared store such as the database or Redis.

## Differences from .NET Hangfire

- Hangfire.Ace throttles jobs before they run and reschedules the ones over the limit, so workers stay free. `WithMaxConcurrency` holds the worker while a job waits.
- Ace limits apply across the fleet. `WithMaxConcurrency` and an in-process rate limiter apply to one process.
- Ace offers fixed, sliding, and dynamic windows, and limits per argument. In Go, you build those in the handler.

## Related pages

- [Configuring the degree of parallelism](configuring-degree-of-parallelism.md)
- [Dealing with exceptions](dealing-with-exceptions.md)
