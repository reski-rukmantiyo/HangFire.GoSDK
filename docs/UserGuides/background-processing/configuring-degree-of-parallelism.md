# Configuring the degree of parallelism

A server runs a fixed number of workers, and each worker runs one job at a time. The worker count is the most jobs one server runs at once. This page shows how to set it and what else to size with it.

## Set the worker count

Set `Options.WorkerCount`. The server starts that many workers in `Start` and keeps the number fixed until `Shutdown`.

```go
srv, err := server.New(storage, server.Options{
	Queues:      []string{"default"},
	WorkerCount: 8,
})
if err != nil {
	log.Fatal(err)
}
```

Zero means the default, `min(runtime.NumCPU()*5, 20)`. A negative value makes `server.New` return an error.

Each worker is a goroutine, so a worker costs little while it waits. Pick the count from what the handlers use, not from the CPU count:

- For handlers that mostly wait on the network or the database, more workers than CPUs is fine. The limit is usually the service the handlers call.
- For handlers that use the CPU, set the count near `runtime.NumCPU()`. More workers then add only scheduling overhead.

## Size the database pool

Workers share the `*sql.DB` that you passed to `sqlserver.NewStorage`. While idle, each worker runs a fetch query every `QueuePollInterval` (200 ms). Twenty idle workers send about 100 fetch queries per second. A running job adds a keep-alive update every fifth of `InvisibilityTimeout` and the final state change, plus whatever queries the handler runs.

If you cap the pool with `db.SetMaxOpenConns`, leave room for every worker plus the background processes. A pool smaller than the worker count makes workers wait for connections, and the extra workers add no throughput.

## Use separate servers for separate pools

Every worker on a server fetches from all of the server's queues. To give a queue its own fixed number of workers, run a second server in the same process with only that queue.

```go
reports, err := server.New(storage, server.Options{
	Name:        "reports",
	Queues:      []string{"reports"},
	WorkerCount: 2,
})
if err != nil {
	log.Fatal(err)
}
general, err := server.New(storage, server.Options{
	Queues:      []string{"critical", "default"},
	WorkerCount: 20,
})
if err != nil {
	log.Fatal(err)
}
reports.Handle("MyApp.Jobs.ReportJob, MyApp", "Build", server.Handler1(buildReport))
general.Handle("MyApp.Jobs.EmailJob, MyApp", "Send", server.Handler1(sendEmail))
```

Start and shut down both servers. At most two report jobs run at once in this process, and they never take workers from the other queues. Each server needs its own handlers, because handlers belong to one server.

Both servers run the default background processes. That is safe, because the processes take distributed locks. To avoid duplicate work, you can turn the processes off on one of them with `DisableDelayedScheduler`, `DisableCancellationWatcher`, and `DisableServerWatchdog`. Keep `DisableCancellationWatcher` false on any server whose handlers must react when a running job is deleted.

To cap one handler without a second server, see `WithMaxConcurrency` in [Concurrency and rate limiting](throttling.md).

## Differences from .NET Hangfire

- The .NET docs give the default as `Environment.ProcessorCount * 5`. The .NET source also caps it at 20, and the Go default matches the source.
- .NET workers are dedicated threads. Go workers are goroutines.

## Related pages

- [Configuring job queues](configuring-job-queues.md)
- [Running multiple server instances](running-multiple-server-instances.md)
