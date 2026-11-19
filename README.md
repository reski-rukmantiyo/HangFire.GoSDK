# Hangfire Go SDK

Produce, manage and process [Hangfire](https://www.hangfire.io) background jobs from Go. The SDK writes the same rows to Hangfire's SQL Server storage that Hangfire 1.8 writes, so Go and .NET services share one job database:

- A Go producer creates jobs that .NET servers run.
- A Go server runs jobs that .NET code created.
- The .NET Dashboard shows jobs that Go created and Go processed.

Packages:

- `hangfire` holds the invocation and state model and the wire codecs.
- `sqlserver` is the storage: `Client` (create and manage jobs), `RecurringManager`, `Monitor` (read model), and the queue and server primitives.
- `server` runs Go handlers: workers, retries, and the background processes of a .NET `BackgroundJobServer`.
- `hangfiretest` is a fake client for unit tests.

The requirements live in [docs/SRS/golang-sdk-srs.md](docs/SRS/golang-sdk-srs.md).

## Install

```sh
go get github.com/reski-rukmantiyo/HangFire.GoSDK@latest
```

Import a SQL Server driver as well, for example `github.com/microsoft/go-mssqldb`. The SDK never installs or migrates the schema. The .NET side owns it.

## Enqueue a job for a .NET server

```go
db, _ := sql.Open("sqlserver", connString)
storage, err := sqlserver.NewStorage(ctx, db, sqlserver.Options{Level: hangfire.CompatLevel180})
if err != nil {
	log.Fatal(err)
}
client := sqlserver.NewClient(storage)

id, err := client.Enqueue(ctx, hangfire.Invocation{
	Type:           "MyApp.Jobs.EmailJob, MyApp",
	Method:         "Send",
	ParameterTypes: []string{"System.String", "System.Int32"},
	Args:           []any{"ops@example.com", 3},
}, hangfire.WithQueue("emails"))
```

Set `Level` to the compatibility level of the oldest .NET server in the fleet.

## Process jobs in Go

```go
srv, err := server.New(storage, server.Options{Queues: []string{"emails"}})
if err != nil {
	log.Fatal(err)
}
srv.Handle("MyApp.Jobs.EmailJob, MyApp", "Send",
	server.Handler2(func(ctx context.Context, to string, attempts int) error {
		job := server.FromContext(ctx) // job id, retry count, parameters
		return send(ctx, job.JobID, to, attempts)
	}),
	server.WithRetry(server.RetryPolicy{Attempts: 5, DelaysInSeconds: []int{10, 60, 600}}))

ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()
log.Fatal(srv.Run(ctx))
```

How a Go server behaves:

- Handlers take a `context.Context` first, then the job arguments. `Handler0` to `Handler4` decode the arguments from JSON. `CancellationToken`, `IJobCancellationToken` and `PerformContext` arguments are skipped.
- A returned error or a panic moves the job to `Failed` with `ExceptionType` `Go:<type>`. An error can choose its own type with a `HangfireExceptionType() string` method.
- The retry policy matches `AutomaticRetryAttribute`. By default a job gets 10 attempts with the same back-off as .NET.
- A job whose arguments do not decode fails without retry, with `ExceptionType` `Hangfire.Go.ArgumentDecodeException`.
- A job with no Go handler fails with the reason .NET writes: `Can not change the state to 'Processing': target method was not found.`
- Deleting or requeueing a running job cancels its handler context within `CancellationCheckInterval` (5 s).
- Jobs run at least once. A crashed worker's job runs again after `InvisibilityTimeout` (5 min), so make handlers idempotent.

The server also runs background processes, each under the same distributed lock as its .NET counterpart:

| Process | Default | Option |
|---|---|---|
| Delayed job scheduler, every 15 s | on | `DisableDelayedScheduler` |
| Cancellation watcher, every 5 s | on | `DisableCancellationWatcher` |
| Server watchdog, every 5 min | on | `DisableServerWatchdog` |
| Server heartbeat, every 30 s | always on | |
| Recurring job scheduler | off | `EnableRecurringScheduler` |
| Expiration manager, every 30 min | off | `EnableExpirationManager` |
| Counters aggregator, every 5 min | off | `EnableCountersAggregator` |

The last three are off because the .NET servers in a mixed fleet usually run them. Turn them on in a Go-only fleet.

See [examples](examples/README.md) for a producer, a worker, a .NET app hosting the Hangfire Dashboard, HTTP embedding, systemd, and Windows service setups.

## Compatibility

| | Supported |
|---|---|
| Hangfire | 1.8 (tested against 1.8.25) |
| Hangfire.SqlServer schema | version 9. `NewStorage` refuses any other version |
| Compatibility level | `CompatLevel170`, `CompatLevel180` |
| Database | SQL Server 2016 and later, Azure SQL Edge |
| Queue fetch | sliding invisibility timeout, the 1.8 default |
| Go | 1.26 |

## Out of scope

- Hangfire.Pro batches and Hangfire.Ace throttling. `WithMaxConcurrency` is a Go-local limit, not Ace interop.
- Redis, PostgreSQL, Mongo, InMemory and MSMQ storages.
- Compatibility level 110 and the legacy transaction-held fetch.
- Schema install and migration.
- A Go dashboard. The .NET Dashboard reads everything the SDK writes.
- .NET filter attributes (`[AutomaticRetry]`, `[Queue]`, `[DisableConcurrentExecution]`) on Go-handled jobs. Use handler options.

## Run the tests

Unit tests need nothing:

```sh
go test ./...
```

Integration tests need a SQL Server and the .NET 9 SDK, which installs the schema through `tools/e2e-dotnet`. Each test package creates and drops its own database.

```sh
scripts/sqlserver.sh up
export HANGFIRE_SQL='Server=localhost,1433;Database=HangfireGoSdk;User Id=sa;Password=Hangfire_Dev_Passw0rd!;TrustServerCertificate=True;Encrypt=False'
(cd tools/e2e-dotnet && dotnet build)
E2E_DOTNET=1 go test -race -count=1 ./...
```

`E2E_DOTNET=1` turns on the `e2e` package, which runs a real .NET Hangfire server against the same database. Set `HANGFIRE_KEEP_DB=1` to keep the test databases for inspection.
