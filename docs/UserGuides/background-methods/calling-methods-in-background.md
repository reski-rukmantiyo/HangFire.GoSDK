# Calling methods in background

A fire-and-forget job runs once, as soon as a server fetches it. This page shows how to enqueue one from Go and what the call writes to SQL Server. It also shows how to decide whether a .NET server or a Go server runs the job.

## Connect to the Hangfire database

Open the database with a SQL Server driver, then wrap it in a `sqlserver.Storage`. Set `Level` to the compatibility level of the oldest .NET server that shares the database. The SDK never creates or migrates the schema. A .NET app with `PrepareSchemaIfNecessary` or the Hangfire install script owns it.

```go
package main

import (
	"context"
	"database/sql"
	"log"
	"os"

	_ "github.com/microsoft/go-mssqldb"

	"github.com/reski-rukmantiyo/HangFire.GoSDK/hangfire"
	"github.com/reski-rukmantiyo/HangFire.GoSDK/sqlserver"
)

func main() {
	ctx := context.Background()
	db, err := sql.Open("sqlserver", os.Getenv("HANGFIRE_SQL"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	storage, err := sqlserver.NewStorage(ctx, db, sqlserver.Options{Level: hangfire.CompatLevel180})
	if err != nil {
		log.Fatal(err)
	}
	client := sqlserver.NewClient(storage)

	id, err := client.Enqueue(ctx, hangfire.Invocation{
		Type:           "MyApp.Jobs.IEmailJobs, MyApp.Contracts",
		Method:         "SendWelcome",
		ParameterTypes: []string{"System.Int64"},
		Args:           []any{int64(42)},
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("enqueued job %s", id)
}
```

`NewStorage` fails with `hangfire.ErrSchemaVersion` when the database does not hold schema version 9.

## What Enqueue writes

`Enqueue` returns as soon as it stores the job. It does not wait for the job to run. In one transaction it does the following:

1. Inserts a `Job` row with the invocation in `InvocationData` and the arguments in `Arguments`.
2. Inserts any job parameters.
3. Adds an `Enqueued` state to the job's history.
4. Inserts a `JobQueue` row, which is what servers poll.

The returned `hangfire.JobID` is the decimal `Job.Id`. Use it to create continuations, or to delete, requeue, or reschedule the job later.

## Choose who runs the job

Any server that listens on the job's queue can fetch it. Pick one of these setups for each `Type`.

If a .NET server runs the job, `Type` and `Method` must name a real .NET method, and `ParameterTypes` must match its signature. A typo is not caught in Go. The .NET server fails the job with a "target method was not found" reason.

If a Go server runs the job, register a handler under the same two strings:

```go
srv, err := server.New(storage, server.Options{Queues: []string{"emails"}})
if err != nil {
	log.Fatal(err)
}
srv.Handle("MyApp.Jobs.IEmailJobs, MyApp.Contracts", "SendWelcome",
	server.Handler1(func(ctx context.Context, userID int64) error {
		log.Printf("welcome user %d", userID)
		return nil
	}))
if err := srv.Run(ctx); err != nil {
	log.Fatal(err)
}
```

A Go server fails every job it fetches that has no registered handler. Give Go handlers their own queues, and do not let Go servers listen on queues that carry .NET-only jobs.

## Put the job on a queue

Jobs go to the `default` queue unless you pass `hangfire.WithQueue`. A queue name must match `^[a-z0-9_-]+$`. The client rejects other names before it writes anything:

```go
_, err := client.Enqueue(ctx, inv, hangfire.WithQueue("Emails"))
if errors.Is(err, hangfire.ErrInvalidQueue) {
	log.Print("queue names are lowercase letters, digits, '_' and '-'")
}
```

The queue name goes into the job payload, as .NET's `Enqueue(queue, call)` writes it. A server must list that queue in its options to fetch the job.

## How a server processes the job

A worker on either platform does the same steps:

1. Fetches a `JobQueue` row and hides it from other workers.
2. Moves the job to `Processing`.
3. Runs the method or handler.
4. Moves the job to `Succeeded`, or to `Failed` and possibly a retry, and deletes the queue row in the same transaction.

If the process dies during step 3, the queue row stays in the table. It becomes visible again after the invisibility timeout, 5 minutes by default on SQL Server, and another worker runs the job again. A job can therefore run more than once. Write handlers so that a second run does no harm.

## See the job in the Dashboard

The Dashboard loads the .NET type that `Type` names so it can show the call. Put the interfaces that Go jobs name into a .NET contracts library and reference it from the Dashboard host. A job that only Go runs still needs a stub interface there. Without it the job page shows `Can not find the target method.`, and the job still runs. [examples/dotnet-dashboard/README.md](../../../examples/dotnet-dashboard/README.md) has a working setup.

## Differences from .NET Hangfire

- There is no `BackgroundJob.Enqueue(() => ...)` lambda. You write `Type`, `Method`, and `ParameterTypes` as strings, so the compiler does not check them.
- There is no static `BackgroundJob` facade. Pass a `hangfire.Client` to the code that creates jobs. [Writing unit tests](writing-unit-tests.md) shows why that helps.
- `Enqueue` takes a `context.Context` and returns an error instead of throwing.

## Related pages

- [Calling methods with delay](calling-methods-with-delay.md)
- [Passing arguments](passing-arguments.md)
- [Passing dependencies](passing-dependencies.md)
