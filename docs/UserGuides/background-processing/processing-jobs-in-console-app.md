# Processing jobs in a console application

A console program can be a dedicated Hangfire worker. `Run(ctx)` starts the server, blocks until `ctx` is done, and then stops the server gracefully. This page shows a complete worker program.

## Add the packages

Add the SDK and a SQL Server driver to your module:

```sh
go get github.com/reski-rukmantiyo/HangFire.GoSDK@latest github.com/microsoft/go-mssqldb
```

The SDK does not install the Hangfire schema. Install it from the .NET side before you start the worker.

## Write the worker

```go
package main

import (
	"context"
	"database/sql"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/microsoft/go-mssqldb"

	"github.com/reski-rukmantiyo/HangFire.GoSDK/hangfire"
	"github.com/reski-rukmantiyo/HangFire.GoSDK/server"
	"github.com/reski-rukmantiyo/HangFire.GoSDK/sqlserver"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := sql.Open("sqlserver", os.Getenv("HANGFIRE_SQL"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	storage, err := sqlserver.NewStorage(ctx, db, sqlserver.Options{Level: hangfire.CompatLevel180})
	if err != nil {
		log.Fatal(err)
	}

	srv, err := server.New(storage, server.Options{Queues: []string{"default"}})
	if err != nil {
		log.Fatal(err)
	}
	srv.Handle("MyApp.Jobs.ReportJob, MyApp", "Build", server.Handler1(func(ctx context.Context, month string) error {
		job := server.FromContext(ctx)
		slog.InfoContext(ctx, "building report", "month", month, "job", job.JobID)
		return nil
	}))

	slog.Info("worker started; press Ctrl+C to stop", "server", srv.ID())
	if err := srv.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

Run it with the connection string in `HANGFIRE_SQL`:

```sh
export HANGFIRE_SQL='Server=localhost,1433;Database=Hangfire;User Id=sa;Password=...;TrustServerCertificate=True'
go run .
```

The worker logs `hangfire: server started` with its worker count and queues. The server appears on the Dashboard's **Servers** page under the id that `srv.ID()` prints.

## Stop the worker

Press Ctrl+C, or send SIGTERM. `signal.NotifyContext` cancels `ctx`, and `Run` calls `Shutdown`. Running handlers get `StopTimeout` (500 ms) before the server cancels their contexts, and up to `ShutdownTimeout` (15 s) in total. The server requeues the jobs that still run after that. The worker then logs `hangfire: server stopped`, with `graceful=true` if every handler returned in time.

`Run` returns an error if `Start` fails, for example when the database is unreachable. It also returns the errors of the final requeue and cleanup writes. A nil error means the server stopped and deleted its row.

The [`examples/worker`](../../../examples/worker/main.go) program in this repository is a worker of this shape.

## Differences from .NET Hangfire

- The .NET console sample blocks on `Console.ReadKey` inside a `using` block. A Go worker blocks in `Run` until a signal arrives, so it also stops cleanly under a process manager.
- .NET warns against the `Hangfire` meta-package in console apps. The Go SDK is one module, and a worker imports only `hangfire`, `sqlserver`, and `server`.

## Related pages

- [Processing background jobs](processing-background-jobs.md)
- [Processing jobs in a Windows service](processing-jobs-in-windows-service.md)
