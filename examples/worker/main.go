// Command worker runs a Go Hangfire server that processes IGoJobs jobs from
// the "goq" queue. It stops gracefully on SIGINT or SIGTERM.
//
//	HANGFIRE_SQL='Server=localhost,1433;Database=HangfireGoSdk;User Id=sa;Password=...;TrustServerCertificate=True' go run ./examples/worker
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

	dsn := os.Getenv("HANGFIRE_SQL")
	if dsn == "" {
		log.Fatal("HANGFIRE_SQL is not set")
	}
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	storage, err := sqlserver.NewStorage(ctx, db, sqlserver.Options{Level: hangfire.CompatLevel180})
	if err != nil {
		log.Fatal(err)
	}

	srv, err := server.New(storage, server.Options{
		Queues: []string{"goq"},
		// Safe next to .NET servers: both take the same recurring-jobs lock.
		EnableRecurringScheduler: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	srv.Handle("E2E.Jobs.IGoJobs, E2E", "Hello", server.Handler1(func(ctx context.Context, name string) error {
		job := server.FromContext(ctx)
		slog.InfoContext(ctx, "hello", "name", name, "job", job.JobID, "retry", job.RetryCount)
		return nil
	}))
	srv.Handle("E2E.Jobs.IGoJobs, E2E", "Add", server.Handler2(func(ctx context.Context, a, b int) error {
		slog.InfoContext(ctx, "add", "sum", a+b)
		return nil
	}), server.WithRetry(server.RetryPolicy{Attempts: 3, DelaysInSeconds: []int{10, 60}}))

	if err := srv.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
