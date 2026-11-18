// Command producer enqueues jobs that a .NET or Go Hangfire server runs.
//
//	HANGFIRE_SQL='Server=localhost,1433;Database=HangfireGoSdk;User Id=sa;Password=...;TrustServerCertificate=True' go run ./examples/producer
package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"time"

	_ "github.com/microsoft/go-mssqldb"

	"hangfire-gosdk/hangfire"
	"hangfire-gosdk/sqlserver"
)

func main() {
	ctx := context.Background()
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
	client := sqlserver.NewClient(storage)

	hello := hangfire.Invocation{
		Type:           "E2E.Jobs.IGoJobs, E2E",
		Method:         "Hello",
		ParameterTypes: []string{"System.String"},
		Args:           []any{"world"},
	}
	id, err := client.Enqueue(ctx, hello, hangfire.WithQueue("goq"))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("enqueued job %s", id)

	later, err := client.Schedule(ctx, hello, time.Minute, hangfire.WithQueue("goq"))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("scheduled job %s", later)

	add := hangfire.Invocation{
		Type:           "E2E.Jobs.IGoJobs, E2E",
		Method:         "Add",
		ParameterTypes: []string{"System.Int32", "System.Int32"},
		Args:           []any{2, 3},
	}
	next, err := client.ContinueJobWith(ctx, id, add, hangfire.Enqueued{}, hangfire.OnlyOnSucceededState, hangfire.WithQueue("goq"))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("continuation job %s runs after job %s succeeds", next, id)

	err = sqlserver.NewRecurringManager(storage).AddOrUpdate(ctx, "nightly-hello", hello, "0 2 * * *",
		hangfire.RecurringOptions{Queue: "goq"})
	if err != nil {
		log.Fatal(err)
	}
	log.Print("recurring job nightly-hello registered for 02:00 UTC")
}
