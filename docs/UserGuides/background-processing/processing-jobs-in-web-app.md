# Processing jobs in a web application

You can run a Hangfire server inside a Go HTTP service, so the service creates jobs and runs them with no separate worker process. This page shows how to start the server with the service and stop it cleanly when the service gets SIGINT or SIGTERM.

## Embed the server in a `net/http` service

The program below opens the storage, starts a server with one handler, and serves an endpoint that enqueues a job. On SIGINT or SIGTERM it stops the HTTP listener first, then the job server.

```go
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/microsoft/go-mssqldb"

	"hangfire-gosdk/hangfire"
	"hangfire-gosdk/server"
	"hangfire-gosdk/sqlserver"
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
	client := sqlserver.NewClient(storage)

	srv, err := server.New(storage, server.Options{Queues: []string{"emails"}})
	if err != nil {
		log.Fatal(err)
	}
	srv.Handle("MyApp.Jobs.EmailJob, MyApp", "SendWelcome", server.Handler1(sendWelcome))
	if err := srv.Start(ctx); err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /signup", func(w http.ResponseWriter, r *http.Request) {
		id, err := client.Enqueue(r.Context(), hangfire.Invocation{
			Type:           "MyApp.Jobs.EmailJob, MyApp",
			Method:         "SendWelcome",
			ParameterTypes: []string{"System.String"},
			Args:           []any{r.FormValue("email")},
		}, hangfire.WithQueue("emails"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, "job %s\n", id)
	})
	httpSrv := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Print(err)
			stop()
		}
	}()

	<-ctx.Done()

	httpCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(httpCtx); err != nil {
		log.Print(err)
	}
	if err := srv.Shutdown(context.Background()); err != nil {
		log.Print(err)
	}
}

func sendWelcome(ctx context.Context, email string) error {
	log.Printf("sending welcome mail to %s", email)
	return nil
}
```

Set `HANGFIRE_SQL` to an ADO connection string and run the program. A `POST /signup` with an `email` form field returns the new job id, and the server in the same process runs the job.

## Plan the shutdown time

Two things limit how long the job server takes to stop:

- `Options.ShutdownTimeout`, 15 s by default. After it, the server requeues jobs whose handlers still run.
- The `ctx` you pass to `Shutdown`. If it ends first, the server stops waiting and requeues the running jobs at that moment.

The program above gives the HTTP listener its own 10-second budget and passes `context.Background()` to `srv.Shutdown`, so the job server always gets its full `ShutdownTimeout`. If you pass one shared deadline to both calls, a slow HTTP shutdown takes time away from the running jobs.

After the jobs are requeued, `Shutdown` can still spend up to 30 s on database writes. Give your platform's stop timeout enough room for the HTTP shutdown, `ShutdownTimeout`, and those writes. On Kubernetes this is `terminationGracePeriodSeconds`, which is 30 s by default.

## Handle exits that skip shutdown

If the process exits without `Shutdown`, for example through `log.Fatal` in a goroutine or a `SIGKILL`, the server row and the running jobs stay behind. The jobs run again on another worker after `InvisibilityTimeout` (5 min). The watchdog of another server removes the server row after `ServerTimeout` (5 min). See [Processing background jobs](processing-background-jobs.md#crashes-and-at-least-once-delivery).

## Run without the server

To keep the job server out of a web process, remove `server.New`, `Start`, and `Shutdown`. The `sqlserver.Client` still creates jobs, and a separate process runs them. See [Placing processing into another process](placing-processing-into-another-process.md).

## Differences from .NET Hangfire

- .NET ties the server to the host with `UseHangfireServer` or `AddHangfireServer`, and the host disposes it. Go has no host, so you call `Start` and `Shutdown` yourself.
- .NET needs `Microsoft.Owin.Host.SystemWeb` for graceful shutdown under IIS. A Go service gets its stop request as a signal, and `signal.NotifyContext` turns that signal into a cancelled context.

## Related pages

- [Processing jobs in a console application](processing-jobs-in-console-app.md)
- [Processing jobs in a Windows service](processing-jobs-in-windows-service.md)
