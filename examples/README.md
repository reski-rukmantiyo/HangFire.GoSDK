# Examples

- `producer` enqueues a job, schedules one a minute later, adds an `Add(2, 3)` continuation that runs after the first job succeeds, and registers a recurring job at 02:00 UTC. All use the `goq` queue.
- `worker` runs a Go server that handles `E2E.Jobs.IGoJobs` jobs (`Hello`, `Add`) from `goq`. It also runs the recurring scheduler, so recurring jobs fire without a .NET server.
- `dotnet-dashboard` is an ASP.NET Core app that hosts the Hangfire Dashboard on the same database, so Go jobs and Go servers show up in the UI. See its README.

Both read the ADO connection string from `HANGFIRE_SQL`. The Hangfire schema must already exist. The .NET side installs it, or run `dotnet run --project tools/e2e-dotnet -- install`.

```sh
export HANGFIRE_SQL='Server=localhost,1433;Database=HangfireGoSdk;User Id=sa;Password=Hangfire_Dev_Passw0rd!;TrustServerCertificate=True;Encrypt=False'
go run ./examples/worker &
go run ./examples/producer
```

## Embed the server in an HTTP service

The server starts with the process and stops when the process gets SIGINT or SIGTERM, before the HTTP listener closes.

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

srv, _ := server.New(storage, server.Options{Queues: []string{"goq"}})
srv.Handle("MyApp.Jobs.EmailJob, MyApp", "Send", server.Handler2(sendEmail))
if err := srv.Start(ctx); err != nil {
	log.Fatal(err)
}

httpSrv := &http.Server{Addr: ":8080", Handler: mux}
go httpSrv.ListenAndServe()

<-ctx.Done()
shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
defer cancel()
httpSrv.Shutdown(shutdownCtx)
srv.Shutdown(shutdownCtx)
```

`Shutdown` stops fetching, cancels handler contexts after `StopTimeout` (500 ms), and waits up to `ShutdownTimeout` (15 s). It then requeues jobs still running and removes the server row. A handler that ignores its context keeps running in the background, but its job is already requeued and its outcome is discarded.

## Run under systemd

Give systemd a stop timeout longer than `ShutdownTimeout`, so running jobs get requeued before the process is killed.

```ini
[Unit]
Description=Hangfire Go worker
After=network-online.target

[Service]
ExecStart=/usr/local/bin/worker
Environment=HANGFIRE_SQL=Server=db,1433;Database=Hangfire;User Id=hangfire;Password=...
KillSignal=SIGTERM
TimeoutStopSec=30
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

## Run as a Windows service

Use `golang.org/x/sys/windows/svc`. Map the service stop request to a context cancel and call `Run`:

```go
type service struct{ srv *server.Server }

func (s *service) Execute(_ []string, req <-chan svc.ChangeRequest, status chan<- svc.Status) (bool, uint32) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.srv.Run(ctx) }()
	status <- svc.Status{State: svc.Running, Accepts: svc.AcceptStop | svc.AcceptShutdown}
	for c := range req {
		if c.Cmd == svc.Stop || c.Cmd == svc.Shutdown {
			status <- svc.Status{State: svc.StopPending}
			cancel()
			<-done
			return false, 0
		}
	}
	return false, 0
}

// in main: svc.Run("HangfireGoWorker", &service{srv})
```

## Write idempotent handlers

A job runs at least once. If a worker crashes, its job becomes visible again after `InvisibilityTimeout` (5 minutes) and runs again. Make each handler safe to run twice, for example by keying side effects on `server.FromContext(ctx).JobID`.
