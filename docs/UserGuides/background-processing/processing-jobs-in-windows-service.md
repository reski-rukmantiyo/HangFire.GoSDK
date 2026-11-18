# Processing jobs in a Windows service

You can run a Go Hangfire worker as a Windows service, so it starts with the machine and the service manager restarts it. This page shows a service built on `golang.org/x/sys/windows/svc`, and the same worker under systemd on Linux.

## Write the service

Add the Windows service package to your module:

```sh
go get golang.org/x/sys/windows/svc
```

The service manager talks to the program through `svc.Handler`. The program below maps the stop request to a context cancel. `Run` then shuts the server down gracefully. When the program runs from a console instead of the service manager, it stops on Ctrl+C.

```go
//go:build windows

package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"

	_ "github.com/microsoft/go-mssqldb"
	"golang.org/x/sys/windows/svc"

	"hangfire-gosdk/hangfire"
	"hangfire-gosdk/server"
	"hangfire-gosdk/sqlserver"
)

const serviceName = "HangfireGoWorker"

type service struct{ srv *server.Server }

func (s *service) Execute(_ []string, req <-chan svc.ChangeRequest, status chan<- svc.Status) (bool, uint32) {
	status <- svc.Status{State: svc.StartPending}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.srv.Run(ctx) }()
	status <- svc.Status{State: svc.Running, Accepts: svc.AcceptStop | svc.AcceptShutdown}

	for {
		select {
		case err := <-done:
			if err != nil {
				log.Print(err)
				return true, 1
			}
			return false, 0
		case c := <-req:
			switch c.Cmd {
			case svc.Interrogate:
				status <- c.CurrentStatus
			case svc.Stop, svc.Shutdown:
				status <- svc.Status{State: svc.StopPending, WaitHint: 20000}
				cancel()
				if err := <-done; err != nil {
					log.Print(err)
				}
				return false, 0
			}
		}
	}
}

func main() {
	db, err := sql.Open("sqlserver", os.Getenv("HANGFIRE_SQL"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	storage, err := sqlserver.NewStorage(context.Background(), db, sqlserver.Options{Level: hangfire.CompatLevel180})
	if err != nil {
		log.Fatal(err)
	}
	srv, err := server.New(storage, server.Options{Queues: []string{"default"}})
	if err != nil {
		log.Fatal(err)
	}
	srv.Handle("MyApp.Jobs.ReportJob, MyApp", "Build", server.Handler1(func(ctx context.Context, month string) error {
		return nil
	}))

	isService, err := svc.IsWindowsService()
	if err != nil {
		log.Fatal(err)
	}
	if isService {
		if err := svc.Run(serviceName, &service{srv: srv}); err != nil {
			log.Fatal(err)
		}
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := srv.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

`WaitHint` tells the service manager how long the stop may take, in milliseconds. Keep it above `ShutdownTimeout` (15 s), so the manager does not give up while jobs are being requeued.

A service has no console. Send logs to a file, or to the Windows event log with `golang.org/x/sys/windows/svc/eventlog`. Pass your logger to the server in `server.Options.Logger`.

## Install and start the service

Build the program for Windows, then register it with `sc.exe` from an elevated prompt:

```powershell
go build -o C:\hangfire\worker.exe .
sc.exe create HangfireGoWorker binPath= "C:\hangfire\worker.exe" start= auto
```

A service does not inherit your shell's environment. To give the worker its connection string, add a multi-string value named `Environment` under the service's registry key:

```powershell
New-ItemProperty -Path HKLM:\SYSTEM\CurrentControlSet\Services\HangfireGoWorker `
  -Name Environment -PropertyType MultiString `
  -Value 'HANGFIRE_SQL=Server=db,1433;Database=Hangfire;Integrated Security=true'
sc.exe start HangfireGoWorker
```

If the service does not start, look in **Event Viewer** under **Windows Logs** > **System** for errors from the Service Control Manager.

## Run under systemd

On Linux, systemd sends SIGTERM to stop a service. The console worker from [Processing jobs in a console application](processing-jobs-in-console-app.md) already stops on SIGTERM, so it needs no service code. Write a unit file such as `/etc/systemd/system/hangfire-worker.service`:

```ini
[Unit]
Description=Hangfire Go worker
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/hangfire-worker
EnvironmentFile=/etc/hangfire-worker.env
KillSignal=SIGTERM
TimeoutStopSec=60
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Put `HANGFIRE_SQL=...` in `/etc/hangfire-worker.env`, then start the service:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now hangfire-worker
journalctl -u hangfire-worker -f
```

Keep `TimeoutStopSec` above `ShutdownTimeout` plus a margin for the final database writes. If systemd sends SIGKILL first, the running jobs are not requeued. They run again after `InvisibilityTimeout` (5 min) instead.

## Differences from .NET Hangfire

- .NET services create a `BackgroundJobServer` in `OnStart` and dispose it in `OnStop`. The Go service runs `Run(ctx)` in a goroutine and cancels `ctx` on the stop request.
- .NET installs services with `installutil`. A Go service is a plain executable that you register with `sc.exe`.

## Related pages

- [Processing background jobs](processing-background-jobs.md)
- [Processing jobs in a web application](processing-jobs-in-web-app.md)
