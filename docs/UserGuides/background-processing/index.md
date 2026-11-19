# Background processing

These pages show how to run Hangfire jobs in a Go process with the `server` package. They follow the structure of the official [Background Processing](https://docs.hangfire.io/en/latest/background-processing/index.html) docs, with each page rewritten for Go and the SDK's real behavior.

A Go server reads the same SQL Server tables as a .NET `BackgroundJobServer`. It runs Go handlers for jobs that Go or .NET code created, and it runs the background processes that keep the job database healthy. Go servers and .NET servers can share one database.

## Pages

- [Processing background jobs](processing-background-jobs.md) explains what a `server.Server` runs, how it starts and stops, and what happens when a process crashes.
- [Processing jobs in a web application](processing-jobs-in-web-app.md) embeds a server in a `net/http` service.
- [Processing jobs in a console application](processing-jobs-in-console-app.md) runs a server with `Run(ctx)` until the process receives a signal.
- [Processing jobs in a Windows service](processing-jobs-in-windows-service.md) runs a server under the Windows service manager or under systemd.
- [Dealing with exceptions](dealing-with-exceptions.md) covers errors, panics, retry policies, and the `Failed` state.
- [Tracking the progress](tracking-progress.md) stores progress in job parameters and reads it.
- [Configuring the degree of parallelism](configuring-degree-of-parallelism.md) sets the number of workers.
- [Placing processing into another process](placing-processing-into-another-process.md) splits producers from servers, and Go servers from .NET servers.
- [Running multiple server instances](running-multiple-server-instances.md) covers server ids, heartbeats, the watchdog, and the locks shared with .NET.
- [Configuring job queues](configuring-job-queues.md) covers queue names and the order in which SQL Server fetches them.
- [Concurrency and rate limiting](throttling.md) explains what the SDK offers in place of Hangfire.Ace throttling.

## Before you start

All pages assume the following setup:

- A SQL Server database with the Hangfire schema, version 9. The .NET side installs it. The SDK never installs or migrates it.
- A SQL Server driver, such as `github.com/microsoft/go-mssqldb`, imported for its side effects.
- A `*sqlserver.Storage` from `sqlserver.NewStorage`, with `Level` set to the compatibility level of the oldest .NET server in the fleet.

The [examples](../../../examples/README.md) directory has a runnable producer, a runnable worker, and a .NET app that hosts the Hangfire Dashboard on the same database.
