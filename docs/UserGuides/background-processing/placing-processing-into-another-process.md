# Placing processing into another process

You can create jobs in one process and run them in another. Do this when jobs use resources the main service needs, or when deploys of the main service repeatedly interrupt long jobs. In a mixed fleet the same idea decides which jobs Go runs and which jobs .NET runs. This page shows both splits.

## Create jobs without running them

A process that only creates jobs needs a storage and a client. It does not call `server.New`, so it runs no workers and no background processes.

```go
client := sqlserver.NewClient(storage)
id, err := client.Enqueue(ctx, hangfire.Invocation{
	Type:           "MyApp.Jobs.ReportJob, MyApp",
	Method:         "Build",
	ParameterTypes: []string{"System.String"},
	Args:           []any{"2026-10"},
}, hangfire.WithQueue("reports"))
if err != nil {
	log.Fatal(err)
}
log.Printf("enqueued job %s", id)
```

Run the jobs in a worker process. See [Processing jobs in a console application](processing-jobs-in-console-app.md) and [Processing jobs in a Windows service](processing-jobs-in-windows-service.md).

## Agree on the job names

In .NET, the client and the server share one code base, so both know the job's class. In Go, the producer and the handler agree on two strings:

- The producer writes `Invocation.Type` and `Invocation.Method`.
- The server finds the handler that `Handle(typeName, method, ...)` registered, by exact string match.

If the strings differ by one character, the server finds no handler and the job fails with `Hangfire.Go.HandlerNotFoundException`. Define the names once, in a package that both sides import:

```go
package jobs

const (
	ReportType   = "MyApp.Jobs.ReportJob, MyApp"
	ReportBuild  = "Build"
	ReportsQueue = "reports"
)
```

The producer then writes `Type: jobs.ReportType, Method: jobs.ReportBuild`, and the worker registers `srv.Handle(jobs.ReportType, jobs.ReportBuild, ...)`.

## Split queues between Go and .NET

A Go handler cannot run a .NET method, and a .NET server cannot run a Go handler. A server fails every job it fetches that it has no code for:

- A Go server fails a .NET job with `Hangfire.Go.HandlerNotFoundException`.
- A .NET server fails a Go job because it cannot load the type or the method.

Therefore, do not let Go servers and .NET servers serve the same queue. Give Go jobs their own queues, such as `go-default` and `go-reports`. List only those queues in the Go servers' `Options.Queues`. Do not list them in any .NET server's `Queues`.

```go
srv, err := server.New(storage, server.Options{
	Queues: []string{"go-default", "go-reports"},
})
if err != nil {
	log.Fatal(err)
}
```

The SDK does not check this rule for you. Before you deploy, compare the queues on the Dashboard's **Servers** page, or read them with `sqlserver.NewMonitor(storage).Servers(ctx)`.

## Show Go jobs in the Dashboard

The .NET Dashboard loads the type each job names to render the job page. For a job that only Go runs, the type exists only as a string, so the job page shows `Can not find the target method.` The job still runs.

To fix the job page, put an interface for each Go job in a small .NET contracts library. Reference that library from the Dashboard host and from any .NET producer.

```csharp
namespace MyApp.Jobs;

public interface IReportJob
{
    void Build(string month);
}
```

The namespace, the interface name, and the assembly name must match the `Type` string. For the interface above in an assembly named `MyApp`, the Go side writes `MyApp.Jobs.IReportJob, MyApp`. The [`examples/dotnet-dashboard`](../../../examples/dotnet-dashboard/README.md) app is a working Dashboard host that references such a library.

## Differences from .NET Hangfire

- .NET requires the client and the server to share one code base, or at least shared interfaces. In Go, they share only the `Type` and `Method` strings, so a typo fails at run time.
- .NET lets a .NET server run any job in its code base. In a mixed fleet, the queue decides whether a Go server or a .NET server runs a job.

## Related pages

- [Configuring job queues](configuring-job-queues.md)
- [Dealing with exceptions](dealing-with-exceptions.md#failures-that-are-not-retried-the-same-way)
