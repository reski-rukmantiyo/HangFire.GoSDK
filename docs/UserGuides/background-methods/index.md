# Background methods

This section shows how to create Hangfire background jobs from Go, and how to run them in Go. You describe a method call as data, the SDK writes it to the Hangfire SQL Server schema, and a server runs it later. That server can be a .NET `BackgroundJobServer` or a Go `server.Server`.

## A job is a row, not a closure

.NET Hangfire turns a lambda such as `() => emails.Send("ops@example.com")` into a job. Go has no expression trees, so you write the same information by hand in a `hangfire.Invocation`:

```go
inv := hangfire.Invocation{
	Type:           "MyApp.Jobs.IEmailJobs, MyApp.Contracts",
	Method:         "Send",
	ParameterTypes: []string{"System.String"},
	Args:           []any{"ops@example.com"},
}
```

The client stores the type name, the method name, the parameter types, and the arguments as JSON in the `[HangFire].Job` table. Nothing else travels with the job. Local variables, open connections, and the request that created the job are gone by the time it runs. The job reads everything it needs from its arguments or from your own database.

`Type` names a .NET type, usually an interface in a small contracts assembly. A .NET server resolves that type and calls the method. A Go server looks up the handler you registered for the exact `Type` and `Method` strings with `server.Server.Handle`. The Dashboard also resolves the type to render the job, so it needs the contracts assembly even for jobs that only Go runs. See [examples/dotnet-dashboard/README.md](../../../examples/dotnet-dashboard/README.md).

## Job parameters carry context

Each job also has named parameters, stored as JSON in `[HangFire].JobParameter`. .NET filters use them for ambient data such as the culture. In Go, add a parameter at creation time with `hangfire.WithParameter`, and read it inside a handler with `server.FromContext(ctx).Parameter`. [Passing arguments](passing-arguments.md) shows both.

## Every job has a state

A job moves through the built-in states: `Scheduled`, `Awaiting`, `Enqueued`, `Processing`, `Succeeded`, `Failed`, and `Deleted`. The Go and .NET servers apply the same transitions, so the Dashboard shows the same history for both. `Succeeded` and `Deleted` jobs expire after 24 hours by default. Set `sqlserver.Options.JobExpiration` to change that for jobs that Go finishes. .NET servers use their own `WithJobExpirationTimeout` setting.

## Pages in this section

The snippets in these pages assume `ctx`, `storage`, and `client` exist, as [Calling methods in background](calling-methods-in-background.md) sets them up.

1. [Calling methods in background](calling-methods-in-background.md). Enqueue a fire-and-forget job and choose who runs it.
2. [Calling methods with delay](calling-methods-with-delay.md). Schedule a job for later.
3. [Performing recurrent tasks](performing-recurrent-tasks.md). Run a job on a cron schedule.
4. [Passing arguments](passing-arguments.md). Encode arguments the way .NET servers read them.
5. [Passing dependencies](passing-dependencies.md). Give Go handlers the services they need.
6. [Using IoC containers](using-ioc-containers.md). Build dependencies once, and create per-job resources.
7. [Using cancellation tokens](using-cancellation-tokens.md). Stop work when the server shuts down or the job is aborted.
8. [Writing unit tests](writing-unit-tests.md). Test producers with `hangfiretest.FakeClient`.
9. [Using batches](using-batches.md). Batches are not supported. This page covers what to use instead.
