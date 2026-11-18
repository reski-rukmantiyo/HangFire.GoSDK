# .NET Hangfire Dashboard

An ASP.NET Core app that hosts the Hangfire Dashboard on the same database the Go SDK uses. Jobs created by the Go producer, Go servers, and .NET servers all appear in one UI.

It follows https://docs.hangfire.io/en/latest/configuration/using-dashboard.html, and uses Hangfire 1.8.25 with the settings the Go SDK expects: `CompatibilityLevel.Version_180`, the simple assembly name type serializer, and the recommended serializer settings.

## Run

Start SQL Server first (`scripts/sqlserver.sh up`), then:

```sh
cd examples/dotnet-dashboard
ASPNETCORE_URLS=http://localhost:5099 dotnet run
```

Open http://localhost:5099/hangfire.

In another terminal, from the repository root, run the Go worker and producer against the same database:

```sh
export HANGFIRE_SQL='Server=localhost,1433;Database=HangfireGoSdk;User Id=sa;Password=Hangfire_Dev_Passw0rd!;TrustServerCertificate=True;Encrypt=False'
go run ./examples/worker &
go run ./examples/producer
```

The Servers page then lists two servers. `dotnet-dashboard:<pid>:<uuid>` is the .NET server on the `dotnet` queue. The Go worker on `goq` has an id of the form `<host>:<pid>:<uuid>`. Go jobs appear under Jobs, and the producer's recurring job appears under Recurring Jobs.

To enqueue a .NET job from the app itself:

```sh
curl -X POST 'http://localhost:5099/enqueue?name=alice'
```

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `HANGFIRE_SQL` | the local test container, database `HangfireGoSdk` | Connection string. Use the same value as the Go side |
| `HANGFIRE_DOTNET_SERVER` | on | Set `0` to host only the Dashboard, with no .NET server |
| `HANGFIRE_DASHBOARD_ALLOW_REMOTE` | off | Set `1` to allow requests from other hosts |
| `ASPNETCORE_URLS` | ASP.NET default | Listen address |

The app installs the schema if it is missing (`PrepareSchemaIfNecessary`). The Go SDK never installs it.

## Security

By default the Dashboard accepts only local requests (`LocalRequestsOnlyAuthorizationFilter`). `HANGFIRE_DASHBOARD_ALLOW_REMOTE=1` turns off all authorization, so anyone who can reach the port can delete, requeue, and trigger jobs. Use it only on a trusted network. For anything else, write an `IDashboardAuthorizationFilter` that checks `context.GetHttpContext().User`, as the Hangfire docs show, or set `IsReadOnlyFunc` for a read-only view.

## Notes

- The Dashboard loads the .NET type each job names, so it can render the call. Go-created jobs here name `E2E.Jobs.IGoJobs, E2E`, so the project references `tools/e2e-dotnet/E2E.csproj`, the assembly that defines that interface. Without that reference, the job page shows `Can not find the target method.` with `Could not resolve assembly 'E2E'.` The job still runs on the Go worker; only the Dashboard view is affected.
- In your own system, put the interfaces your Go jobs name in a small .NET contracts library. Reference it from the Dashboard host and from any .NET producer. A job that exists only in Go still needs a stub interface there: `namespace MyApp.Jobs; public interface IGoJobs { void Hello(string name); }`. The `Type` string the Go producer writes must match that interface's namespace and assembly name.
- Set `HANGFIRE_DOTNET_SERVER=0` if no .NET server should run in this process.
