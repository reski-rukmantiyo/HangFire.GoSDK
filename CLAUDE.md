# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Documentation rule

For documentation, use skill `asd-ste100` for English and skill `asd-ste100-indonesian` for Indonesian. This applies to `README.md`, `docs/`, `examples/**/README.md`, and `tools/**/README.md`.

## What this repository is

A Go SDK for Hangfire 1.8 on SQL Server. The SDK writes the same rows that .NET Hangfire 1.8 writes, so Go and .NET clients, servers, and the .NET Dashboard share one job database. Wire compatibility with .NET is the main constraint on every change.

- The SDK never installs or migrates the schema. The .NET side owns it. `NewStorage` refuses any schema version other than 9.
- Supported compatibility levels: `CompatLevel170`, `CompatLevel180`. Level 110 is out of scope.
- Out of scope: Hangfire.Pro batches, Hangfire.Ace throttling, other storages, a Go dashboard, .NET filter attributes on Go-handled jobs.
- Requirements: `docs/SRS/golang-sdk-srs.md`. User guides: `docs/UserGuides/`.
- Go 1.26.11 or later is required (security fixes). Do not lower the `go` directive in `go.mod`.

## Commands

```sh
go build ./...
go vet ./...
go test ./...                                  # unit tests, no dependencies
go test ./hangfire -run TestName -v            # one test
```

Integration tests skip unless `HANGFIRE_SQL` is set. They need SQL Server and the .NET 9 SDK:

```sh
scripts/sqlserver.sh up
export HANGFIRE_SQL='Server=localhost,1433;Database=HangfireGoSdk;User Id=sa;Password=Hangfire_Dev_Passw0rd!;TrustServerCertificate=True;Encrypt=False'
(cd tools/e2e-dotnet && dotnet build)
E2E_DOTNET=1 go test -race -count=1 ./...
```

- `E2E_DOTNET=1` enables the `e2e` package, which runs a real .NET Hangfire server against the same database.
- `HANGFIRE_KEEP_DB=1` keeps the per-test databases for inspection.
- `internal/sqltest` creates a fresh database for each test and installs the schema through `tools/e2e-dotnet install`. It replaces the `Database=` part of `HANGFIRE_SQL`.

Regenerate golden fixtures from the real Hangfire.Core package (needs .NET):

```sh
tools/golden-dotnet/regen.sh   # writes hangfire/testdata/golden.json
```

## Architecture

Dependency order: `hangfire` <- `sqlserver` <- `server`. `hangfiretest` depends only on `hangfire`.

- `hangfire`: storage-independent model and wire codecs. `Invocation` names a .NET type and method as strings, with parameter types and JSON arguments. `codec.go` and `netjson.go` reproduce .NET serialization (dates as Unix ms after 1978-01-11, Newtonsoft.Json forms). `cron.go` reproduces Hangfire cron and time-zone behavior. `retry.go` holds the `AutomaticRetryAttribute` back-off. `golden_test.go` checks all of these against `testdata/golden.json`, which the real .NET library produced.
- `sqlserver`: the storage. `Storage` holds options and SQL templates with the schema name. `Session` wraps one connection with `InTx`, `Lock` (`sp_getapplock`), and `LockJob`. `statemachine.go` ports the .NET state machine: state rules per state name, election, and continuations. Every state change goes through `ChangeState`. `client.go` (create and manage jobs), `recurring.go` (`RecurringManager`), `monitor.go` (read model), `worker.go` (queue fetch with sliding invisibility timeout), and `processes.go` (scheduler, watchdog, expiration, counters) sit on top.
- `server`: runs Go handlers. `Server.Handle` registers a handler under the exact .NET `Type` and `Method` strings. `Handler0` to `Handler4` decode JSON args. `worker.go` fetches and runs jobs. `outcome.go` maps results to states (error or panic gives `Failed` with `ExceptionType` `Go:<type>`). `server.go` runs the heartbeat and the background processes under the same distributed locks as their .NET counterparts.
- `hangfiretest`: in-memory fake client for users' unit tests.
- `tools/e2e-dotnet`: .NET 9 harness on Hangfire 1.8.25. It installs the schema, runs a `BackgroundJobServer`, enqueues sample jobs, and dumps jobs as JSON.
- `e2e`: cross-runtime tests. A .NET server runs Go-created jobs, Go manages .NET-created jobs, and `TestGoldenRows` compares raw table rows.

## Conventions

- When Go code ports .NET behavior, the comment cites the .NET source file and lines, for example `(EnqueuedState.cs:251-262)`. Keep these citations, and add them for new ported behavior.
- To change wire output, first confirm the .NET behavior: regenerate golden fixtures or run the e2e tests. Do not guess the .NET format.
- Jobs run at least once. A crashed worker's job runs again after `InvisibilityTimeout` (5 min).
