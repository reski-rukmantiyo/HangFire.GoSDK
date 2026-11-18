# e2e-dotnet

.NET 9 harness on Hangfire 1.8.25 (SqlServer storage, schema v9). The Go SDK never installs the schema; this tool does.

Connection string comes from `HANGFIRE_SQL`. The default targets `scripts/sqlserver.sh up`, database `HangfireGoSdk`, user `sa`.

Run from this directory with `dotnet run -- <command>`:

- `install` creates the database and schema, then exits.
- `server --queues dotnet,default --seconds N` runs a BackgroundJobServer until N seconds pass or SIGTERM. Jobs live in `E2E.Jobs.Recorder` (`Record`, `RecordInt`, `RecordDate`, `RecordPoco`, `Fail`). Each appends `tag|args` to `E2E_RECORD_FILE` (default `/tmp/e2e-record.txt`).
- `enqueue-samples --queue goq` creates jobs on stub interface `E2E.Jobs.IGoJobs` (`Hello`, `Add`) for a Go worker: one enqueued, one scheduled +5s, one continuation. Prints their ids as JSON. The scheduled job lands in `default` when it fires, because Hangfire's scheduler enqueues with the default queue.
- `dump-job <id>` prints the invocation data, properties and state history as JSON.
- `smoke` enqueues `Recorder.Record(tag)`, runs a server for 10s and checks the record file.
