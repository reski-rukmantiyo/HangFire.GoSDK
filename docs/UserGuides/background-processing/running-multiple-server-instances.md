# Running multiple server instances

You can run many servers against one database, in one process or in many processes on many machines. Go servers and .NET servers can share the database. This page explains how servers distinguish each other and how the watchdog deletes dead servers. It also explains which locks prevent servers from doing the same work twice.

## Server ids

Each server registers itself in the `Server` table under an id of the form `name:pid:uuid`. `srv.ID()` returns it.

- `name` is `Options.Name` in lower case. If `Name` is empty, the server uses the `COMPUTERNAME` or `HOSTNAME` environment variable, or else the host name.
- `pid` is the process id.
- `uuid` is a new random UUID for each server.

The UUID makes every id unique, even for two servers in one process. You do not need to set `Name` for that. Set it to make the Dashboard's **Servers** page easier to read:

```go
reports, err := server.New(storage, server.Options{Name: "reports", Queues: []string{"reports"}})
if err != nil {
	log.Fatal(err)
}
mail, err := server.New(storage, server.Options{Name: "mail", Queues: []string{"emails"}})
if err != nil {
	log.Fatal(err)
}
log.Print(reports.ID(), " ", mail.ID())
```

The server row also holds the worker count, the queues, and the start time. The Dashboard shows them.

## Heartbeat and watchdog

A running server updates its row's `LastHeartbeat` every `HeartbeatInterval` (30 s). On `Shutdown`, it deletes its row.

A server that dies without `Shutdown` does not delete its row. The server watchdog deletes such rows. Every `ServerCheckInterval` (5 min), it deletes each row whose last heartbeat is older than `ServerTimeout` (5 min). The watchdog of any server can delete the row of any other server, Go or .NET.

A watchdog can delete the row of a server that is still alive, for example after a long network outage. Then the next heartbeat of that server finds no row. The server logs `hangfire: server was considered dead by other servers; announcing again` and registers itself again under the same id.

Keep `ServerTimeout` well above `HeartbeatInterval` on every server in the fleet. A Go server with a short `ServerTimeout` deletes the rows of .NET servers whose heartbeat interval is longer. Leave both at their defaults unless you change them on every server.

When the watchdog deletes a dead server's row, it does not move the jobs of that server. The jobs become visible again after `InvisibilityTimeout`, as [Processing background jobs](processing-background-jobs.md#crashes-and-at-least-once-delivery) explains.

## How servers avoid duplicate work

**Fetching.** The fetch query claims one queue row with `UPDATE TOP (1)` and the `READPAST`, `UPDLOCK`, and `ROWLOCK` hints. Two workers never claim the same row while it is invisible, whether they run in Go or .NET.

**State changes.** Every state change takes the job's state lock, `HangFire:job:<id>:state-lock`, and checks the current state first. If one worker moved a job to `Processing`, a worker that fetched an older copy does not move the job again.

**Background processes.** The schedulers and the expiration manager take the same SQL Server application locks as their .NET counterparts:

| Process | Lock |
|---|---|
| Delayed job scheduler | `HangFire:locks:schedulepoller` |
| Recurring job scheduler | `HangFire:recurring-jobs:lock`, then `HangFire:lock:recurring-job:<id>` for each job |
| Expiration manager | `HangFire:locks:expirationmanager` |

The counters aggregator takes no lock. It skips rows another aggregator holds. The heartbeat, the watchdog, and the cancellation watcher need no lock.

Because of these locks, you can run every background process on every server. The extra servers wait for the lock and skip the pass. If you disable a process on some servers, you only save those waits.

## Differences from .NET Hangfire

- .NET 1.8 server ids have the same `name:pid:uuid` form. The .NET docs page about naming servers in one process is obsolete for both.
- In Go, the recurring scheduler, the expiration manager, and the counters aggregator are off by default. In a fleet with no .NET server, enable them in at least one Go server.

## Related pages

- [Processing background jobs](processing-background-jobs.md)
- [Configuring the degree of parallelism](configuring-degree-of-parallelism.md)
