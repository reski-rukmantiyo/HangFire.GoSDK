# Software Requirements Specification: Hangfire Go SDK

| Item | Value |
|---|---|
| Document | SRS for `hangfire-go`, a Go SDK that produces and processes Hangfire background jobs |
| Status | Draft 0.2 (corrected against the implementation, see Appendix B) |
| Date | 2026-11-18 |
| Reference implementation | Hangfire 1.8.25 (`src/SharedAssemblyInfo.cs:12`), this repository at commit `8bedc725` |
| Reference docs | https://docs.hangfire.io/en/latest/background-methods/index.html and https://docs.hangfire.io/en/latest/background-processing/index.html |
| Standard | Loosely follows ISO/IEC/IEEE 29148:2018 |

All source paths are relative to `src/`. `Core/` means `src/Hangfire.Core/`. `SS/` means `src/Hangfire.SqlServer/`.

---

## 1. Introduction

### 1.1 Purpose

Hangfire is an in-process .NET library with no network API. A .NET client serializes a method call into storage. A .NET server polls that storage and runs the call. The storage schema, and the JSON stored in it, are the only integration contract.

This SRS defines a Go SDK that speaks that contract directly. With it, Go services can:

1. **Produce** jobs that existing .NET Hangfire servers execute: fire-and-forget, delayed, recurring, and continuations.
2. **Process** jobs with Go handlers. A Go worker fetches from Hangfire queues, runs the handler, and records state transitions that the Hangfire Dashboard and .NET servers understand.
3. **Manage and monitor** jobs: delete, requeue, reschedule and trigger them, and read the same data the Dashboard shows.

### 1.2 Scope

In scope:

- Storage: the Hangfire.SqlServer schema, version 9 (`SS/Install.sql:20`). It is the only storage in this repository.
- Every topic under the two reference doc sections. Section 3 maps each page to its requirements.
- Wire compatibility with Hangfire 1.7.x and 1.8.x servers at the `Version_170` and `Version_180` compatibility levels.

Section 9 lists what is out of scope for v1 and why.

### 1.3 Definitions

| Term | Meaning |
|---|---|
| Job | A row in `[HangFire].Job` holding a serialized invocation, plus its parameters and state history |
| Invocation | The type, method, parameter types, arguments, and queue that name what to run |
| Producer | Go code that creates jobs or changes their state |
| Worker | A goroutine that fetches a queued job and runs a Go handler |
| Server | A Go process unit that owns workers and background processes, registered in `[HangFire].Server` |
| Handler | A Go function registered under an invocation key `(Type, Method)` |
| Compatibility level | The .NET `CompatibilityLevel` setting (`Core/GlobalConfiguration.cs:28-35`). It controls payload and date formats |
| Final state | `Succeeded` or `Deleted`. `Failed` is not final |
| Fleet | All .NET and Go clients and servers that share one Hangfire database |

### 1.4 Requirement conventions

- **IDs.** `FR-` is functional, `NFR-` non-functional, `DR-` data and wire format, `CR-` constraint.
- **Priority.** MoSCoW: **M**ust, **S**hould, **C**ould, **W**on't.
- **Phase.** **P1** is the producer MVP, **P2** management and monitoring, **P3** the Go worker.
- **Source of truth.** Every `DR-` cites the C# source line it mirrors. Where this document and the code disagree, the code wins.

---

## 2. Overall description

### 2.1 How Hangfire works for a Go integrator

```
 Go producer ──┐                                   ┌── .NET BackgroundJobServer (runs .NET methods)
               ├──► SQL Server [HangFire] schema ◄─┤
 .NET client ──┘    Job, State, JobParameter,      └── Go server (runs Go handlers)
                    JobQueue, Set, Hash, Counter,
                    Server, ...
```

Clients never talk to servers. They write rows, and servers poll for them. A job created in Go looks exactly like one created in .NET, as long as the bytes match.

The job lifecycle uses the built-in states in `Core/States/*.cs`:

```
             Create
               │
   ┌───────────┼───────────────┐
   ▼           ▼               ▼
Scheduled   Enqueued ◄──── Awaiting (parent finished)
   │           │
   └──(due)───►│ (worker fetch)
               ▼
          Processing ──► Succeeded (final, expires after 1 day)
               │
               ├──► Failed ──(AutomaticRetry)──► Scheduled
               │      (not final, never expires)
               └──► Deleted (final, expires after 1 day)
```

### 2.2 Users

| User | Needs |
|---|---|
| Go service developer | Enqueue work for .NET workers without a .NET sidecar. Write Go background jobs that show up in the same Dashboard |
| Platform operator | Run a mixed .NET and Go fleet on one database without corrupting state. Get a clear failure when a job cannot run |
| Test author | Mock the producer and assert on the job it built, without a database |

### 2.3 Operating environment

- Go 1.22 or later, using `database/sql` with `github.com/microsoft/go-mssqldb`.
- SQL Server 2012 or later, or Azure SQL. A .NET Hangfire process has already installed the schema at version 9.
- Linux, macOS, and Windows.

### 2.4 Constraints and assumptions

- **CR-01 (M).** The SDK must not install or migrate the schema. At startup it reads `[HangFire].Schema.Version` and refuses to run unless the value is 9. The .NET side owns the schema.
- **CR-02 (M).** The caller must set the compatibility level of the oldest .NET server in the fleet, `170` or `180`.
  - `110` is not supported. Readers accept both formats (`Core/Storage/InvocationData.cs:139-181`), but writers must not send 170-only formats to a 110 fleet.
  - Supporting 110 would double the format surface for fleets older than Hangfire 1.7.
- **CR-03 (M).** The schema name is configurable. It defaults to `HangFire` (`SS/Constants.cs:20`).
- **CR-04 (M).** Go handlers and .NET methods cannot run each other. Each queue must be served only by Go servers or only by .NET servers.
  - A .NET server that fetches a job for a Go-only type fails it with `JobLoadException` (`Core/States/BackgroundJobStateChanger.cs:88-110`).
  - A Go server that fetches an unknown key fails it the same way (FR-W12).
- **CR-05 (S).** The getting-started guide recommends `UseSimpleAssemblyNameTypeSerializer()` and `UseRecommendedSerializerSettings()` on the .NET fleet. The SDK must also work without them (DR-03, DR-06).

---

## 3. Coverage map: doc pages to requirements

Each sub-page of both reference sections, with the requirements that cover it.

### 3.1 Background Methods

| Doc page | Covered by | Notes |
|---|---|---|
| Calling Methods in Background | FR-P01 to FR-P05, DR-01 to DR-09 | Two-step create, queue insert |
| Calling Methods with Delay | FR-P10 to FR-P12, FR-W20 | `schedule` set, 15 s poller |
| Performing Recurrent Tasks | FR-P30 to FR-P36, FR-W22 | `recurring-job:{id}` hash and `recurring-jobs` set |
| Passing Arguments | DR-05 to DR-07, FR-W05, NFR-04 | Double-encoded JSON arguments, size guidance |
| Passing Dependencies | FR-W02, FR-W03 | Handler factories replace `JobActivator` |
| Using IoC Containers | FR-W03, FR-W04 | Optional per-job scope hook |
| Using Cancellation Tokens | DR-07, FR-W06, FR-W24 | `context.Context`, 5 s cancellation watcher |
| Writing Unit Tests | FR-P50, FR-P51 | `Client` interface and in-memory fake |
| Using Batches | Section 9 | Hangfire.Pro. Won't |

### 3.2 Background Processing

| Doc page | Covered by | Notes |
|---|---|---|
| Processing Background Jobs | FR-W01, FR-W20 to FR-W27 | Server with workers and background processes |
| Processing Jobs in a Web Application | FR-W30, FR-W31 | Embeddable server, graceful shutdown |
| Processing Jobs in a Console Application | FR-W32 | Blocking `Run(ctx)` |
| Processing Jobs in a Windows Service | FR-W33 | Windows service and systemd guidance |
| Dealing with Exceptions | FR-W10 to FR-W14 | Retry policy, Failed state payload |
| Tracking the Progress | FR-W07 | Job context for app-side progress. Hangfire defines no wire format for it |
| Configuring the Degree of Parallelism | FR-W08 | Default `min(NumCPU*5, 20)` |
| Placing Processing into Another Process | CR-04, FR-P01, FR-W12 | Producer-only processes, unknown-handler behavior |
| Running Multiple Server Instances | FR-W25, FR-W26, FR-W27 | Unique id, heartbeat, watchdog, locks |
| Configuring Job Queues | DR-08, FR-P04, FR-W09 | `^[a-z0-9_-]+$`, alphanumeric order on SQL Server |
| Concurrency & Rate Limiting | Section 9, FR-W40 | Hangfire.Ace only. A Go-local limiter is a Could |

---

## 4. Functional requirements: producer (P1, P2)

### 4.1 Creating jobs

- **FR-P01 (M, P1).** `Enqueue(ctx, inv Invocation, opts ...Option) (JobID, error)` creates a job in the `Enqueued` state and makes it visible to workers. The producing process does not need a server.
- **FR-P02 (M, P1).** Creation runs in the two steps of `Core/Client/CoreBackgroundJobFactory.cs:71-137`. Nobody else knows the new id yet, so no lock is needed (`:105-110`). The SDK may therefore merge both steps into one SQL transaction.
  1. Insert the `Job` row with `ExpireAt = CreatedAt + 30 days`, plus its `JobParameter` rows (`SS/SqlServerConnection.cs:98-126`).
  2. Insert the initial `State` row and set `Job.StateId` and `Job.StateName`. Run the state's index writes (FR-P03). For a non-final state, set `Job.ExpireAt = NULL` (`Core/States/CoreStateMachine.cs:52-58`).
- **FR-P03 (M, P1).** Each initial state has its own index writes:

  | State | Write | Source |
  |---|---|---|
  | Enqueued | `insert into [HangFire].JobQueue (JobId, Queue) values (@jobId, @queue)` with `FetchedAt` NULL | `SS/SqlServerWriteOnlyTransaction.cs:222-235` |
  | Scheduled | Upsert into `[Set]`: key `schedule`, value `{jobId}`, score `EnqueueAt` in Unix seconds | `Core/States/ScheduledState.cs:177-182` |
  | Awaiting | See FR-P20 | `Core/ContinuationsSupportAttribute.cs:118-185` |

- **FR-P04 (M, P1).** The caller chooses the queue on each call. The default is `default` (`Core/States/EnqueuedState.cs:72`). Go cannot see the .NET `[Queue]` attribute, so a Go producer must always pass the queue the .NET method expects. The SDK documentation must say so.
- **FR-P05 (S, P1).** `CreateInState(ctx, inv, state, params map[string]any)` is the general form, matching `IBackgroundJobClientV2.Create(Job, IState, parameters)` (`Core/IBackgroundJobClient.cs:29-51`). FR-P01, FR-P10 and FR-P20 are thin wrappers around it.
- **FR-P06 (S, P1).** Culture capture is optional, and off by default. When on, it writes the `CurrentCulture` parameter. At compat 180 it writes `CurrentUICulture` only when that differs (`Core/CaptureCultureAttribute.cs:68-98`). Omitting both is safe, because the server then leaves the thread culture unchanged.

### 4.2 Delayed jobs

- **FR-P10 (M, P1).** `Schedule(ctx, inv, delay time.Duration)` and `ScheduleAt(ctx, inv, at time.Time)` create a job in the `Scheduled` state with `EnqueueAt = at` and `ScheduledAt = now`.
- **FR-P11 (M, P1).** The `schedule` set member is the plain job id when the payload has no `q`, and `queue:jobId` when it does (`Core/States/ScheduledState.cs:177-193`). Writing a plain id for a job with `q` would leave a stray member after .NET unapplies the state.
- **FR-P12 (M, P1).** The `[Set]` upsert must use the storage's `UseIgnoreDupKeyOption` form: an insert, then an update if `@@ROWCOUNT = 0` (`SS/SqlServerWriteOnlyTransaction.cs:316-322`).
  - The fleet may run with global locks on, either at schema version 5 or lower or through explicit config. In that case, take `sp_getapplock 'HangFire:Set:Lock'` in the same transaction (`:594-611`).
  - With schema 9 and default settings, no lock is taken (`SS/SqlServerStorage.cs:500`).

### 4.3 Continuations

- **FR-P20 (M, P1).** `ContinueJobWith(ctx, parentID, inv, opts ContinuationOptions) (JobID, error)` creates the child in the `Awaiting` state. It must do exactly what `ContinuationsSupportAttribute.AddContinuation` does (`Core/ContinuationsSupportAttribute.cs:118-185`):
  1. Acquire `sp_getapplock 'HangFire:job:{parentId}:state-lock'` as Exclusive, with a Session owner and a 60 s timeout (`:32`, `SS/SqlServerDistributedLock.cs:33-34`).
  2. If the parent row is gone, fail with `ErrParentNotFound`.
  3. Read the parent's `Continuations` parameter. Append `{"JobId":"<child>","Options":<n>}` if it is not already there, then write it back (DR-11).
  4. If the parent is already `Succeeded` or `Deleted`, apply the continuation rules at once (`:156-182`, rules at `:328-345`). The child moves to `NextState`. If the options do not match, it moves to `Deleted` with Reason `Continuation condition was not met`. Record both the `Awaiting` state and the resolved state in the history.
  5. Release the lock.
- **FR-P21 (M, P1).** `ContinuationOptions` values are `OnAnyFinishedState = 0`, `OnlyOnSucceededState = 1`, and `OnlyOnDeletedState = 2` (`Core/JobContinuationOptions.cs:21-25`). The default is `OnlyOnSucceededState`.
- **FR-P22 (S, P1).** `NextState` defaults to `Enqueued` on the child's queue. Callers may pass any enqueued or scheduled state.
- **FR-P23 (M, P1).** The SDK must never skip the lock in step 1. Without it, a parent that finishes between steps 3 and 4 leaves the child `Awaiting` forever.

### 4.4 Recurring jobs

- **FR-P30 (M, P1).** `AddOrUpdateRecurring(ctx, id string, inv Invocation, cron string, opts RecurringOptions)` creates or updates a recurring job. The options are:
  - `TimeZone`, default UTC.
  - `Queue`, default `default`.
  - `Misfire`: `Relaxed=0`, `Strict=1` or `Ignorable=2` (`Core/MisfireHandlingMode.cs:31-44`).
- **FR-P31 (M, P1).** The write runs in one transaction under `sp_getapplock 'HangFire:lock:recurring-job:{id}'` (`Core/RecurringJobExtensions.cs:39`).
  - It writes the hash `recurring-job:{id}` (DR-12).
  - It upserts `{id}` into the `recurring-jobs` set, with score = `NextExecution` in Unix seconds (`:83-93`).
  - The hash alone is not enough. The scheduler only polls the set.
- **FR-P32 (M, P1).** The SDK computes `NextExecution` from the cron expression and time zone, starting at `now - 1s`, as the .NET manager does (`Core/RecurringJobManager.cs:97-155`). With Relaxed misfire handling, a score in the past fires at once (`Core/RecurringJobEntity.cs:150-170`).
- **FR-P33 (M, P1).** Cron parsing must match Cronos (`Core/RecurringJobEntity.cs:285-305`):
  - Five fields is standard cron.
  - Six fields puts seconds first.
  - Macros starting with `@` are allowed.
  - Any other field count is rejected.
  An invalid cron expression fails in Go before anything is written.
- **FR-P34 (M, P1).** `TimeZoneId` must be an id the .NET servers can resolve. IANA ids work on .NET 6 and later on every OS. Windows ids work on Windows. The SDK writes the caller's value unchanged and documents the risk.
- **FR-P35 (M, P1).** `RemoveRecurringIfExists(ctx, id)` deletes the hash and the set member under the same lock (`Core/RecurringJobManager.cs:234-235`). A missing id is not an error.
- **FR-P36 (M, P1).** `TriggerRecurring(ctx, id) (JobID, error)` creates an Enqueued job from the hash's `Job` field.
  - The job gets the parameters `RecurringJobId` (a JSON string) and `Time` (Unix seconds) (`Core/RecurringJobExtensions.cs:118-119`).
  - The call updates `LastJobId` and recomputes `NextExecution` from now (`Core/RecurringJobManager.cs:189`). For a cron schedule the next run is the same, which is why the docs describe the run as unchanged.

### 4.5 Managing existing jobs (P2)

- **FR-P40 (M, P2).** `ChangeState(ctx, id, newState, expectedState string) (bool, error)` mirrors `IBackgroundJobClient.ChangeState` (`Core/IBackgroundJobClient.cs:125`):
  1. Acquire `HangFire:job:{id}:state-lock` (`Core/Storage/StorageConnectionExtensions.cs:34`).
  2. Read `Job.StateName`. If `expectedState` is set and differs, return `false`.
  3. Run the old state's unapply writes (DR-14), insert the new `State`, and run the new state's apply writes. Then call `PersistJob` or `ExpireJob` (DR-15).
  4. If the new state is final, fire continuations (FR-W15).
- **FR-P41 (M, P2).** Three wrappers sit on top of `ChangeState` (`Core/BackgroundJobClientExtensions.cs:686-798`):
  - `Delete(id)` moves the job to `DeletedState`.
  - `Requeue(id)` moves it to `EnqueuedState`.
  - `Reschedule(id, delay|at)` moves it to `ScheduledState`.
- **FR-P42 (S, P2).** `Delete` must work even when the job's invocation cannot be resolved. In .NET, only `DeletedState` ignores load errors (`Core/States/DeletedState.cs:111`). The Go producer never resolves invocations, so this holds by design.

### 4.6 Testability

- **FR-P50 (M, P1).** All producer operations sit behind a Go `Client` interface, the equivalent of `IBackgroundJobClient`. `Invocation` is a plain, comparable struct. Tests can assert on its type, method, args, queue and state.
- **FR-P51 (S, P1).** Ship `hangfiretest.NewFakeClient()`, which records calls in memory.

### 4.7 Monitoring and read model (P2)

- **FR-M01 (M, P2).** `Monitor` exposes the read side of `IMonitoringApi` (`Core/Storage/IMonitoringApi.cs:22-50`) for SQL Server. It uses the same `nolock` queries as `SS/SqlServerMonitoringApi.cs`:
  - `Statistics()` returns counts per state, servers, recurring jobs and retries, plus `stats:succeeded` and `stats:deleted` totals from `Counter` and `AggregatedCounter` (`:447-480`).
  - `JobDetails(id)` returns the job, its parameters, and its state history, newest first (`:364-372`).
  - Paged lists by state cover Enqueued and Fetched per queue, Processing, Scheduled, Awaiting, Succeeded, Failed and Deleted. Ordering matches .NET (`:90-248`): ascending for Processing, Scheduled and Awaiting, descending for Succeeded, Failed and Deleted.
  - `Queues()`, `Servers()`, and `RecurringJobs()`.
  - Daily and hourly time series for succeeded, failed and deleted jobs, read from `AggregatedCounter` (`:501-558`).
- **FR-M02 (S, P2).** Monitoring DTOs decode both date formats (DR-04) and both payload formats (DR-02).
- **FR-M03 (C, P2).** An optional `DashboardClient` drives the existing .NET Dashboard over HTTP (`Core/Dashboard/DashboardRoutes.cs`).
  - **What it can do.** Requeue and delete jobs, trigger and remove recurring jobs, and read `/stats`.
  - **What it cannot do.** Create jobs.
  - **What it needs.** The Dashboard must allow remote access, and the client must handle antiforgery (`src/Hangfire.AspNetCore/Dashboard/AspNetCoreDashboardMiddleware.cs:85-117`).
  It is a convenience adapter, not the integration mode.

---

## 5. Functional requirements: Go worker and server (P3)

### 5.1 Handler registry

- **FR-W01 (M, P3).** `NewServer(storage, ServerOptions)` returns a server that owns workers and background processes. No job is processed without a running server (Processing Background Jobs page).
- **FR-W02 (M, P3).** Handlers register under an invocation key, for example `server.Handle("MyApp.Jobs.EmailJob, MyApp", "Send", handler)`.
  - The key is the `(Type, Method)` pair as it appears in the payload.
  - The `p` parameter types join the key only when overloads are registered.
  - Go has no reflection-based activation, so the registry replaces `JobActivator` (Passing Dependencies page).
- **FR-W03 (M, P3).** Handlers are typed with generics, for example `hangfire.Handler2[string, int](func(ctx context.Context, to string, n int) error { ... })`. The registry decodes each argument (DR-05) into its Go type before calling the handler.
- **FR-W04 (C, P3).** An optional `Scope` hook runs before and after each job, for per-job resources. It matches the per-job scope on the IoC page. A scope must not depend on request-scoped state.
- **FR-W05 (M, P3).** If an argument fails to decode, the job fails with `ExceptionType = "Hangfire.Go.ArgumentDecodeException"` and is not retried. A retry cannot fix a bad payload.

### 5.2 Running a job

- **FR-W06 (M, P3).** The first handler parameter is `context.Context`. It is cancelled when the server shuts down, or when the cancellation watcher sees the job leave `Processing` for this worker (FR-W24). A `CancellationToken` argument is stored as `null` in the payload (DR-07). The binder skips it when binding Go parameters.
- **FR-W07 (S, P3).** The context carries a `JobContext` with the job id, parameters (get and set on `JobParameter`), retry count and server id. Progress reporting is up to the application. The Tracking Progress page defines no wire format.
- **FR-W08 (M, P3).** `WorkerCount` defaults to `min(runtime.NumCPU()*5, 20)`, matching `Core/BackgroundJobServerOptions.cs:29,42`. The docs page gives only `ProcessorCount * 5`. The cap of 20 comes from the source. The value must be positive.
- **FR-W09 (M, P3).** `Queues []string` defaults to `["default"]`, and each name is validated with DR-08. On SQL Server, queues are fetched in alphanumeric order and the slice order is ignored, as in .NET (Configuring Job Queues page). The SDK documents this and does not promise priority by slice order.

### 5.3 Fetch and state flow

For each job, a worker runs these steps, mirroring `Core/Server/Worker.cs`:

1. **Fetch.** Run the sliding-invisibility fetch (DR-16). Only sliding mode is supported. It is the default for 1.8 storages (`SS/SqlServerStorageOptions.cs:38`).
2. **Keep alive.** While the job runs, refresh `FetchedAt` every `SlidingInvisibilityTimeout / 5`, guarded by `FetchedAt = @fetchedAt` (`SS/SqlServerTimeoutJob.cs:61,162`).
3. **Move to Processing.** Under the job lock, run `ChangeState(id, Processing{ServerId, WorkerId, StartedAt})`, expecting `Enqueued` or `Scheduled`. If the job is no longer in an expected state, acknowledge it and skip it. A job with no Go handler is failed instead (FR-W12).
4. **Run the handler**, with a context bound to shutdown and cancellation.
5. **Record the outcome.** Move the job to Succeeded, or to Failed under the retry policy (FR-W10). This `ChangeState` expects `Processing`.
6. **Acknowledge.** Delete the `JobQueue` row `where Queue=@queue and Id=@id and FetchedAt=@fetchedAt` (`SS/SqlServerTimeoutJob.cs:83`). If shutdown comes before step 5, set `FetchedAt = NULL` instead (`:104`).

- **FR-W10 (M, P3).** The retry policy matches `AutomaticRetryAttribute` (`Core/AutomaticRetryAttribute.cs:86-115, 300-360`):
  - **Attempts.** The default is 10, configurable globally and per handler. `Attempts = 0` disables retries.
  - **Delay.** For attempt `n` the delay is `round((n-1)^4 + 15 + rand(0..29) * n)` seconds. An optional fixed `DelaysInSeconds` slice overrides this, and reuses its last value once attempts run past its end.
  - **On retry.** Set the parameter `RetryCount = n`. Move the job to `Scheduled` with Reason `Retry attempt {n} of {Attempts}: {message}`, where a message over 50 chars is cut to 49 plus `…`. If the delay is 0, move it to `Enqueued` instead. Add the id to the `retries` set.
  - **When attempts run out.** With `OnAttemptsExceeded = Fail` (the default), the job stays `Failed`. With `Delete`, it moves to `Deleted`.
  - **Leaving retry.** When a job leaves `Scheduled` or `Failed`, its id is removed from `retries` (`:308-315`).
- **FR-W11 (M, P3).** A handler error or panic becomes a `Failed` state (DR-10).
  - Panics are recovered and reported with their stack.
  - `ExceptionType` is the Go error's dynamic type with a `Go:` prefix, for example `Go:*fmt.wrapError`. Dashboard users can then tell Go failures apart.
  - A handler can override the type by implementing `interface{ HangfireExceptionType() string }`.
- **FR-W12 (M, P3).** A job whose `(Type, Method)` has no Go handler moves to `Failed`. Its Reason is `Can not change the state to 'Processing': target method was not found.`, the same text .NET writes (`Core/States/BackgroundJobStateChanger.cs:88-110`). The job is never silently deleted, and the worker never crashes.
- **FR-W13 (M, P3).** `Failed` jobs do not expire (`PersistJob`). They can be requeued from the Dashboard or with FR-P41.
- **FR-W14 (M, P3).** When a job reaches a final state, the worker applies the core state handlers .NET would run:
  - `Succeeded` adds `stats:succeeded +1` (`Core/States/SucceededState.cs:198`).
  - `Deleted` adds `stats:deleted +1` (`Core/States/DeletedState.cs:164`).
  - Statistics history writes `stats:{succeeded|failed|deleted}:yyyy-MM-dd` with a 1-month expiry, and `:yyyy-MM-dd-HH` with a 1-day expiry (`Core/StatisticsHistoryAttribute.cs:30-61`).
  - `ExpireJob` sets `ExpireAt = now + JobExpirationTimeout` on the job and its parameters and states. The default is 1 day (`Core/JobStorage.cs:32`). It is configurable to match `WithJobExpirationTimeout`.
- **FR-W15 (M, P3).** When a job reaches a final state, the worker runs the equivalent of `ExecuteContinuationsIfExist` (`Core/ContinuationsSupportAttribute.cs:187-245`).
  - It reads the `Continuations` parameter.
  - It moves each child still in `Awaiting` to its `NextState`, or to `Deleted` when the options do not match.
  - Without this, continuations of Go-processed parents never run.

### 5.4 Background processes

The Go server runs these processes alongside its workers. Each takes the same distributed lock as its .NET counterpart, so .NET and Go servers can share one database safely.

| ID | Process | Default interval | Lock | Source |
|---|---|---|---|---|
| FR-W20 (M) | Delayed job scheduler. Moves due `schedule` members to Enqueued, 1000 per batch | 15 s (`SchedulePollingInterval`) | `HangFire:locks:schedulepoller` | `Core/Server/DelayedJobScheduler.cs:73,178,407` |
| FR-W21 (C) | Counters aggregator. Merges `Counter` rows into `AggregatedCounter` | 5 min | storage default | `SS/CountersAggregator.cs:99-128` |
| FR-W22 (S) | Recurring job scheduler. Reads due `recurring-jobs` members, creates each job, computes `NextExecution`, honors `Misfire` | Every minute | `HangFire:recurring-jobs:lock` | `Core/Server/RecurringJobScheduler.cs:216,325-330,419-423` |
| FR-W23 (S) | Expiration manager. Deletes rows past `ExpireAt` in batches | storage default | `HangFire:locks:expirationmanager` | `SS/ExpirationManager.cs:32` |
| FR-W24 (M) | Cancellation watcher. Cancels running handlers whose job left Processing for this server and worker | 5 s (`CancellationCheckInterval`) | none | `Core/Server/ServerJobCancellationWatcher.cs:23` |
| FR-W25 (M) | Server heartbeat. Updates `Server.LastHeartbeat` | 30 s (`HeartbeatInterval`) | none | `Core/Server/BackgroundProcessingServerOptions.cs:25`, `SS/SqlServerConnection.cs:551` |
| FR-W26 (M) | Server watchdog. Deletes servers with no heartbeat within `ServerTimeout` | check every 5 min, timeout 5 min | none | `Core/Server/ServerWatchdog.cs:23-24`, `SS/SqlServerConnection.cs:576` |

- **FR-W27 (M, P3).** Each server registers itself with a `MERGE` into `[HangFire].Server` (`SS/SqlServerConnection.cs:505-525`).
  - The id has the form `{name}:{pid}:{uuid}`.
  - `Data` is `{"WorkerCount":n,"Queues":[...],"StartedAt":"<ISO>"}`, the shape of `SS/Entities/ServerData.cs`.
  - On graceful shutdown, the server removes its row.
- **FR-W28 (S, P3).** Each background process can be turned off on its own, so a mixed fleet can leave scheduling and cleanup to the .NET servers.
  - On by default: FR-W20, FR-W24, FR-W25, FR-W26.
  - Off by default: FR-W21, FR-W22, FR-W23, because .NET servers usually already run them.

### 5.5 Hosting and lifecycle

- **FR-W30 (M, P3).** `server.Start(ctx)` returns immediately. `server.Shutdown(ctx)` runs these steps:
  1. Stop fetching.
  2. Cancel handler contexts after `StopTimeout` (default 500 ms).
  3. Wait up to `ShutdownTimeout` (default 15 s, `Core/Server/BackgroundProcessingServer.cs:43`).
  4. Requeue unfinished jobs by setting `FetchedAt = NULL`.
  5. Remove the server row.
- **FR-W31 (M, P3).** The server can be embedded in an HTTP service. It starts with the process and stops on SIGTERM and SIGINT. This is the Go equivalent of disposal in `UseHangfireServer` (Web Application page).
- **FR-W32 (M, P3).** `server.Run(ctx)` blocks until `ctx` is done, then shuts down gracefully. It is the entry point for console apps.
- **FR-W33 (C, P3).** Provide examples for running under systemd, and as a Windows service with `golang.org/x/sys/windows/svc`.
- **FR-W34 (M, P3).** A crash loses no job. After the sliding invisibility timeout (default 5 min), the queue row can be fetched again and the job reruns. Handlers must therefore be idempotent, and the SDK documentation says so.

### 5.6 Optional extras

- **FR-W40 (C, P3).** A Go-local concurrency limiter per handler, `MaxConcurrency int`. It does not interoperate with Hangfire.Ace throttling, and its name makes that clear.

---

## 6. Data and wire-format requirements

These requirements define the byte-level contract. For every DR below, the SDK must ship golden fixtures captured from a real .NET 1.8 client (NFR-01).

### 6.1 Invocation payload

- **DR-01 (M).** `Job.InvocationData` holds the payload without arguments. `Job.Arguments` holds the argument array (`SS/SqlServerConnection.cs:99-109`). On read, the `Arguments` column takes precedence (`:260-265`).
- **DR-02 (M).** At compat 170 and above, the payload uses the compact format (`Core/Storage/InvocationData.cs:185-197, 432-449`):

  ```json
  {"t":"MyApp.IEmailSender, MyApp","m":"Send","p":["System.String","System.Int32","System.DateTime"],"q":"critical"}
  ```

  - `p` is omitted when there are no parameters.
  - `q` is omitted when no queue is set. Using `q` requires `JobStorageFeatures.JobQueueProperty`, which SQL Server storage 1.8 provides (`Core/Client/CoreBackgroundJobFactory.cs:61-64`).
  - `a` is omitted, because the arguments live in `Job.Arguments`.
- **DR-03 (M).** Type names are opaque strings supplied by the caller, and the SDK writes them unchanged.
  - The SDK recommends the simple form `Namespace.Type, Assembly`. The .NET resolver loads it by partial name on .NET Framework and .NET Core alike (`Core/Common/TypeHelper.cs:73-90, 198-260`).
  - In parameter types, `string`, `object` and primitives have no assembly. Nested types use `+`. Generics use `` `1[[Arg, Asm]] `` (`:104-191`).
  - The SDK provides constants for common BCL types.
- **DR-04 (M).** Dates are serialized by `JobHelper.SerializeDateTime` (`Core/Common/JobHelper.cs:75-97`). At compat 170 and above, a date is Unix milliseconds as a decimal string, for example `"1763460000000"`. Readers accept three forms: milliseconds, seconds (values ≤ 253402300799), and ISO 8601.

### 6.2 Arguments

- **DR-05 (M).** `Job.Arguments` is a JSON array of JSON strings, each the JSON of one argument (`Core/Storage/InvocationData.cs:229-276`). For `("a@b.c", 42, 2026-11-18T10:00:00Z)` it is:

  ```json
  ["\"a@b.c\"","42","\"2026-11-18T10:00:00Z\""]
  ```

  With no arguments it is `[]`. The argument count must equal the parameter count (`Core/Common/Job.cs:218-290`).
- **DR-06 (M).** Argument JSON must match what Newtonsoft.Json on the server expects:
  - Field names map to the .NET property names. The SDK uses each struct's `json` tags as written and never changes case.
  - If the fleet uses `UseRecommendedSerializerSettings()`, polymorphic arguments need `"$type":"Ns.T, Asm"`. The SDK provides a `WithDotNetType(v, "Ns.T, Asm")` helper. By default it adds nothing.
  - `time.Time` serializes as RFC 3339 with `Z`. Newtonsoft parses that for `DateTime` and `DateTimeOffset`.
  - `int64` values beyond 2^53 are safe, because Go writes them as integer literals.
- **DR-07 (M).** A `CancellationToken` or `null` argument is written as the JSON literal `null` inside the array, not the string `"null"` (`Core/Storage/InvocationData.cs:248-266`).
- **DR-08 (M).** Queue names must match `^[a-z0-9_-]+$` (`Core/States/EnqueuedState.cs:251-262`). The SDK rejects any other name before writing.

### 6.3 Job parameters

- **DR-09 (M).** `JobParameter.Value` holds the value as JSON, so strings are quoted, for example `"\"en-US\""` (`Core/Client/CoreBackgroundJobFactory.cs:65-68`). `Name` is at most 40 characters (`SS/Install.sql:123-128`).
- **DR-11 (M).** The `Continuations` parameter value has this format (`Core/ContinuationsSupportAttribute.cs:350-383`). `Options` is omitted when it is 0:

  ```json
  [{"JobId":"123"},{"JobId":"124","Options":1}]
  ```

### 6.4 States

- **DR-10 (M).** `State.Data` is a JSON object of string values. `Reason` is a separate column of at most 100 characters, truncated to 99 on write (`SS/SqlServerWriteOnlyTransaction.cs:183-197`).

  | Name | Data keys | Source |
  |---|---|---|
  | `Enqueued` | `EnqueuedAt`, `Queue` | `Core/States/EnqueuedState.cs:217-223` |
  | `Scheduled` | `EnqueueAt`, `ScheduledAt` | `Core/States/ScheduledState.cs:156-162` |
  | `Awaiting` | `ParentId`, `NextState`, `Options` (`"0"`, `"1"` or `"2"`). `Expiration` is not written at 170 or 180, only read from older rows | `Core/States/AwaitingState.cs:209-217` |
  | `Processing` | `StartedAt`, `ServerId`, `WorkerId` | `Core/States/ProcessingState.cs:132-139` |
  | `Succeeded` | `SucceededAt`, `PerformanceDuration`, `Latency` (ms), optional `Result` (JSON) | `Core/States/SucceededState.cs:163-188` |
  | `Failed` | `FailedAt`, `ExceptionType`, `ExceptionMessage`, `ExceptionDetails`, optional `ServerId` | `Core/States/FailedState.cs:195-208` |
  | `Deleted` | `DeletedAt`, optional `Exception` in compact form `{"e":"...","m":"..."}` | `Core/States/DeletedState.cs:145-155` |

  `NextState` is Newtonsoft JSON with a type discriminator:

  ```json
  {"$type":"Hangfire.States.EnqueuedState, Hangfire.Core","Queue":"default"}
  ```

- **DR-13 (M).** `SetJobState` is two statements in one transaction:

  ```sql
  insert into [HangFire].State (JobId, Name, Reason, CreatedAt, Data) values (@jobId, @name, @reason, @createdAt, @data);
  update [HangFire].Job set StateId = SCOPE_IDENTITY(), StateName = @name where Id = @jobId;
  ```

- **DR-14 (M).** Leaving a state runs its unapply writes:

  | Leaving | Unapply writes |
  |---|---|
  | `Enqueued` | None. The worker acknowledges the queue row |
  | `Scheduled` | Remove the `schedule` member. Remove the `retries` member |
  | `Awaiting` | Remove the `awaiting` member, if present |
  | `Succeeded` | Add `stats:succeeded -1` |
  | `Deleted` | Add `stats:deleted -1` |
  | `Failed` | Remove the `retries` member |
- **DR-15 (M).** Final states call `ExpireJob`, which sets `Job.ExpireAt`. Non-final states call `PersistJob`, which sets `Job.ExpireAt = NULL`. Both touch only the `Job` row; parameters and states are removed by the cascade delete (`SS/SqlServerWriteOnlyTransaction.cs:156-170`).

### 6.5 Recurring jobs

- **DR-12 (M).** A recurring job is the hash `recurring-job:{id}` in `[HangFire].Hash` (`Core/RecurringJobEntity.cs:206-275`):

  | Field | Value |
  |---|---|
  | `Job` | The DR-02 payload, with arguments included in `a`. Recurring jobs have no separate arguments column (`Core/RecurringJobManager.cs:118`) |
  | `Cron` | The cron string |
  | `TimeZoneId` | The time zone id, `UTC` by default |
  | `Queue` | The queue name |
  | `CreatedAt`, `NextExecution`, `LastExecution` | Dates in DR-04 format |
  | `LastJobId` | The id of the last job created |
  | `Misfire` | `"1"` or `"2"`. Omitted for Relaxed |
  | `V` | `"2"`. The scheduler rejects higher values (`Core/Server/RecurringJobScheduler.cs:71`) |
  | `Error`, `RetryAttempt` | Written by the scheduler, never by the producer |

  The `recurring-jobs` set holds the member `{id}`. Its score is `NextExecution` in Unix seconds, or `-1` when the job is disabled.

### 6.6 Queue

- **DR-16 (M).** A Go worker fetches with the exact sliding-invisibility statement (`SS/SqlServerJobQueue.cs:213-221`):

  ```sql
  update top (1) JQ set FetchedAt = GETUTCDATE()
  output INSERTED.Id, INSERTED.JobId, INSERTED.Queue, INSERTED.FetchedAt
  from [HangFire].JobQueue JQ with (forceseek, readpast, updlock, rowlock)
  where Queue in @queues and (FetchedAt is null or FetchedAt < DATEADD(second, @timeoutSs, GETUTCDATE()));
  ```

  `@timeoutSs` is the sliding timeout in seconds, as a negative number. When the storage's `QueuePollInterval` is zero, the poll interval is 200 ms (`:42, 116-123`). A Go producer never updates or deletes `JobQueue` rows.

---

## 7. Non-functional requirements

- **NFR-01 (M). Byte compatibility.** A CI suite runs a .NET 8 Hangfire 1.8 container against SQL Server and proves compatibility both ways:
  - A Go-created job of each kind (enqueue, schedule, continuation, recurring) runs on the .NET server and reaches `Succeeded`.
  - A .NET-created job runs on a Go server and shows correctly in the .NET Dashboard.
  - Golden fixtures compare Go's rows with the rows .NET writes for the same call.
- **NFR-02 (M). Atomicity.** Every producer operation runs as one SQL transaction. A crash mid-call leaves either nothing visible or a fully created job. A half-created job (step 1 done, step 2 not) stays invisible and expires after 30 days, as in .NET.
- **NFR-03 (M). Concurrency safety.** Every lock uses `sp_getapplock` with `@LockOwner='Session'`. Resource names are the same as in .NET, prefixed by the schema name (`SS/SqlServerConnection.cs:67`, `SS/SqlServerDistributedLock.cs:195-199`). Go and .NET servers can therefore run side by side.
- **NFR-04 (S). Payload size.** Above 64 KB of argument JSON, the SDK logs a warning. Above a configurable limit (default 1 MB), it rejects the call. The docs advise passing ids rather than data.
- **NFR-05 (M). Performance.** These targets are to be measured. They are not measured facts.
  - `Enqueue` costs one round trip (a single batch), with p99 under 10 ms against a LAN SQL Server.
  - A Go worker with default options sustains 500 jobs per second with no-op handlers on a 4-core host.
- **NFR-06 (M). Context everywhere.** Every I/O method takes a `context.Context` and honors its cancellation.
- **NFR-07 (M). Observability.** Structured logs go through `log/slog`. Optional OpenTelemetry spans cover create, fetch, perform and state change. Spans carry the job id, queue and type.
- **NFR-08 (M). Errors.** The exported sentinel errors are `ErrParentNotFound`, `ErrInvalidQueue`, `ErrInvalidCron`, `ErrSchemaVersion`, `ErrStateMismatch` and `ErrLockTimeout`. Errors are wrapped with `%w`.
- **NFR-09 (M). Security.**
  - All SQL is parameterized.
  - The schema name is validated against `^[A-Za-z_][A-Za-z0-9_]*$` before it goes into SQL text.
  - Connection strings never appear in logs.
  - `ExceptionDetails` can contain stack traces with file paths, so the SDK offers a redaction hook.
- **NFR-10 (S). Dependencies.** The standard library, `go-mssqldb`, and a cron parser with Cronos semantics. No ORM.
- **NFR-11 (M). Versioning.** The SDK uses semantic versioning. The README states the supported Hangfire range (1.7.x and 1.8.x) and the schema version (9).

---

## 8. Proposed package shape (informative)

```go
package hangfire

type Invocation struct {
    Type           string   // "MyApp.Jobs.EmailJob, MyApp"
    Method         string   // "Send"
    ParameterTypes []string // ["System.String", "System.Int32"]
    Args           []any
}

type Client interface {
    Enqueue(ctx context.Context, inv Invocation, opts ...Option) (JobID, error)
    Schedule(ctx context.Context, inv Invocation, delay time.Duration, opts ...Option) (JobID, error)
    ScheduleAt(ctx context.Context, inv Invocation, at time.Time, opts ...Option) (JobID, error)
    ContinueJobWith(ctx context.Context, parent JobID, inv Invocation, opts ...Option) (JobID, error)
    ChangeState(ctx context.Context, id JobID, s State, expected string) (bool, error)
    Delete(ctx context.Context, id JobID) (bool, error)
    Requeue(ctx context.Context, id JobID) (bool, error)
}

type RecurringManager interface {
    AddOrUpdate(ctx context.Context, id string, inv Invocation, cron string, o RecurringOptions) error
    RemoveIfExists(ctx context.Context, id string) error
    Trigger(ctx context.Context, id string) (JobID, error)
}

type State interface{ isState() } // Enqueued, Scheduled, Awaiting, Processing, Succeeded, Failed, Deleted

// package hangfire/sqlserver:    NewStorage(db *sql.DB, Options) implements the storage contract.
// package hangfire/server:       NewServer(storage, ServerOptions); Handle(type, method, handler); Run, Start, Shutdown.
// package hangfire/monitor:      read model (FR-M01).
// package hangfire/hangfiretest: FakeClient.
```

`State` is a closed sum type. Each concrete state struct carries only its own fields, so an `Enqueued` value cannot hold an `EnqueueAt`. `JobID` is a named string type, so a recurring job id cannot be passed where a job id is expected.

---

## 9. Out of scope, with reasons

| Item | Reason |
|---|---|
| Batches (Using Batches page) | A Hangfire.Pro feature. Its storage format is proprietary and not in this repository |
| Throttling, mutexes, semaphores and rate limiters (Concurrency & Rate Limiting page) | A Hangfire.Ace feature, so the same reason applies. FR-W40 is a Go-local alternative, not interop |
| Redis, PostgreSQL, Mongo and InMemory storages | Not in this repository. A `Storage` interface keeps them possible later |
| The MSMQ queue provider | Windows-only, needs TransactionScope, and keeps queue items outside SQL |
| Compatibility level 110 | Would double the format surface for fleets older than Hangfire 1.7 |
| Legacy (transaction-held) fetch mode | No longer the default since 1.8. Holding a SQL transaction open for a whole job run is a poor fit for Go workers |
| Schema install and migration | Owned by the .NET side (CR-01) |
| A Dashboard UI in Go | The .NET Dashboard already reads everything the SDK writes |
| .NET method filters on Go-handled jobs (`[AutomaticRetry]`, `[DisableConcurrentExecution]`, `[Queue]`) | The attributes live in .NET assemblies. Go handlers get equivalents through handler options (FR-W10, FR-W40) |

---

## 10. Risks and open questions

| # | Risk or question | Mitigation |
|---|---|---|
| R1 | Go does not catch a typo in a type or method name. It shows up later as a Failed job on the .NET side (`Core/Storage/InvocationData.cs:94-120`) | Offer a typed contract registry, `hangfire.Contract`, that teams generate from .NET assemblies. Add golden tests per contract |
| R2 | Newtonsoft's argument JSON for custom types (enums as numbers or strings, `$type`) depends on server settings | DR-06 helpers, plus a fleet-settings option covered by the fixtures |
| R3 | `TimeZoneId` resolution differs between Windows and Linux .NET hosts | Document it in FR-P34. Default to UTC |
| R4 | Hangfire 1.9 or a schema version 10 may change formats | CR-01 refuses unknown schema versions. NFR-01 CI runs against each new Hangfire release |
| R5 | In a mixed fleet, .NET servers that listen on Go-only queues fail those jobs | CR-04. At startup, the Go server logs a warning when a .NET `Server` row lists one of its queues |
| Q1 | Should the recurring scheduler (FR-W22) be on by default for Go-only fleets? | Proposal: turn it on automatically when no .NET server row exists at startup. Needs a decision before P3 |
| Q2 | Is SQL Server the only target, or must Redis follow in v2? | The answer decides whether to invest in the `Storage` interface in P1 |

---

## 11. Delivery phases

| Phase | Contents | Exit criteria |
|---|---|---|
| P1 Producer MVP | FR-P01 to FR-P36, FR-P50, FR-P51, DR-01 to DR-13, NFR-01 to NFR-03 | In CI, a .NET 1.8 server runs Go-created enqueued, scheduled, continuation and recurring jobs |
| P2 Management and monitoring | FR-P40 to FR-P42, FR-M01 to FR-M03, DR-14, DR-15 | Go reads match the .NET `IMonitoringApi` for the same database snapshot |
| P3 Go worker | FR-W01 to FR-W40, DR-16 | A Go server processes .NET-created and Go-created jobs and retries per FR-W10. It survives `kill -9` without losing a job and shows correctly in the .NET Dashboard |

---

## Appendix A. Key defaults

| Setting | Value | Source |
|---|---|---|
| Job expiry before the first state is set | 30 days | `Core/Client/CoreBackgroundJobFactory.cs:71` |
| Finished job expiry | 1 day | `Core/JobStorage.cs:32` |
| Awaiting expiry | 365 days | `Core/States/AwaitingState.cs:40` |
| Retry attempts | 10 | `Core/AutomaticRetryAttribute.cs:86` |
| Retry delay | `(n-1)^4 + 15 + rand(30)*n` seconds | `Core/AutomaticRetryAttribute.cs:88-93` |
| Workers | `min(CPU*5, 20)` | `Core/BackgroundJobServerOptions.cs:29,42` |
| Schedule poll | 15 s | `Core/Server/DelayedJobScheduler.cs:73` |
| Cancellation check | 5 s | `Core/Server/ServerJobCancellationWatcher.cs:23` |
| Heartbeat | 30 s | `Core/Server/BackgroundProcessingServerOptions.cs:25` |
| Server timeout and check interval | 5 min and 5 min | `Core/Server/ServerWatchdog.cs:23-24` |
| Stop and shutdown timeouts | 500 ms and 15 s | `Core/Server/BackgroundProcessingServerOptions.cs:23`, `Core/Server/BackgroundProcessingServer.cs:43` |
| Sliding invisibility timeout | 5 min | `SS/SqlServerStorageOptions.cs:38` |
| Queue poll when unset | 200 ms | `SS/SqlServerJobQueue.cs:42` |
| Counter aggregation | 5 min | `SS/SqlServerStorageOptions.cs:43` |
| Continuation parent lock timeout | 60 s | `Core/ContinuationsSupportAttribute.cs:32` |

---

## Appendix B. Corrections found during implementation

The golden fixtures (generated by Hangfire.Core 1.8.25) and the end-to-end runs against a real .NET server corrected these points. Sections above already reflect them.

| Item | Draft 0.1 said | Source shows |
|---|---|---|
| FR-P11 | Always write a plain job id to `schedule` | `queue:id` when the payload has `q` (`Core/States/ScheduledState.cs:177-193`) |
| FR-P04 | Queue goes into the Enqueued state | `Enqueue(queue, call)` writes `q` in the payload and keeps the state's Queue `default`; `q` wins at enqueue (`Core/BackgroundJobClientExtensions.cs:62-68`, `Core/States/EnqueuedState.cs:281-285`) |
| FR-P32 | A past-due score fires at once | Under every misfire mode, `NextExecution` is the first occurrence strictly after now (`Core/RecurringJobEntity.cs:150-170`) |
| FR-P36 | Trigger leaves `NextExecution` alone | Trigger recomputes it from now (`Core/RecurringJobManager.cs:189`) |
| DR-10 | Awaiting writes `Expiration` | Not written at 170/180 (`Core/States/AwaitingState.cs:209-217`) |
| DR-10 | Deleted `Exception` free-form | Compact `{"e","m"}` object |
| DR-15 | Expire touches parameters and states | Only the `Job` row (`SS/SqlServerWriteOnlyTransaction.cs:156-170`) |
| Awaiting set | Written when storage lacks the AwaitingJobs feature | Written only at level 170 (`Core/States/AwaitingState.cs:226-230`) |
| FR-P21 | Default `OnlyOnSucceededState` | `AwaitingState(parent, next)` defaults to `OnAnyFinishedState` (`Core/States/AwaitingState.cs:68-69`); the Go API takes the option explicitly |
| FR-W14 | Stats history counts the final state | It counts the elected state before continuations replace it (filter Order 30 vs 1000); a retried failure is not counted |
| FR-P33 | Cron "standard" semantics | Cronos: day-of-month AND day-of-week must both match; reversed ranges (`FRI-MON`) and `?` in any field are accepted |
| Test environment | SQL Server 2022 | Azure SQL Edge on arm64 (2022 amd64 does not start without Rosetta in the VM) |
