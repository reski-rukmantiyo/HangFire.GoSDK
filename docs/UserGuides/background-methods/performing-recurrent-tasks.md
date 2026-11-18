# Performing recurrent tasks

A recurring job is a template plus a cron schedule. At each occurrence a scheduler creates an ordinary fire-and-forget job from the template, so every run has its own id, state history, and retries. This page shows how to register, change, remove, and trigger recurring jobs from Go, and what has to run for them to fire.

## Register a recurring job

`sqlserver.RecurringManager.AddOrUpdate` creates the job, or updates it when the id already exists:

```go
recurring := sqlserver.NewRecurringManager(storage)
err := recurring.AddOrUpdate(ctx, "nightly-report", hangfire.Invocation{
	Type:   "MyApp.Jobs.IReportJobs, MyApp.Contracts",
	Method: "BuildNightly",
}, "0 2 * * *", hangfire.RecurringOptions{
	TimeZone: "Europe/Berlin",
	Queue:    "reports",
})
if err != nil {
	log.Fatal(err)
}
```

The job runs at 02:00 Berlin time. Calling `AddOrUpdate` again with the same id replaces the template, the schedule, and the options. It recomputes the next run only when the cron expression or the time zone changed.

The id names the job everywhere: in the Dashboard, in `RemoveIfExists`, and in `Trigger`. Two calls with the same id give you one job, not two. Whether `Nightly-Report` and `nightly-report` are the same id depends on the database collation. SQL Server's default collation is case-insensitive.

## Write the cron expression

The SDK parses cron expressions with the rules of Cronos, the library .NET Hangfire uses. `AddOrUpdate` rejects an expression that Cronos would reject, with `hangfire.ErrInvalidCron`.

- Five fields are minute, hour, day of month, month, and day of week.
- Six fields add seconds in front.
- Macros are `@every_second`, `@every_minute`, `@hourly`, `@daily`, `@midnight`, `@weekly`, `@monthly`, `@yearly`, and `@annually`.
- When both day of month and day of week are set, a day must match both. `0 9 13 * FRI` fires only on Friday the 13th. Standard Unix cron would fire on every 13th and every Friday.

The table translates the .NET `Cron` helpers:

| .NET | Go cron string |
|---|---|
| `Cron.Minutely()` | `* * * * *` |
| `Cron.Hourly()` | `0 * * * *` |
| `Cron.Daily()` | `0 0 * * *` |
| `Cron.Weekly()` | `0 0 * * 1` |
| `Cron.Monthly()` | `0 0 1 * *` |
| `Cron.Yearly()` | `0 0 1 1 *` |

To check an expression before you register it, parse it and print the next runs:

```go
sched, err := hangfire.ParseCron("0 9 13 * FRI", "UTC")
if err != nil {
	log.Fatal(err)
}
t := time.Now()
for range 3 {
	next, ok := sched.Next(t)
	if !ok {
		break
	}
	log.Print(next)
	t = next
}
```

## Pick the time zone

`RecurringOptions.TimeZone` is stored as you write it, and every server that schedules the job resolves it. An empty value means `UTC`. The Go scheduler accepts IANA ids such as `Europe/Berlin` and the common Windows ids such as `W. Europe Standard Time`. Use an id that your .NET servers can also resolve on their operating system.

## Choose what happens after downtime

`RecurringOptions.Misfire` decides what the scheduler does with occurrences it missed while no scheduler was running:

| Value | Behavior |
|---|---|
| `hangfire.MisfireRelaxed` (default) | Runs once for all missed occurrences. |
| `hangfire.MisfireStrict` | Runs once for each missed occurrence. |
| `hangfire.MisfireIgnorable` | Skips missed occurrences, except those within twice the polling interval. |

## Run a recurring scheduler

A recurring job fires only while some server runs a recurring job scheduler. Every .NET `BackgroundJobServer` runs one. A Go server runs one only when you set `EnableRecurringScheduler`:

```go
srv, err := server.New(storage, server.Options{
	Queues:                   []string{"reports"},
	EnableRecurringScheduler: true,
})
if err != nil {
	log.Fatal(err)
}
```

The Go and .NET schedulers take the same distributed lock, so running both is safe. The scheduler checks every `SchedulePollingInterval`, 15 seconds by default. Whichever server runs the scheduler, the created job still goes to `Queue`, and only a server that listens on that queue runs it.

## Remove a recurring job

`RemoveIfExists` deletes the template and its schedule. It returns no error when the id does not exist. Jobs already created from the template are not touched.

```go
if err := recurring.RemoveIfExists(ctx, "nightly-report"); err != nil {
	log.Fatal(err)
}
```

## Trigger a recurring job now

`Trigger` creates a job from the template immediately and returns its id. It returns an empty id when the recurring job does not exist:

```go
id, err := recurring.Trigger(ctx, "nightly-report")
if err != nil {
	log.Fatal(err)
}
if id == "" {
	log.Print("no recurring job named nightly-report")
}
```

`Trigger` also sets the next run to the first occurrence after now, as Hangfire 1.8 does. A weekly Wednesday job triggered on Friday still runs next Wednesday. If a due occurrence was waiting for the scheduler, the trigger replaces it.

## Differences from .NET Hangfire

- `RecurringJob.AddOrUpdate("id", () => ..., Cron.Daily)` becomes `NewRecurringManager(storage).AddOrUpdate(ctx, "id", inv, "0 0 * * *", opts)`. There are no `Cron` helper methods.
- Go servers do not run the recurring scheduler by default, because .NET servers usually do. Turn it on with `EnableRecurringScheduler` when no .NET server shares the database.
- The time zone is a string id, not a `TimeZoneInfo`.

## Related pages

- [Calling methods with delay](calling-methods-with-delay.md)
- [Writing unit tests](writing-unit-tests.md) shows how to fake `hangfire.RecurringManager`.
