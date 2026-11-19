# Calling methods with delay

A delayed job waits in the `Scheduled` state until its time comes, then a scheduler moves it to `Enqueued` and a worker runs it. This page shows how to schedule a job from Go and what has to run for it to fire on time.

## Schedule a job after a delay

`Schedule` takes a `time.Duration`. This example sends a reminder one day after sign-up:

```go
id, err := client.Schedule(ctx, hangfire.Invocation{
	Type:           "MyApp.Jobs.IEmailJobs, MyApp.Contracts",
	Method:         "SendReminder",
	ParameterTypes: []string{"System.Int64"},
	Args:           []any{int64(42)},
}, 24*time.Hour, hangfire.WithQueue("emails"))
if err != nil {
	log.Fatal(err)
}
log.Printf("job %s runs in a day", id)
```

The job gets a `Scheduled` state whose `EnqueueAt` is the producer's clock plus the delay, and an entry in the `schedule` set.

## Schedule a job at a point in time

`ScheduleAt` takes a `time.Time`. Any location works, because the SDK stores the instant:

```go
at := time.Date(2026, time.December, 1, 9, 0, 0, 0, time.UTC)
id, err := client.ScheduleAt(ctx, inv, at)
if err != nil {
	log.Fatal(err)
}
log.Printf("job %s runs at %s", id, at)
```

## Keep a scheduler running

Nothing moves a scheduled job to its queue except a delayed job scheduler. Every .NET `BackgroundJobServer` runs one. Every Go `server.Server` runs one too, unless you set `DisableDelayedScheduler`. Both take the same distributed lock, so a mixed fleet runs one pass at a time.

The scheduler checks for due jobs every 15 seconds, so a job can start up to that long after `EnqueueAt`. Change the interval on a Go server with `SchedulePollingInterval`:

```go
srv, err := server.New(storage, server.Options{
	Queues:                  []string{"emails"},
	SchedulePollingInterval: 5 * time.Second,
})
if err != nil {
	log.Fatal(err)
}
log.Printf("server %s polls the schedule every 5s", srv.ID())
```

The scheduler compares `EnqueueAt` with the database clock, not with the server's clock. A producer whose clock is ahead of the database still schedules the job later than it intended by that skew.

## Change a scheduled job

Move a job to another time with `Reschedule`, or run it now with `Requeue`. Both return `false` when the job does not exist:

```go
ok, err := client.Reschedule(ctx, id, time.Now().Add(2*time.Hour))
if err != nil {
	log.Fatal(err)
}
if !ok {
	log.Printf("job %s not found", id)
}
if _, err := client.Requeue(ctx, id); err != nil {
	log.Fatal(err)
}
```

## Differences from .NET Hangfire

- `BackgroundJob.Schedule(() => ..., TimeSpan.FromDays(1))` becomes `client.Schedule(ctx, inv, 24*time.Hour)`. The `DateTimeOffset` overload becomes `ScheduleAt`.
- A Go process has no application pool idle timeout. The process that runs the scheduler only has to continue to run, as any long-running Go service does.

## Related pages

- [Calling methods in background](calling-methods-in-background.md)
- [Performing recurrent tasks](performing-recurrent-tasks.md)
