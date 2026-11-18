# Using batches

Batches are a Hangfire.Pro feature. They group jobs so that the group is created atomically and a continuation can wait for the whole group. The Go SDK does not support batches. It cannot create them, and a Go server does not run the filters that track them. This page shows what the SDK offers instead: continuations for chains of jobs, a pattern for waiting on several jobs, and atomic creation of several jobs through the storage API.

## Why the SDK has no batches

Hangfire.Pro stores batches in its own format, with its own job filters and Dashboard pages. That format is proprietary and not part of the open Hangfire.SqlServer schema that the SDK writes. A Go producer cannot create a batch, and a Go server ignores batch membership if a .NET producer adds one of its jobs to a batch.

## Chain jobs with continuations

A continuation waits in the `Awaiting` state until its parent job finishes, then moves to its next state. `ContinueJobWith` creates one:

```go
parent, err := client.Enqueue(ctx, hangfire.Invocation{
	Type:           "MyApp.Jobs.IReportJobs, MyApp.Contracts",
	Method:         "Export",
	ParameterTypes: []string{"System.Int64"},
	Args:           []any{int64(7)},
}, hangfire.WithQueue("reports"))
if err != nil {
	log.Fatal(err)
}
child, err := client.ContinueJobWith(ctx, parent, hangfire.Invocation{
	Type:           "MyApp.Jobs.IReportJobs, MyApp.Contracts",
	Method:         "Email",
	ParameterTypes: []string{"System.Int64"},
	Args:           []any{int64(7)},
}, nil, hangfire.OnlyOnSucceededState, hangfire.WithQueue("reports"))
if err != nil {
	log.Fatal(err)
}
log.Printf("job %s runs after job %s succeeds", child, parent)
```

The `cont` argument decides when the child runs:

| Option | The child runs when the parent is |
|---|---|
| `hangfire.OnlyOnSucceededState` | `Succeeded` |
| `hangfire.OnlyOnDeletedState` | `Deleted` |
| `hangfire.OnAnyFinishedState` | `Succeeded` or `Deleted` |

If the parent finishes in a state the option does not accept, the child moves to `Deleted` with the reason `Continuation condition was not met`. A `Failed` parent is not finished, because it can still be retried, so its continuations keep waiting.

The `next` argument is the state the child moves to. `nil` means `Enqueued` on the child's queue. Pass a `hangfire.Scheduled` value to delay the child after the parent finishes. If the parent has already finished when you call `ContinueJobWith`, the child moves to its next state at once. `ContinueJobWith` returns an error wrapping `hangfire.ErrParentNotFound` when the parent id does not exist.

A child can be the parent of another continuation, so you can build a chain of any length.

## Wait for several jobs

A continuation has exactly one parent. To run a job after a group of jobs, track the group in your own database. Each job in the group decrements a counter when it succeeds, and the job that brings the counter to zero enqueues the follow-up:

```go
func finishPart(ctx context.Context, db *sql.DB, jobs hangfire.Client, exportID int64) error {
	var remaining int
	err := db.QueryRowContext(ctx,
		"update ExportRuns set Remaining = Remaining - 1 output inserted.Remaining where Id = @p1",
		exportID).Scan(&remaining)
	if err != nil {
		return err
	}
	if remaining > 0 {
		return nil
	}
	_, err = jobs.Enqueue(ctx, hangfire.Invocation{
		Type:           "MyApp.Jobs.IReportJobs, MyApp.Contracts",
		Method:         "Merge",
		ParameterTypes: []string{"System.Int64"},
		Args:           []any{exportID},
	}, hangfire.WithQueue("reports"))
	return err
}
```

Jobs can run more than once, so a retried part must not decrement the counter twice. Record which parts finished, for example in a table keyed by part id, and decrement only on the first insert.

## Create several jobs in one transaction

`hangfire.Client` creates each job in its own transaction. If the third of five calls fails, the first two jobs already exist. To create a group of jobs that all exist or none do, use the storage API: open a `sqlserver.Session`, start a transaction with `InTx`, and create each job with `Session.CreateJob`. Calls inside `InTx` join its transaction.

```go
func enqueueAll(ctx context.Context, storage *sqlserver.Storage, invs []hangfire.Invocation) ([]hangfire.JobID, error) {
	se, err := storage.Session(ctx)
	if err != nil {
		return nil, err
	}
	defer se.Close()

	var ids []hangfire.JobID
	err = se.InTx(ctx, func() error {
		for _, inv := range invs {
			nj, err := storage.EncodeJob(inv, "reports", nil, hangfire.Enqueued{})
			if err != nil {
				return err
			}
			id, _, err := se.CreateJob(ctx, nj)
			if err != nil {
				return err
			}
			ids = append(ids, id)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}
```

If any step returns an error, the transaction rolls back and no job from the group exists. A long group holds its locks until the commit, so keep groups small. This path bypasses `hangfire.Client`, so `hangfiretest.FakeClient` cannot record it.

## Differences from .NET Hangfire

- There is no `BatchJob.StartNew`, `BatchJob.ContinueBatchWith`, `Attach`, or batch cancellation. The Dashboard shows no Batches page for Go-created work.
- `ContinueJobWith` takes the continuation option explicitly. .NET's `BackgroundJob.ContinueJobWith` defaults to `OnlyOnSucceededState`.

## Related pages

- [Calling methods in background](calling-methods-in-background.md)
- [Calling methods with delay](calling-methods-with-delay.md)
