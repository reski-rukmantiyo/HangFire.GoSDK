# Dealing with exceptions

A Go handler fails by returning an error or by panicking. The server never crashes on a failed job. It records the failure in the `Failed` state, as .NET does, and retries the job under a retry policy. This page shows how failures are recorded, how to change the retry policy, and which failures are never retried.

## Return an error

A handler that returns a non-nil error fails its job. The server writes a `Failed` state with these fields, which the Dashboard shows on the job page:

| Field | Value |
|---|---|
| `ExceptionType` | `Go:` and the error's dynamic type, for example `Go:*errors.errorString` or `Go:*fmt.wrapError` |
| `ExceptionMessage` | `err.Error()` |
| `ExceptionDetails` | the type and the message, `Go:*fmt.wrapError: charge invoice 42: card declined` |
| `Reason` | `An exception occurred during performance of the job.` |

The type comes from the outermost error. `fmt.Errorf("...: %w", err)` therefore reports `Go:*fmt.wrapError`, whatever it wraps.

## Choose the exception type

To show a clearer type in the Dashboard, give your error type a `HangfireExceptionType() string` method. The server looks for it through the whole wrap chain with `errors.As`, so wrapping the error keeps the type.

```go
type DeclinedError struct{ Code string }

func (e *DeclinedError) Error() string { return "card declined: " + e.Code }

func (e *DeclinedError) HangfireExceptionType() string { return "MyApp.Billing.CardDeclinedException" }

func charge(ctx context.Context, invoiceID int64) error {
	return fmt.Errorf("charge invoice %d: %w", invoiceID, &DeclinedError{Code: "insufficient_funds"})
}
```

A job that fails with `charge` shows `ExceptionType` `MyApp.Billing.CardDeclinedException` and `ExceptionMessage` `charge invoice 42: card declined: insufficient_funds`.

## Panics

The server recovers a panic in a handler and treats it as a failure. `ExceptionType` is `Go:` and the type of the panic value, for example `Go:string` for `panic("boom")`. If the panic value is an error, the server reads its type as it does for a returned error. `ExceptionDetails` holds `panic:`, the message, and the goroutine's stack trace.

A panic in a goroutine that the handler starts is not recovered. It crashes the process, as any unrecovered panic in Go does.

## Automatic retries

By default a failed job gets 10 retries. This matches .NET's `AutomaticRetryAttribute`. For retry `n`, the server does these steps:

1. Sets the job parameter `RetryCount` to `n`.
2. Moves the job to `Scheduled`, with the reason `Retry attempt n of 10: <message>`. Messages longer than 50 characters are cut to 49 characters and `…`.
3. Waits `round((n-1)^4 + 15 + rand(0..29)*n)` seconds before the job is enqueued again. The first retry runs after 15 to 44 seconds. The tenth runs after 1.8 to 1.9 hours.

The delayed job scheduler moves the job back to its queue when the delay ends. Some server in the fleet must run that scheduler. See [Processing background jobs](processing-background-jobs.md#what-a-server-runs).

When the retries run out, the job stays in `Failed`. Failed jobs never expire. Requeue one from the Dashboard, or with `Client.Requeue`:

```go
ok, err := sqlserver.NewClient(storage).Requeue(ctx, hangfire.JobID("42"))
if err != nil {
	log.Fatal(err)
}
log.Printf("requeued: %v", ok)
```

## Change the retry policy

`server.RetryPolicy` has three fields:

- `Attempts` is the number of retries. Zero disables retries.
- `DelaysInSeconds`, when set, gives the delay before each retry. Retries past the end of the slice reuse its last value. A delay of zero enqueues the job at once instead of scheduling it.
- `OnAttemptsExceeded` says what happens after the last retry. `FailOnAttemptsExceeded`, the default, keeps the job in `Failed`. `DeleteOnAttemptsExceeded` moves it to `Deleted`.

To change the policy for every handler on a server, set `Options.Retry`:

```go
srv, err := server.New(storage, server.Options{
	Retry: &server.RetryPolicy{Attempts: 5},
})
if err != nil {
	log.Fatal(err)
}
```

To change it for one handler, pass `server.WithRetry`. The handler policy replaces the server policy for that handler.

```go
srv.Handle("MyApp.Jobs.WebhookJob, MyApp", "Deliver", server.Handler1(deliver),
	server.WithRetry(server.RetryPolicy{
		Attempts:           3,
		DelaysInSeconds:    []int{10, 60, 600},
		OnAttemptsExceeded: server.DeleteOnAttemptsExceeded,
	}))
```

To turn retries off for one handler, pass a policy with no attempts:

```go
srv.Handle("MyApp.Jobs.ReportJob, MyApp", "Build", server.Handler1(buildReport),
	server.WithRetry(server.RetryPolicy{Attempts: 0}))
```

A nil `Options.Retry` means the default policy, 10 attempts. A zero `RetryPolicy{}` means no retries. Take care not to mix the two up.

Inside a handler, `server.FromContext(ctx).RetryCount` tells you how many retries came before this run. It is 0 on the first run.

## Failures that are not retried the same way

Some jobs fail before their handler runs.

**Arguments that do not decode.** If the job's arguments do not decode into the handler's parameter types, or their number is wrong, the job fails at once with no retry. A retry cannot fix the payload. `ExceptionType` is `Hangfire.Go.ArgumentDecodeException`, and the message names the argument, for example `argument 1: json: cannot unmarshal string into Go value of type int`. In Go code this is a `*server.ArgumentDecodeError`.

**No handler for the job.** If the server has no handler for the job's `Type` and `Method`, or cannot read the job's payload, the job fails with the reason that .NET writes in the same case: `Can not change the state to 'Processing': target method was not found.` `ExceptionType` is `Hangfire.Go.HandlerNotFoundException`. The server's global retry policy still applies, so by default the job is retried 10 times before it stays in `Failed`. Each retry goes back to the same queue, so it fails the same way unless a server with the handler serves that queue. See [Placing processing into another process](placing-processing-into-another-process.md).

## Cancellation is not a failure

Two events cancel a handler's context:

- `Shutdown`. If the handler then returns `ctx.Err()`, or an error that wraps it, the server requeues the job instead of failing it. Return any other error and the job fails.
- A state change from outside. If someone deletes or requeues the job while it runs, the cancellation watcher sees it within `CancellationCheckInterval` (5 s) and cancels the context. The server discards whatever the handler returns, because the job has already moved on.

## Errors in the server itself

The server logs its own errors through `Options.Logger`, a `*slog.Logger` that defaults to `slog.Default()`. A failed fetch, a failed keep-alive, or a failed background process pass is logged and tried again. None of them stops the server. Keep error-level logs visible in production.

## Differences from .NET Hangfire

- .NET sets the retry policy with `[AutomaticRetry]` on the method or with a global filter. In Go, set `Options.Retry` for the server and `WithRetry` for one handler. .NET filter attributes on the C# interface have no effect on Go handlers.
- `ExceptionType` holds a Go type with a `Go:` prefix, not a .NET exception type, unless the error sets its own type.
- .NET has no special case for arguments that do not deserialize. The Go server fails such jobs without retry.

## Related pages

- [Processing background jobs](processing-background-jobs.md)
- [Tracking the progress](tracking-progress.md)
