# Using IoC containers

.NET Hangfire lets you replace `JobActivator` so that an IoC container builds each job class. The Go SDK has no activator to replace. You wire dependencies in `main`, which is where Go programs build their object graph, and the server calls the handlers you registered. This page shows that wiring, how to create resources per job, and what the SDK does not offer yet.

## Wire everything in main

Build long-lived services once, then pass them to the code that registers handlers. This uses `EmailJobs` and `register` from [Passing dependencies](passing-dependencies.md):

```go
func run(ctx context.Context, storage *sqlserver.Storage, users Users, mailer Mailer) error {
	srv, err := server.New(storage, server.Options{Queues: []string{"emails"}})
	if err != nil {
		return err
	}
	register(srv, &EmailJobs{Users: users, Mailer: mailer})
	return srv.Run(ctx)
}
```

If your program already uses a dependency injection tool such as Wire or Fx, let it produce the `EmailJobs` value and call `register` with it. The server does not care where the value came from. Register every handler before `Start` or `Run`. `Handle` panics after the server has started.

## Create per-job resources inside the handler

A .NET container can give each job its own scope, so scoped services such as a `DbContext` live for one run. In Go, open the resource at the start of the handler and release it with `defer`. A small wrapper keeps that out of each handler:

```go
func inTx[A any](db *sql.DB, f func(context.Context, *sql.Tx, A) error) func(context.Context, A) error {
	return func(ctx context.Context, a A) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := f(ctx, tx, a); err != nil {
			return err
		}
		return tx.Commit()
	}
}

func registerBilling(srv *server.Server, db *sql.DB) {
	srv.Handle("MyApp.Jobs.IBillingJobs, MyApp.Contracts", "Charge",
		server.Handler1(inTx(db, func(ctx context.Context, tx *sql.Tx, invoiceID int64) error {
			_, err := tx.ExecContext(ctx, "update Invoices set Charged = 1 where Id = @p1", invoiceID)
			return err
		})))
}
```

Each run gets its own transaction, and the transaction ends when the handler returns.

## There is no per-job scope hook

The SDK requirements describe an optional `Scope` hook that the server would call before and after every job (FR-W04). It is not implemented. No server option runs code around all handlers. Wrap each handler, as `inTx` does above.

## The request is gone when the job runs

A job runs in a server process long after the HTTP request that created it has ended. Do not capture request-scoped values, such as the request's `context.Context`, the logged-in user, or a per-request database transaction, in a handler or its dependencies. The handler's `ctx` comes from the server, not from any request. If the job needs the user, pass the user id as an argument or a job parameter.

## Differences from .NET Hangfire

- There is no `JobActivator`, `UseActivator`, or container integration package. Handlers are plain Go functions that you register by hand.
- A .NET container resolves a new job object per run. A Go handler and the values it captured are shared by all runs and workers, so they must be safe for concurrent use.
- There is no per-job scope hook. See [There is no per-job scope hook](#there-is-no-per-job-scope-hook).

## Related pages

- [Passing dependencies](passing-dependencies.md)
- [Using cancellation tokens](using-cancellation-tokens.md)
