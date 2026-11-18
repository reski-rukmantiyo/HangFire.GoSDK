# Passing arguments

Jobs get their input from arguments. The SDK serializes each argument to JSON and stores the list with the job, because the job runs in another process, maybe in another language. This page shows how to write arguments that .NET servers can read, how Go handlers receive them, and what to keep out of them.

## Match arguments to parameter types

`Args` holds one value per entry in `ParameterTypes`, in the same order. A count mismatch is an error before anything is written:

```go
inv := hangfire.Invocation{
	Type:           "MyApp.Jobs.IGreeter, MyApp.Contracts",
	Method:         "Greet",
	ParameterTypes: []string{"System.String", "System.Int32", "System.DateTime"},
	Args:           []any{"world", 3, time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)},
}
id, err := client.Enqueue(ctx, inv)
if err != nil {
	log.Fatal(err)
}
log.Printf("enqueued job %s", id)
```

The SDK stores these arguments as `["\"world\"","3","\"2026-11-18T10:00:00Z\""]`, which is the format .NET Hangfire writes. Each item is the JSON text of one argument.

Use these Go types for common .NET parameter types:

| .NET type | Go value |
|---|---|
| `System.String` | `string` |
| `System.Int32`, `System.Int64` | `int`, `int32`, `int64` |
| `System.Boolean` | `bool` |
| `System.Double`, `System.Decimal` | `float64`, or `json.Number` to keep every digit |
| `System.DateTime`, `System.DateTimeOffset` | `time.Time` in UTC |
| `System.Guid` | the canonical string form |
| an enum | its number, or its name if the fleet uses `StringEnumConverter` |
| `System.Threading.CancellationToken` | `nil` |
| any class or struct | a Go struct |

A `nil` argument is stored as JSON `null`.

## Pass structs

A Go struct becomes a JSON object. Name the fields as the .NET properties are named, with `json` tags or with exported Go field names. The SDK never changes their case:

```go
type Order struct {
	ID       int64    `json:"Id"`
	Customer string   `json:"Customer"`
	Lines    []string `json:"Lines"`
}

func enqueueOrder(ctx context.Context, client hangfire.Client, o Order) (hangfire.JobID, error) {
	return client.Enqueue(ctx, hangfire.Invocation{
		Type:           "MyApp.Jobs.IOrderJobs, MyApp.Contracts",
		Method:         "Ship",
		ParameterTypes: []string{"MyApp.Contracts.Order, MyApp.Contracts"},
		Args:           []any{o},
	})
}
```

If the .NET parameter is a base class or an interface, and the fleet uses `UseRecommendedSerializerSettings()`, Newtonsoft.Json needs a `$type` member to pick the concrete class. Wrap the value with `hangfire.WithDotNetType`:

```go
arg := hangfire.WithDotNetType(Order{ID: 7}, "MyApp.Contracts.Order, MyApp.Contracts")
args := []any{arg}
```

The value must encode as a JSON object. Without that fleet setting, leave the wrapper out.

## Receive arguments in a Go handler

`server.Handler1` to `server.Handler4` decode each argument into the function's parameter type with `encoding/json`:

```go
srv.Handle("MyApp.Jobs.IOrderJobs, MyApp.Contracts", "Ship",
	server.Handler1(func(ctx context.Context, o Order) error {
		log.Printf("shipping order %d for %s", o.ID, o.Customer)
		return nil
	}))
```

`CancellationToken`, `IJobCancellationToken`, and `PerformContext` parameters are dropped before decoding, so the handler does not declare them. A handler takes at most four job arguments. For a method with more parameters, change the contract to take one class instead.

If an argument does not decode, the job fails with the exception type `Hangfire.Go.ArgumentDecodeException`, and it is not retried, because a retry gets the same bytes.

## Use job parameters for context

Job parameters carry data that is not part of the call, such as a tenant or a trace id. Add them when you create the job:

```go
id, err := client.Enqueue(ctx, inv, hangfire.WithParameter("TenantId", "acme"))
if err != nil {
	log.Fatal(err)
}
log.Printf("enqueued job %s", id)
```

Read them in a handler through the job context:

```go
handler := server.Handler0(func(ctx context.Context) error {
	var tenant string
	found, err := server.FromContext(ctx).Parameter(ctx, "TenantId", &tenant)
	if err != nil {
		return err
	}
	if !found {
		tenant = "default"
	}
	log.Printf("running for tenant %s", tenant)
	return nil
})
srv.Handle("MyApp.Jobs.ITenantJobs, MyApp.Contracts", "Sync", handler)
```

A parameter name has at most 40 characters.

## Keep arguments small

Arguments live in the `Job` table until the job expires. Pass the id of a row in your own database, not the row itself. The SDK logs a warning when a job's arguments exceed 64 KiB, and rejects a job above `sqlserver.Options.MaxArgumentBytes`, 1 MiB by default, with `hangfire.ErrPayloadTooLarge`.

A job can run minutes or weeks after it was created. By then the row an id points to may have changed or gone. Read it when the job runs and decide what to do if it is missing.

## Differences from .NET Hangfire

- .NET captures arguments from the lambda. In Go you list them in `Args`, and nothing checks them against `ParameterTypes` except the count.
- Go has no `ref` or `out` parameters, so that .NET limit does not come up.
- A .NET `DateTime` with an unspecified kind is written without a time zone suffix. Go's `time.Time` cannot decode that, so the Go handler fails with `ArgumentDecodeException`. If .NET code creates jobs for a Go handler, have it pass UTC values or `DateTimeOffset`, or take the parameter as a `string` in Go.

## Related pages

- [Passing dependencies](passing-dependencies.md)
- [Calling methods in background](calling-methods-in-background.md)
