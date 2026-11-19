# Writing unit tests

You can test code that creates jobs without a database when the code takes a `hangfire.Client` interface instead of a concrete client. In tests you pass `hangfiretest.FakeClient`, which records each call in memory and returns job ids. This page shows how to test a producer, a recurring job registration, and a handler.

## Depend on the interface

`hangfire.Client` is the interface that `*sqlserver.Client` implements. Accept it wherever you create jobs:

```go
package signup

import (
	"context"

	"github.com/reski-rukmantiyo/HangFire.GoSDK/hangfire"
)

type Service struct {
	Jobs hangfire.Client
}

func (s *Service) Register(ctx context.Context, userID int64) error {
	_, err := s.Jobs.Enqueue(ctx, hangfire.Invocation{
		Type:           "MyApp.Jobs.IEmailJobs, MyApp.Contracts",
		Method:         "SendWelcome",
		ParameterTypes: []string{"System.Int64"},
		Args:           []any{userID},
	}, hangfire.WithQueue("emails"))
	return err
}
```

In production, set `Jobs` to `sqlserver.NewClient(storage)`.

## Assert on the recorded calls

`FakeClient.Created` returns the calls that created a job, in order. Each `hangfiretest.Call` has the method name, the invocation, the initial state, and the options:

```go
package signup

import (
	"context"
	"testing"

	"github.com/reski-rukmantiyo/HangFire.GoSDK/hangfire"
	"github.com/reski-rukmantiyo/HangFire.GoSDK/hangfiretest"
)

func TestRegisterEnqueuesWelcomeEmail(t *testing.T) {
	jobs := hangfiretest.NewFakeClient()
	svc := &Service{Jobs: jobs}

	if err := svc.Register(context.Background(), 42); err != nil {
		t.Fatal(err)
	}

	created := jobs.Created()
	if len(created) != 1 {
		t.Fatalf("created %d jobs, want 1", len(created))
	}
	c := created[0]
	if c.Invocation.Type != "MyApp.Jobs.IEmailJobs, MyApp.Contracts" || c.Invocation.Method != "SendWelcome" {
		t.Errorf("invocation = %s.%s", c.Invocation.Type, c.Invocation.Method)
	}
	if c.Invocation.Args[0] != int64(42) {
		t.Errorf("user id = %v", c.Invocation.Args[0])
	}
	if _, ok := c.State.(hangfire.Enqueued); !ok {
		t.Errorf("state = %T, want hangfire.Enqueued", c.State)
	}
	if c.Options.Queue != "emails" {
		t.Errorf("queue = %q", c.Options.Queue)
	}
}
```

Check both `Type` and `Method`. A method name alone matches any type that has a method of that name.

The fake checks queue names as the real client does, so a bad queue fails the test with `hangfire.ErrInvalidQueue`. It does not check that `Args` and `ParameterTypes` have the same length, and it never touches a database. `Delete`, `Requeue`, `Reschedule`, and `ChangeState` return `true` for ids the fake created and `false` for any other. `Calls` returns every call, including those.

## Fake the recurring job manager

The SDK ships no fake for `hangfire.RecurringManager`. The interface has three methods, so a test fake is short:

```go
type fakeRecurring struct {
	crons map[hangfire.RecurringJobID]string
}

func (f *fakeRecurring) AddOrUpdate(_ context.Context, id hangfire.RecurringJobID, _ hangfire.Invocation, cron string, _ hangfire.RecurringOptions) error {
	if _, err := hangfire.ParseCron(cron, ""); err != nil {
		return err
	}
	f.crons[id] = cron
	return nil
}

func (f *fakeRecurring) RemoveIfExists(_ context.Context, id hangfire.RecurringJobID) error {
	delete(f.crons, id)
	return nil
}

func (f *fakeRecurring) Trigger(_ context.Context, id hangfire.RecurringJobID) (hangfire.JobID, error) {
	if _, ok := f.crons[id]; !ok {
		return "", nil
	}
	return "1", nil
}

var _ hangfire.RecurringManager = (*fakeRecurring)(nil)
```

Calling `hangfire.ParseCron` makes the fake reject the same expressions that `AddOrUpdate` rejects.

## Test handlers as plain functions

A handler is an ordinary Go function, so call it directly with a `context.Background()`. [Passing dependencies](passing-dependencies.md) shows a full example with fake services.

`server.FromContext` returns `nil` outside a running server, and the SDK has no way to build a `JobContext` in a test. Keep handler logic that a test must cover in a function that does not call `FromContext`, and read job parameters in a thin wrapper around it.

## Differences from .NET Hangfire

- .NET tests mock `IBackgroundJobClient.Create` and inspect a `Job` and an `IState`. In Go, `FakeClient` records the higher-level call, such as `Enqueue` or `Schedule`, with the `Invocation` you passed.
- .NET has `JobCancellationToken.Null` for tests. A Go handler takes a `context.Context`, so tests pass `context.Background()` or a context they cancel.

## Related pages

- [Passing dependencies](passing-dependencies.md)
- [Performing recurrent tasks](performing-recurrent-tasks.md)
