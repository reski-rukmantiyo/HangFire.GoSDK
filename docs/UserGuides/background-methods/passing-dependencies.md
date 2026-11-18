# Passing dependencies

Most jobs need more than their arguments: a database handle, an HTTP client, a mail sender. .NET Hangfire builds the job class through a `JobActivator`. A Go server has no activator. You build the dependencies yourself and hand the server a function that already holds them. This page shows the two ways to do that and how each one stays testable.

## Put dependencies in a struct

Define the services a job needs as interfaces, keep them in a struct, and register its methods as handlers. A method value carries its receiver, so the handler sees the struct's fields:

```go
type Mailer interface {
	Send(ctx context.Context, to, subject string) error
}

type Users interface {
	Email(ctx context.Context, id int64) (string, error)
}

type EmailJobs struct {
	Users  Users
	Mailer Mailer
}

func (j *EmailJobs) SendWelcome(ctx context.Context, userID int64) error {
	to, err := j.Users.Email(ctx, userID)
	if err != nil {
		return err
	}
	return j.Mailer.Send(ctx, to, "Welcome")
}

func register(srv *server.Server, jobs *EmailJobs) {
	srv.Handle("MyApp.Jobs.IEmailJobs, MyApp.Contracts", "SendWelcome",
		server.Handler1(jobs.SendWelcome))
}
```

`main` creates the real `Users` and `Mailer` once and calls `register` before `srv.Run`. All workers share the one `EmailJobs` value, so its fields must be safe for concurrent use. A `*sql.DB` and an `*http.Client` already are.

## Or capture dependencies in a closure

For a short handler, a closure over local variables does the same job without a struct:

```go
func registerCleanup(srv *server.Server, db *sql.DB) {
	srv.Handle("MyApp.Jobs.IMaintenance, MyApp.Contracts", "PurgeSessions",
		server.Handler0(func(ctx context.Context) error {
			_, err := db.ExecContext(ctx, "delete from Sessions where ExpiresAt < sysutcdatetime()")
			return err
		}))
}
```

## Test the job without a server

Because `SendWelcome` is a plain method, a test builds `EmailJobs` with fakes and calls it directly, with no database or queue:

```go
type fakeUsers map[int64]string

func (f fakeUsers) Email(_ context.Context, id int64) (string, error) { return f[id], nil }

type fakeMailer struct{ sent []string }

func (f *fakeMailer) Send(_ context.Context, to, _ string) error {
	f.sent = append(f.sent, to)
	return nil
}

func TestSendWelcome(t *testing.T) {
	mailer := &fakeMailer{}
	jobs := &EmailJobs{Users: fakeUsers{42: "ada@example.com"}, Mailer: mailer}
	if err := jobs.SendWelcome(context.Background(), 42); err != nil {
		t.Fatal(err)
	}
	if len(mailer.sent) != 1 || mailer.sent[0] != "ada@example.com" {
		t.Fatalf("sent = %v", mailer.sent)
	}
}
```

## Differences from .NET Hangfire

- .NET creates a new instance of the job class for every run, through `JobActivator`. A Go handler is one function value that every run shares. Create per-run objects inside the handler.
- .NET's default activator needs a parameterless constructor. Go has no such rule, because the server never constructs anything.
- `Type` in the invocation still names a .NET type. It only selects the handler. It does not have to match any Go type.

## Related pages

- [Using IoC containers](using-ioc-containers.md)
- [Writing unit tests](writing-unit-tests.md)
