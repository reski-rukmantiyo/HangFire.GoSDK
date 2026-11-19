package server_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/reski-rukmantiyo/HangFire.GoSDK/hangfire"
	"github.com/reski-rukmantiyo/HangFire.GoSDK/internal/sqltest"
	"github.com/reski-rukmantiyo/HangFire.GoSDK/server"
	"github.com/reski-rukmantiyo/HangFire.GoSDK/sqlserver"
)

// These tests need a SQL Server: set HANGFIRE_SQL (see internal/sqltest).
// One database serves the package; each test uses its own queue.

var testDB *sqltest.DB

func TestMain(m *testing.M) {
	base := os.Getenv("HANGFIRE_SQL")
	if base == "" {
		os.Exit(m.Run())
	}
	wd, _ := os.Getwd()
	d, drop, err := sqltest.Create(base, wd+"/..")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	testDB = d
	code := m.Run()
	if os.Getenv("HANGFIRE_KEEP_DB") == "" {
		if err := drop(); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}
	os.Exit(code)
}

const jobsType = "Go.Tests.Jobs, Go.Tests"

type env struct {
	t      *testing.T
	s      *sqlserver.Storage
	c      *sqlserver.Client
	m      *sqlserver.Monitor
	queue  string
	ctx    context.Context
	cancel context.CancelFunc
}

var queueSeq atomic.Int64

func setup(t *testing.T) *env {
	t.Helper()
	if testDB == nil {
		t.Skip("HANGFIRE_SQL not set")
	}
	s, err := sqlserver.NewStorage(context.Background(), testDB.DB, sqlserver.Options{Level: hangfire.CompatLevel180})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return &env{t: t, s: s, c: sqlserver.NewClient(s), m: sqlserver.NewMonitor(s),
		queue: fmt.Sprintf("q%d_%d", time.Now().Unix()%100000, queueSeq.Add(1)), ctx: ctx, cancel: cancel}
}

// fast is Options with short intervals so tests finish in seconds.
func (e *env) fast(o server.Options) server.Options {
	if o.Queues == nil {
		o.Queues = []string{e.queue}
	}
	if o.WorkerCount == 0 {
		o.WorkerCount = 2
	}
	o.QueuePollInterval = 50 * time.Millisecond
	if o.SchedulePollingInterval == 0 {
		o.SchedulePollingInterval = 200 * time.Millisecond
	}
	if o.CancellationCheckInterval == 0 {
		o.CancellationCheckInterval = 100 * time.Millisecond
	}
	return o
}

// start runs a server and shuts it down at test end, then checks no server
// goroutine outlived it.
func (e *env) start(o server.Options, register func(*server.Server)) *server.Server {
	e.t.Helper()
	srv, err := server.New(e.s, e.fast(o))
	if err != nil {
		e.t.Fatal(err)
	}
	register(srv)
	if err := srv.Start(e.ctx); err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() {
		if err := srv.Shutdown(context.Background()); err != nil {
			e.t.Errorf("shutdown: %v", err)
		}
		checkNoLeaks(e.t)
	})
	return srv
}

func checkNoLeaks(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		buf := make([]byte, 1<<20)
		stacks := string(buf[:runtime.Stack(buf, true)])
		var leaked []string
		for _, g := range strings.Split(stacks, "\n\n") {
			if strings.Contains(g, "github.com/reski-rukmantiyo/HangFire.GoSDK/server.") && !strings.Contains(g, "_test.go") {
				leaked = append(leaked, g)
			}
		}
		if len(leaked) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("%d server goroutines leaked:\n%s", len(leaked), strings.Join(leaked, "\n\n"))
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (e *env) enqueue(method string, args ...any) hangfire.JobID {
	e.t.Helper()
	id, err := e.c.Enqueue(e.ctx, e.inv(method, args...), hangfire.WithQueue(e.queue))
	if err != nil {
		e.t.Fatal(err)
	}
	return id
}

func (e *env) inv(method string, args ...any) hangfire.Invocation {
	types := make([]string, len(args))
	for i, a := range args {
		switch a.(type) {
		case int:
			types[i] = "System.Int32"
		default:
			types[i] = "System.String"
		}
	}
	return hangfire.Invocation{Type: jobsType, Method: method, ParameterTypes: types, Args: args}
}

// waitState polls until the job's current state is name and returns its details.
func (e *env) waitState(id hangfire.JobID, name string) *sqlserver.JobDetails {
	e.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		// Monitor reads with nolock, like the .NET Dashboard, so it can see a
		// transaction half applied. Wait on a read-committed read first.
		jd, err := e.s.JobData(e.ctx, id)
		if err != nil {
			e.t.Fatal(err)
		}
		d, err := e.m.JobDetails(e.ctx, id)
		if err != nil {
			e.t.Fatal(err)
		}
		if jd != nil && jd.StateName == name && d != nil && len(d.History) > 0 && d.History[0].StateName == name {
			return d
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("job %s never reached %s; history: %s", id, name, history(d))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func history(d *sqlserver.JobDetails) string {
	if d == nil {
		return "<no job>"
	}
	var parts []string
	for i := len(d.History) - 1; i >= 0; i-- {
		parts = append(parts, d.History[i].StateName+"("+d.History[i].Reason+")")
	}
	return strings.Join(parts, " -> ")
}

func names(d *sqlserver.JobDetails) string {
	var parts []string
	for i := len(d.History) - 1; i >= 0; i-- {
		parts = append(parts, d.History[i].StateName)
	}
	return strings.Join(parts, ",")
}

func (e *env) queueRow(id hangfire.JobID) (exists bool, fetched bool) {
	e.t.Helper()
	var at sql.NullTime
	err := testDB.QueryRowContext(e.ctx, `select FetchedAt from HangFire.JobQueue where JobId = @p1`, string(id)).Scan(&at)
	if errors.Is(err, sql.ErrNoRows) {
		return false, false
	}
	if err != nil {
		e.t.Fatal(err)
	}
	return true, at.Valid
}

func noRetry() *server.RetryPolicy { return &server.RetryPolicy{} }

func TestSucceeds(t *testing.T) {
	e := setup(t)
	type seen struct {
		name   string
		n      int
		jc     server.JobContext
		hasCtx bool
	}
	got := make(chan seen, 1)
	var srv *server.Server
	srv = e.start(server.Options{}, func(s *server.Server) {
		s.Handle(jobsType, "Greet", server.Handler2(func(ctx context.Context, name string, n int) error {
			jc := server.FromContext(ctx)
			if err := jc.SetParameter(ctx, "Greeted", name); err != nil {
				return err
			}
			got <- seen{name, n, *jc, jc != nil}
			return nil
		}))
	})
	id := e.enqueue("Greet", "world", 42)
	d := e.waitState(id, hangfire.StateSucceeded)
	g := <-got
	if g.name != "world" || g.n != 42 || g.jc.JobID != id || g.jc.ServerID != srv.ID() || g.jc.RetryCount != 0 || g.jc.Queue != e.queue {
		t.Errorf("handler saw %+v", g)
	}
	if names(d) != "Enqueued,Processing,Succeeded" {
		t.Errorf("history = %s", history(d))
	}
	proc := d.History[1].Data
	if proc["ServerId"] != srv.ID() || proc["WorkerId"] == "" || proc["StartedAt"] == "" {
		t.Errorf("processing data = %v", proc)
	}
	ok := d.History[0].Data
	for _, k := range []string{"SucceededAt", "PerformanceDuration", "Latency"} {
		if _, has := ok[k]; !has {
			t.Errorf("succeeded data lacks %s: %v", k, ok)
		}
	}
	if d.Properties["Greeted"] != `"world"` {
		t.Errorf("parameter = %q", d.Properties["Greeted"])
	}
	if d.ExpireAt == nil {
		t.Error("succeeded job has no ExpireAt")
	}
	if exists, _ := e.queueRow(id); exists {
		t.Error("queue row not acknowledged")
	}
}

func TestServerRow(t *testing.T) {
	e := setup(t)
	srv, err := server.New(e.s, e.fast(server.Options{Name: "GoBox", WorkerCount: 3, Queues: []string{e.queue, "default"}}))
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^gobox:\d+:[0-9a-f-]{36}$`).MatchString(srv.ID()) {
		t.Errorf("server id = %q", srv.ID())
	}
	if err := srv.Start(e.ctx); err != nil {
		t.Fatal(err)
	}
	var data string
	if err := testDB.QueryRowContext(e.ctx, `select Data from HangFire.Server where Id = @p1`, srv.ID()).Scan(&data); err != nil {
		t.Fatal(err)
	}
	var sd struct {
		WorkerCount int
		Queues      []string
		StartedAt   string
	}
	if err := json.Unmarshal([]byte(data), &sd); err != nil || sd.WorkerCount != 3 || len(sd.Queues) != 2 || sd.StartedAt == "" {
		t.Errorf("server data = %s (%v)", data, err)
	}
	if err := srv.Shutdown(e.ctx); err != nil {
		t.Fatal(err)
	}
	var n int
	testDB.QueryRowContext(e.ctx, `select count(*) from HangFire.Server where Id = @p1`, srv.ID()).Scan(&n)
	if n != 0 {
		t.Error("server row not removed on shutdown")
	}
	checkNoLeaks(t)
}

func TestRetryThenSucceed(t *testing.T) {
	e := setup(t)
	var runs atomic.Int32
	counts := make(chan int, 3)
	e.start(server.Options{Retry: &server.RetryPolicy{Attempts: 3, DelaysInSeconds: []int{1, 0}}}, func(s *server.Server) {
		s.Handle(jobsType, "Flaky", server.Handler0(func(ctx context.Context) error {
			counts <- server.FromContext(ctx).RetryCount
			if runs.Add(1) <= 2 {
				return errors.New("transient failure")
			}
			return nil
		}))
	})
	id := e.enqueue("Flaky")
	d := e.waitState(id, hangfire.StateSucceeded)
	want := "Enqueued,Processing,Failed,Scheduled,Processing,Failed,Enqueued,Processing,Succeeded"
	if names(d) != want {
		t.Fatalf("history = %s", history(d))
	}
	if r := d.History[5].Reason; r != "Retry attempt 1 of 3: transient failure" {
		t.Errorf("scheduled reason = %q", r)
	}
	if r := d.History[2].Reason; r != "Retry attempt 2 of 3: transient failure" {
		t.Errorf("enqueued reason = %q", r)
	}
	if d.Properties["RetryCount"] != "2" {
		t.Errorf("RetryCount = %q", d.Properties["RetryCount"])
	}
	if a, b, c := <-counts, <-counts, <-counts; a != 0 || b != 1 || c != 2 {
		t.Errorf("JobContext.RetryCount = %d,%d,%d", a, b, c)
	}
}

func TestAttemptsExceeded(t *testing.T) {
	e := setup(t)
	fail := server.Handler0(func(context.Context) error { return errors.New("always") })
	e.start(server.Options{Retry: &server.RetryPolicy{Attempts: 1, DelaysInSeconds: []int{0}}}, func(s *server.Server) {
		s.Handle(jobsType, "Fail", fail)
		s.Handle(jobsType, "FailDelete", fail, server.WithRetry(server.RetryPolicy{Attempts: 1, DelaysInSeconds: []int{0}, OnAttemptsExceeded: server.DeleteOnAttemptsExceeded}))
	})

	failID := e.enqueue("Fail")
	d := e.waitState(failID, hangfire.StateFailed)
	if names(d) != "Enqueued,Processing,Failed,Enqueued,Processing,Failed" {
		t.Errorf("fail history = %s", history(d))
	}
	f := d.History[0]
	if f.Reason != "An exception occurred during performance of the job." || f.Data["ExceptionType"] != "Go:*errors.errorString" || f.Data["ExceptionMessage"] != "always" {
		t.Errorf("failed state = %+v", f)
	}
	if d.ExpireAt != nil {
		t.Error("failed job must not expire")
	}

	delID := e.enqueue("FailDelete")
	d = e.waitState(delID, hangfire.StateDeleted)
	if names(d) != "Enqueued,Processing,Failed,Enqueued,Processing,Failed,Deleted" {
		t.Errorf("delete history = %s", history(d))
	}
	if r, x := d.History[0].Reason, d.History[0].Data["Exception"]; r != "Exceeded the maximum number of retry attempts." || x != `{"e":"Go:*errors.errorString","m":"always"}` {
		t.Errorf("deleted state = %q %q", r, x)
	}
}

func TestUnknownHandler(t *testing.T) {
	e := setup(t)
	e.start(server.Options{Retry: noRetry()}, func(*server.Server) {})
	id := e.enqueue("Nobody")
	d := e.waitState(id, hangfire.StateFailed)
	if names(d) != "Enqueued,Failed" {
		t.Errorf("history = %s", history(d))
	}
	if r := d.History[0].Reason; r != "Can not change the state to 'Processing': target method was not found." {
		t.Errorf("reason = %q", r)
	}
	if exists, _ := e.queueRow(id); exists {
		t.Error("queue row not acknowledged")
	}
}

func TestPanicAndBadArguments(t *testing.T) {
	e := setup(t)
	e.start(server.Options{}, func(s *server.Server) {
		s.Handle(jobsType, "Panics", server.Handler0(func(context.Context) error { panic("kaboom") }), server.WithRetry(server.RetryPolicy{}))
		s.Handle(jobsType, "WantsInt", server.Handler1(func(context.Context, int) error { return nil }))
	})
	p := e.waitState(e.enqueue("Panics"), hangfire.StateFailed).History[0].Data
	if p["ExceptionType"] != "Go:string" || p["ExceptionMessage"] != "kaboom" || !strings.Contains(p["ExceptionDetails"], "panic: kaboom") {
		t.Errorf("panic failure = %v", p)
	}
	// The default policy would retry; a bad payload must not be.
	d := e.waitState(e.enqueue("WantsInt", "not a number"), hangfire.StateFailed)
	if names(d) != "Enqueued,Processing,Failed" || d.History[0].Data["ExceptionType"] != "Hangfire.Go.ArgumentDecodeException" {
		t.Errorf("decode failure history = %s data = %v", history(d), d.History[0].Data)
	}
}

func TestDeleteCancelsRunningJob(t *testing.T) {
	e := setup(t)
	started := make(chan struct{})
	cause := make(chan error, 1)
	e.start(server.Options{}, func(s *server.Server) {
		s.Handle(jobsType, "Long", server.Handler0(func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			cause <- context.Cause(ctx)
			return ctx.Err()
		}))
	})
	id := e.enqueue("Long")
	<-started
	if ok, err := e.c.Delete(e.ctx, id); !ok || err != nil {
		t.Fatalf("delete = %v, %v", ok, err)
	}
	select {
	case c := <-cause:
		if c == nil || c == context.Canceled {
			t.Errorf("cancel cause = %v", c)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handler context was not cancelled after Delete")
	}
	time.Sleep(300 * time.Millisecond)
	d := e.waitState(id, hangfire.StateDeleted)
	if names(d) != "Enqueued,Processing,Deleted" {
		t.Errorf("history = %s", history(d))
	}
	if exists, _ := e.queueRow(id); exists {
		t.Error("queue row not acknowledged")
	}
}

func TestShutdownRequeues(t *testing.T) {
	e := setup(t)
	running := make(chan struct{}, 2)
	release := make(chan struct{})
	srv, err := server.New(e.s, e.fast(server.Options{StopTimeout: 100 * time.Millisecond, ShutdownTimeout: 700 * time.Millisecond}))
	if err != nil {
		t.Fatal(err)
	}
	srv.Handle(jobsType, "Cooperative", server.Handler0(func(ctx context.Context) error {
		running <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}))
	srv.Handle(jobsType, "Stuck", server.Handler0(func(ctx context.Context) error {
		running <- struct{}{}
		<-release
		return nil
	}))
	if err := srv.Start(e.ctx); err != nil {
		t.Fatal(err)
	}
	coop, stuck := e.enqueue("Cooperative"), e.enqueue("Stuck")
	<-running
	<-running
	if err := srv.Shutdown(e.ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []hangfire.JobID{coop, stuck} {
		if exists, fetched := e.queueRow(id); !exists || fetched {
			t.Errorf("job %s: queue row exists=%v fetched=%v, want requeued", id, exists, fetched)
		}
	}
	close(release) // the abandoned handler returns; it must not record an outcome
	time.Sleep(300 * time.Millisecond)
	if d, _ := e.m.JobDetails(e.ctx, stuck); d.History[0].StateName != hangfire.StateProcessing {
		t.Errorf("abandoned job state = %s", history(d))
	}
	checkNoLeaks(t)

	// Processing is an eligible state, so another server picks both up.
	e.start(server.Options{}, func(s *server.Server) {
		s.Handle(jobsType, "Cooperative", server.Handler0(func(context.Context) error { return nil }))
		s.Handle(jobsType, "Stuck", server.Handler0(func(context.Context) error { return nil }))
	})
	e.waitState(coop, hangfire.StateSucceeded)
	if d := e.waitState(stuck, hangfire.StateSucceeded); names(d) != "Enqueued,Processing,Processing,Succeeded" {
		t.Errorf("history = %s", history(d))
	}
}

func TestCrashRecovery(t *testing.T) {
	e := setup(t)
	id := e.enqueue("Recover")
	// A worker that fetched the job, moved it to Processing and died.
	f, err := e.s.FetchNext(e.ctx, []string{e.queue}, time.Minute)
	if err != nil || f == nil || f.JobID != id {
		t.Fatalf("fetch = %+v, %v", f, err)
	}
	if _, err := e.s.ChangeState(e.ctx, sqlserver.StateChange{JobID: id, State: hangfire.Processing{ServerID: "dead:1:x", WorkerID: "w"}}); err != nil {
		t.Fatal(err)
	}
	e.start(server.Options{InvisibilityTimeout: 2 * time.Second}, func(s *server.Server) {
		s.Handle(jobsType, "Recover", server.Handler0(func(context.Context) error { return nil }))
	})
	d := e.waitState(id, hangfire.StateSucceeded)
	if names(d) != "Enqueued,Processing,Processing,Succeeded" {
		t.Errorf("history = %s", history(d))
	}
}

func TestDelayedScheduler(t *testing.T) {
	e := setup(t)
	done := make(chan struct{}, 2)
	e.start(server.Options{Queues: []string{e.queue, "default"}}, func(s *server.Server) {
		s.Handle(jobsType, "Later", server.Handler0(func(context.Context) error { done <- struct{}{}; return nil }))
	})
	withQueue, err := e.c.Schedule(e.ctx, e.inv("Later"), time.Second, hangfire.WithQueue(e.queue))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := e.c.Schedule(e.ctx, e.inv("Later"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// "queue:id" members move straight to the queue (DelayedJobScheduler.cs:264-273).
	if d := e.waitState(withQueue, hangfire.StateSucceeded); names(d) != "Scheduled,Processing,Succeeded" {
		t.Errorf("queued history = %s", history(d))
	}
	d := e.waitState(plain, hangfire.StateSucceeded)
	if names(d) != "Scheduled,Enqueued,Processing,Succeeded" || d.History[2].Reason != "Triggered by DelayedJobScheduler" {
		t.Errorf("plain history = %s", history(d))
	}
}

func TestRecurringScheduler(t *testing.T) {
	e := setup(t)
	rm := sqlserver.NewRecurringManager(e.s)
	rid := hangfire.RecurringJobID("go-tick-" + e.queue)
	t.Cleanup(func() { rm.RemoveIfExists(context.Background(), rid) })
	got := make(chan string, 10)
	e.start(server.Options{EnableRecurringScheduler: true}, func(s *server.Server) {
		s.Handle(jobsType, "Tick", server.Handler1(func(ctx context.Context, label string) error {
			var recurringID string
			if _, err := server.FromContext(ctx).Parameter(ctx, "RecurringJobId", &recurringID); err != nil {
				return err
			}
			got <- label + "|" + recurringID
			return nil
		}))
	})
	if err := rm.AddOrUpdate(e.ctx, rid, e.inv("Tick", "tock"), "* * * * * *", hangfire.RecurringOptions{Queue: e.queue}); err != nil {
		t.Fatal(err)
	}
	select {
	case g := <-got:
		if g != "tock|"+string(rid) {
			t.Errorf("handler saw %q", g)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("recurring job never ran")
	}
	h, err := e.s.Hash(e.ctx, "recurring-job:"+string(rid))
	if err != nil || h["LastJobId"] == "" || h["LastExecution"] == "" {
		t.Errorf("recurring hash = %v, %v", h, err)
	}
}

func TestContinuationAfterGoParent(t *testing.T) {
	e := setup(t)
	order := make(chan string, 2)
	e.start(server.Options{}, func(s *server.Server) {
		s.Handle(jobsType, "Step", server.Handler1(func(_ context.Context, name string) error { order <- name; return nil }))
	})
	parent, err := e.c.Schedule(e.ctx, e.inv("Step", "parent"), time.Hour, hangfire.WithQueue(e.queue))
	if err != nil {
		t.Fatal(err)
	}
	child, err := e.c.ContinueJobWith(e.ctx, parent, e.inv("Step", "child"), nil, hangfire.OnlyOnSucceededState, hangfire.WithQueue(e.queue))
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := e.c.Requeue(e.ctx, parent); !ok || err != nil {
		t.Fatal(ok, err)
	}
	e.waitState(parent, hangfire.StateSucceeded)
	if d := e.waitState(child, hangfire.StateSucceeded); names(d) != "Awaiting,Enqueued,Processing,Succeeded" {
		t.Errorf("child history = %s", history(d))
	}
	if a, b := <-order, <-order; a != "parent" || b != "child" {
		t.Errorf("order = %s, %s", a, b)
	}
}
