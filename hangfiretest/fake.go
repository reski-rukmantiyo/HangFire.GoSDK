// Package hangfiretest provides an in-memory hangfire.Client for tests (SRS FR-P51).
package hangfiretest

import (
	"context"
	"strconv"
	"sync"
	"time"

	"hangfire-gosdk/hangfire"
)

// Call is one recorded client call. Fields not relevant to Method are zero.
type Call struct {
	Method     string // "Enqueue", "Schedule", "ScheduleAt", "ContinueJobWith", "Create", "ChangeState", "Delete", "Requeue", "Reschedule"
	JobID      hangfire.JobID
	Invocation hangfire.Invocation
	State      hangfire.State
	Options    hangfire.EnqueueOptions
	Parent     hangfire.JobID
	Delay      time.Duration
	At         time.Time
	Expected   string
}

// FakeClient records calls and hands out sequential job ids. Management
// calls report success for ids it created and false for any other.
type FakeClient struct {
	mu     sync.Mutex
	calls  []Call
	nextID int
}

var _ hangfire.Client = (*FakeClient)(nil)

func NewFakeClient() *FakeClient { return &FakeClient{} }

// Calls returns a copy of the recorded calls in order.
func (f *FakeClient) Calls() []Call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Call(nil), f.calls...)
}

// Created returns the calls that created a job.
func (f *FakeClient) Created() []Call {
	var out []Call
	for _, c := range f.Calls() {
		switch c.Method {
		case "Enqueue", "Schedule", "ScheduleAt", "ContinueJobWith", "Create":
			out = append(out, c)
		}
	}
	return out
}

func options(opts []hangfire.Option) hangfire.EnqueueOptions {
	var o hangfire.EnqueueOptions
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

func (f *FakeClient) create(c Call, opts []hangfire.Option) (hangfire.JobID, error) {
	o := options(opts)
	if o.Queue != "" {
		if err := hangfire.ValidateQueue(o.Queue); err != nil {
			return "", err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	c.JobID = hangfire.JobID(strconv.Itoa(f.nextID))
	c.Options = o
	f.calls = append(f.calls, c)
	return c.JobID, nil
}

func (f *FakeClient) manage(c Call) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, c)
	n, err := strconv.Atoi(string(c.JobID))
	return err == nil && n >= 1 && n <= f.nextID, nil
}

func (f *FakeClient) Enqueue(_ context.Context, inv hangfire.Invocation, opts ...hangfire.Option) (hangfire.JobID, error) {
	return f.create(Call{Method: "Enqueue", Invocation: inv, State: hangfire.Enqueued{}}, opts)
}

func (f *FakeClient) Schedule(_ context.Context, inv hangfire.Invocation, delay time.Duration, opts ...hangfire.Option) (hangfire.JobID, error) {
	return f.create(Call{Method: "Schedule", Invocation: inv, Delay: delay, State: hangfire.Scheduled{EnqueueAt: time.Now().Add(delay)}}, opts)
}

func (f *FakeClient) ScheduleAt(_ context.Context, inv hangfire.Invocation, at time.Time, opts ...hangfire.Option) (hangfire.JobID, error) {
	return f.create(Call{Method: "ScheduleAt", Invocation: inv, At: at, State: hangfire.Scheduled{EnqueueAt: at}}, opts)
}

func (f *FakeClient) ContinueJobWith(_ context.Context, parent hangfire.JobID, inv hangfire.Invocation, next hangfire.State, cont hangfire.ContinuationOptions, opts ...hangfire.Option) (hangfire.JobID, error) {
	st := hangfire.Awaiting{ParentID: parent, NextState: next, Options: cont}
	return f.create(Call{Method: "ContinueJobWith", Invocation: inv, Parent: parent, State: st}, opts)
}

func (f *FakeClient) Create(_ context.Context, inv hangfire.Invocation, initial hangfire.State, opts ...hangfire.Option) (hangfire.JobID, error) {
	return f.create(Call{Method: "Create", Invocation: inv, State: initial}, opts)
}

func (f *FakeClient) ChangeState(_ context.Context, id hangfire.JobID, s hangfire.State, expected string) (bool, error) {
	return f.manage(Call{Method: "ChangeState", JobID: id, State: s, Expected: expected})
}

func (f *FakeClient) Delete(_ context.Context, id hangfire.JobID) (bool, error) {
	return f.manage(Call{Method: "Delete", JobID: id, State: hangfire.Deleted{}})
}

func (f *FakeClient) Requeue(_ context.Context, id hangfire.JobID) (bool, error) {
	return f.manage(Call{Method: "Requeue", JobID: id, State: hangfire.Enqueued{}})
}

func (f *FakeClient) Reschedule(_ context.Context, id hangfire.JobID, at time.Time) (bool, error) {
	return f.manage(Call{Method: "Reschedule", JobID: id, At: at, State: hangfire.Scheduled{EnqueueAt: at}})
}
