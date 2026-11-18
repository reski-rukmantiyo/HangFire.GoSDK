package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"hangfire-gosdk/hangfire"
)

func TestRetryElection(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f := failureFromError(errors.New("boom"))
	failed := f.failed("srv", "An exception occurred during performance of the job.")

	cases := []struct {
		name       string
		policy     RetryPolicy
		retryCount int
		want       hangfire.State
		wantCount  string
	}{
		{"scheduled", RetryPolicy{Attempts: 3, DelaysInSeconds: []int{5, 7}}, 0,
			hangfire.Scheduled{EnqueueAt: now.Add(5 * time.Second), Reason: "Retry attempt 1 of 3: boom"}, "1"},
		{"last delay reused", RetryPolicy{Attempts: 3, DelaysInSeconds: []int{5, 7}}, 2,
			hangfire.Scheduled{EnqueueAt: now.Add(7 * time.Second), Reason: "Retry attempt 3 of 3: boom"}, "3"},
		{"zero delay enqueues", RetryPolicy{Attempts: 1, DelaysInSeconds: []int{0}}, 0,
			hangfire.Enqueued{Reason: "Retry attempt 1 of 1: boom"}, "1"},
		{"exceeded fails", RetryPolicy{Attempts: 1}, 1, failed, ""},
		{"exceeded deletes", RetryPolicy{Attempts: 1, OnAttemptsExceeded: DeleteOnAttemptsExceeded}, 1,
			hangfire.Deleted{Exception: `{"e":"Go:*errors.errorString","m":"boom"}`, Reason: "Exceeded the maximum number of retry attempts."}, ""},
		{"disabled deletes", RetryPolicy{OnAttemptsExceeded: DeleteOnAttemptsExceeded}, 0,
			hangfire.Deleted{Exception: `{"e":"Go:*errors.errorString","m":"boom"}`, Reason: "Retries were disabled for this job."}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := retryElection(c.policy, c.retryCount, failed, f, now)
			if fmt.Sprint(got.state) != fmt.Sprint(c.want) {
				t.Errorf("state = %+v, want %+v", got.state, c.want)
			}
			if got.params["RetryCount"] != c.wantCount {
				t.Errorf("RetryCount = %q, want %q", got.params["RetryCount"], c.wantCount)
			}
			if _, isFailed := c.want.(hangfire.Failed); isFailed != (len(got.traversed) == 0) {
				t.Errorf("traversed = %v", got.traversed)
			}
		})
	}
}

func TestDefaultRetryDelay(t *testing.T) {
	for attempt := 1; attempt <= 10; attempt++ {
		d := DefaultRetryPolicy().delay(attempt)
		n := float64(attempt)
		lo := time.Duration((n-1)*(n-1)*(n-1)*(n-1)+15) * time.Second
		hi := lo + time.Duration(29*attempt)*time.Second
		if d < lo || d > hi {
			t.Errorf("attempt %d: delay %v outside [%v, %v]", attempt, d, lo, hi)
		}
	}
}

type quotaError struct{}

func (quotaError) Error() string                 { return "quota" }
func (quotaError) HangfireExceptionType() string { return "MyApp.QuotaException" }

func TestFailureTypes(t *testing.T) {
	if got := failureFromError(fmt.Errorf("wrap: %w", errors.New("x"))).typ; got != "Go:*fmt.wrapError" {
		t.Errorf("wrapped error type = %q", got)
	}
	if got := failureFromError(fmt.Errorf("wrap: %w", quotaError{})).typ; got != "MyApp.QuotaException" {
		t.Errorf("override through wrap = %q", got)
	}
	p := failureFromPanic("kaboom", []byte("stack"))
	if p.typ != "Go:string" || p.message != "kaboom" || !strings.Contains(p.details, "panic: kaboom\nstack") {
		t.Errorf("panic failure = %+v", p)
	}
}

func TestHandlerBinding(t *testing.T) {
	raw := func(ss ...string) []json.RawMessage {
		out := make([]json.RawMessage, len(ss))
		for i, s := range ss {
			if s != "null" {
				out[i] = json.RawMessage(s)
			}
		}
		return out
	}
	var gotName string
	var gotN int
	h := Handler2(func(_ context.Context, name string, n int) error { gotName, gotN = name, n; return nil })

	args := handlerArgs([]string{"System.String", "System.Threading.CancellationToken", "System.Int32"}, raw(`"a"`, "null", "7"))
	call, err := h.bind(args)
	if err != nil {
		t.Fatal(err)
	}
	if err := call(context.Background()); err != nil || gotName != "a" || gotN != 7 {
		t.Errorf("call = %v, %q, %d", err, gotName, gotN)
	}

	_, err = h.bind(raw(`"a"`, `"seven"`))
	var de *ArgumentDecodeError
	if !errors.As(err, &de) || de.Index != 1 || failureFromError(err).typ != "Hangfire.Go.ArgumentDecodeException" {
		t.Errorf("bad arg error = %v", err)
	}
	if _, err := h.bind(raw(`"a"`)); !errors.As(err, &de) {
		t.Errorf("arity error = %v", err)
	}
	if call, err := Handler1(func(_ context.Context, s *string) error {
		if s != nil {
			return errors.New("want nil")
		}
		return nil
	}).bind(raw("null")); err != nil || call(context.Background()) != nil {
		t.Errorf("null arg: %v", err)
	}
}

func TestOptionsValidation(t *testing.T) {
	if _, err := (Options{Queues: []string{"Bad Queue"}}).withDefaults(); !errors.Is(err, hangfire.ErrInvalidQueue) {
		t.Errorf("queue validation: %v", err)
	}
	if _, err := (Options{WorkerCount: -1}).withDefaults(); err == nil {
		t.Error("negative WorkerCount accepted")
	}
	o, err := Options{}.withDefaults()
	if err != nil || o.WorkerCount < 1 || o.WorkerCount > 20 || o.Queues[0] != "default" || o.Retry.Attempts != 10 {
		t.Errorf("defaults = %+v, %v", o, err)
	}
}
