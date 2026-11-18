package server

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"hangfire-gosdk/hangfire"
)

// AttemptsExceeded mirrors AttemptsExceededAction.
type AttemptsExceeded int

const (
	FailOnAttemptsExceeded AttemptsExceeded = iota
	DeleteOnAttemptsExceeded
)

// RetryPolicy mirrors AutomaticRetryAttribute (SRS FR-W10).
type RetryPolicy struct {
	// Attempts is the number of retries; 0 disables retries.
	Attempts int
	// DelaysInSeconds, when set, gives the delay per attempt; attempts past
	// its end reuse the last value. Nil means the default back-off.
	DelaysInSeconds    []int
	OnAttemptsExceeded AttemptsExceeded
}

// DefaultRetryPolicy is AutomaticRetryAttribute's default: 10 attempts with
// the polynomial back-off, then Failed (AutomaticRetryAttribute.cs:86-115).
func DefaultRetryPolicy() RetryPolicy { return RetryPolicy{Attempts: 10} }

func (p RetryPolicy) delay(attempt int) time.Duration {
	if len(p.DelaysInSeconds) == 0 {
		return hangfire.RetryDelay(attempt, rand.New(rand.NewSource(time.Now().UnixNano())))
	}
	i := min(attempt, len(p.DelaysInSeconds)) - 1
	return time.Duration(p.DelaysInSeconds[i]) * time.Second
}

// failure is what .NET keeps of an exception: FailedState's type, message and details.
type failure struct {
	typ, message, details string
}

// typedError lets an error choose the ExceptionType shown in the Dashboard.
type typedError interface {
	error
	HangfireExceptionType() string
}

func failureFromError(err error) failure {
	typ := fmt.Sprintf("Go:%T", err)
	if te, ok := errors.AsType[typedError](err); ok {
		typ = te.HangfireExceptionType()
	}
	return failure{typ: typ, message: err.Error(), details: typ + ": " + err.Error()}
}

func failureFromPanic(v any, stack []byte) failure {
	f := failure{typ: fmt.Sprintf("Go:%T", v), message: fmt.Sprint(v)}
	if err, ok := v.(error); ok {
		f = failureFromError(err)
	}
	f.details = f.typ + ": panic: " + f.message + "\n" + string(stack)
	return f
}

func (f failure) failed(serverID, reason string) hangfire.Failed {
	return hangfire.Failed{
		ExceptionType:    f.typ,
		ExceptionMessage: f.message,
		ExceptionDetails: f.details,
		ServerID:         serverID,
		Reason:           reason,
	}
}

// exceptionInfo is DeletedState's "Exception" value, ExceptionInfo serialized
// with SerializationOption.User (DeletedState.cs:152, ExceptionInfo.cs:49-58).
func (f failure) exceptionInfo() string {
	s, _ := hangfire.EncodeParameter(struct {
		E string `json:"e"`
		M string `json:"m,omitempty"`
	}{f.typ, f.message})
	return s
}

// transition is a state change decided by the worker: the state to apply,
// the states traversed on the way, and job parameters to set with it.
type transition struct {
	state     hangfire.State
	traversed []hangfire.State
	params    map[string]string
}

// retryElection is AutomaticRetryAttribute.OnStateElection (:240-296) for a
// failed candidate: schedule a retry, delete, or keep Failed.
func retryElection(p RetryPolicy, retryCount int, failed hangfire.Failed, f failure, now time.Time) transition {
	attempt := retryCount + 1
	switch {
	case attempt <= p.Attempts:
		reason := hangfire.RetryReason(attempt, p.Attempts, f.message)
		params := map[string]string{"RetryCount": strconv.Itoa(attempt)}
		var next hangfire.State = hangfire.Enqueued{Reason: reason}
		if d := p.delay(attempt); d > 0 {
			next = hangfire.Scheduled{EnqueueAt: now.Add(d), Reason: reason}
		}
		return transition{state: next, traversed: []hangfire.State{failed}, params: params}
	case p.OnAttemptsExceeded == DeleteOnAttemptsExceeded:
		reason := "Exceeded the maximum number of retry attempts."
		if p.Attempts <= 0 {
			reason = "Retries were disabled for this job."
		}
		return transition{state: hangfire.Deleted{Exception: f.exceptionInfo(), Reason: reason}, traversed: []hangfire.State{failed}}
	}
	return transition{state: failed}
}
