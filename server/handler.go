package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// Handler runs one job method. Build it with Handler0..Handler4, which
// decode the job's arguments into the function's parameter types.
type Handler struct {
	// bind decodes the job's arguments and returns the call to run, or an
	// argument decode error (FR-W05).
	bind func(args []json.RawMessage) (func(context.Context) error, error)
}

// HandlerOption configures one registered handler.
type HandlerOption func(*registration)

// WithRetry overrides the server's retry policy for this handler (FR-W10).
func WithRetry(p RetryPolicy) HandlerOption {
	return func(r *registration) { r.retry = &p }
}

// WithMaxConcurrency caps how many jobs of this handler run at once in this
// process (FR-W40). It is Go-local and does not interoperate with
// Hangfire.Ace throttling.
func WithMaxConcurrency(n int) HandlerOption {
	return func(r *registration) {
		if n > 0 {
			r.slots = make(chan struct{}, n)
		}
	}
}

type registration struct {
	h     Handler
	retry *RetryPolicy
	slots chan struct{} // nil means unlimited
}

type handlerKey struct{ typ, method string }

// ArgumentDecodeError is the failure for a job whose arguments do not decode
// into the handler's parameter types. Such jobs are not retried.
type ArgumentDecodeError struct {
	Index int
	Err   error
}

func (e *ArgumentDecodeError) Error() string {
	if e.Index < 0 {
		return e.Err.Error()
	}
	return fmt.Sprintf("argument %d: %v", e.Index, e.Err)
}

func (e *ArgumentDecodeError) Unwrap() error { return e.Err }

func (e *ArgumentDecodeError) HangfireExceptionType() string {
	return "Hangfire.Go.ArgumentDecodeException"
}

// injectedTypes are parameters .NET fills at perform time and stores as null
// in the payload (SRS DR-07), so they never reach a Go handler.
var injectedTypes = map[string]bool{
	"System.Threading.CancellationToken": true,
	"Hangfire.IJobCancellationToken":     true,
	"Hangfire.Server.PerformContext":     true,
}

// handlerArgs drops the arguments of injected parameter types. Type names
// may be assembly-qualified, so only the part before the first comma counts.
func handlerArgs(paramTypes []string, args []json.RawMessage) []json.RawMessage {
	if len(paramTypes) != len(args) {
		return args
	}
	out := make([]json.RawMessage, 0, len(args))
	for i, a := range args {
		name, _, _ := strings.Cut(paramTypes[i], ",")
		if !injectedTypes[strings.TrimSpace(name)] {
			out = append(out, a)
		}
	}
	return out
}

func checkArity(args []json.RawMessage, n int) error {
	if len(args) != n {
		return &ArgumentDecodeError{Index: -1, Err: fmt.Errorf("handler takes %d arguments, job has %d", n, len(args))}
	}
	return nil
}

func decodeArg[T any](args []json.RawMessage, i int) (T, error) {
	var v T
	raw := args[i]
	if raw == nil {
		raw = json.RawMessage("null")
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		return v, &ArgumentDecodeError{Index: i, Err: err}
	}
	return v, nil
}

func Handler0(f func(context.Context) error) Handler {
	return Handler{bind: func(args []json.RawMessage) (func(context.Context) error, error) {
		if err := checkArity(args, 0); err != nil {
			return nil, err
		}
		return f, nil
	}}
}

func Handler1[A any](f func(context.Context, A) error) Handler {
	return Handler{bind: func(args []json.RawMessage) (func(context.Context) error, error) {
		if err := checkArity(args, 1); err != nil {
			return nil, err
		}
		a, err := decodeArg[A](args, 0)
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context) error { return f(ctx, a) }, nil
	}}
}

func Handler2[A, B any](f func(context.Context, A, B) error) Handler {
	return Handler{bind: func(args []json.RawMessage) (func(context.Context) error, error) {
		if err := checkArity(args, 2); err != nil {
			return nil, err
		}
		a, errA := decodeArg[A](args, 0)
		b, errB := decodeArg[B](args, 1)
		if err := firstDecodeError(errA, errB); err != nil {
			return nil, err
		}
		return func(ctx context.Context) error { return f(ctx, a, b) }, nil
	}}
}

func Handler3[A, B, C any](f func(context.Context, A, B, C) error) Handler {
	return Handler{bind: func(args []json.RawMessage) (func(context.Context) error, error) {
		if err := checkArity(args, 3); err != nil {
			return nil, err
		}
		a, errA := decodeArg[A](args, 0)
		b, errB := decodeArg[B](args, 1)
		c, errC := decodeArg[C](args, 2)
		if err := firstDecodeError(errA, errB, errC); err != nil {
			return nil, err
		}
		return func(ctx context.Context) error { return f(ctx, a, b, c) }, nil
	}}
}

func Handler4[A, B, C, D any](f func(context.Context, A, B, C, D) error) Handler {
	return Handler{bind: func(args []json.RawMessage) (func(context.Context) error, error) {
		if err := checkArity(args, 4); err != nil {
			return nil, err
		}
		a, errA := decodeArg[A](args, 0)
		b, errB := decodeArg[B](args, 1)
		c, errC := decodeArg[C](args, 2)
		d, errD := decodeArg[D](args, 3)
		if err := firstDecodeError(errA, errB, errC, errD); err != nil {
			return nil, err
		}
		return func(ctx context.Context) error { return f(ctx, a, b, c, d) }, nil
	}}
}

func firstDecodeError(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
