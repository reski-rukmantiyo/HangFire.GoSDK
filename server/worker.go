package server

import (
	"context"
	"errors"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/reski-rukmantiyo/HangFire.GoSDK/hangfire"
	"github.com/reski-rukmantiyo/HangFire.GoSDK/sqlserver"
)

// errAborted cancels a handler whose job left Processing for its worker.
var errAborted = errors.New("server: job was aborted by a state change")

// Worker.cs:49: Processing is eligible so a job abandoned by a crashed
// worker can run again once its queue row becomes visible.
var eligibleStates = []string{hangfire.StateEnqueued, hangfire.StateScheduled, hangfire.StateProcessing}

const writeTimeout = 30 * time.Second

// inflight is one fetched queue row. Whoever claims it first decides its
// fate: the worker (ack or requeue) or Shutdown (requeue after timeout).
type inflight struct {
	jobID    hangfire.JobID
	workerID string
	cancel   context.CancelCauseFunc
	aborted  atomic.Bool

	mu      sync.Mutex
	fetched sqlserver.FetchedJob // FetchedAt moves with each keep-alive
	claimed bool
}

func (j *inflight) claim() (sqlserver.FetchedJob, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.claimed {
		return j.fetched, false
	}
	j.claimed = true
	return j.fetched, true
}

type worker struct {
	s  *Server
	id string
}

func (s *Server) newWorker() *worker { return &worker{s: s, id: uuid.NewString()} }

func (w *worker) run() {
	s := w.s
	for s.stopping.Err() == nil {
		f, err := s.storage.FetchNext(s.stopping, s.opts.Queues, s.opts.InvisibilityTimeout)
		if err != nil && s.stopping.Err() == nil {
			s.log.Error("hangfire: fetching a job failed", "worker", w.id, "err", err)
		}
		if f == nil {
			sleep(s.stopping, s.opts.QueuePollInterval)
			continue
		}
		w.process(f)
	}
}

// verdict is what happens to a fetched row: a state change acknowledged in
// the same transaction, a plain acknowledgement, or a requeue.
type verdict struct {
	change  *sqlserver.StateChange
	requeue bool
}

func (w *worker) process(f *sqlserver.FetchedJob) {
	j := &inflight{jobID: f.JobID, workerID: w.id, fetched: *f}
	stopKeepAlive := w.s.keepAlive(j)
	v := w.decide(j)
	stopKeepAlive()
	w.s.finish(j, v)
}

// decide runs Worker.Execute (Worker.cs:101-191) up to the outcome.
func (w *worker) decide(j *inflight) verdict {
	s := w.s
	ctx := s.stopping
	jd, err := s.storage.JobData(ctx, j.jobID)
	if err != nil {
		s.log.Error("hangfire: reading job failed", "job", j.jobID, "err", err)
		return verdict{requeue: true}
	}
	if jd == nil {
		return verdict{} // expired after it was queued
	}
	retryCount := 0
	if raw, ok := jd.Parameters["RetryCount"]; ok {
		_ = hangfire.DecodeParameter(raw, &retryCount)
	}

	reg := s.handlers[handlerKey{jd.Payload.Type, jd.Payload.Method}]
	if jd.PayloadErr != nil || reg == nil {
		// BackgroundJobStateChanger.cs:88-110 swaps the Processing state for
		// Failed; the global retry filter still elects on it.
		f := failure{typ: "Hangfire.Go.HandlerNotFoundException", message: "no Go handler is registered for " + jd.Payload.Type + "." + jd.Payload.Method}
		if jd.PayloadErr != nil {
			f.message = jd.PayloadErr.Error()
		}
		f.details = f.typ + ": " + f.message
		failed := f.failed(s.id, "Can not change the state to 'Processing': target method was not found.")
		t := retryElection(*s.opts.Retry, retryCount, failed, f, time.Now())
		return verdict{change: &sqlserver.StateChange{JobID: j.jobID, State: t.state, Traversed: t.traversed, Parameters: t.params, Expected: eligibleStates}}
	}

	if reg.slots != nil {
		select {
		case reg.slots <- struct{}{}:
			defer func() { <-reg.slots }()
		case <-ctx.Done():
			return verdict{requeue: true}
		}
	}

	applied, err := s.storage.ChangeState(ctx, sqlserver.StateChange{
		JobID:    j.jobID,
		State:    hangfire.Processing{ServerID: s.id, WorkerID: w.id},
		Expected: eligibleStates,
	})
	if err != nil {
		s.log.Error("hangfire: moving job to Processing failed", "job", j.jobID, "err", err)
		return verdict{requeue: true}
	}
	if applied == nil || applied.StateName() != hangfire.StateProcessing {
		return verdict{} // gone or no longer eligible: forget it
	}

	policy := *s.opts.Retry
	if reg.retry != nil {
		policy = *reg.retry
	}
	expectProcessing := []string{hangfire.StateProcessing}
	call, err := reg.h.bind(handlerArgs(jd.Payload.ParameterTypes, jd.Payload.Args))
	if err != nil {
		// A retry cannot fix a payload that does not decode (FR-W05).
		failed := failureFromError(err).failed(s.id, "An exception occurred during performance of the job.")
		return verdict{change: &sqlserver.StateChange{JobID: j.jobID, State: failed, Expected: expectProcessing}}
	}

	jc := &JobContext{
		JobID: j.jobID, ServerID: s.id, WorkerID: w.id, Queue: j.fetched.Queue,
		RetryCount: retryCount, CreatedAt: jd.CreatedAt, storage: s.storage,
	}
	start := time.Now()
	latency := start.Sub(jd.CreatedAt)
	err, panicked := s.perform(j, jc, call)
	duration := time.Since(start)

	switch {
	case j.aborted.Load():
		return verdict{} // JobAbortedException: the state already moved on (Worker.cs:287-293)
	case err == nil:
		return verdict{change: &sqlserver.StateChange{JobID: j.jobID, State: hangfire.Succeeded{PerformanceDuration: duration, Latency: latency}, Expected: expectProcessing}}
	case !panicked && s.stopped.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)):
		return verdict{requeue: true} // shutdown, not failure (CoreBackgroundJobPerformer.cs:93-101)
	}
	var f failure
	if p, ok := err.(panicError); ok {
		f = failureFromPanic(p.value, p.stack)
	} else {
		f = failureFromError(err)
	}
	failed := f.failed(s.id, "An exception occurred during performance of the job.")
	t := retryElection(policy, retryCount, failed, f, time.Now())
	return verdict{change: &sqlserver.StateChange{JobID: j.jobID, State: t.state, Traversed: t.traversed, Parameters: t.params, Expected: expectProcessing}}
}

type panicError struct {
	value any
	stack []byte
}

func (p panicError) Error() string { return "panic" }

// perform runs the handler with a context cancelled on shutdown (after
// StopTimeout) or when the cancellation watcher sees the job aborted.
func (s *Server) perform(j *inflight, jc *JobContext, call func(context.Context) error) (err error, panicked bool) {
	ctx, cancel := context.WithCancelCause(context.WithValue(s.stopped, jobContextKey{}, jc))
	defer cancel(nil)
	j.cancel = cancel
	s.mu.Lock()
	s.inflight[j] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.inflight, j)
		s.mu.Unlock()
	}()
	defer func() {
		if v := recover(); v != nil {
			err, panicked = panicError{value: v, stack: debug.Stack()}, true
		}
	}()
	return call(ctx), false
}

// finish applies the verdict unless Shutdown already requeued the row.
func (s *Server) finish(j *inflight, v verdict) {
	f, ok := j.claim()
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()
	requeue := func() {
		if err := s.storage.Requeue(ctx, &f); err != nil {
			s.log.Warn("hangfire: requeue failed; the job runs again after the invisibility timeout", "job", j.jobID, "err", err)
		}
	}
	if v.requeue {
		requeue()
		return
	}
	if v.change != nil {
		c := *v.change
		c.Ack = &f
		applied, err := s.storage.ChangeState(ctx, c)
		if err != nil {
			s.log.Error("hangfire: recording job outcome failed; requeued", "job", j.jobID, "err", err)
			requeue()
			return
		}
		if applied != nil {
			return
		}
	}
	if err := s.storage.RemoveFromQueue(ctx, &f); err != nil {
		s.log.Warn("hangfire: acknowledging job failed", "job", j.jobID, "err", err)
	}
}

// keepAlive refreshes FetchedAt every InvisibilityTimeout/5 until stopped
// (SqlServerTimeoutJob.cs:61,143-170).
func (s *Server) keepAlive(j *inflight) (stop func()) {
	ctx, cancel := context.WithCancel(s.aborted)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for sleep(ctx, s.opts.InvisibilityTimeout/5) {
			j.mu.Lock()
			if !j.claimed {
				if err := s.storage.KeepAlive(ctx, &j.fetched); err != nil && ctx.Err() == nil {
					s.log.Warn("hangfire: keep-alive failed", "job", j.jobID, "err", err)
				}
			}
			j.mu.Unlock()
		}
	}()
	return func() { cancel(); <-done }
}

// checkAborted is ServerJobCancellationWatcher (:33-58) with the check of
// ServerJobCancellationToken.TryCheckJobIsAborted (:163-215): a running job
// whose state is no longer Processing on this server and worker is cancelled.
func (s *Server) checkAborted(ctx context.Context) error {
	s.mu.Lock()
	running := make([]*inflight, 0, len(s.inflight))
	for j := range s.inflight {
		running = append(running, j)
	}
	s.mu.Unlock()
	var errs []error
	for _, j := range running {
		st, err := s.storage.StateData(ctx, j.jobID)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if st != nil && strings.EqualFold(st.Name, hangfire.StateProcessing) &&
			strings.EqualFold(st.Data["ServerId"], s.id) && strings.EqualFold(st.Data["WorkerId"], j.workerID) {
			continue
		}
		s.log.Info("hangfire: job was aborted; cancelling its handler", "job", j.jobID)
		j.aborted.Store(true)
		j.cancel(errAborted)
	}
	return errors.Join(errs...)
}
