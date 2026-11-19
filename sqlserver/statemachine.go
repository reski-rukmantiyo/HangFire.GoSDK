package sqlserver

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/reski-rukmantiyo/HangFire.GoSDK/hangfire"
)

// jobRef is what state rules need to know about a job: its id and the
// payload's queue (Job.Queue), which changes where Enqueued and Scheduled write.
type jobRef struct {
	id    hangfire.JobID
	queue string
}

type applyContext struct {
	b     *batch
	job   jobRef
	state hangfire.State // the state being applied; nil for unapply
	now   time.Time
}

// stateRule gathers everything .NET does per state name on a state change:
// the core IStateHandler (GlobalStateHandlers.cs:25-31), the
// AutomaticRetryAttribute apply/unapply hooks (:297-315) and the
// StatisticsHistoryAttribute counters (StatisticsHistoryAttribute.cs:30-61).
type stateRule struct {
	apply   func(applyContext)
	unapply func(applyContext)
	history string // "stats:{history}:..." counters when this state is elected
}

var stateRules = map[string]stateRule{
	strings.ToLower(hangfire.StateEnqueued): {
		apply: func(c applyContext) {
			// EnqueuedState.cs:281-285: a default-queue state defers to Job.Queue.
			queue := queueOrDefault(c.state.(hangfire.Enqueued).Queue)
			if c.job.queue != "" && strings.EqualFold(queue, "default") {
				queue = c.job.queue
			}
			c.b.addToQueue(queue, c.job.id)
		},
	},
	strings.ToLower(hangfire.StateScheduled): {
		apply: func(c applyContext) {
			st := c.state.(hangfire.Scheduled)
			c.b.addToSet("schedule", scheduleMember(c.job), unixSeconds(st.EnqueueAt))
			if hasPrefixFold(st.Reason, "Retry attempt") {
				c.b.addToSet("retries", string(c.job.id), 0)
			}
		},
		unapply: func(c applyContext) {
			c.b.removeFromSet("schedule", scheduleMember(c.job))
			c.b.removeFromSet("retries", string(c.job.id))
		},
	},
	strings.ToLower(hangfire.StateAwaiting): {
		apply: func(c applyContext) {
			// AwaitingState.cs:226-230: SQL Server has the AwaitingJobs feature, so only below 180.
			if c.b.s.opts.Level < hangfire.CompatLevel180 {
				c.b.addToSet("awaiting", string(c.job.id), unixSeconds(c.now))
			}
		},
		unapply: func(c applyContext) { c.b.removeFromSet("awaiting", string(c.job.id)) },
	},
	strings.ToLower(hangfire.StateProcessing): {},
	strings.ToLower(hangfire.StateSucceeded): {
		apply:   func(c applyContext) { c.b.addCounter("stats:succeeded", 1, time.Time{}) },
		unapply: func(c applyContext) { c.b.addCounter("stats:succeeded", -1, time.Time{}) },
		history: "succeeded",
	},
	strings.ToLower(hangfire.StateFailed): {
		unapply: func(c applyContext) { c.b.removeFromSet("retries", string(c.job.id)) },
		history: "failed",
	},
	strings.ToLower(hangfire.StateDeleted): {
		apply:   func(c applyContext) { c.b.addCounter("stats:deleted", 1, time.Time{}) },
		unapply: func(c applyContext) { c.b.addCounter("stats:deleted", -1, time.Time{}) },
		history: "deleted",
	},
}

func ruleFor(name string) stateRule { return stateRules[strings.ToLower(name)] }

// scheduleMember is ScheduledState.cs:177-182: "queue:id" when the job has a queue.
func scheduleMember(j jobRef) string {
	if j.queue == "" {
		return string(j.id)
	}
	return j.queue + ":" + string(j.id)
}

func queueOrDefault(q string) string {
	if q == "" {
		return "default"
	}
	return q
}

func hasPrefixFold(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
}

func isKnownFinal(name string) bool {
	return name == hangfire.StateSucceeded || name == hangfire.StateDeleted
}

// stamp fills the timestamps .NET state constructors set to DateTime.UtcNow.
func stamp(st hangfire.State, now time.Time) hangfire.State {
	switch s := st.(type) {
	case hangfire.Enqueued:
		if s.EnqueuedAt.IsZero() {
			s.EnqueuedAt = now
		}
		return s
	case hangfire.Scheduled:
		if s.ScheduledAt.IsZero() {
			s.ScheduledAt = now
		}
		return s
	case hangfire.Processing:
		if s.StartedAt.IsZero() {
			s.StartedAt = now
		}
		return s
	case hangfire.Succeeded:
		if s.SucceededAt.IsZero() {
			s.SucceededAt = now
		}
		return s
	case hangfire.Failed:
		if s.FailedAt.IsZero() {
			s.FailedAt = now
		}
		return s
	case hangfire.Deleted:
		if s.DeletedAt.IsZero() {
			s.DeletedAt = now
		}
		return s
	}
	return st
}

// StateChange is a request to move a job to State, as StateChangeContext.
type StateChange struct {
	JobID hangfire.JobID
	State hangfire.State
	// Traversed states are recorded in the history before State, as an
	// election filter would (e.g. Failed before a retry's Scheduled).
	Traversed []hangfire.State
	// Expected, when non-empty, lists the states the job must be in (case-insensitive).
	Expected []string
	// Ack, when set, deletes the fetched queue row in the same transaction
	// (transactional acknowledge, BackgroundJobStateChanger.cs CompleteJob).
	Ack *FetchedJob
	// Parameters are raw (JSON) job parameters written under the job lock once
	// the expected state matched, as election filters do with SetJobParameter
	// (AutomaticRetryAttribute.cs:323 sets RetryCount this way).
	Parameters map[string]string
}

// ChangeState applies c under the job's state lock, as BackgroundJobStateChanger.ChangeState.
// It returns the state actually applied, or nil when the job does not exist
// or is not in an expected state.
func (s *Storage) ChangeState(ctx context.Context, c StateChange) (applied hangfire.State, err error) {
	err = s.withSession(ctx, func(se *Session) error {
		applied, err = se.ChangeState(ctx, c)
		return err
	})
	return applied, err
}

func (se *Session) ChangeState(ctx context.Context, c StateChange) (hangfire.State, error) {
	var applied hangfire.State
	err := se.InTx(ctx, func() error {
		if err := se.LockJob(ctx, c.JobID, jobLockTimeout); err != nil {
			return err
		}
		jd, err := se.JobData(ctx, c.JobID)
		if err != nil || jd == nil {
			return err
		}
		if len(c.Expected) > 0 && !slices.ContainsFunc(c.Expected, func(e string) bool { return strings.EqualFold(e, jd.StateName) }) {
			return nil
		}
		for name, v := range c.Parameters {
			if err := se.s.setJobParameter(ctx, se.q(), c.JobID, name, v); err != nil {
				return err
			}
		}
		b := se.s.newBatch()
		job := jobRef{id: c.JobID, queue: jd.Payload.Queue}
		if applied, err = se.transition(ctx, b, job, jd.StateName, c.State, c.Traversed); err != nil {
			return err
		}
		if c.Ack != nil {
			b.removeFromQueue(c.Ack)
		}
		return b.exec(ctx, se.q())
	})
	return applied, err
}

// transition is the one place state changes are applied: election
// (statistics history, then continuations, in filter order), then
// StateMachine/CoreStateMachine.ApplyState. Writes go to b; continuation
// side effects run immediately on the session.
func (se *Session) transition(ctx context.Context, b *batch, job jobRef, oldState string, candidate hangfire.State, traversed []hangfire.State) (hangfire.State, error) {
	now := time.Now().UTC()
	candidate = stamp(candidate, now)

	// StatisticsHistoryAttribute (Order 30) sees the candidate before
	// ContinuationsSupportAttribute (Order 1000) may replace it.
	if h := ruleFor(candidate.StateName()).history; h != "" {
		day := now.Format("2006-01-02")
		hour := now.Format("2006-01-02-15")
		b.addCounter("stats:"+h+":"+day, 1, addMonthsNet(now, 1))
		b.addCounter("stats:"+h+":"+hour, 1, now.Add(24*time.Hour))
	}

	switch c := candidate.(type) {
	case hangfire.Awaiting:
		next, err := se.addContinuation(ctx, job.id, c, now)
		if err != nil {
			return nil, err
		}
		if next != nil {
			traversed = append(slices.Clone(traversed), c)
			candidate = next
		}
	default:
		if isKnownFinal(candidate.StateName()) {
			if err := se.executeContinuations(ctx, job.id, candidate.StateName()); err != nil {
				return nil, err
			}
		}
	}

	for _, t := range traversed {
		b.addJobState(job.id, stamp(t, now), now)
	}
	if r := ruleFor(oldState); r.unapply != nil {
		r.unapply(applyContext{b: b, job: job, now: now})
	}
	b.setJobState(job.id, candidate, now)
	if r := ruleFor(candidate.StateName()); r.apply != nil {
		r.apply(applyContext{b: b, job: job, state: candidate, now: now})
	}
	if candidate.IsFinal() {
		b.expireJob(job.id, now.Add(se.s.opts.JobExpiration))
	} else {
		b.persistJob(job.id)
	}
	return candidate, nil
}

// addMonthsNet is DateTime.AddMonths: the day clamps to the target month's length.
func addMonthsNet(t time.Time, n int) time.Time {
	y, m, d := t.Date()
	first := time.Date(y, m+time.Month(n), 1, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), t.Location())
	last := first.AddDate(0, 1, -1).Day()
	return first.AddDate(0, 0, min(d, last)-1)
}

const continuationNotMet = "Continuation condition was not met"

// addContinuation is ContinuationsSupportAttribute.AddContinuation (:118-185).
// It returns the state that replaces Awaiting when the parent already finished.
func (se *Session) addContinuation(ctx context.Context, child hangfire.JobID, aw hangfire.Awaiting, now time.Time) (hangfire.State, error) {
	parent := aw.ParentID
	if err := se.LockJob(ctx, parent, continuationLockTimeout); err != nil {
		return nil, err
	}
	pd, err := se.JobData(ctx, parent)
	if err != nil {
		return nil, err
	}
	if pd == nil {
		return nil, fmt.Errorf("%w: %s", hangfire.ErrParentNotFound, parent)
	}
	conts, err := hangfire.DecodeContinuations(pd.Parameters["Continuations"])
	if err != nil {
		return nil, err
	}
	if !slices.ContainsFunc(conts, func(c hangfire.Continuation) bool { return c.JobID == child }) {
		conts = append(conts, hangfire.Continuation{JobID: child, Options: aw.Options})
		raw, err := hangfire.EncodeContinuations(conts)
		if err != nil {
			return nil, err
		}
		if err := se.s.setJobParameter(ctx, se.q(), parent, "Continuations", raw); err != nil {
			return nil, err
		}
	}
	ps, err := se.s.stateData(ctx, se.q(), parent)
	if err != nil || ps == nil || !isKnownFinal(ps.Name) {
		return nil, err
	}
	if !shouldStartContinuation(ps.Name, aw.Options) {
		return stamp(hangfire.Deleted{Reason: continuationNotMet}, now), nil
	}
	if aw.NextState == nil {
		return stamp(hangfire.Enqueued{}, now), nil
	}
	return stamp(aw.NextState, now), nil
}

// executeContinuations is ContinuationsSupportAttribute.ExecuteContinuationsIfExist
// (:187-245): every child still Awaiting moves to its next state, or to
// Deleted when its options don't match the parent's final state.
func (se *Session) executeContinuations(ctx context.Context, parent hangfire.JobID, finalState string) error {
	raw, _, err := se.s.jobParameter(ctx, se.q(), parent, "Continuations")
	if err != nil {
		return err
	}
	conts, err := hangfire.DecodeContinuations(raw)
	if err != nil {
		return err
	}
	type move struct {
		id   hangfire.JobID
		next hangfire.State
	}
	var moves []move
	for _, c := range conts {
		if strings.TrimSpace(string(c.JobID)) == "" || slices.ContainsFunc(moves, func(m move) bool { return m.id == c.JobID }) {
			continue
		}
		cs, err := se.s.stateData(ctx, se.q(), c.JobID)
		if err != nil {
			return err
		}
		if cs == nil {
			se.s.log.Warn("hangfire: continuation has no state; skipped", "parent", parent, "continuation", c.JobID)
			continue
		}
		if cs.Name != hangfire.StateAwaiting {
			continue
		}
		var next hangfire.State = hangfire.Deleted{Reason: continuationNotMet}
		if shouldStartContinuation(finalState, c.Options) {
			if next, err = hangfire.DecodeNextState(cs.Data["NextState"]); err != nil {
				next = hangfire.Failed{
					ExceptionType:    "Hangfire.Go.ContinuationDecodeException",
					ExceptionMessage: err.Error(),
					ExceptionDetails: err.Error(),
					Reason:           "An error occurred while deserializing the continuation",
				}
			}
		}
		moves = append(moves, move{c.JobID, next})
	}
	for _, m := range moves {
		if _, err := se.ChangeState(ctx, StateChange{JobID: m.id, State: m.next, Expected: []string{hangfire.StateAwaiting}}); err != nil {
			return err
		}
	}
	return nil
}

// shouldStartContinuation is ContinuationsSupportAttribute.ShouldStartContinuation (:328-345).
func shouldStartContinuation(antecedent string, o hangfire.ContinuationOptions) bool {
	if o == hangfire.OnAnyFinishedState {
		return isKnownFinal(antecedent)
	}
	return o&hangfire.OnlyOnSucceededState != 0 && strings.EqualFold(antecedent, hangfire.StateSucceeded) ||
		o&hangfire.OnlyOnDeletedState != 0 && strings.EqualFold(antecedent, hangfire.StateDeleted)
}
