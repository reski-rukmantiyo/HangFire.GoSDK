package sqlserver

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/reski-rukmantiyo/HangFire.GoSDK/hangfire"
)

// RecurringManager is the SQL Server implementation of hangfire.RecurringManager,
// mirroring RecurringJobManager.
type RecurringManager struct {
	s   *Storage
	now func() time.Time
}

var _ hangfire.RecurringManager = (*RecurringManager)(nil)

func NewRecurringManager(s *Storage) *RecurringManager {
	return &RecurringManager{s: s, now: func() time.Time { return time.Now().UTC() }}
}

// recurringEntity is RecurringJobEntity: the parsed hash plus the original
// fields, so only changed fields are written back. "" stands for .NET null.
type recurringEntity struct {
	id            string
	orig          map[string]string
	queue         string
	cron          string
	timeZoneID    string
	job           string
	misfire       hangfire.MisfireHandling
	createdAt     *time.Time
	nextExecution *time.Time
	lastExecution *time.Time
	version       int // 0 when absent
	lastJobID     string
	retryAttempt  int
	errText       string
}

func newRecurringEntity(id string, hash map[string]string) (*recurringEntity, error) {
	if hash == nil {
		hash = map[string]string{}
	}
	e := &recurringEntity{id: id, orig: hash, timeZoneID: "UTC"}
	nonBlank := func(k string) string {
		if v := hash[k]; strings.TrimSpace(v) != "" {
			return v
		}
		return ""
	}
	e.queue, e.cron, e.job, e.lastJobID, e.errText = nonBlank("Queue"), nonBlank("Cron"), nonBlank("Job"), nonBlank("LastJobId"), nonBlank("Error")
	if tz := nonBlank("TimeZoneId"); tz != "" {
		e.timeZoneID = tz
	}
	for key, dst := range map[string]**time.Time{"CreatedAt": &e.createdAt, "NextExecution": &e.nextExecution, "LastExecution": &e.lastExecution} {
		if v := nonBlank(key); v != "" {
			t, err := hangfire.ParseTime(v)
			if err != nil {
				return nil, fmt.Errorf("sqlserver: recurring job %s %s: %w", id, key, err)
			}
			*dst = &t
		}
	}
	if v, ok := hash["Misfire"]; ok {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > 2 {
			return nil, fmt.Errorf("sqlserver: recurring job %s: misfire option %q is not supported", id, v)
		}
		e.misfire = hangfire.MisfireHandling(n)
	}
	if n, err := strconv.Atoi(hash["RetryAttempt"]); err == nil {
		e.retryAttempt = n
	}
	if n, err := strconv.Atoi(hash["V"]); err == nil {
		e.version = n
	}
	return e, nil
}

// scheduleNext is RecurringJobEntity.ScheduleNext with zero precision, as the manager calls it.
func (e *recurringEntity) scheduleNext(from, now time.Time) error {
	_, err := e.scheduleExecutions(from, now, 0)
	return err
}

// scheduleExecutions is RecurringJobEntity.ScheduleNext (RecurringJobEntity.cs:136-178):
// it returns the occurrences in (from, now] to run under the misfire mode
// and moves NextExecution past now.
func (e *recurringEntity) scheduleExecutions(from, now time.Time, precision time.Duration) ([]time.Time, error) {
	sched, err := hangfire.ParseCron(e.cron, e.timeZoneID)
	if err != nil {
		return nil, err
	}
	var runs []time.Time
	next, ok := from, true
	for {
		if next, ok = sched.Next(next); !ok || next.After(now) {
			break
		}
		switch {
		case next.Equal(now), e.misfire == hangfire.MisfireStrict:
			runs = append(runs, next)
		case e.misfire == hangfire.MisfireRelaxed:
			next = now
			runs = append(runs, next)
		case e.misfire == hangfire.MisfireIgnorable && !next.Before(now.Add(-precision)):
			runs = append(runs, next)
		}
	}
	e.nextExecution = nil
	if ok {
		e.nextExecution = &next
	}
	e.errText = ""
	return runs, nil
}

// changedFields is RecurringJobEntity.GetChangedFields (:206-275), in its order.
func (e *recurringEntity) changedFields(now time.Time, level hangfire.CompatLevel) [][2]string {
	var out [][2]string
	differs := func(key, v string) bool {
		o, ok := e.orig[key]
		return ok && o != v || !ok && v != ""
	}
	for _, f := range [][2]string{{"Queue", e.queue}, {"Cron", e.cron}, {"TimeZoneId", e.timeZoneID}, {"Job", e.job}} {
		if differs(f[0], f[1]) {
			out = append(out, f)
		}
	}
	if _, ok := e.orig["CreatedAt"]; !ok {
		out = append(out, [2]string{"CreatedAt", hangfire.FormatTime(now, level)})
	}
	for _, f := range []struct {
		key string
		t   *time.Time
	}{{"LastExecution", e.lastExecution}, {"NextExecution", e.nextExecution}} {
		v := ""
		if f.t != nil {
			v = hangfire.FormatTime(*f.t, level)
		}
		if differs(f.key, v) {
			out = append(out, [2]string{f.key, v})
		}
	}
	if o := e.orig["LastJobId"]; strings.TrimSpace(o) != e.lastJobID {
		out = append(out, [2]string{"LastJobId", e.lastJobID})
	}
	misfire := strconv.Itoa(int(e.misfire))
	if o, ok := e.orig["Misfire"]; ok && o != misfire || !ok && e.misfire != hangfire.MisfireRelaxed {
		out = append(out, [2]string{"Misfire", misfire})
	}
	if _, ok := e.orig["V"]; !ok {
		out = append(out, [2]string{"V", "2"})
	}
	if o := e.orig["Error"]; strings.TrimSpace(o) != e.errText {
		out = append(out, [2]string{"Error", e.errText})
	}
	attempt := strconv.Itoa(e.retryAttempt)
	if o, ok := e.orig["RetryAttempt"]; ok && o != attempt || !ok && attempt != "0" {
		out = append(out, [2]string{"RetryAttempt", attempt})
	}
	return out
}

// write is RecurringJobExtensions.UpdateRecurringJob (:62-85).
func (e *recurringEntity) write(b *batch, changed [][2]string) {
	if len(changed) > 0 {
		b.setRangeInHash("recurring-job:"+e.id, changed)
	}
	score := -1.0
	if e.nextExecution != nil {
		score = unixSeconds(*e.nextExecution)
	}
	b.addToSet("recurring-jobs", e.id, score)
}

func recurringLock(id hangfire.RecurringJobID) string { return "lock:recurring-job:" + string(id) }

// AddOrUpdate is RecurringJobManager.AddOrUpdate (RecurringJobManager.cs:97-155).
// The queue goes into the hash's Queue field, so the payload carries no "q".
func (m *RecurringManager) AddOrUpdate(ctx context.Context, id hangfire.RecurringJobID, inv hangfire.Invocation, cron string, opts hangfire.RecurringOptions) error {
	if id == "" {
		return fmt.Errorf("sqlserver: empty recurring job id")
	}
	tz := opts.TimeZone
	if tz == "" {
		tz = "UTC"
	}
	if _, err := hangfire.ParseCron(cron, tz); err != nil {
		return err
	}
	queue := queueOrDefault(opts.Queue)
	if err := hangfire.ValidateQueue(queue); err != nil {
		return err
	}
	payload, err := hangfire.EncodePayload(inv, "", true, m.s.opts.Level)
	if err != nil {
		return err
	}
	return m.s.withSession(ctx, func(se *Session) error {
		if err := se.Lock(ctx, recurringLock(id), recurringLockTimeout); err != nil {
			return err
		}
		now := m.now()
		hash, err := m.s.hash(ctx, se.q(), "recurring-job:"+string(id))
		if err != nil {
			return err
		}
		e, err := newRecurringEntity(string(id), hash)
		if err != nil {
			return err
		}
		e.job = payload
		scheduleChanged := false
		if !strings.EqualFold(cron, e.cron) {
			e.cron, scheduleChanged = cron, true
		}
		if !strings.EqualFold(tz, e.timeZoneID) {
			e.timeZoneID, scheduleChanged = tz, true
		}
		e.queue, e.misfire, e.retryAttempt = queue, opts.Misfire, 0
		if scheduleChanged || e.errText != "" {
			if err := e.scheduleNext(now.Add(-time.Second), now); err != nil {
				return err
			}
		}
		changed := e.changedFields(now, m.s.opts.Level)
		if len(changed) == 0 {
			return nil
		}
		b := m.s.newBatch()
		e.write(b, changed)
		return se.InTx(ctx, func() error { return b.exec(ctx, se.q()) })
	})
}

// RemoveIfExists is RecurringJobManager.RemoveIfExists (:229-237).
func (m *RecurringManager) RemoveIfExists(ctx context.Context, id hangfire.RecurringJobID) error {
	return m.s.withSession(ctx, func(se *Session) error {
		if err := se.Lock(ctx, recurringLock(id), recurringLockTimeout); err != nil {
			return err
		}
		b := m.s.newBatch()
		b.removeHash("recurring-job:" + string(id))
		b.removeFromSet("recurring-jobs", string(id))
		return se.InTx(ctx, func() error { return b.exec(ctx, se.q()) })
	})
}

// Trigger is RecurringJobManager.TriggerJob (:166-205). It returns "" when
// the recurring job does not exist. Like .NET 1.8 it also moves NextExecution
// to the next occurrence after now.
func (m *RecurringManager) Trigger(ctx context.Context, id hangfire.RecurringJobID) (hangfire.JobID, error) {
	var jobID hangfire.JobID
	err := m.s.withSession(ctx, func(se *Session) error {
		if err := se.Lock(ctx, recurringLock(id), recurringLockTimeout); err != nil {
			return err
		}
		now := m.now()
		hash, err := m.s.hash(ctx, se.q(), "recurring-job:"+string(id))
		if err != nil || hash == nil {
			return err
		}
		e, err := newRecurringEntity(string(id), hash)
		if err != nil {
			return err
		}
		nj, err := m.s.recurringNewJob(e, now, "Triggered using recurring job manager")
		if err != nil {
			return err
		}
		if err := e.scheduleNext(now, now); err != nil {
			return err
		}
		return se.InTx(ctx, func() error {
			if jobID, _, err = se.CreateJob(ctx, nj); err != nil {
				return err
			}
			e.lastExecution, e.lastJobID = &now, string(jobID)
			b := m.s.newBatch()
			e.write(b, e.changedFields(now, m.s.opts.Level))
			return b.exec(ctx, se.q())
		})
	})
	return jobID, err
}

// recurringNewJob is RecurringJobExtensions.TriggerRecurringJob (:96-121)
// plus EnqueueBackgroundJob (:123-150): the hash payload re-split into
// InvocationData and Arguments, and an Enqueued state on the hash queue.
func (s *Storage) recurringNewJob(e *recurringEntity, at time.Time, reason string) (NewJob, error) {
	if e.job == "" {
		return NewJob{}, fmt.Errorf("sqlserver: recurring job %s: the 'Job' field has a null or empty value", e.id)
	}
	p, err := hangfire.DecodePayload(e.job, "")
	if err != nil {
		return NewJob{}, err
	}
	args := make([]any, len(p.Args))
	for i, a := range p.Args {
		if a != nil {
			args[i] = json.RawMessage(a)
		}
	}
	inv := hangfire.Invocation{Type: p.Type, Method: p.Method, ParameterTypes: p.ParameterTypes, Args: args}
	return s.EncodeJob(inv, p.Queue,
		map[string]any{"RecurringJobId": e.id, "Time": at.Unix()},
		hangfire.Enqueued{Queue: e.queue, Reason: reason})
}
