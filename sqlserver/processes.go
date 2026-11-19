package sqlserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/reski-rukmantiyo/HangFire.GoSDK/hangfire"
)

// Background-process passes. Each takes the same distributed lock as its
// .NET counterpart, so Go and .NET servers can share one database. The
// caller owns the polling interval.

const (
	processBatchSize       = 1000            // DelayedJobScheduler.cs:75, RecurringJobScheduler.cs:69, ExpirationManager.cs:39
	schedulerLockTimeout   = time.Minute     // DelayedJobScheduler.cs:74, RecurringJobScheduler.cs:68
	expirationLockTimeout  = 5 * time.Minute // ExpirationManager.cs:33
	recurringMaxRetries    = 5               // RecurringJobScheduler.cs:70
	recurringMaxVersion    = 2               // RecurringJobScheduler.cs:71
	aggregatorPassDelay    = 500 * time.Millisecond
	jobLockPoisonedTimeout = 5 * time.Second // DelayedJobScheduler.cs:319
)

// UtcNow is the database clock (JobStorageConnection.GetUtcDateTime), which
// the schedulers use so servers with skewed clocks agree on what is due.
func (s *Storage) UtcNow(ctx context.Context) (time.Time, error) {
	return s.utcNow(ctx, s.db)
}

func (s *Storage) utcNow(ctx context.Context, q querier) (time.Time, error) {
	var t time.Time
	err := q.QueryRowContext(ctx, `select getutcdate()`).Scan(&t)
	return t.UTC(), err
}

func (s *Storage) rangeByScore(ctx context.Context, q querier, key string, to time.Time) ([]string, error) {
	rows, err := q.QueryContext(ctx, s.sql(`select top (@count) Value from [HF].[Set] with (forceseek) where [Key] = @key and Score between @from and @to order by Score`),
		sql.Named("count", processBatchSize), sql.Named("key", key), sql.Named("from", 0.0), sql.Named("to", unixSeconds(to)))
	if err != nil {
		return nil, err
	}
	return scanStrings(rows)
}

// EnqueueDueScheduledJobs is one DelayedJobScheduler.EnqueueNextScheduledJobs
// pass (DelayedJobScheduler.cs:165-262). It returns how many schedule
// entries it handled, or ErrLockTimeout when another server holds the lock.
func (s *Storage) EnqueueDueScheduledJobs(ctx context.Context) (int, error) {
	handled := 0
	err := s.withSession(ctx, func(se *Session) error {
		if err := se.Lock(ctx, "locks:schedulepoller", schedulerLockTimeout); err != nil {
			return err
		}
		now, err := s.utcNow(ctx, se.q())
		if err != nil {
			return err
		}
		entries, err := s.rangeByScore(ctx, se.q(), "schedule", now)
		if err != nil {
			return err
		}
		b := s.newBatch()
		for _, entry := range entries {
			if ctx.Err() != nil {
				break
			}
			handled++
			// "queue:id" entries (jobs with a payload queue) move straight to
			// their queue in the Scheduled state, as EnqueueEntry does (:264-273).
			if queue, id, ok := strings.Cut(entry, ":"); ok {
				b.removeFromSet("schedule", entry)
				b.addToQueue(queue, hangfire.JobID(id))
				continue
			}
			if err := s.enqueueScheduled(ctx, hangfire.JobID(entry)); err != nil {
				return err
			}
		}
		return se.InTx(ctx, func() error { return b.exec(ctx, se.q()) })
	})
	return handled, err
}

// enqueueScheduled is DelayedJobScheduler.EnqueueBackgroundJob (:291-346).
// Each job gets its own session so its state lock is released right away.
func (s *Storage) enqueueScheduled(ctx context.Context, id hangfire.JobID) error {
	applied, err := s.ChangeState(ctx, StateChange{
		JobID:    id,
		State:    hangfire.Enqueued{Reason: "Triggered by DelayedJobScheduler"},
		Expected: []string{hangfire.StateScheduled},
	})
	if err != nil || applied != nil {
		return err
	}
	// A missing or no-longer-scheduled job would poison the schedule, so its
	// entry is removed by hand.
	return s.withSession(ctx, func(se *Session) error {
		if err := se.LockJob(ctx, id, jobLockPoisonedTimeout); err != nil {
			return err
		}
		jd, err := se.JobData(ctx, id)
		if err != nil {
			return err
		}
		if jd != nil && strings.EqualFold(jd.StateName, hangfire.StateScheduled) {
			return nil
		}
		s.log.Warn("hangfire: background job removed from the schedule, because it's expired or its state was changed", "job", id)
		b := s.newBatch()
		b.removeFromSet("schedule", string(id))
		return b.exec(ctx, se.q())
	})
}

// TriggerDueRecurringJobs is one RecurringJobScheduler.EnqueueNextRecurringJobs
// pass (RecurringJobScheduler.cs:206-280). pollingDelay is the scheduler's
// polling interval; misfire precision is twice that (:351).
func (s *Storage) TriggerDueRecurringJobs(ctx context.Context, pollingDelay time.Duration) (int, error) {
	handled := 0
	err := s.withSession(ctx, func(se *Session) error {
		if err := se.Lock(ctx, "recurring-jobs:lock", schedulerLockTimeout); err != nil {
			return err
		}
		now, err := s.utcNow(ctx, se.q())
		if err != nil {
			return err
		}
		ids, err := s.rangeByScore(ctx, se.q(), "recurring-jobs", now)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if ctx.Err() != nil {
				break
			}
			if err := s.scheduleRecurringJob(ctx, id, now, pollingDelay); err != nil {
				return err
			}
			handled++
		}
		return nil
	})
	return handled, err
}

// scheduleRecurringJob is TryEnqueueBackgroundJob plus ScheduleRecurringJob
// (:282-372): create a job per due execution, then write the entity back.
// Errors in creating jobs are recorded on the entity and retried later.
func (s *Storage) scheduleRecurringJob(ctx context.Context, id string, now time.Time, pollingDelay time.Duration) error {
	return s.withSession(ctx, func(se *Session) error {
		if err := se.Lock(ctx, recurringLock(hangfire.RecurringJobID(id)), schedulerLockTimeout); err != nil {
			return err
		}
		hash, err := s.hash(ctx, se.q(), "recurring-job:"+id)
		if err != nil {
			return err
		}
		b := s.newBatch()
		if hash == nil {
			b.removeFromSet("recurring-jobs", id)
			return b.exec(ctx, se.q())
		}
		e, err := newRecurringEntity(id, hash)
		if err != nil {
			return err
		}
		return se.InTx(ctx, func() error {
			if err := s.runRecurringExecutions(ctx, se, e, now, pollingDelay); err != nil {
				delay := pollingDelay
				if delay <= 0 {
					delay = time.Minute
				}
				if e.retryAttempt < recurringMaxRetries {
					s.log.Warn("hangfire: recurring job can't be scheduled and will be retried", "recurring_job", id, "in", delay, "err", err)
					next := now.Add(delay)
					e.retryAttempt, e.errText, e.nextExecution = e.retryAttempt+1, err.Error(), &next
				} else {
					s.log.Error("hangfire: recurring job can't be scheduled and will be disabled", "recurring_job", id, "err", err)
					e.errText, e.nextExecution = err.Error(), nil
				}
			} else {
				e.retryAttempt = 0
			}
			e.write(b, e.changedFields(now, s.opts.Level))
			return b.exec(ctx, se.q())
		})
	})
}

func (s *Storage) runRecurringExecutions(ctx context.Context, se *Session, e *recurringEntity, now time.Time, pollingDelay time.Duration) error {
	if e.version > recurringMaxVersion {
		return fmt.Errorf("can't process recurring job '%s' of version '%d'. Max supported version of this server is '%d'", e.id, e.version, recurringMaxVersion)
	}
	from := now.Add(-time.Second)
	if e.lastExecution != nil {
		from = *e.lastExecution
	} else if e.createdAt != nil {
		from = e.createdAt.Add(-time.Second)
	}
	runs, err := e.scheduleExecutions(from, now, 2*pollingDelay)
	if err != nil {
		return err
	}
	for _, at := range runs {
		nj, err := s.recurringNewJob(e, at, "Triggered by recurring job scheduler")
		if err != nil {
			return err
		}
		jobID, _, err := se.CreateJob(ctx, nj)
		if err != nil {
			return err
		}
		e.lastExecution, e.lastJobID = &at, string(jobID)
	}
	return nil
}

var expiringTables = []string{"AggregatedCounter", "Job", "List", "Set", "Hash"} // ExpirationManager.cs:41-48

// DeleteExpired is one ExpirationManager pass (ExpirationManager.cs:93-200):
// rows past ExpireAt are deleted in batches, table by table, under
// "locks:expirationmanager". A table whose lock is held elsewhere is skipped.
func (s *Storage) DeleteExpired(ctx context.Context) error {
	var errs []error
	for _, table := range expiringTables {
		hint := " with (forceseek)"
		if table == "AggregatedCounter" {
			hint = ""
		}
		query := s.sql(`set deadlock_priority low;
set transaction isolation level read committed;
set xact_abort on;
set lock_timeout 1000;
delete top (@count) T from [HF].[` + table + `] T` + hint + `
where ExpireAt < @now
option (loop join, optimize for (@count = 20000));`)
		err := s.withSession(ctx, func(se *Session) error {
			if err := se.Lock(ctx, "locks:expirationmanager", expirationLockTimeout); err != nil {
				return err
			}
			for {
				r, err := se.q().ExecContext(ctx, query, sql.Named("count", processBatchSize), sql.Named("now", sqlTime(time.Now())))
				if err != nil {
					if strings.Contains(err.Error(), "Lock request time out period exceeded") {
						return nil
					}
					return err
				}
				if n, _ := r.RowsAffected(); n < processBatchSize {
					return nil
				}
			}
		})
		if err != nil && !errors.Is(err, hangfire.ErrLockTimeout) && ctx.Err() == nil {
			errs = append(errs, fmt.Errorf("sqlserver: cleaning up %s: %w", table, err))
		}
	}
	return errors.Join(errs...)
}

// AggregateCounters is one CountersAggregator pass (CountersAggregator.cs:63-128):
// Counter rows merge into AggregatedCounter in batches. It takes no
// distributed lock; READPAST keeps concurrent aggregators apart.
func (s *Storage) AggregateCounters(ctx context.Context) error {
	query := s.sql(`DECLARE @RecordsToAggregate TABLE
(
	[Key] NVARCHAR(100) COLLATE DATABASE_DEFAULT NOT NULL,
	[Value] INT NOT NULL,
	[ExpireAt] DATETIME NULL
)

SET XACT_ABORT ON
SET TRANSACTION ISOLATION LEVEL READ COMMITTED
SET DEADLOCK_PRIORITY LOW
BEGIN TRAN

DELETE TOP (@count) C
OUTPUT DELETED.[Key], DELETED.[Value], DELETED.[ExpireAt] INTO @RecordsToAggregate
FROM [HF].[Counter] C WITH (READPAST, XLOCK, INDEX(0))

DECLARE @removed INT = @@ROWCOUNT
SET NOCOUNT ON

;MERGE [HF].[AggregatedCounter] WITH (FORCESEEK, HOLDLOCK) AS [Target]
USING (
	SELECT [Key], SUM([Value]) as [Value], MAX([ExpireAt]) AS [ExpireAt] FROM @RecordsToAggregate
	GROUP BY [Key]) AS [Source] ([Key], [Value], [ExpireAt])
ON [Target].[Key] COLLATE DATABASE_DEFAULT = [Source].[Key] COLLATE DATABASE_DEFAULT
WHEN MATCHED THEN UPDATE SET
	[Target].[Value] = [Target].[Value] + [Source].[Value],
	[Target].[ExpireAt] = (SELECT MAX([ExpireAt]) FROM (VALUES ([Source].ExpireAt), ([Target].[ExpireAt])) AS MaxExpireAt([ExpireAt]))
WHEN NOT MATCHED THEN INSERT ([Key], [Value], [ExpireAt]) VALUES ([Source].[Key], [Source].[Value], [Source].[ExpireAt]);

COMMIT TRAN
SELECT @removed`)
	for {
		var removed int
		if err := s.db.QueryRowContext(ctx, query, sql.Named("count", processBatchSize)).Scan(&removed); err != nil {
			return err
		}
		if removed < processBatchSize {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(aggregatorPassDelay):
		}
	}
}
