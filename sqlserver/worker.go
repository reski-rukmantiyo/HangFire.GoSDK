package sqlserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/reski-rukmantiyo/HangFire.GoSDK/hangfire"
)

// FetchedJob is a JobQueue row claimed by the sliding-invisibility fetch (SRS DR-16).
type FetchedJob struct {
	ID        int64
	JobID     hangfire.JobID
	Queue     string
	FetchedAt time.Time
}

// ErrServerGone is returned by Heartbeat when the server row was removed,
// as BackgroundServerGoneException.
var ErrServerGone = errors.New("sqlserver: server row is gone")

// FetchNext claims one queued job, or returns nil when none is visible
// (SqlServerJobQueue.cs:213-221). The caller polls.
func (s *Storage) FetchNext(ctx context.Context, queues []string, invisibility time.Duration) (*FetchedJob, error) {
	if len(queues) == 0 {
		return nil, errors.New("sqlserver: no queues")
	}
	names := make([]string, len(queues))
	args := []any{sql.Named("timeoutSs", -int(invisibility/time.Second))}
	for i, q := range queues {
		names[i] = "@q" + strconv.Itoa(i)
		args = append(args, sql.Named("q"+strconv.Itoa(i), q))
	}
	var f FetchedJob
	var jobID int64
	err := s.db.QueryRowContext(ctx, s.sql(`set nocount on;set xact_abort on;set tran isolation level read committed;
update top (1) JQ
set FetchedAt = GETUTCDATE()
output INSERTED.Id, INSERTED.JobId, INSERTED.Queue, INSERTED.FetchedAt
from [HF].JobQueue JQ with (forceseek, readpast, updlock, rowlock)
where Queue in (`+strings.Join(names, ",")+`) and
(FetchedAt is null or FetchedAt < DATEADD(second, @timeoutSs, GETUTCDATE()));`), args...).Scan(&f.ID, &jobID, &f.Queue, &f.FetchedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	f.JobID = hangfire.JobID(strconv.FormatInt(jobID, 10))
	return &f, nil
}

// KeepAlive refreshes FetchedAt (SqlServerTimeoutJob.cs:162). It returns
// ErrJobNotFound when the row is gone or was re-fetched by someone else.
func (s *Storage) KeepAlive(ctx context.Context, f *FetchedJob) error {
	var at time.Time
	err := s.db.QueryRowContext(ctx, s.sql(`update JQ set FetchedAt = getutcdate() output INSERTED.FetchedAt from [HF].JobQueue JQ with (forceseek, rowlock) where Queue = @queue and Id = @id and FetchedAt = @fetchedAt`),
		sql.Named("queue", f.Queue), sql.Named("id", f.ID), sql.Named("fetchedAt", sqlTime(f.FetchedAt))).Scan(&at)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: queue row %d lost", hangfire.ErrJobNotFound, f.ID)
	}
	if err == nil {
		f.FetchedAt = at
	}
	return err
}

// RemoveFromQueue acknowledges a fetched job (SqlServerTimeoutJob.cs:83).
func (s *Storage) RemoveFromQueue(ctx context.Context, f *FetchedJob) error {
	b := s.newBatch()
	b.removeFromQueue(f)
	return b.exec(ctx, s.db)
}

// Requeue makes a fetched job visible again (SqlServerTimeoutJob.cs:104).
func (s *Storage) Requeue(ctx context.Context, f *FetchedJob) error {
	_, err := s.db.ExecContext(ctx, s.sql(`update JQ set FetchedAt = null from [HF].JobQueue JQ with (forceseek, rowlock) where Queue = @queue and Id = @id and FetchedAt = @fetchedAt`),
		sql.Named("queue", f.Queue), sql.Named("id", f.ID), sql.Named("fetchedAt", sqlTime(f.FetchedAt)))
	return err
}

// ServerData is the Server.Data JSON (SS/Entities/ServerData.cs).
type ServerData struct {
	WorkerCount int
	Queues      []string
	StartedAt   time.Time
}

func (d ServerData) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		WorkerCount int
		Queues      []string
		StartedAt   string
	}{d.WorkerCount, d.Queues, d.StartedAt.UTC().Format("2006-01-02T15:04:05.9999999Z")})
}

// AnnounceServer registers or refreshes a server row (SqlServerConnection.cs:505-525).
func (s *Storage) AnnounceServer(ctx context.Context, id string, d ServerData) error {
	data, err := json.Marshal(d)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, s.sql(`;merge [HF].Server with (holdlock) as Target
using (VALUES (@id, @data, sysutcdatetime())) as Source (Id, Data, Heartbeat)
on Target.Id = Source.Id
when matched then update set Data = Source.Data, LastHeartbeat = Source.Heartbeat
when not matched then insert (Id, Data, LastHeartbeat) values (Source.Id, Source.Data, Source.Heartbeat);`),
		sql.Named("id", id), sql.Named("data", string(data)))
	return err
}

func (s *Storage) Heartbeat(ctx context.Context, id string) error {
	r, err := s.db.ExecContext(ctx, s.sql(`update [HF].Server set LastHeartbeat = sysutcdatetime() where Id = @id`), sql.Named("id", id))
	if err != nil {
		return err
	}
	if n, _ := r.RowsAffected(); n == 0 {
		return ErrServerGone
	}
	return nil
}

func (s *Storage) RemoveServer(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, s.sql(`delete S from [HF].Server S with (forceseek) where Id = @id`), sql.Named("id", id))
	return err
}

func (s *Storage) RemoveTimedOutServers(ctx context.Context, timeout time.Duration) (int64, error) {
	r, err := s.db.ExecContext(ctx, s.sql(`delete s from [HF].Server s with (readpast, readcommitted) where LastHeartbeat < dateadd(ms, @timeoutMsNeg, sysutcdatetime())`),
		sql.Named("timeoutMsNeg", -timeout.Milliseconds()))
	if err != nil {
		return 0, err
	}
	return r.RowsAffected()
}

// RangeByScore returns up to count set members with from <= score <= to,
// lowest score first (GetFirstByLowestScoreFromSet). Schedulers read due
// "schedule" and "recurring-jobs" members with it.
func (s *Storage) RangeByScore(ctx context.Context, key string, from, to float64, count int) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, s.sql(`select top (@count) Value from [HF].[Set] with (forceseek) where [Key] = @key and Score between @from and @to order by Score`),
		sql.Named("count", count), sql.Named("key", key), sql.Named("from", from), sql.Named("to", to))
	if err != nil {
		return nil, err
	}
	return scanStrings(rows)
}

// Hash returns all fields of a hash, or nil when it does not exist.
func (s *Storage) Hash(ctx context.Context, key string) (map[string]string, error) {
	return s.hash(ctx, s.db, key)
}

func (s *Storage) hash(ctx context.Context, q querier, key string) (map[string]string, error) {
	rows, err := q.QueryContext(ctx, s.sql(`select Field, Value from [HF].Hash with (forceseek) where [Key] = @key`), sql.Named("key", key))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var m map[string]string
	for rows.Next() {
		var f string
		var v sql.NullString
		if err := rows.Scan(&f, &v); err != nil {
			return nil, err
		}
		if m == nil {
			m = map[string]string{}
		}
		m[f] = v.String
	}
	return m, rows.Err()
}

// Counter sums Counter and AggregatedCounter rows for key (GetCounter).
func (s *Storage) Counter(ctx context.Context, key string) (int64, error) {
	var v sql.NullInt64
	err := s.db.QueryRowContext(ctx, s.sql(`select sum(s.[Value]) from (select sum([Value]) as [Value] from [HF].Counter with (forceseek)
where [Key] = @key
union all
select [Value] from [HF].AggregatedCounter with (forceseek)
where [Key] = @key) as s`), sql.Named("key", key)).Scan(&v)
	return v.Int64, err
}

func scanStrings(rows *sql.Rows) ([]string, error) {
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
