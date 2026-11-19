package sqlserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/reski-rukmantiyo/HangFire.GoSDK/hangfire"
)

// Monitor is the read side of IMonitoringApi, with the queries of SqlServerMonitoringApi.cs.
type Monitor struct{ s *Storage }

func NewMonitor(s *Storage) *Monitor { return &Monitor{s: s} }

type Statistics struct {
	Enqueued, Failed, Processing, Scheduled, Awaiting int64
	Servers, Recurring, Retries, Queues               int64
	Succeeded, Deleted                                int64
}

// Statistics is GetStatistics (SqlServerMonitoringApi.cs:447-480).
func (m *Monitor) Statistics(ctx context.Context) (Statistics, error) {
	var st Statistics
	q := m.s.sql(`select
 (select count(Id) from [HF].Job with (nolock, forceseek) where StateName = N'Enqueued'),
 (select count(Id) from [HF].Job with (nolock, forceseek) where StateName = N'Failed'),
 (select count(Id) from [HF].Job with (nolock, forceseek) where StateName = N'Processing'),
 (select count(Id) from [HF].Job with (nolock, forceseek) where StateName = N'Scheduled'),
 (select count(Id) from [HF].Job with (nolock, forceseek) where StateName = N'Awaiting'),
 (select count(Id) from [HF].Server with (nolock)),
 (select isnull(sum(s.[Value]), 0) from (
    select sum([Value]) as [Value] from [HF].Counter with (nolock, forceseek) where [Key] = N'stats:succeeded'
    union all
    select [Value] from [HF].AggregatedCounter with (nolock, forceseek) where [Key] = N'stats:succeeded') as s),
 (select isnull(sum(s.[Value]), 0) from (
    select sum([Value]) as [Value] from [HF].Counter with (nolock, forceseek) where [Key] = N'stats:deleted'
    union all
    select [Value] from [HF].AggregatedCounter with (nolock, forceseek) where [Key] = N'stats:deleted') as s),
 (select count(*) from [HF].[Set] with (nolock, forceseek) where [Key] = N'recurring-jobs'),
 (select count(*) from [HF].[Set] with (nolock, forceseek) where [Key] = N'retries'),
 (select count(distinct Queue) from [HF].JobQueue with (nolock))`)
	err := m.s.db.QueryRowContext(ctx, q).Scan(&st.Enqueued, &st.Failed, &st.Processing, &st.Scheduled, &st.Awaiting,
		&st.Servers, &st.Succeeded, &st.Deleted, &st.Recurring, &st.Retries, &st.Queues)
	return st, err
}

type StateHistory struct {
	StateName string
	Reason    string
	CreatedAt time.Time
	Data      map[string]string
}

type JobDetails struct {
	ID         hangfire.JobID
	CreatedAt  time.Time
	ExpireAt   *time.Time
	Payload    hangfire.Payload
	PayloadErr error
	Properties map[string]string // job parameters, raw JSON values
	History    []StateHistory    // newest first
}

// JobDetails returns nil when the job does not exist (SqlServerMonitoringApi.cs:364-420).
func (m *Monitor) JobDetails(ctx context.Context, id hangfire.JobID) (*JobDetails, error) {
	n, ok := parseJobID(id)
	if !ok {
		return nil, nil
	}
	d := JobDetails{ID: id, Properties: map[string]string{}}
	var invocation, arguments sql.NullString
	var expireAt sql.NullTime
	err := m.s.db.QueryRowContext(ctx, m.s.sql(`select InvocationData, Arguments, CreatedAt, ExpireAt from [HF].Job with (nolock, forceseek) where Id = @id`),
		sql.Named("id", n)).Scan(&invocation, &arguments, &d.CreatedAt, &expireAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if expireAt.Valid {
		d.ExpireAt = &expireAt.Time
	}
	d.Payload, d.PayloadErr = hangfire.DecodePayload(invocation.String, arguments.String)
	rows, err := m.s.db.QueryContext(ctx, m.s.sql(`select Name, Value from [HF].JobParameter with (nolock, forceseek) where JobId = @id`), sql.Named("id", n))
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var name string
		var value sql.NullString
		if err := rows.Scan(&name, &value); err != nil {
			rows.Close()
			return nil, err
		}
		if _, dup := d.Properties[name]; !dup {
			d.Properties[name] = value.String
		}
	}
	if err := closeRows(rows); err != nil {
		return nil, err
	}
	rows, err = m.s.db.QueryContext(ctx, m.s.sql(`select Name, Reason, CreatedAt, Data from [HF].State with (nolock, forceseek) where JobId = @id order by Id desc`), sql.Named("id", n))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var h StateHistory
		var reason, data sql.NullString
		if err := rows.Scan(&h.StateName, &reason, &h.CreatedAt, &data); err != nil {
			return nil, err
		}
		h.Reason = reason.String
		if h.Data, err = decodeStateData(data.String); err != nil {
			return nil, fmt.Errorf("sqlserver: job %s history: %w", id, err)
		}
		d.History = append(d.History, h)
	}
	return &d, rows.Err()
}

func closeRows(rows *sql.Rows) error {
	err := rows.Err()
	if cerr := rows.Close(); err == nil {
		err = cerr
	}
	return err
}

// JobSummary is one entry of a paged job list. Payload, State* and InState
// are zero when the job row has vanished (enqueued lists keep the id).
type JobSummary struct {
	ID           hangfire.JobID
	Payload      hangfire.Payload
	PayloadErr   error
	StateName    string
	InState      bool // the job is still in the listed state
	StateReason  string
	StateChanged *time.Time
	StateData    map[string]string
	ParentState  string // Awaiting lists only: the parent's current state
}

// listOrder is the per-state sort of SqlServerMonitoringApi.cs:90-248.
var listOrder = map[string]string{
	hangfire.StateProcessing: "asc",
	hangfire.StateScheduled:  "asc",
	hangfire.StateAwaiting:   "asc",
	hangfire.StateSucceeded:  "desc",
	hangfire.StateFailed:     "desc",
	hangfire.StateDeleted:    "desc",
}

// JobsByState pages through jobs in one of the listable states.
func (m *Monitor) JobsByState(ctx context.Context, state string, from, count int) ([]JobSummary, error) {
	order, ok := listOrder[state]
	if !ok {
		return nil, fmt.Errorf("sqlserver: no paged list for state %q", state)
	}
	rows, err := m.s.db.QueryContext(ctx, m.s.sql(`;with cte as
(
  select j.Id, row_number() over (order by j.Id `+order+`) as row_num
  from [HF].Job j with (nolock, forceseek)
  where j.StateName = @stateName
)
select j.Id, j.InvocationData, j.Arguments, j.StateName, s.Reason, s.Data, s.CreatedAt
from [HF].Job j with (nolock, forceseek)
inner join cte on cte.Id = j.Id
left join [HF].State s with (nolock, forceseek) on j.StateId = s.Id and j.Id = s.JobId
where cte.row_num between @start and @end
order by cte.row_num`), sql.Named("stateName", state), sql.Named("start", from+1), sql.Named("end", from+count))
	if err != nil {
		return nil, err
	}
	out, err := scanSummaries(rows, state)
	if err != nil || state != hangfire.StateAwaiting {
		return out, err
	}
	return out, m.fillParentStates(ctx, out)
}

func scanSummaries(rows *sql.Rows, listed string) ([]JobSummary, error) {
	defer rows.Close()
	var out []JobSummary
	for rows.Next() {
		var (
			id                                 int64
			inv, args, stateName, reason, data sql.NullString
			changed                            sql.NullTime
		)
		if err := rows.Scan(&id, &inv, &args, &stateName, &reason, &data, &changed); err != nil {
			return nil, err
		}
		js := JobSummary{ID: hangfire.JobID(strconv.FormatInt(id, 10)), StateName: stateName.String, StateReason: reason.String}
		js.InState = strings.EqualFold(js.StateName, listed)
		if changed.Valid {
			js.StateChanged = &changed.Time
		}
		js.Payload, js.PayloadErr = hangfire.DecodePayload(inv.String, args.String)
		if data.Valid {
			var err error
			if js.StateData, err = decodeStateData(data.String); err != nil {
				return nil, err
			}
		}
		out = append(out, js)
	}
	return out, rows.Err()
}

func (m *Monitor) fillParentStates(ctx context.Context, jobs []JobSummary) error {
	for i := range jobs {
		if !jobs[i].InState || jobs[i].StateData["ParentId"] == "" {
			continue
		}
		pid, ok := parseJobID(hangfire.JobID(jobs[i].StateData["ParentId"]))
		if !ok {
			continue
		}
		var name sql.NullString
		err := m.s.db.QueryRowContext(ctx, m.s.sql(`select StateName from [HF].Job with (nolock, forceseek) where Id = @id`), sql.Named("id", pid)).Scan(&name)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		jobs[i].ParentState = name.String
	}
	return nil
}

// EnqueuedJobs and FetchedJobs page through one queue, oldest row first
// (SqlServerJobQueueMonitoringApi.cs:66-100).
func (m *Monitor) EnqueuedJobs(ctx context.Context, queue string, from, count int) ([]JobSummary, error) {
	return m.queueJobs(ctx, queue, "is null", from, count, hangfire.StateEnqueued)
}

func (m *Monitor) FetchedJobs(ctx context.Context, queue string, from, count int) ([]JobSummary, error) {
	return m.queueJobs(ctx, queue, "is not null", from, count, hangfire.StateProcessing)
}

func (m *Monitor) queueJobs(ctx context.Context, queue, fetched string, from, count int, listed string) ([]JobSummary, error) {
	rows, err := m.s.db.QueryContext(ctx, m.s.sql(`select r.JobId from (
  select jq.JobId, row_number() over (order by jq.Id) as row_num
  from [HF].JobQueue jq with (nolock, forceseek)
  where jq.Queue = @queue and jq.FetchedAt `+fetched+`
) as r
where r.row_num between @start and @end
order by r.row_num`), sql.Named("queue", queue), sql.Named("start", from+1), sql.Named("end", from+count))
	if err != nil {
		return nil, err
	}
	ids, err := scanStrings(rows)
	if err != nil {
		return nil, err
	}
	out := make([]JobSummary, 0, len(ids))
	for _, id := range ids {
		n, _ := strconv.ParseInt(id, 10, 64)
		rows, err := m.s.db.QueryContext(ctx, m.s.sql(`select j.Id, j.InvocationData, j.Arguments, j.StateName, s.Reason, s.Data, s.CreatedAt
from [HF].Job j with (nolock, forceseek)
left join [HF].State s with (nolock, forceseek) on s.Id = j.StateId and s.JobId = j.Id
where j.Id = @id`), sql.Named("id", n))
		if err != nil {
			return nil, err
		}
		js, err := scanSummaries(rows, listed)
		if err != nil {
			return nil, err
		}
		if len(js) == 0 {
			js = []JobSummary{{ID: hangfire.JobID(id)}}
		}
		out = append(out, js[0])
	}
	return out, nil
}

type QueueSummary struct {
	Name      string
	Length    int64 // rows not fetched
	Fetched   int64
	FirstJobs []JobSummary // up to 5
}

// Queues is SqlServerMonitoringApi.Queues (:250-279), ordered by name.
func (m *Monitor) Queues(ctx context.Context) ([]QueueSummary, error) {
	rows, err := m.s.db.QueryContext(ctx, m.s.sql(`select Queue,
  sum(case when FetchedAt is null then 1 else 0 end),
  sum(case when FetchedAt is not null then 1 else 0 end)
from [HF].JobQueue with (nolock) group by Queue`))
	if err != nil {
		return nil, err
	}
	var out []QueueSummary
	for rows.Next() {
		var q QueueSummary
		if err := rows.Scan(&q.Name, &q.Length, &q.Fetched); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, q)
	}
	if err := closeRows(rows); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	for i := range out {
		if out[i].FirstJobs, err = m.EnqueuedJobs(ctx, out[i].Name, 0, 5); err != nil {
			return nil, err
		}
	}
	return out, nil
}

type Server struct {
	Name         string
	Heartbeat    *time.Time
	Queues       []string
	StartedAt    time.Time
	WorkersCount int
}

func (m *Monitor) Servers(ctx context.Context) ([]Server, error) {
	rows, err := m.s.db.QueryContext(ctx, m.s.sql(`select Id, Data, LastHeartbeat from [HF].Server with (nolock)`))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Server
	for rows.Next() {
		var sv Server
		var data sql.NullString
		var hb sql.NullTime
		if err := rows.Scan(&sv.Name, &data, &hb); err != nil {
			return nil, err
		}
		if hb.Valid {
			sv.Heartbeat = &hb.Time
		}
		var d struct {
			WorkerCount int
			Queues      []string
			StartedAt   string
		}
		if err := json.Unmarshal([]byte(data.String), &d); err != nil {
			return nil, fmt.Errorf("sqlserver: server %s data: %w", sv.Name, err)
		}
		sv.Queues, sv.WorkersCount = d.Queues, d.WorkerCount
		if d.StartedAt != "" {
			if sv.StartedAt, err = hangfire.ParseTime(d.StartedAt); err != nil {
				return nil, err
			}
		}
		out = append(out, sv)
	}
	return out, rows.Err()
}

type RecurringJob struct {
	ID            hangfire.RecurringJobID
	Removed       bool // listed in the set but the hash is gone
	Cron          string
	Queue         string
	TimeZoneID    string
	Payload       hangfire.Payload
	PayloadErr    error
	NextExecution *time.Time
	LastExecution *time.Time
	CreatedAt     *time.Time
	LastJobID     hangfire.JobID
	LastJobState  string
	Error         string
	RetryAttempt  int
}

// RecurringJobs is StorageConnectionExtensions.GetRecurringJobs over the whole set.
func (m *Monitor) RecurringJobs(ctx context.Context) ([]RecurringJob, error) {
	rows, err := m.s.db.QueryContext(ctx, m.s.sql(`select Value from [HF].[Set] with (forceseek) where [Key] = N'recurring-jobs' order by Score`))
	if err != nil {
		return nil, err
	}
	ids, err := scanStrings(rows)
	if err != nil {
		return nil, err
	}
	out := make([]RecurringJob, 0, len(ids))
	for _, id := range ids {
		h, err := m.s.Hash(ctx, "recurring-job:"+id)
		if err != nil {
			return nil, err
		}
		rj := RecurringJob{ID: hangfire.RecurringJobID(id)}
		if h == nil {
			rj.Removed = true
			out = append(out, rj)
			continue
		}
		rj.Cron, rj.Queue, rj.TimeZoneID, rj.Error = h["Cron"], h["Queue"], h["TimeZoneId"], h["Error"]
		rj.RetryAttempt, _ = strconv.Atoi(h["RetryAttempt"])
		if h["Job"] != "" {
			rj.Payload, rj.PayloadErr = hangfire.DecodePayload(h["Job"], "")
		}
		for key, dst := range map[string]**time.Time{"NextExecution": &rj.NextExecution, "LastExecution": &rj.LastExecution, "CreatedAt": &rj.CreatedAt} {
			if v := h[key]; v != "" {
				if t, err := hangfire.ParseTime(v); err == nil {
					*dst = &t
				}
			}
		}
		if last := strings.TrimSpace(h["LastJobId"]); last != "" {
			rj.LastJobID = hangfire.JobID(last)
			sd, err := m.s.StateData(ctx, rj.LastJobID)
			if err != nil {
				return nil, err
			}
			if sd != nil {
				rj.LastJobState = sd.Name
			}
		}
		out = append(out, rj)
	}
	return out, nil
}

// StatsKind names a statistics history series.
type StatsKind string

const (
	StatsSucceeded StatsKind = "succeeded"
	StatsFailed    StatsKind = "failed"
	StatsDeleted   StatsKind = "deleted"
)

// DailyStats returns the last 7 days of kind from AggregatedCounter, keyed by
// UTC day (GetTimelineStats, :501-530). Only aggregated rows count, as in .NET.
func (m *Monitor) DailyStats(ctx context.Context, kind StatsKind) (map[time.Time]int64, error) {
	end := time.Now().UTC().Truncate(24 * time.Hour)
	keys := map[string]time.Time{}
	for i := range 7 {
		d := end.AddDate(0, 0, -i)
		keys["stats:"+string(kind)+":"+d.Format("2006-01-02")] = d
	}
	return m.timeline(ctx, keys)
}

// HourlyStats returns the last 24 hours of kind, keyed by the hour's instant (:483-499).
func (m *Monitor) HourlyStats(ctx context.Context, kind StatsKind) (map[time.Time]int64, error) {
	end := time.Now().UTC()
	keys := map[string]time.Time{}
	for i := range 24 {
		h := end.Add(-time.Duration(i) * time.Hour)
		keys["stats:"+string(kind)+":"+h.Format("2006-01-02-15")] = h
	}
	return m.timeline(ctx, keys)
}

func (m *Monitor) timeline(ctx context.Context, keys map[string]time.Time) (map[time.Time]int64, error) {
	names := make([]string, 0, len(keys))
	args := make([]any, 0, len(keys))
	out := make(map[time.Time]int64, len(keys))
	for k, t := range keys {
		p := "k" + strconv.Itoa(len(names))
		names = append(names, "@"+p)
		args = append(args, sql.Named(p, k))
		out[t] = 0
	}
	rows, err := m.s.db.QueryContext(ctx, m.s.sql(`select [Key], [Value] from [HF].AggregatedCounter with (nolock, forceseek) where [Key] in (`+strings.Join(names, ",")+`)`), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		var v int64
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		out[keys[k]] = v
	}
	return out, rows.Err()
}
