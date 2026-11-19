package sqlserver_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/reski-rukmantiyo/HangFire.GoSDK/hangfire"
	"github.com/reski-rukmantiyo/HangFire.GoSDK/internal/sqltest"
	"github.com/reski-rukmantiyo/HangFire.GoSDK/sqlserver"
)

// These tests need a SQL Server: set HANGFIRE_SQL (see internal/sqltest).
// One database is installed for the package and dropped at the end.

var testDB *sqltest.DB

func TestMain(m *testing.M) {
	base := os.Getenv("HANGFIRE_SQL")
	if base == "" {
		os.Exit(m.Run())
	}
	wd, _ := os.Getwd()
	d, drop, err := sqltest.Create(base, wd+"/..")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	testDB = d
	code := m.Run()
	if os.Getenv("HANGFIRE_KEEP_DB") == "" {
		if err := drop(); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	} else {
		fmt.Fprintln(os.Stderr, "kept database", d.Name)
	}
	os.Exit(code)
}

func setup(t *testing.T, level hangfire.CompatLevel) (*sqlserver.Storage, *sqlserver.Client) {
	t.Helper()
	if testDB == nil {
		t.Skip("HANGFIRE_SQL not set")
	}
	s, err := sqlserver.NewStorage(context.Background(), testDB.DB, sqlserver.Options{Level: level})
	if err != nil {
		t.Fatal(err)
	}
	return s, sqlserver.NewClient(s)
}

var recordInv = hangfire.Invocation{
	Type: "E2E.Jobs.Recorder, E2E", Method: "RecordInt",
	ParameterTypes: []string{"System.String", "System.Int32"},
	Args:           []any{"tag", 42},
}

type jobRow struct {
	InvocationData, Arguments, StateName string
	ExpireAt                             sql.NullTime
}

func readJob(t *testing.T, id hangfire.JobID) jobRow {
	t.Helper()
	var r jobRow
	var state sql.NullString
	err := testDB.QueryRow(`select InvocationData, Arguments, StateName, ExpireAt from HangFire.Job where Id = @p1`, string(id)).
		Scan(&r.InvocationData, &r.Arguments, &state, &r.ExpireAt)
	if err != nil {
		t.Fatalf("job %s: %v", id, err)
	}
	r.StateName = state.String
	return r
}

type stateRow struct{ Name, Reason, Data string }

func readStates(t *testing.T, id hangfire.JobID) []stateRow {
	t.Helper()
	rows, err := testDB.Query(`select Name, isnull(Reason, ''), Data from HangFire.State where JobId = @p1 order by Id`, string(id))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []stateRow
	for rows.Next() {
		var r stateRow
		if err := rows.Scan(&r.Name, &r.Reason, &r.Data); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	return out
}

func stateNames(rows []stateRow) []string {
	var out []string
	for _, r := range rows {
		out = append(out, r.Name)
	}
	return out
}

func queryStrings(t *testing.T, q string, args ...any) []string {
	t.Helper()
	rows, err := testDB.Query(q, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	return out
}

func counter(t *testing.T, key string) int64 {
	t.Helper()
	var n sql.NullInt64
	if err := testDB.QueryRow(`select sum(Value) from HangFire.Counter where [Key] = @p1`, key).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n.Int64
}

func TestSchemaChecks(t *testing.T) {
	setup(t, hangfire.CompatLevel180)
	ctx := context.Background()
	if _, err := sqlserver.NewStorage(ctx, testDB.DB, sqlserver.Options{Schema: "Nope", Level: 180}); !errors.Is(err, hangfire.ErrSchemaVersion) {
		t.Errorf("missing schema: got %v, want ErrSchemaVersion", err)
	}
	if _, err := sqlserver.NewStorage(ctx, testDB.DB, sqlserver.Options{Schema: "HangFire]; drop table x;--", Level: 180}); err == nil {
		t.Error("schema name with SQL accepted")
	}
	if _, err := sqlserver.NewStorage(ctx, testDB.DB, sqlserver.Options{Level: 110}); err == nil {
		t.Error("level 110 accepted")
	}
}

func TestEnqueueWritesRows(t *testing.T) {
	_, c := setup(t, hangfire.CompatLevel180)
	id, err := c.Enqueue(context.Background(), recordInv, hangfire.WithQueue("critical"), hangfire.WithParameter("Tenant", "acme"))
	if err != nil {
		t.Fatal(err)
	}
	j := readJob(t, id)
	if want := `{"t":"E2E.Jobs.Recorder, E2E","m":"RecordInt","p":["System.String","System.Int32"],"q":"critical"}`; j.InvocationData != want {
		t.Errorf("InvocationData = %s, want %s", j.InvocationData, want)
	}
	if want := `["\"tag\"","42"]`; j.Arguments != want {
		t.Errorf("Arguments = %s, want %s", j.Arguments, want)
	}
	if j.StateName != "Enqueued" || j.ExpireAt.Valid {
		t.Errorf("StateName=%q ExpireAt=%v, want Enqueued and NULL", j.StateName, j.ExpireAt)
	}
	st := readStates(t, id)
	if len(st) != 1 || !strings.HasPrefix(st[0].Data, `{"EnqueuedAt":"`) || !strings.HasSuffix(st[0].Data, `","Queue":"default"}`) {
		t.Errorf("states = %+v", st)
	}
	if q := queryStrings(t, `select Queue from HangFire.JobQueue where JobId = @p1`, string(id)); !slices.Equal(q, []string{"critical"}) {
		t.Errorf("JobQueue = %v, want [critical]", q)
	}
	if p := queryStrings(t, `select Value from HangFire.JobParameter where JobId = @p1 and Name = 'Tenant'`, string(id)); !slices.Equal(p, []string{`"acme"`}) {
		t.Errorf("Tenant parameter = %v", p)
	}
}

func TestScheduleUsesQueueMember(t *testing.T) {
	_, c := setup(t, hangfire.CompatLevel180)
	ctx := context.Background()
	at := time.Unix(1900000000, 0)
	plain, err := c.ScheduleAt(ctx, recordInv, at)
	if err != nil {
		t.Fatal(err)
	}
	queued, err := c.ScheduleAt(ctx, recordInv, at, hangfire.WithQueue("critical"))
	if err != nil {
		t.Fatal(err)
	}
	for member, id := range map[string]hangfire.JobID{string(plain): plain, "critical:" + string(queued): queued} {
		scores := queryStrings(t, `select convert(nvarchar, convert(bigint, Score)) from HangFire.[Set] where [Key] = 'schedule' and Value = @p1`, member)
		if !slices.Equal(scores, []string{"1900000000"}) {
			t.Errorf("schedule member %q scores = %v", member, scores)
		}
		if j := readJob(t, id); j.StateName != "Scheduled" {
			t.Errorf("job %s state %s", id, j.StateName)
		}
	}
}

func TestContinuationLifecycle(t *testing.T) {
	s, c := setup(t, hangfire.CompatLevel180)
	ctx := context.Background()
	parent, err := c.Enqueue(ctx, recordInv)
	if err != nil {
		t.Fatal(err)
	}
	child, err := c.ContinueJobWith(ctx, parent, recordInv, nil, hangfire.OnlyOnSucceededState, hangfire.WithQueue("critical"))
	if err != nil {
		t.Fatal(err)
	}
	if j := readJob(t, child); j.StateName != "Awaiting" {
		t.Fatalf("child state %s", j.StateName)
	}
	conts := queryStrings(t, `select Value from HangFire.JobParameter where JobId = @p1 and Name = 'Continuations'`, string(parent))
	if want := fmt.Sprintf(`[{"JobId":"%s","Options":1}]`, child); !slices.Equal(conts, []string{want}) {
		t.Errorf("Continuations = %v, want %s", conts, want)
	}
	if st := readStates(t, child); st[0].Data != fmt.Sprintf(`{"ParentId":"%s","NextState":"{\"$type\":\"Hangfire.States.EnqueuedState, Hangfire.Core\",\"Queue\":\"default\"}","Options":"1"}`, parent) {
		t.Errorf("Awaiting data = %s", st[0].Data)
	}

	succeededBefore := counter(t, "stats:succeeded")
	ok, err := c.ChangeState(ctx, parent, hangfire.Succeeded{PerformanceDuration: 5 * time.Millisecond}, "Enqueued")
	if err != nil || !ok {
		t.Fatalf("parent -> Succeeded: %v %v", ok, err)
	}
	if got := counter(t, "stats:succeeded") - succeededBefore; got != 1 {
		t.Errorf("stats:succeeded delta = %d", got)
	}
	day := "stats:succeeded:" + time.Now().UTC().Format("2006-01-02")
	if counter(t, day) < 1 {
		t.Errorf("%s not incremented", day)
	}
	if j := readJob(t, parent); !j.ExpireAt.Valid || time.Until(j.ExpireAt.Time) < 23*time.Hour {
		t.Errorf("parent ExpireAt = %v, want ~now+1d", j.ExpireAt)
	}
	if names := stateNames(readStates(t, child)); !slices.Equal(names, []string{"Awaiting", "Enqueued"}) {
		t.Errorf("child history = %v", names)
	}
	if q := queryStrings(t, `select Queue from HangFire.JobQueue where JobId = @p1`, string(child)); !slices.Equal(q, []string{"critical"}) {
		t.Errorf("child queued on %v, want [critical] from its payload", q)
	}

	late, err := c.ContinueJobWith(ctx, parent, recordInv, nil, hangfire.OnlyOnDeletedState)
	if err != nil {
		t.Fatal(err)
	}
	st := readStates(t, late)
	if names := stateNames(st); !slices.Equal(names, []string{"Awaiting", "Deleted"}) || st[1].Reason != "Continuation condition was not met" {
		t.Errorf("late continuation history = %+v", st)
	}
	if _, err := c.ContinueJobWith(ctx, "999999999", recordInv, nil, 0); !errors.Is(err, hangfire.ErrParentNotFound) {
		t.Errorf("missing parent: %v", err)
	}
	if sd, _ := s.StateData(ctx, child); sd == nil || sd.Name != "Enqueued" {
		t.Errorf("StateData(child) = %+v", sd)
	}
}

func TestManagementAndRetries(t *testing.T) {
	s, c := setup(t, hangfire.CompatLevel180)
	ctx := context.Background()
	id, err := c.Enqueue(ctx, recordInv)
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := c.ChangeState(ctx, id, hangfire.Deleted{}, "Scheduled"); ok {
		t.Error("ChangeState with wrong expected state applied")
	}
	if ok, _ := c.Delete(ctx, "424242424"); ok {
		t.Error("Delete of a missing job reported true")
	}
	deleted := counter(t, "stats:deleted")
	if ok, err := c.Delete(ctx, id); !ok || err != nil {
		t.Fatalf("Delete: %v %v", ok, err)
	}
	if ok, err := c.Requeue(ctx, id); !ok || err != nil {
		t.Fatalf("Requeue: %v %v", ok, err)
	}
	if d := counter(t, "stats:deleted") - deleted; d != 0 {
		t.Errorf("stats:deleted delta after delete+requeue = %d, want 0", d)
	}
	if j := readJob(t, id); j.StateName != "Enqueued" || j.ExpireAt.Valid {
		t.Errorf("after requeue: %+v", j)
	}

	retry := hangfire.Scheduled{EnqueueAt: time.Now().Add(time.Minute), Reason: hangfire.RetryReason(1, 10, "boom")}
	failed := hangfire.Failed{ExceptionType: "Go:*errors.errorString", ExceptionMessage: "boom", ExceptionDetails: "boom"}
	applied, err := s.ChangeState(ctx, sqlserver.StateChange{JobID: id, State: retry, Traversed: []hangfire.State{failed}})
	if err != nil || applied == nil {
		t.Fatalf("retry: %v %v", applied, err)
	}
	if r := queryStrings(t, `select Value from HangFire.[Set] where [Key] = 'retries' and Value = @p1`, string(id)); len(r) != 1 {
		t.Errorf("retries set = %v", r)
	}
	if names := stateNames(readStates(t, id)); !slices.Equal(names[len(names)-2:], []string{"Failed", "Scheduled"}) {
		t.Errorf("history tail = %v", names)
	}
	if ok, err := c.Requeue(ctx, id); !ok || err != nil {
		t.Fatalf("Requeue: %v %v", ok, err)
	}
	if r := queryStrings(t, `select Value from HangFire.[Set] where [Key] in ('retries','schedule') and Value = @p1`, string(id)); len(r) != 0 {
		t.Errorf("retries/schedule left after requeue: %v", r)
	}
}

func TestLockTimeout(t *testing.T) {
	s, _ := setup(t, hangfire.CompatLevel180)
	ctx := context.Background()
	a, err := s.Session(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if err := a.LockJob(ctx, "1", time.Second); err != nil {
		t.Fatal(err)
	}
	b, err := s.Session(ctx)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	err = b.LockJob(ctx, "1", 1500*time.Millisecond)
	b.Close()
	if !errors.Is(err, hangfire.ErrLockTimeout) {
		t.Fatalf("second lock: %v, want ErrLockTimeout", err)
	}
	if el := time.Since(started); el < 1500*time.Millisecond {
		t.Errorf("gave up after %v", el)
	}
	var owner sql.NullString
	if err := testDB.QueryRow(`select top 1 request_owner_type from sys.dm_tran_locks where resource_type = 'APPLICATION' and resource_description like '%HangFire:job:1:state-lock%'`).Scan(&owner); err != nil {
		t.Logf("dm_tran_locks lookup: %v", err)
	} else if owner.String != "SESSION" {
		t.Errorf("lock owner = %q, want SESSION", owner.String)
	}
}

func TestRecurring(t *testing.T) {
	s, _ := setup(t, hangfire.CompatLevel180)
	ctx := context.Background()
	m := sqlserver.NewRecurringManager(s)
	inv := hangfire.Invocation{Type: "E2E.Jobs.Recorder, E2E", Method: "Record", ParameterTypes: []string{"System.String"}, Args: []any{"rec"}}
	if err := m.AddOrUpdate(ctx, "rj-1", inv, "not a cron", hangfire.RecurringOptions{}); !errors.Is(err, hangfire.ErrInvalidCron) {
		t.Errorf("bad cron: %v", err)
	}
	before := time.Now()
	if err := m.AddOrUpdate(ctx, "rj-1", inv, "0 3 * * *", hangfire.RecurringOptions{Queue: "critical", Misfire: hangfire.MisfireStrict}); err != nil {
		t.Fatal(err)
	}
	h, err := s.Hash(ctx, "recurring-job:rj-1")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"Queue": "critical", "Cron": "0 3 * * *", "TimeZoneId": "UTC", "Misfire": "1", "V": "2",
		"Job": `{"t":"E2E.Jobs.Recorder, E2E","m":"Record","p":["System.String"],"a":["\"rec\""]}`,
	}
	for k, v := range want {
		if h[k] != v {
			t.Errorf("hash %s = %q, want %q", k, h[k], v)
		}
	}
	next, err := hangfire.ParseTime(h["NextExecution"])
	if err != nil || !next.After(before) || next.Hour() != 3 || next.Minute() != 0 {
		t.Errorf("NextExecution = %q (%v)", h["NextExecution"], err)
	}
	if score := queryStrings(t, `select convert(nvarchar, convert(bigint, Score)) from HangFire.[Set] where [Key]='recurring-jobs' and Value='rj-1'`); !slices.Equal(score, []string{fmt.Sprint(next.Unix())}) {
		t.Errorf("recurring-jobs score = %v, want %d", score, next.Unix())
	}

	id, err := m.Trigger(ctx, "rj-1")
	if err != nil || id == "" {
		t.Fatalf("Trigger: %q %v", id, err)
	}
	jd, err := s.JobData(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if jd.StateName != "Enqueued" || jd.Parameters["RecurringJobId"] != `"rj-1"` || jd.Parameters["Time"] == "" {
		t.Errorf("triggered job = %+v", jd)
	}
	if j := readJob(t, id); j.InvocationData != `{"t":"E2E.Jobs.Recorder, E2E","m":"Record","p":["System.String"]}` || j.Arguments != `["\"rec\""]` {
		t.Errorf("triggered job row = %+v", j)
	}
	if q := queryStrings(t, `select Queue from HangFire.JobQueue where JobId = @p1`, string(id)); !slices.Equal(q, []string{"critical"}) {
		t.Errorf("triggered job queue = %v", q)
	}
	if h, _ := s.Hash(ctx, "recurring-job:rj-1"); h["LastJobId"] != string(id) || h["LastExecution"] == "" {
		t.Errorf("hash after trigger: LastJobId=%q LastExecution=%q", h["LastJobId"], h["LastExecution"])
	}
	if id, err := m.Trigger(ctx, "missing"); id != "" || err != nil {
		t.Errorf("Trigger(missing) = %q %v", id, err)
	}

	recs, err := sqlserver.NewMonitor(s).RecurringJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if i := slices.IndexFunc(recs, func(r sqlserver.RecurringJob) bool { return r.ID == "rj-1" }); i < 0 || recs[i].LastJobState != "Enqueued" || recs[i].Payload.Method != "Record" {
		t.Errorf("monitor recurring = %+v", recs)
	}

	if err := m.RemoveIfExists(ctx, "rj-1"); err != nil {
		t.Fatal(err)
	}
	if h, _ := s.Hash(ctx, "recurring-job:rj-1"); h != nil {
		t.Errorf("hash survives removal: %v", h)
	}
	if err := m.RemoveIfExists(ctx, "rj-1"); err != nil {
		t.Errorf("second removal: %v", err)
	}
}

func TestFetchKeepAliveAck(t *testing.T) {
	s, c := setup(t, hangfire.CompatLevel180)
	ctx := context.Background()
	id, err := c.Enqueue(ctx, recordInv, hangfire.WithQueue("fetchq"))
	if err != nil {
		t.Fatal(err)
	}
	f, err := s.FetchNext(ctx, []string{"fetchq"}, 5*time.Minute)
	if err != nil || f == nil || f.JobID != id {
		t.Fatalf("FetchNext = %+v, %v", f, err)
	}
	if again, _ := s.FetchNext(ctx, []string{"fetchq"}, 5*time.Minute); again != nil {
		t.Errorf("fetched row is still visible: %+v", again)
	}
	if err := s.KeepAlive(ctx, f); err != nil {
		t.Fatalf("KeepAlive: %v", err)
	}
	proc := hangfire.Processing{ServerID: "go-test:1", WorkerID: "w1"}
	if st, err := s.ChangeState(ctx, sqlserver.StateChange{JobID: id, State: proc, Expected: []string{"Enqueued", "Scheduled"}}); err != nil || st == nil {
		t.Fatalf("-> Processing: %v %v", st, err)
	}
	if st, err := s.ChangeState(ctx, sqlserver.StateChange{JobID: id, State: hangfire.Succeeded{}, Expected: []string{"Processing"}, Ack: f}); err != nil || st == nil {
		t.Fatalf("-> Succeeded: %v %v", st, err)
	}
	if q := queryStrings(t, `select Queue from HangFire.JobQueue where JobId = @p1`, string(id)); len(q) != 0 {
		t.Errorf("queue row survives ack: %v", q)
	}
	if names := stateNames(readStates(t, id)); !slices.Equal(names, []string{"Enqueued", "Processing", "Succeeded"}) {
		t.Errorf("history = %v", names)
	}
}

func TestMonitor(t *testing.T) {
	s, c := setup(t, hangfire.CompatLevel180)
	ctx := context.Background()
	m := sqlserver.NewMonitor(s)
	before, err := m.Statistics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := c.Enqueue(ctx, recordInv, hangfire.WithQueue("monq"))
	if err != nil {
		t.Fatal(err)
	}
	sched, err := c.Schedule(ctx, recordInv, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AnnounceServer(ctx, "go-mon:1:abc", sqlserver.ServerData{WorkerCount: 3, Queues: []string{"monq"}, StartedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	defer s.RemoveServer(ctx, "go-mon:1:abc")
	after, err := m.Statistics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.Enqueued-before.Enqueued != 1 || after.Scheduled-before.Scheduled != 1 || after.Servers-before.Servers != 1 {
		t.Errorf("statistics delta: before %+v after %+v", before, after)
	}
	d, err := m.JobDetails(ctx, id)
	if err != nil || d == nil {
		t.Fatalf("JobDetails: %v", err)
	}
	if d.Payload.Queue != "monq" || len(d.History) != 1 || d.History[0].StateName != "Enqueued" || d.History[0].Data["Queue"] != "default" {
		t.Errorf("JobDetails = %+v", d)
	}
	list, err := m.JobsByState(ctx, "Scheduled", 0, 1000)
	if err != nil || !slices.ContainsFunc(list, func(j sqlserver.JobSummary) bool { return j.ID == sched && j.InState && j.StateData["EnqueueAt"] != "" }) {
		t.Errorf("Scheduled list = %+v, %v", list, err)
	}
	qs, err := m.Queues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	i := slices.IndexFunc(qs, func(q sqlserver.QueueSummary) bool { return q.Name == "monq" })
	if i < 0 || qs[i].Length != 1 || len(qs[i].FirstJobs) != 1 || qs[i].FirstJobs[0].ID != id {
		t.Errorf("Queues = %+v", qs)
	}
	servers, err := m.Servers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if i := slices.IndexFunc(servers, func(s sqlserver.Server) bool { return s.Name == "go-mon:1:abc" }); i < 0 || servers[i].WorkersCount != 3 || servers[i].StartedAt.IsZero() {
		t.Errorf("Servers = %+v", servers)
	}
	if err := s.Heartbeat(ctx, "go-mon:1:abc"); err != nil {
		t.Errorf("Heartbeat: %v", err)
	}
	if err := s.Heartbeat(ctx, "gone"); !errors.Is(err, sqlserver.ErrServerGone) {
		t.Errorf("Heartbeat(gone) = %v", err)
	}
	if daily, err := m.DailyStats(ctx, sqlserver.StatsSucceeded); err != nil || len(daily) != 7 {
		t.Errorf("DailyStats = %v %v", daily, err)
	}
	if hourly, err := m.HourlyStats(ctx, sqlserver.StatsFailed); err != nil || len(hourly) != 24 {
		t.Errorf("HourlyStats = %v %v", hourly, err)
	}
}

func TestAwaitingSetAtLevel170(t *testing.T) {
	_, c := setup(t, hangfire.CompatLevel170)
	ctx := context.Background()
	parent, err := c.Schedule(ctx, recordInv, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	child, err := c.ContinueJobWith(ctx, parent, recordInv, hangfire.Enqueued{Queue: "critical"}, hangfire.OnAnyFinishedState)
	if err != nil {
		t.Fatal(err)
	}
	if r := queryStrings(t, `select Value from HangFire.[Set] where [Key] = 'awaiting' and Value = @p1`, string(child)); len(r) != 1 {
		t.Errorf("awaiting set at 170 = %v", r)
	}
	if ok, err := c.Delete(ctx, parent); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if r := queryStrings(t, `select Value from HangFire.[Set] where [Key] = 'awaiting' and Value = @p1`, string(child)); len(r) != 0 {
		t.Errorf("awaiting member left after continuation ran: %v", r)
	}
	if q := queryStrings(t, `select Queue from HangFire.JobQueue where JobId = @p1`, string(child)); !slices.Equal(q, []string{"critical"}) {
		t.Errorf("OnAnyFinishedState child queue = %v", q)
	}
}
