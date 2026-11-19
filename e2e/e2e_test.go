// Package e2e runs Go-produced jobs on a real .NET Hangfire 1.8 server
// (tools/e2e-dotnet) and checks both sides read the same rows.
// It needs HANGFIRE_SQL (see internal/sqltest) and E2E_DOTNET=1.
package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/reski-rukmantiyo/HangFire.GoSDK/hangfire"
	"github.com/reski-rukmantiyo/HangFire.GoSDK/internal/sqltest"
	"github.com/reski-rukmantiyo/HangFire.GoSDK/sqlserver"
)

func setup(t *testing.T) (*sqltest.DB, *sqlserver.Storage) {
	t.Helper()
	if os.Getenv("E2E_DOTNET") != "1" {
		t.Skip("E2E_DOTNET != 1")
	}
	db := sqltest.New(t)
	s, err := sqlserver.NewStorage(context.Background(), db.DB, sqlserver.Options{Level: hangfire.CompatLevel180})
	if err != nil {
		t.Fatal(err)
	}
	return db, s
}

const recorder = "E2E.Jobs.Recorder, E2E"

func record(tag string) hangfire.Invocation {
	return hangfire.Invocation{Type: recorder, Method: "Record", ParameterTypes: []string{"System.String"}, Args: []any{tag}}
}

type payload struct {
	Name  string `json:"Name"`
	Count int    `json:"Count"`
}

// dump is the JSON `dump-job` prints: the .NET IMonitoringApi.JobDetails view.
type dump struct {
	Invocation *struct {
		Type, Method, ParameterTypes, Arguments string
	} `json:"invocation"`
	Properties map[string]string `json:"properties"`
	History    []struct {
		StateName, Reason, CreatedAt string
		Data                         map[string]string
	} `json:"history"`
}

func dumpJob(t *testing.T, db *sqltest.DB, id hangfire.JobID) dump {
	t.Helper()
	var d dump
	if err := json.Unmarshal([]byte(db.Harness(t, nil, "dump-job", string(id))), &d); err != nil {
		t.Fatalf("dump-job %s: %v", id, err)
	}
	return d
}

func TestDotnetServerRunsGoJobs(t *testing.T) {
	db, s := setup(t)
	ctx := context.Background()
	c := sqlserver.NewClient(s)
	rm := sqlserver.NewRecurringManager(s)
	rec := filepath.Join(t.TempDir(), "record.txt")

	date := time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)
	jobs := map[string]hangfire.JobID{}
	mustCreate := func(line string, id hangfire.JobID, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", line, err)
		}
		jobs[line] = id
	}
	id, err := c.Enqueue(ctx, record("go-enqueue"), hangfire.WithQueue("dotnet"))
	mustCreate("go-enqueue|", id, err)
	id, err = c.Enqueue(ctx, hangfire.Invocation{Type: recorder, Method: "RecordInt", ParameterTypes: []string{"System.String", "System.Int32"}, Args: []any{"go-int", 42}})
	mustCreate("go-int|42", id, err)
	id, err = c.Enqueue(ctx, hangfire.Invocation{Type: recorder, Method: "RecordDate", ParameterTypes: []string{"System.String", "System.DateTime"}, Args: []any{"go-date", date}})
	mustCreate("go-date|2026-11-18T10:00:00.0000000Z", id, err)
	id, err = c.Enqueue(ctx, hangfire.Invocation{Type: recorder, Method: "RecordPoco", ParameterTypes: []string{"System.String", "E2E.Jobs.Payload, E2E"}, Args: []any{"go-poco", payload{"widget", 3}}})
	mustCreate("go-poco|widget,3", id, err)
	id, err = c.Schedule(ctx, record("go-scheduled"), 3*time.Second, hangfire.WithQueue("dotnet"))
	mustCreate("go-scheduled|", id, err)
	parent, err := c.Enqueue(ctx, record("go-parent"))
	mustCreate("go-parent|", parent, err)
	id, err = c.ContinueJobWith(ctx, parent, record("go-continuation"), nil, hangfire.OnlyOnSucceededState, hangfire.WithQueue("dotnet"))
	mustCreate("go-continuation|", id, err)

	if err := rm.AddOrUpdate(ctx, "go-every-second", record("go-recurring"), "* * * * * *", hangfire.RecurringOptions{Queue: "dotnet"}); err != nil {
		t.Fatal(err)
	}
	if err := rm.AddOrUpdate(ctx, "go-yearly", record("go-triggered"), "0 0 1 1 *", hangfire.RecurringOptions{}); err != nil {
		t.Fatal(err)
	}
	id, err = rm.Trigger(ctx, "go-yearly")
	mustCreate("go-triggered|", id, err)

	out := db.Harness(t, []string{"E2E_RECORD_FILE=" + rec}, "server", "--queues", "dotnet,default", "--seconds", "20")
	t.Logf("server stdout: %q", out)

	raw, err := os.ReadFile(rec)
	if err != nil {
		t.Fatalf("record file: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	t.Logf("record file:\n%s", raw)
	for line, id := range jobs {
		if !slices.Contains(lines, line) {
			t.Errorf("job %s: line %q not recorded", id, line)
		}
		if d := dumpJob(t, db, id); len(d.History) == 0 || d.History[0].StateName != "Succeeded" {
			t.Errorf("job %s (%s): .NET JobDetails history %+v, want Succeeded first", id, line, d.History)
		}
	}
	if !slices.Contains(lines, "go-recurring|") {
		t.Errorf("recurring job never ran")
	}
	recs, err := sqlserver.NewMonitor(s).RecurringJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		t.Logf("recurring %s: last job %s (%s), next %v, error %q", r.ID, r.LastJobID, r.LastJobState, r.NextExecution, r.Error)
		if r.ID == "go-every-second" && r.LastJobID == "" {
			t.Errorf("go-every-second never triggered by the .NET scheduler")
		}
	}
}

func TestGoManagesDotnetJobs(t *testing.T) {
	db, s := setup(t)
	ctx := context.Background()
	c := sqlserver.NewClient(s)
	var ids struct{ Enqueued, Scheduled, Continuation hangfire.JobID }
	if err := json.Unmarshal([]byte(db.Harness(t, nil, "enqueue-samples", "--queue", "goq")), &ids); err != nil {
		t.Fatal(err)
	}

	m := sqlserver.NewMonitor(s)
	for _, id := range []hangfire.JobID{ids.Enqueued, ids.Scheduled, ids.Continuation} {
		compareDetails(t, m, dumpJob(t, db, id), id)
	}

	if ok, err := c.ChangeState(ctx, ids.Scheduled, hangfire.Deleted{}, "Awaiting"); ok || err != nil {
		t.Errorf("ChangeState with wrong expected state: %v %v", ok, err)
	}
	if ok, err := c.Requeue(ctx, ids.Scheduled); !ok || err != nil {
		t.Fatalf("Requeue: %v %v", ok, err)
	}
	if ok, err := c.Delete(ctx, ids.Enqueued); !ok || err != nil {
		t.Fatalf("Delete: %v %v", ok, err)
	}

	sched := dumpJob(t, db, ids.Scheduled)
	if h := sched.History[0]; h.StateName != "Enqueued" || h.Data["Queue"] != "default" {
		t.Errorf("requeued .NET job: %+v", h)
	}
	enq := dumpJob(t, db, ids.Enqueued)
	if h := enq.History[0]; h.StateName != "Deleted" || h.Data["DeletedAt"] == "" {
		t.Errorf("deleted .NET job: %+v", h)
	}
	cont := dumpJob(t, db, ids.Continuation)
	// enqueue-samples uses AwaitingState(parent, next), whose options are OnAnyFinishedState.
	if h := cont.History[0]; h.StateName != "Enqueued" || h.Data["Queue"] != "goq" || cont.History[1].StateName != "Awaiting" {
		t.Errorf(".NET continuation after Go deleted its parent: %+v", cont.History)
	}
	for _, id := range []hangfire.JobID{ids.Enqueued, ids.Scheduled, ids.Continuation} {
		compareDetails(t, m, dumpJob(t, db, id), id)
	}
	var queue string
	if err := db.QueryRow(`select Queue from HangFire.JobQueue where JobId = @p1 and FetchedAt is null`, string(ids.Scheduled)).Scan(&queue); err != nil || queue != "default" {
		t.Errorf("requeued job queue row: %q %v", queue, err)
	}
}

// compareDetails checks Go's Monitor.JobDetails against .NET's for the same job.
func compareDetails(t *testing.T, m *sqlserver.Monitor, want dump, id hangfire.JobID) {
	t.Helper()
	got, err := m.JobDetails(context.Background(), id)
	if err != nil || got == nil {
		t.Fatalf("Go JobDetails(%s): %v", id, err)
	}
	if want.Invocation == nil || got.Payload.Type != want.Invocation.Type || got.Payload.Method != want.Invocation.Method {
		t.Errorf("job %s invocation: Go %s.%s, .NET %+v", id, got.Payload.Type, got.Payload.Method, want.Invocation)
	}
	var wantArgs []*string
	if err := json.Unmarshal([]byte(want.Invocation.Arguments), &wantArgs); err != nil {
		t.Fatal(err)
	}
	if len(wantArgs) != len(got.Payload.Args) {
		t.Errorf("job %s args: Go %d, .NET %d", id, len(got.Payload.Args), len(wantArgs))
	}
	for i := range min(len(wantArgs), len(got.Payload.Args)) {
		if wantArgs[i] == nil != (got.Payload.Args[i] == nil) || wantArgs[i] != nil && *wantArgs[i] != string(got.Payload.Args[i]) {
			t.Errorf("job %s arg %d: Go %s, .NET %v", id, i, got.Payload.Args[i], wantArgs[i])
		}
	}
	if !reflect.DeepEqual(got.Properties, want.Properties) {
		t.Errorf("job %s properties: Go %v, .NET %v", id, got.Properties, want.Properties)
	}
	if len(got.History) != len(want.History) {
		t.Fatalf("job %s history: Go %d states, .NET %d", id, len(got.History), len(want.History))
	}
	for i, w := range want.History {
		g := got.History[i]
		if g.StateName != w.StateName || g.Reason != w.Reason || !reflect.DeepEqual(g.Data, w.Data) {
			t.Errorf("job %s history[%d]: Go %+v, .NET %+v", id, i, g, w)
		}
		wantAt, err := time.Parse("2006-01-02T15:04:05.999999999", w.CreatedAt)
		if err != nil || g.CreatedAt.Sub(wantAt).Abs() > time.Millisecond {
			t.Errorf("job %s history[%d] CreatedAt: Go %v, .NET %s", id, i, g.CreatedAt, w.CreatedAt)
		}
	}
}

// TestGoldenRows makes the calls `enqueue-samples` makes, from Go, and
// compares every row both sides write, with ids and timestamps normalized.
func TestGoldenRows(t *testing.T) {
	db, s := setup(t)
	ctx := context.Background()
	var net struct{ Enqueued, Scheduled, Continuation hangfire.JobID }
	if err := json.Unmarshal([]byte(db.Harness(t, nil, "enqueue-samples", "--queue", "goq")), &net); err != nil {
		t.Fatal(err)
	}

	// CaptureCultureAttribute is on in .NET; Go writes the same values only when asked (SRS FR-P06).
	var cultureOpts []hangfire.Option
	for _, name := range []string{"CurrentCulture", "CurrentUICulture"} {
		var v string
		err := db.QueryRow(`select Value from HangFire.JobParameter where JobId = @p1 and Name = @p2`, string(net.Enqueued), name).Scan(&v)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		var decoded string
		if err := json.Unmarshal([]byte(v), &decoded); err != nil {
			t.Fatal(err)
		}
		cultureOpts = append(cultureOpts, hangfire.WithParameter(name, decoded))
	}

	c := sqlserver.NewClient(s)
	hello := func(n string) hangfire.Invocation {
		return hangfire.Invocation{Type: "E2E.Jobs.IGoJobs, E2E", Method: "Hello", ParameterTypes: []string{"System.String"}, Args: []any{n}}
	}
	add := hangfire.Invocation{Type: "E2E.Jobs.IGoJobs, E2E", Method: "Add", ParameterTypes: []string{"System.Int32", "System.Int32"}, Args: []any{2, 3}}
	enq, err := c.Create(ctx, hello("world"), hangfire.Enqueued{Queue: "goq"}, cultureOpts...)
	if err != nil {
		t.Fatal(err)
	}
	sched, err := c.Create(ctx, add, hangfire.Scheduled{EnqueueAt: time.Now().Add(5 * time.Second)}, cultureOpts...)
	if err != nil {
		t.Fatal(err)
	}
	cont, err := c.Create(ctx, hello("continuation"), hangfire.Awaiting{ParentID: enq, NextState: hangfire.Enqueued{Queue: "goq"}, Options: hangfire.OnAnyFinishedState}, cultureOpts...)
	if err != nil {
		t.Fatal(err)
	}

	netIDs := []hangfire.JobID{net.Enqueued, net.Scheduled, net.Continuation}
	goIDs := []hangfire.JobID{enq, sched, cont}
	netRows := snapshot(t, db, netIDs)
	goRows := snapshot(t, db, goIDs)
	if netRows != goRows {
		t.Errorf("rows differ\n.NET:\n%s\nGo:\n%s", netRows, goRows)
	} else {
		t.Logf("identical normalized rows:\n%s", goRows)
	}
}

var (
	unixMillis = regexp.MustCompile(`\b1\d{12}\b`)
	isoDate    = regexp.MustCompile(`\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?Z?`)
)

// snapshot renders the Job, JobParameter, State, JobQueue and Set rows of
// ids, with ids replaced by J1..Jn and timestamps by placeholders.
func snapshot(t *testing.T, db *sqltest.DB, ids []hangfire.JobID) string {
	t.Helper()
	var b strings.Builder
	rowsOf := func(q string, args ...any) []string {
		rows, err := db.Query(q, args...)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		var out []string
		for rows.Next() {
			vals := make([]sql.NullString, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			var parts []string
			for i, v := range vals {
				s := "NULL"
				if v.Valid {
					s = v.String
				}
				parts = append(parts, cols[i]+"="+s)
			}
			out = append(out, strings.Join(parts, " | "))
		}
		return out
	}
	for i, id := range ids {
		fmt.Fprintf(&b, "== J%d\n", i+1)
		for _, r := range rowsOf(`select InvocationData, Arguments, StateName, case when ExpireAt is null then 'null' else 'set' end as ExpireAt from HangFire.Job where Id = @p1`, string(id)) {
			b.WriteString("Job: " + r + "\n")
		}
		params := rowsOf(`select Name, Value from HangFire.JobParameter where JobId = @p1`, string(id))
		sort.Strings(params)
		for _, r := range params {
			b.WriteString("JobParameter: " + r + "\n")
		}
		for _, r := range rowsOf(`select Name, Reason, Data from HangFire.State where JobId = @p1 order by Id`, string(id)) {
			b.WriteString("State: " + r + "\n")
		}
		for _, r := range rowsOf(`select Queue, case when FetchedAt is null then 'null' else 'set' end as FetchedAt from HangFire.JobQueue where JobId = @p1`, string(id)) {
			b.WriteString("JobQueue: " + r + "\n")
		}
		for _, r := range rowsOf(`select [Key], Value from HangFire.[Set] where Value = @p1 or Value like '%:' + @p1`, string(id)) {
			b.WriteString("Set: " + regexp.MustCompile(`\b`+string(id)+`$`).ReplaceAllString(r, fmt.Sprintf("J%d", i+1)) + "\n")
		}
	}
	out := b.String()
	for i, id := range ids {
		for _, key := range []string{`"JobId":"`, `"ParentId":"`} {
			out = strings.ReplaceAll(out, key+string(id)+`"`, key+fmt.Sprintf("J%d", i+1)+`"`)
		}
	}
	out = unixMillis.ReplaceAllString(out, "<ms>")
	return isoDate.ReplaceAllString(out, "<iso>")
}
