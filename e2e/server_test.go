package e2e

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/reski-rukmantiyo/HangFire.GoSDK/hangfire"
	"github.com/reski-rukmantiyo/HangFire.GoSDK/internal/sqltest"
	"github.com/reski-rukmantiyo/HangFire.GoSDK/server"
	"github.com/reski-rukmantiyo/HangFire.GoSDK/sqlserver"
)

const goJobs = "E2E.Jobs.IGoJobs, E2E"

// calls records what the Go handlers for IGoJobs ran.
type calls struct {
	mu    sync.Mutex
	lines []string
}

func (c *calls) add(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, s)
}

func (c *calls) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.lines)
}

func startGoServer(t *testing.T, s *sqlserver.Storage, queues ...string) (*server.Server, *calls) {
	t.Helper()
	srv, err := server.New(s, server.Options{
		Name:                    "go-e2e",
		WorkerCount:             4,
		Queues:                  queues,
		QueuePollInterval:       100 * time.Millisecond,
		SchedulePollingInterval: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	c := &calls{}
	srv.Handle(goJobs, "Hello", server.Handler1(func(_ context.Context, name string) error {
		c.add("Hello " + name)
		return nil
	}))
	srv.Handle(goJobs, "Add", server.Handler2(func(_ context.Context, a, b int) error {
		c.add("Add " + itoa(a+b))
		return nil
	}))
	if err := srv.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := srv.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})
	return srv, c
}

func itoa(n int) string { b, _ := json.Marshal(n); return string(b) }

func waitSucceeded(t *testing.T, s *sqlserver.Storage, ids ...hangfire.JobID) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for _, id := range ids {
		for {
			st, err := s.StateData(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			if st != nil && st.Name == hangfire.StateSucceeded {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("job %s not Succeeded in time (state %+v)", id, st)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// processedBy reads, through the .NET monitoring API, which server ran the job
// and checks .NET sees it Succeeded with the Go-written state data.
func processedBy(t *testing.T, db *sqltest.DB, id hangfire.JobID) string {
	t.Helper()
	d := dumpJob(t, db, id)
	if len(d.History) < 2 || d.History[0].StateName != "Succeeded" || d.History[1].StateName != "Processing" {
		t.Fatalf("job %s: .NET history %+v, want Succeeded after Processing", id, d.History)
	}
	for _, k := range []string{"SucceededAt", "PerformanceDuration", "Latency"} {
		if d.History[0].Data[k] == "" {
			t.Errorf("job %s: .NET sees no %s in %v", id, k, d.History[0].Data)
		}
	}
	return d.History[1].Data["ServerId"]
}

func TestGoServerRunsDotnetJobs(t *testing.T) {
	db, s := setup(t)
	var ids struct{ Enqueued, Scheduled, Continuation hangfire.JobID }
	if err := json.Unmarshal([]byte(db.Harness(t, nil, "enqueue-samples", "--queue", "goq")), &ids); err != nil {
		t.Fatal(err)
	}
	srv, c := startGoServer(t, s, "goq", "default")
	jobs := []hangfire.JobID{ids.Enqueued, ids.Scheduled, ids.Continuation}
	waitSucceeded(t, s, jobs...)

	for _, id := range jobs {
		if by := processedBy(t, db, id); by != srv.ID() {
			t.Errorf("job %s processed by %q, want the Go server %q", id, by, srv.ID())
		}
	}
	got := c.snapshot()
	slices.Sort(got)
	if want := []string{"Add 5", "Hello continuation", "Hello world"}; !slices.Equal(got, want) {
		t.Errorf("handler calls = %q, want %q", got, want)
	}
}

func TestDotnetAndGoServersShareDatabase(t *testing.T) {
	db, s := setup(t)
	rec := filepath.Join(t.TempDir(), "record.txt")
	cmd := exec.Command("dotnet", "run", "--no-build", "--project", filepath.Join(sqltest.RepoRoot(t), "tools", "e2e-dotnet"), "--",
		"server", "--queues", "dotnetq", "--seconds", "90")
	cmd.Env = append(os.Environ(), "HANGFIRE_SQL="+db.ConnStr, "E2E_RECORD_FILE="+rec)
	var dotnetErr strings.Builder
	cmd.Stderr = &dotnetErr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cmd.Process.Signal(syscall.SIGTERM)
		cmd.Wait()
		t.Logf(".NET server stderr:\n%s", dotnetErr.String())
	})
	srv, c := startGoServer(t, s, "goq")

	ctx := context.Background()
	client := sqlserver.NewClient(s)
	var dotnetJobs, goJobIDs []hangfire.JobID
	for i := range 5 {
		tag := "shared-" + itoa(i)
		id, err := client.Enqueue(ctx, record(tag), hangfire.WithQueue("dotnetq"))
		if err != nil {
			t.Fatal(err)
		}
		dotnetJobs = append(dotnetJobs, id)
		id, err = client.Enqueue(ctx, hangfire.Invocation{Type: goJobs, Method: "Hello", ParameterTypes: []string{"System.String"}, Args: []any{tag}}, hangfire.WithQueue("goq"))
		if err != nil {
			t.Fatal(err)
		}
		goJobIDs = append(goJobIDs, id)
	}
	// Both servers run a delayed scheduler under the same lock; either may promote it.
	sched, err := client.Schedule(ctx, hangfire.Invocation{Type: goJobs, Method: "Add", ParameterTypes: []string{"System.Int32", "System.Int32"}, Args: []any{20, 22}}, 2*time.Second, hangfire.WithQueue("goq"))
	if err != nil {
		t.Fatal(err)
	}
	goJobIDs = append(goJobIDs, sched)

	waitSucceeded(t, s, append(slices.Clone(goJobIDs), dotnetJobs...)...)
	for _, id := range goJobIDs {
		if by := processedBy(t, db, id); by != srv.ID() {
			t.Errorf("Go job %s processed by %q", id, by)
		}
	}
	for _, id := range dotnetJobs {
		if by := processedBy(t, db, id); by == srv.ID() || by == "" {
			t.Errorf(".NET job %s processed by %q", id, by)
		}
	}
	raw, _ := os.ReadFile(rec)
	for i := range 5 {
		if !strings.Contains(string(raw), "shared-"+itoa(i)+"|") {
			t.Errorf(".NET recorder missing shared-%d; file:\n%s", i, raw)
		}
	}
	if got := c.snapshot(); len(got) != 6 || !slices.Contains(got, "Add 42") {
		t.Errorf("Go handler calls = %q", got)
	}
	servers, err := sqlserver.NewMonitor(s).Servers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(servers) != 2 {
		t.Errorf("servers = %+v, want the Go and the .NET server", servers)
	}
}
