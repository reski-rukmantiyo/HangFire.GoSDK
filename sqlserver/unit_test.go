package sqlserver

import (
	"strings"
	"testing"
	"time"

	"github.com/reski-rukmantiyo/HangFire.GoSDK/hangfire"
)

func TestBatchRenamesParametersOnly(t *testing.T) {
	s := &Storage{opts: Options{Schema: "HangFire"}}
	b := s.newBatch()
	b.addToSet("schedule", "1", 2)
	b.addToSet("retries", "1", 0)
	got := paramRef.ReplaceAllString(b.stmts[0].text, "${1}@${2}_0")
	if !strings.Contains(got, "@@ROWCOUNT") || !strings.Contains(got, "values (@key_0, @value_0, @score_0)") || !strings.Contains(got, "[HangFire].[Set]") {
		t.Errorf("renamed statement = %s", got)
	}
}

func TestAddMonthsNetClampsDay(t *testing.T) {
	for in, want := range map[string]string{
		"2026-01-31T10:00:00Z": "2026-02-28T10:00:00Z",
		"2024-01-31T10:00:00Z": "2024-02-29T10:00:00Z",
		"2026-11-18T10:00:00Z": "2026-12-18T10:00:00Z",
		"2026-12-31T23:59:00Z": "2027-01-31T23:59:00Z",
	} {
		ts, _ := time.Parse(time.RFC3339, in)
		if got := addMonthsNet(ts, 1).Format(time.RFC3339); got != want {
			t.Errorf("addMonthsNet(%s) = %s, want %s", in, got, want)
		}
	}
}

func TestRecurringFreshHashFields(t *testing.T) {
	e, err := newRecurringEntity("r", nil)
	if err != nil {
		t.Fatal(err)
	}
	e.job, e.cron, e.queue = `{"t":"T, A","m":"M"}`, "*/5 * * * *", "default"
	now := time.Date(2026, 11, 18, 10, 2, 30, 0, time.UTC)
	if err := e.scheduleNext(now.Add(-time.Second), now); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, f := range e.changedFields(now, hangfire.CompatLevel180) {
		keys = append(keys, f[0]+"="+f[1])
	}
	want := "Queue=default Cron=*/5 * * * * TimeZoneId=UTC Job={\"t\":\"T, A\",\"m\":\"M\"} CreatedAt=1794996150000 NextExecution=1794996300000 V=2"
	if got := strings.Join(keys, " "); got != want {
		t.Errorf("fields\n got %s\nwant %s", got, want)
	}
}

func TestScheduleNextRelaxedSkipsToFuture(t *testing.T) {
	e := &recurringEntity{cron: "* * * * * *", timeZoneID: "UTC"}
	now := time.Date(2026, 11, 18, 10, 0, 0, 500_000_000, time.UTC)
	if err := e.scheduleNext(now.Add(-time.Second), now); err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 11, 18, 10, 0, 1, 0, time.UTC); !e.nextExecution.Equal(want) {
		t.Errorf("NextExecution = %v, want %v", e.nextExecution, want)
	}
}
