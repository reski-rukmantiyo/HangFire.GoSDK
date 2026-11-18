package hangfire

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

type goldenFile struct {
	Hangfire   string                       `json:"hangfire"`
	TrickyText string                       `json:"trickyText"`
	Strings    map[string]string            `json:"strings"`
	States     map[string]map[string]string `json:"states"`
	Cron       []struct {
		Expr  string `json:"expr"`
		TZ    string `json:"tz"`
		From  string `json:"from"`
		Next  string `json:"next"`
		Error bool   `json:"error"`
	} `json:"cron"`
}

var loadGolden = sync.OnceValues(func() (*goldenFile, error) {
	raw, err := os.ReadFile("testdata/golden.json")
	if err != nil {
		return nil, err
	}
	var g goldenFile
	return &g, json.Unmarshal(raw, &g)
})

func golden(t *testing.T) *goldenFile {
	t.Helper()
	g, err := loadGolden()
	if err != nil {
		t.Fatalf("load golden fixtures (regenerate with tools/golden-dotnet/regen.sh): %v", err)
	}
	return g
}

func (g *goldenFile) str(t *testing.T, key string) string {
	t.Helper()
	v, ok := g.Strings[key]
	if !ok {
		t.Fatalf("golden fixture %q missing", key)
	}
	return v
}

type order struct {
	Id        int       `json:"Id"`
	Name      string    `json:"Name"`
	CreatedAt time.Time `json:"CreatedAt"`
	Amount    float64   `json:"Amount"`
	Tags      []string  `json:"Tags"`
	Big       int64     `json:"Big"`
	Note      *string   `json:"Note"`
}

var (
	t1 = time.Date(2026, 11, 18, 10, 0, 0, 123_000_000, time.UTC)
	t2 = time.Date(2026, 11, 19, 8, 30, 0, 0, time.UTC)
)

const (
	emailJob = "Golden.EmailJob, GoldenDotnet"
	nullable = "System.Nullable`1[[System.Int32]], mscorlib"
)

func TestGoldenJobs(t *testing.T) {
	g := golden(t)
	send := Invocation{Type: emailJob, Method: "Send",
		ParameterTypes: []string{"System.String", "System.Int32", "System.DateTime, mscorlib"},
		Args:           []any{"a@b.c", 42, t1}}
	cases := []struct {
		name  string
		inv   Invocation
		queue string
	}{
		{"send", send, ""},
		{"send_queue", send, "critical"},
		{"ping", Invocation{Type: emailJob, Method: "Ping"}, ""},
		{"ping_queue", Invocation{Type: emailJob, Method: "Ping"}, "low"},
		{"nulls", Invocation{Type: emailJob, Method: "Nullable", ParameterTypes: []string{"System.String", nullable}, Args: []any{nil, (*int)(nil)}}, ""},
		{"nullable_value", Invocation{Type: emailJob, Method: "Nullable", ParameterTypes: []string{"System.String", nullable}, Args: []any{"s", 7}}, ""},
		{"token", Invocation{Type: emailJob, Method: "WithToken", ParameterTypes: []string{"System.String", "System.Threading.CancellationToken, mscorlib"}, Args: []any{"s", nil}}, ""},
		{"text", Invocation{Type: emailJob, Method: "Text", ParameterTypes: []string{"System.String"}, Args: []any{g.TrickyText}}, ""},
		{"interface", Invocation{Type: "Golden.IEmailSender, GoldenDotnet", Method: "Send", ParameterTypes: []string{"System.String"}, Args: []any{"x"}}, ""},
		{"nested_generic", Invocation{Type: "Golden.EmailJob+Nested, GoldenDotnet", Method: "Run",
			ParameterTypes: []string{"System.Collections.Generic.List`1[[System.Int32]], mscorlib"}, Args: []any{[]int{1, 2}}}, ""},
		{"poco", Invocation{Type: emailJob, Method: "Process", ParameterTypes: []string{"Golden.Order, GoldenDotnet"},
			Args: []any{order{Id: 7, Name: "widget", CreatedAt: t1, Amount: 12.5, Tags: []string{"a", "b"}, Big: 9007199254740993}}}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, level := range []CompatLevel{CompatLevel170, CompatLevel180} {
				payload, err := EncodePayload(tc.inv, tc.queue, false, level)
				if err != nil {
					t.Fatal(err)
				}
				withArgs, err := EncodePayload(tc.inv, tc.queue, true, level)
				if err != nil {
					t.Fatal(err)
				}
				assertEqual(t, "payload", payload, g.str(t, "job/"+tc.name+"/payload"))
				assertEqual(t, "payload with args", withArgs, g.str(t, "job/"+tc.name+"/payload_with_args"))
			}
			args, err := EncodeArguments(tc.inv.Args)
			if err != nil {
				t.Fatal(err)
			}
			assertEqual(t, "arguments", args, g.str(t, "job/"+tc.name+"/arguments"))

			decoded, err := DecodePayload(g.str(t, "job/"+tc.name+"/payload"), g.str(t, "job/"+tc.name+"/arguments"))
			if err != nil {
				t.Fatal(err)
			}
			fromHash, err := DecodePayload(g.str(t, "job/"+tc.name+"/payload_with_args"), "")
			if err != nil {
				t.Fatal(err)
			}
			for _, p := range []Payload{decoded, fromHash} {
				if p.Type != tc.inv.Type || p.Method != tc.inv.Method || p.Queue != tc.queue || len(p.Args) != len(tc.inv.Args) {
					t.Fatalf("decoded payload %+v does not match invocation %+v queue %q", p, tc.inv, tc.queue)
				}
				reencoded, err := EncodeArguments(rawToAny(p.Args))
				if err != nil {
					t.Fatal(err)
				}
				assertEqual(t, "re-encoded arguments", reencoded, g.str(t, "job/"+tc.name+"/arguments"))
			}
		})
	}
}

func rawToAny(raw []json.RawMessage) []any {
	out := make([]any, len(raw))
	for i, r := range raw {
		if r != nil {
			out[i] = r
		}
	}
	return out
}

func TestGoldenDates(t *testing.T) {
	g := golden(t)
	dates := map[string]time.Time{
		"t1":             t1,
		"t2":             t2,
		"epoch":          time.Unix(0, 0),
		"boundary":       time.Date(1978, 1, 11, 21, 31, 40, 799_000_000, time.UTC),
		"after_boundary": time.Date(1978, 1, 11, 21, 31, 40, 800_000_000, time.UTC),
		"ticks":          time.Date(2026, 11, 18, 10, 0, 0, 123_456_700, time.UTC),
		"max":            time.Date(9999, 12, 31, 23, 59, 59, 999_999_900, time.UTC),
	}
	for name, d := range dates {
		for _, level := range []string{"170", "180"} {
			want := g.str(t, "date/"+level+"/"+name)
			got := FormatTime(d.In(time.FixedZone("x", 7*3600)), levelOf(level))
			assertEqual(t, "date "+level+"/"+name, got, want)

			back, err := ParseTime(want)
			if err != nil {
				t.Fatal(err)
			}
			if !back.Equal(d.Truncate(time.Millisecond)) && !back.Equal(d) {
				t.Errorf("ParseTime(%q) = %v, want %v", want, back, d)
			}
		}
	}
}

func levelOf(s string) CompatLevel {
	if s == "170" {
		return CompatLevel170
	}
	return CompatLevel180
}

func TestGoldenParameters(t *testing.T) {
	g := golden(t)
	cases := map[string]any{
		"string": "en-US",
		"long":   int64(1763460000),
		"bool":   true,
		"date":   t1,
		"text":   g.TrickyText,
		"poco":   order{Id: 1, Name: "n", CreatedAt: t2, Amount: 0.25, Tags: []string{}, Big: -5},
	}
	for name, v := range cases {
		got, err := EncodeParameter(v)
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "parameter "+name, got, g.str(t, "param/"+name))
	}
	assertEqual(t, "type-name quoting", quoteNet(g.TrickyText), g.str(t, "param/text"))
	var text string
	if err := DecodeParameter(g.str(t, "param/text"), &text); err != nil || text != g.TrickyText {
		t.Errorf("DecodeParameter(text) = %q, %v; want the original text", text, err)
	}
}

func TestGoldenContinuations(t *testing.T) {
	g := golden(t)
	cases := map[string][]Continuation{
		"empty": {},
		"mixed": {{"123", OnAnyFinishedState}, {"124", OnlyOnSucceededState}, {"125", OnlyOnDeletedState}},
	}
	for name, cs := range cases {
		want := g.str(t, "continuations/"+name)
		got, err := EncodeContinuations(cs)
		if err != nil {
			t.Fatal(err)
		}
		assertEqual(t, "continuations "+name, got, want)
		back, err := DecodeContinuations(want)
		if err != nil {
			t.Fatal(err)
		}
		if len(back) != len(cs) {
			t.Fatalf("DecodeContinuations(%s) = %v, want %v", want, back, cs)
		}
		for i := range cs {
			if back[i] != cs[i] {
				t.Errorf("DecodeContinuations(%s)[%d] = %v, want %v", want, i, back[i], cs[i])
			}
		}
	}
}

func TestGoldenStates(t *testing.T) {
	g := golden(t)
	const exceptionInfo = `{"e":"System.InvalidOperationException, mscorlib","m":"boom"}`
	states := map[string]State{
		"enqueued":                 Enqueued{Queue: "critical", EnqueuedAt: t1, Reason: "r"},
		"enqueued_default":         Enqueued{EnqueuedAt: t1},
		"scheduled":                Scheduled{EnqueueAt: t2, ScheduledAt: t1},
		"awaiting":                 Awaiting{ParentID: "123", NextState: Enqueued{Queue: "critical"}, Options: OnlyOnSucceededState},
		"awaiting_default":         Awaiting{ParentID: "123"},
		"awaiting_scheduled":       Awaiting{ParentID: "123", NextState: Scheduled{EnqueueAt: t2, Reason: "later"}, Options: OnlyOnDeletedState, Expiration: 7 * 24 * time.Hour},
		"awaiting_enqueued_reason": Awaiting{ParentID: "9", NextState: Enqueued{Reason: "go"}, Expiration: 36 * time.Hour},
		"processing":               Processing{ServerID: "server-1:1234:abc", WorkerID: "8d0f6c1e-0000-0000-0000-000000000001", StartedAt: t1},
		"succeeded":                Succeeded{SucceededAt: t1, Result: json.RawMessage("42"), PerformanceDuration: 250 * time.Millisecond, Latency: 1500 * time.Millisecond},
		"succeeded_void":           Succeeded{SucceededAt: t1, PerformanceDuration: 3 * time.Millisecond},
		"failed":                   Failed{FailedAt: t1, ExceptionType: "System.InvalidOperationException", ExceptionMessage: "boom", ExceptionDetails: "System.InvalidOperationException: boom", ServerID: "server-1"},
		"failed_noserver":          Failed{FailedAt: t1, ExceptionType: "System.InvalidOperationException", ExceptionMessage: "boom", ExceptionDetails: "System.InvalidOperationException: boom"},
		"deleted":                  Deleted{DeletedAt: t1},
		"deleted_exception":        Deleted{DeletedAt: t1, Exception: exceptionInfo},
	}
	for name, s := range states {
		for _, level := range []string{"170", "180"} {
			key := level + "/" + name
			t.Run(key, func(t *testing.T) {
				want, ok := g.States[key]
				if !ok {
					t.Fatalf("golden state %q missing", key)
				}
				got := s.SerializeData(levelOf(level))
				gotJSON, _ := json.Marshal(got)
				wantJSON, _ := json.Marshal(want)
				assertEqual(t, "SerializeData", string(gotJSON), string(wantJSON))
				assertEqual(t, "State.Data column", EncodeStateData(s, levelOf(level)), g.str(t, "state/"+key+"/data"))
				assertEqual(t, "name", s.StateName(), g.str(t, "state/"+key+"/name"))
				assertEqual(t, "final", map[bool]string{true: "true", false: "false"}[s.IsFinal()], g.str(t, "state/"+key+"/final"))

				back, err := DecodeState(s.StateName(), s.StateReason(), want)
				if err != nil {
					t.Fatal(err)
				}
				backJSON, _ := json.Marshal(back.SerializeData(levelOf(level)))
				assertEqual(t, "decoded state re-serialized", string(backJSON), string(wantJSON))
			})
		}
	}
}

func TestGoldenCron(t *testing.T) {
	g := golden(t)
	if len(g.Cron) == 0 {
		t.Fatal("no cron fixtures")
	}
	for _, c := range g.Cron {
		sched, err := ParseCron(c.Expr, c.TZ)
		if c.Error {
			if !errors.Is(err, ErrInvalidCron) {
				t.Errorf("ParseCron(%q) error = %v, want ErrInvalidCron", c.Expr, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseCron(%q, %q): %v", c.Expr, c.TZ, err)
			continue
		}
		from, _ := time.Parse(time.RFC3339, c.From)
		var got []string
		for cur, i := from, 0; i < 6; i++ {
			next, ok := sched.Next(cur)
			if !ok {
				break
			}
			got = append(got, next.UTC().Format("2006-01-02T15:04:05Z"))
			cur = next
		}
		if strings.Join(got, " ") != c.Next {
			t.Errorf("cron %q tz %s from %s:\n got  %s\n want %s", c.Expr, c.TZ, c.From, strings.Join(got, " "), c.Next)
		}
	}
}

func assertEqual(t *testing.T, what, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("%s differs from .NET:\n got  %s\n want %s", what, got, want)
	}
}
