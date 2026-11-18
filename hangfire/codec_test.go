package hangfire

import (
	"encoding/json"
	"errors"
	"math/rand"
	"strings"
	"testing"
	"time"
)

func TestValidateQueue(t *testing.T) {
	cases := []struct {
		queue string
		ok    bool
	}{
		{"default", true},
		{"critical_1-a", true},
		{"", false},
		{"Critical", false},
		{"with space", false},
		{"queue:1", false},
		{"émail", false},
	}
	for _, tc := range cases {
		err := ValidateQueue(tc.queue)
		if tc.ok != (err == nil) || (err != nil && !errors.Is(err, ErrInvalidQueue)) {
			t.Errorf("ValidateQueue(%q) = %v, want ok=%v", tc.queue, err, tc.ok)
		}
	}
}

func TestParseTime(t *testing.T) {
	cases := []struct {
		in   string
		want time.Time
		ok   bool
	}{
		{"1794996000123", t1, true},
		{"253402300799", time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC), true},
		{"1794996000", time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC), true},
		{"2026-11-18T10:00:00.1230000Z", t1, true},
		{"2026-11-18T17:00:00.123+07:00", t1, true},
		{"2026-11-18T10:00:00.123", t1, true},
		{"-5", time.Time{}, false},
		{" 1794996000123", time.Time{}, false},
		{"yesterday", time.Time{}, false},
	}
	for _, tc := range cases {
		got, err := ParseTime(tc.in)
		if tc.ok != (err == nil) || !got.Equal(tc.want) {
			t.Errorf("ParseTime(%q) = %v, %v; want %v ok=%v", tc.in, got, err, tc.want, tc.ok)
		}
	}
}

func TestEncodePayloadRejects(t *testing.T) {
	inv := Invocation{Type: "T, A", Method: "M", ParameterTypes: []string{"System.String"}, Args: []any{"x"}}
	cases := []struct {
		name  string
		inv   Invocation
		queue string
		level CompatLevel
	}{
		{"bad queue", inv, "Bad", CompatLevel180},
		{"arity mismatch", Invocation{Type: "T, A", Method: "M", Args: []any{1}}, "", CompatLevel180},
		{"unsupported level", inv, "", 110},
		{"unencodable argument", Invocation{Type: "T, A", Method: "M", ParameterTypes: []string{"x"}, Args: []any{make(chan int)}}, "", CompatLevel180},
	}
	for _, tc := range cases {
		if _, err := EncodePayload(tc.inv, tc.queue, true, tc.level); err == nil {
			t.Errorf("%s: EncodePayload succeeded, want error", tc.name)
		}
	}
}

func TestEncodeArgumentsEmpty(t *testing.T) {
	for _, args := range [][]any{nil, {}} {
		got, err := EncodeArguments(args)
		if err != nil || got != "[]" {
			t.Errorf("EncodeArguments(%#v) = %q, %v; want []", args, got, err)
		}
	}
}

func TestDecodePayloadLegacyFormat(t *testing.T) {
	legacy := `{"Type":"MyApp.Jobs, MyApp","Method":"Run","ParameterTypes":"[\"System.String\",\"System.Int32\"]","Arguments":"[\"\\\"x\\\"\",null]","Queue":"low"}`
	cases := []struct {
		name      string
		arguments string
		wantArgs  []string
	}{
		{"payload arguments", "", []string{`"x"`, ""}},
		{"column overrides", `["\"y\"","5"]`, []string{`"y"`, "5"}},
	}
	for _, tc := range cases {
		p, err := DecodePayload(legacy, tc.arguments)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if p.Type != "MyApp.Jobs, MyApp" || p.Method != "Run" || p.Queue != "low" || strings.Join(p.ParameterTypes, "|") != "System.String|System.Int32" {
			t.Errorf("%s: decoded %+v", tc.name, p)
		}
		var got []string
		for _, a := range p.Args {
			got = append(got, string(a))
		}
		if strings.Join(got, "|") != strings.Join(tc.wantArgs, "|") || (tc.wantArgs[1] == "" && p.Args[1] != nil) {
			t.Errorf("%s: args = %q, want %q", tc.name, got, tc.wantArgs)
		}
	}
	if _, err := DecodePayload(`{"x":1}`, ""); err == nil {
		t.Error("DecodePayload accepted a payload with no type or method")
	}
}

func TestDecodeContinuationsVariants(t *testing.T) {
	cases := []struct {
		in   string
		want []Continuation
	}{
		{"", nil},
		{"null", nil},
		{`[{"jobId":"7","options":1}]`, []Continuation{{"7", OnlyOnSucceededState}}},
		{`[{"JobId":"8","Options":"OnlyOnDeletedState"}]`, []Continuation{{"8", OnlyOnDeletedState}}},
	}
	for _, tc := range cases {
		got, err := DecodeContinuations(tc.in)
		if err != nil || len(got) != len(tc.want) {
			t.Fatalf("DecodeContinuations(%q) = %v, %v; want %v", tc.in, got, err, tc.want)
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("DecodeContinuations(%q)[%d] = %v, want %v", tc.in, i, got[i], tc.want[i])
			}
		}
	}
}

func TestNextStateRoundTrip(t *testing.T) {
	cases := []struct {
		in   string
		want State
	}{
		{`{"$type":"Hangfire.States.EnqueuedState, Hangfire.Core, Version=1.7.0.0","Queue":"critical"}`, Enqueued{Queue: "critical"}},
		{`{"$type":"Hangfire.States.EnqueuedState, Hangfire.Core","Queue":"default"}`, Enqueued{}},
		{`{"$type":"Hangfire.States.ScheduledState, Hangfire.Core","EnqueueAt":"2026-11-19T08:30:00Z","Reason":"later"}`, Scheduled{EnqueueAt: t2, Reason: "later"}},
	}
	for _, tc := range cases {
		got, err := DecodeNextState(tc.in)
		if err != nil {
			t.Fatalf("DecodeNextState(%s): %v", tc.in, err)
		}
		if got != tc.want {
			t.Errorf("DecodeNextState(%s) = %#v, want %#v", tc.in, got, tc.want)
		}
	}
	if _, err := DecodeNextState(`{"$type":"Hangfire.States.FailedState, Hangfire.Core"}`); err == nil {
		t.Error("DecodeNextState accepted an unsupported state type")
	}
	if _, err := EncodeNextState(Deleted{}); err == nil {
		t.Error("EncodeNextState accepted a Deleted next state")
	}
	got, err := EncodeNextState(Scheduled{EnqueueAt: time.Date(2026, 1, 2, 3, 4, 5, 123_456_789, time.FixedZone("x", 3600))})
	if want := `{"$type":"Hangfire.States.ScheduledState, Hangfire.Core","EnqueueAt":"2026-01-02T02:04:05.1234567Z"}`; err != nil || got != want {
		t.Errorf("EncodeNextState(scheduled) = %s, %v; want %s", got, err, want)
	}
}

func TestDecodeLegacyAwaitingState(t *testing.T) {
	s, err := DecodeState("Awaiting", "", map[string]string{
		"ParentId":   "5",
		"NextState":  `{"$type":"Hangfire.States.EnqueuedState, Hangfire.Core","Queue":"default"}`,
		"Options":    "OnlyOnSucceededState",
		"Expiration": "365.00:00:00",
	})
	want := Awaiting{ParentID: "5", NextState: Enqueued{}, Options: OnlyOnSucceededState, Expiration: 365 * 24 * time.Hour}
	if err != nil || s != want {
		t.Errorf("DecodeState(legacy awaiting) = %#v, %v; want %#v", s, err, want)
	}
	if _, err := DecodeState("Enqueued", "", map[string]string{"EnqueuedAt": "soon"}); err == nil {
		t.Error("DecodeState accepted a malformed date")
	}
	if _, err := DecodeState("Custom", "", nil); err == nil {
		t.Error("DecodeState accepted an unknown state")
	}
}

func TestRetryDelay(t *testing.T) {
	rnd := rand.New(rand.NewSource(1))
	for attempt := 1; attempt <= 10; attempt++ {
		base := time.Duration((attempt-1)*(attempt-1)*(attempt-1)*(attempt-1)+15) * time.Second
		for i := 0; i < 200; i++ {
			d := RetryDelay(attempt, rnd)
			extra := d - base
			if extra < 0 || extra > time.Duration(29*attempt)*time.Second || extra%(time.Duration(attempt)*time.Second) != 0 {
				t.Fatalf("RetryDelay(%d) = %v, want %v + k*%ds with k in [0,29]", attempt, d, base, attempt)
			}
		}
	}
}

func TestRetryReason(t *testing.T) {
	cases := []struct {
		msg, want string
	}{
		{"boom", "Retry attempt 1 of 10: boom"},
		{strings.Repeat("a", 50), "Retry attempt 1 of 10: " + strings.Repeat("a", 50)},
		{strings.Repeat("a", 51), "Retry attempt 1 of 10: " + strings.Repeat("a", 49) + "…"},
		{strings.Repeat("é", 60), "Retry attempt 1 of 10: " + strings.Repeat("é", 49) + "…"},
	}
	for _, tc := range cases {
		if got := RetryReason(1, 10, tc.msg); got != tc.want {
			t.Errorf("RetryReason(%q) = %q, want %q", tc.msg, got, tc.want)
		}
	}
}

func TestWithDotNetType(t *testing.T) {
	type dog struct {
		Name string `json:"Name"`
	}
	cases := []struct {
		v    any
		want string
	}{
		{dog{Name: "Rex"}, `["{\"$type\":\"Zoo.Dog, Zoo\",\"Name\":\"Rex\"}"]`},
		{struct{}{}, `["{\"$type\":\"Zoo.Dog, Zoo\"}"]`},
	}
	for _, tc := range cases {
		got, err := EncodeArguments([]any{WithDotNetType(tc.v, "Zoo.Dog, Zoo")})
		if err != nil || got != tc.want {
			t.Errorf("EncodeArguments(WithDotNetType(%#v)) = %s, %v; want %s", tc.v, got, err, tc.want)
		}
	}
	if _, err := json.Marshal(WithDotNetType(5, "System.Int32")); err == nil {
		t.Error("WithDotNetType accepted a non-object value")
	}
}

func TestParseCronTimeZones(t *testing.T) {
	cases := []struct {
		tz   string
		want string
	}{
		{"", "2026-11-19T09:00:00Z"},
		{"UTC", "2026-11-19T09:00:00Z"},
		{"SE Asia Standard Time", "2026-11-19T02:00:00Z"},
		{"Eastern Standard Time", "2026-11-18T14:00:00Z"},
	}
	for _, tc := range cases {
		s, err := ParseCron("0 9 * * *", tc.tz)
		if err != nil {
			t.Fatalf("ParseCron tz %q: %v", tc.tz, err)
		}
		next, ok := s.Next(time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC))
		if !ok || next.UTC().Format(time.RFC3339) != tc.want {
			t.Errorf("tz %q: next = %v, want %s", tc.tz, next.UTC(), tc.want)
		}
	}
	if _, err := ParseCron("* * * * *", "Mars/Olympus"); err == nil {
		t.Error("ParseCron accepted an unknown time zone")
	}
}
