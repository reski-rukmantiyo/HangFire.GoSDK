package hangfire

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// field is one State.Data entry. Order matters: .NET writes State.Data from a
// Dictionary in insertion order, and EncodeStateData reproduces it.
type field struct{ key, value string }

type stateFields interface {
	State
	fields(level CompatLevel) []field
}

func dataMap(fs []field) map[string]string {
	m := make(map[string]string, len(fs))
	for _, f := range fs {
		m[f.key] = f.value
	}
	return m
}

// EncodeStateData produces the State.Data column exactly as
// SqlServerWriteOnlyTransaction.cs:200 writes it.
func EncodeStateData(s State, level CompatLevel) string {
	var o jsonObject
	for _, f := range s.(stateFields).fields(level) {
		o.str(f.key, f.value)
	}
	return o.String()
}

func queueOrDefault(q string) string {
	if q == "" {
		return "default"
	}
	return q
}

func (s Enqueued) StateName() string   { return StateEnqueued }
func (s Enqueued) StateReason() string { return s.Reason }
func (s Enqueued) IsFinal() bool       { return false }
func (s Enqueued) isState()            {}
func (s Enqueued) fields(l CompatLevel) []field {
	return []field{{"EnqueuedAt", FormatTime(s.EnqueuedAt, l)}, {"Queue", queueOrDefault(s.Queue)}}
}
func (s Enqueued) SerializeData(l CompatLevel) map[string]string { return dataMap(s.fields(l)) }

func (s Scheduled) StateName() string   { return StateScheduled }
func (s Scheduled) StateReason() string { return s.Reason }
func (s Scheduled) IsFinal() bool       { return false }
func (s Scheduled) isState()            {}
func (s Scheduled) fields(l CompatLevel) []field {
	return []field{{"EnqueueAt", FormatTime(s.EnqueueAt, l)}, {"ScheduledAt", FormatTime(s.ScheduledAt, l)}}
}
func (s Scheduled) SerializeData(l CompatLevel) map[string]string { return dataMap(s.fields(l)) }

func (s Awaiting) StateName() string   { return StateAwaiting }
func (s Awaiting) StateReason() string { return s.Reason }
func (s Awaiting) IsFinal() bool       { return false }
func (s Awaiting) isState()            {}

// fields panics when NextState is neither Enqueued nor Scheduled; callers
// validate it with EncodeNextState at the API boundary.
func (s Awaiting) fields(CompatLevel) []field {
	next, err := EncodeNextState(s.NextState)
	if err != nil {
		panic(err)
	}
	// Expiration is written only below level 170 (AwaitingState.cs:209-217).
	return []field{{"ParentId", string(s.ParentID)}, {"NextState", next}, {"Options", strconv.Itoa(int(s.Options))}}
}
func (s Awaiting) SerializeData(l CompatLevel) map[string]string { return dataMap(s.fields(l)) }

func (s Processing) StateName() string   { return StateProcessing }
func (s Processing) StateReason() string { return s.Reason }
func (s Processing) IsFinal() bool       { return false }
func (s Processing) isState()            {}
func (s Processing) fields(l CompatLevel) []field {
	return []field{{"StartedAt", FormatTime(s.StartedAt, l)}, {"ServerId", s.ServerID}, {"WorkerId", s.WorkerID}}
}
func (s Processing) SerializeData(l CompatLevel) map[string]string { return dataMap(s.fields(l)) }

func (s Succeeded) StateName() string   { return StateSucceeded }
func (s Succeeded) StateReason() string { return s.Reason }
func (s Succeeded) IsFinal() bool       { return true }
func (s Succeeded) isState()            {}
func (s Succeeded) fields(l CompatLevel) []field {
	fs := []field{
		{"SucceededAt", FormatTime(s.SucceededAt, l)},
		{"PerformanceDuration", strconv.FormatInt(s.PerformanceDuration.Milliseconds(), 10)},
		{"Latency", strconv.FormatInt(s.Latency.Milliseconds(), 10)},
	}
	if s.Result != nil {
		fs = append(fs, field{"Result", string(s.Result)})
	}
	return fs
}
func (s Succeeded) SerializeData(l CompatLevel) map[string]string { return dataMap(s.fields(l)) }

func (s Failed) StateName() string   { return StateFailed }
func (s Failed) StateReason() string { return s.Reason }
func (s Failed) IsFinal() bool       { return false }
func (s Failed) isState()            {}
func (s Failed) fields(l CompatLevel) []field {
	fs := []field{
		{"FailedAt", FormatTime(s.FailedAt, l)},
		{"ExceptionType", s.ExceptionType},
		{"ExceptionMessage", s.ExceptionMessage},
		{"ExceptionDetails", s.ExceptionDetails},
	}
	if s.ServerID != "" {
		fs = append(fs, field{"ServerId", s.ServerID})
	}
	return fs
}
func (s Failed) SerializeData(l CompatLevel) map[string]string { return dataMap(s.fields(l)) }

func (s Deleted) StateName() string   { return StateDeleted }
func (s Deleted) StateReason() string { return s.Reason }
func (s Deleted) IsFinal() bool       { return true }
func (s Deleted) isState()            {}
func (s Deleted) fields(l CompatLevel) []field {
	fs := []field{{"DeletedAt", FormatTime(s.DeletedAt, l)}}
	if s.Exception != "" {
		fs = append(fs, field{"Exception", s.Exception})
	}
	return fs
}
func (s Deleted) SerializeData(l CompatLevel) map[string]string { return dataMap(s.fields(l)) }

const (
	enqueuedStateType  = "Hangfire.States.EnqueuedState"
	scheduledStateType = "Hangfire.States.ScheduledState"
)

// EncodeNextState serializes an Awaiting state's next state the way
// SerializationHelper's TypedInternal settings do (AwaitingState.cs:206).
// A nil state means Enqueued on the default queue.
func EncodeNextState(s State) (string, error) {
	var o jsonObject
	switch st := s.(type) {
	case nil:
		return EncodeNextState(Enqueued{})
	case Enqueued:
		if st.Queue != "" {
			if err := ValidateQueue(st.Queue); err != nil {
				return "", err
			}
		}
		o.str("$type", enqueuedStateType+", Hangfire.Core")
		o.str("Queue", queueOrDefault(st.Queue))
		if st.Reason != "" {
			o.str("Reason", st.Reason)
		}
	case Scheduled:
		o.str("$type", scheduledStateType+", Hangfire.Core")
		o.str("EnqueueAt", st.EnqueueAt.UTC().Truncate(100*time.Nanosecond).Format("2006-01-02T15:04:05.9999999Z07:00"))
		if st.Reason != "" {
			o.str("Reason", st.Reason)
		}
	default:
		return "", fmt.Errorf("hangfire: next state must be Enqueued or Scheduled, got %s", s.StateName())
	}
	return o.String(), nil
}

// DecodeNextState reads a serialized next state. The "$type" assembly part
// and any version suffix are ignored.
func DecodeNextState(s string) (State, error) {
	var v struct {
		Type      string  `json:"$type"`
		Queue     *string `json:"Queue"`
		EnqueueAt string  `json:"EnqueueAt"`
		Reason    string  `json:"Reason"`
	}
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return nil, fmt.Errorf("hangfire: next state: %w", err)
	}
	typeName, _, _ := strings.Cut(v.Type, ",")
	switch strings.TrimSpace(typeName) {
	case enqueuedStateType:
		st := Enqueued{Reason: v.Reason}
		if v.Queue != nil && *v.Queue != "default" {
			st.Queue = *v.Queue
		}
		return st, nil
	case scheduledStateType:
		at, err := ParseTime(v.EnqueueAt)
		if err != nil {
			return nil, err
		}
		return Scheduled{EnqueueAt: at, Reason: v.Reason}, nil
	}
	return nil, fmt.Errorf("hangfire: unsupported next state type %q", v.Type)
}

// DecodeState rebuilds a State from a State row. Missing date keys decode as
// the zero time; malformed values are errors.
func DecodeState(name, reason string, data map[string]string) (State, error) {
	var err error
	date := func(key string) time.Time {
		v, ok := data[key]
		if !ok || v == "" || err != nil {
			return time.Time{}
		}
		var t time.Time
		t, err = ParseTime(v)
		return t
	}
	millis := func(key string) time.Duration {
		v, ok := data[key]
		if !ok || err != nil {
			return 0
		}
		var n int64
		n, err = strconv.ParseInt(v, 10, 64)
		return time.Duration(n) * time.Millisecond
	}

	var s State
	switch {
	case strings.EqualFold(name, StateEnqueued):
		q := data["Queue"]
		if q == "default" {
			q = ""
		}
		s = Enqueued{Queue: q, EnqueuedAt: date("EnqueuedAt"), Reason: reason}
	case strings.EqualFold(name, StateScheduled):
		s = Scheduled{EnqueueAt: date("EnqueueAt"), ScheduledAt: date("ScheduledAt"), Reason: reason}
	case strings.EqualFold(name, StateAwaiting):
		a := Awaiting{ParentID: JobID(data["ParentId"]), Reason: reason}
		if v := data["NextState"]; v != "" {
			a.NextState, err = DecodeNextState(v)
		}
		if v, ok := data["Options"]; ok && err == nil {
			var n int
			n, err = parseEnumText(v, continuationOptionNames)
			a.Options = ContinuationOptions(n)
		}
		if v, ok := data["Expiration"]; ok && err == nil {
			a.Expiration, err = parseTimeSpan(v)
		}
		s = a
	case strings.EqualFold(name, StateProcessing):
		s = Processing{ServerID: data["ServerId"], WorkerID: data["WorkerId"], StartedAt: date("StartedAt"), Reason: reason}
	case strings.EqualFold(name, StateSucceeded):
		st := Succeeded{SucceededAt: date("SucceededAt"), PerformanceDuration: millis("PerformanceDuration"), Latency: millis("Latency"), Reason: reason}
		if v, ok := data["Result"]; ok {
			st.Result = json.RawMessage(v)
		}
		s = st
	case strings.EqualFold(name, StateFailed):
		s = Failed{
			FailedAt:         date("FailedAt"),
			ExceptionType:    data["ExceptionType"],
			ExceptionMessage: data["ExceptionMessage"],
			ExceptionDetails: data["ExceptionDetails"],
			ServerID:         data["ServerId"],
			Reason:           reason,
		}
	case strings.EqualFold(name, StateDeleted):
		s = Deleted{DeletedAt: date("DeletedAt"), Exception: data["Exception"], Reason: reason}
	default:
		return nil, fmt.Errorf("hangfire: unknown state %q", name)
	}
	if err != nil {
		return nil, fmt.Errorf("hangfire: %s state: %w", name, err)
	}
	return s, nil
}

// parseTimeSpan reads the .NET TimeSpan "c" format: [-][d.]hh:mm:ss[.fffffff].
func parseTimeSpan(s string) (time.Duration, error) {
	orig := s
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	var days int64
	if dot, colon := strings.IndexByte(s, '.'), strings.IndexByte(s, ':'); dot >= 0 && dot < colon {
		d, err := strconv.ParseInt(s[:dot], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("hangfire: timespan %q", orig)
		}
		days, s = d, s[dot+1:]
	}
	clock, frac, _ := strings.Cut(s, ".")
	parts := strings.Split(clock, ":")
	if len(parts) != 3 {
		return 0, fmt.Errorf("hangfire: timespan %q", orig)
	}
	var hms [3]int64
	for i, p := range parts {
		n, err := strconv.ParseInt(p, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("hangfire: timespan %q", orig)
		}
		hms[i] = n
	}
	d := time.Duration(days)*24*time.Hour + time.Duration(hms[0])*time.Hour + time.Duration(hms[1])*time.Minute + time.Duration(hms[2])*time.Second
	if frac != "" {
		f, err := strconv.ParseInt((frac + "0000000")[:7], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("hangfire: timespan %q", orig)
		}
		d += time.Duration(f) * 100
	}
	if neg {
		d = -d
	}
	return d, nil
}
