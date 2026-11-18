package hangfire

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// ValidateQueue reports whether q is a queue name .NET Hangfire accepts:
// ^[a-z0-9_-]+$ (EnqueuedState.cs:251-262).
func ValidateQueue(q string) error {
	if q == "" {
		return fmt.Errorf("%w: empty", ErrInvalidQueue)
	}
	for i := 0; i < len(q); i++ {
		c := q[i]
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return fmt.Errorf("%w: %q", ErrInvalidQueue, q)
		}
	}
	return nil
}

var (
	unixEpoch = time.Unix(0, 0).UTC()
	// JobHelper.MillisecondTimestampBoundaryDate: dates at or before it keep the ISO form.
	msBoundaryDate = time.Date(1978, 1, 11, 21, 31, 40, 799_000_000, time.UTC)
	dotNetMaxValue = time.Date(9999, 12, 31, 23, 59, 59, 999_999_900, time.UTC)
)

const msTimestampBoundary = 253402300799

// FormatTime serializes t as JobHelper.SerializeDateTime does (JobHelper.cs:75-85).
// Both supported levels write Unix milliseconds for dates after 1978-01-11.
func FormatTime(t time.Time, level CompatLevel) string {
	t = t.UTC().Truncate(100 * time.Nanosecond)
	if t.After(msBoundaryDate) && t.Before(dotNetMaxValue) {
		return strconv.FormatInt(t.UnixMilli(), 10)
	}
	return t.Format("2006-01-02T15:04:05.0000000Z")
}

// ParseTime reads every date form JobHelper.DeserializeDateTime accepts:
// Unix milliseconds, Unix seconds (values <= 253402300799), and ISO 8601.
func ParseTime(s string) (time.Time, error) {
	if isDigits(s) {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return time.Time{}, fmt.Errorf("hangfire: date %q: %w", s, err)
		}
		if n > msTimestampBoundary {
			return time.UnixMilli(n).UTC(), nil
		}
		return time.Unix(n, 0).UTC(), nil
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999", "2006-01-02T15:04:05.999999999Z0700"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("hangfire: unrecognized date %q", s)
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// EncodeArguments produces Job.Arguments: a JSON array whose items are the
// JSON text of each argument, or the null literal for nil (InvocationData.cs:229-276).
func EncodeArguments(args []any) (string, error) {
	items, err := encodeArgs(args)
	if err != nil {
		return "", err
	}
	return argsArray(items), nil
}

// encodeArgs returns each argument's JSON; a nil entry stands for null.
func encodeArgs(args []any) ([]*string, error) {
	items := make([]*string, len(args))
	for i, a := range args {
		if a == nil {
			continue
		}
		s, err := marshalNet(a)
		if err != nil {
			return nil, fmt.Errorf("hangfire: argument %d: %w", i, err)
		}
		if s == "null" {
			continue
		}
		items[i] = &s
	}
	return items, nil
}

func argsArray(items []*string) string {
	b := []byte{'['}
	for i, it := range items {
		if i > 0 {
			b = append(b, ',')
		}
		if it == nil {
			b = append(b, "null"...)
		} else {
			b = append(b, quoteNet(*it)...)
		}
	}
	return string(append(b, ']'))
}

// EncodePayload produces the compact InvocationData payload written at
// compatibility level 170 and above (InvocationData.cs:183-197, 432-449).
// includeArgs adds the "a" member, as recurring-job hashes need (SRS DR-12);
// Job.InvocationData omits it because Job.Arguments holds them (DR-01).
func EncodePayload(inv Invocation, queue string, includeArgs bool, level CompatLevel) (string, error) {
	if level != CompatLevel170 && level != CompatLevel180 {
		return "", fmt.Errorf("hangfire: unsupported compatibility level %d", level)
	}
	if queue != "" {
		if err := ValidateQueue(queue); err != nil {
			return "", err
		}
	}
	if len(inv.Args) != len(inv.ParameterTypes) {
		return "", fmt.Errorf("hangfire: %d arguments for %d parameter types", len(inv.Args), len(inv.ParameterTypes))
	}
	var o jsonObject
	o.str("t", inv.Type)
	o.str("m", inv.Method)
	if len(inv.ParameterTypes) > 0 {
		o.raw("p", jsonStringArray(inv.ParameterTypes))
	}
	if includeArgs && len(inv.Args) > 0 {
		items, err := encodeArgs(inv.Args)
		if err != nil {
			return "", err
		}
		o.raw("a", argsArray(items))
	}
	if queue != "" {
		o.str("q", queue)
	}
	return o.String(), nil
}

// Payload is a decoded invocation. Args holds each argument's JSON text;
// a nil entry means the argument was null.
type Payload struct {
	Type           string
	Method         string
	ParameterTypes []string
	Args           []json.RawMessage
	Queue          string
}

// DecodePayload reads Job.InvocationData in the compact format or the legacy
// pre-1.7 format, mirroring InvocationData.DeserializePayload. A non-empty
// arguments value (the Job.Arguments column) replaces the payload's own
// arguments, as SqlServerConnection.cs:260-265 does.
func DecodePayload(invocationData, arguments string) (Payload, error) {
	var compact struct {
		T *string   `json:"t"`
		M *string   `json:"m"`
		P []string  `json:"p"`
		A []*string `json:"a"`
		Q string    `json:"q"`
	}
	var p Payload
	if err := json.Unmarshal([]byte(invocationData), &compact); err == nil && compact.T != nil && compact.M != nil {
		p = Payload{Type: *compact.T, Method: *compact.M, ParameterTypes: compact.P, Queue: compact.Q, Args: rawArgs(compact.A)}
	} else {
		var legacy struct {
			Type           string
			Method         string
			ParameterTypes string
			Arguments      string
			Queue          string
		}
		if err := json.Unmarshal([]byte(invocationData), &legacy); err != nil {
			return Payload{}, fmt.Errorf("hangfire: invocation data: %w", err)
		}
		if legacy.Type == "" || legacy.Method == "" {
			return Payload{}, errors.New("hangfire: invocation data has no type or method")
		}
		p = Payload{Type: legacy.Type, Method: legacy.Method, Queue: legacy.Queue}
		if legacy.ParameterTypes != "" {
			if err := json.Unmarshal([]byte(legacy.ParameterTypes), &p.ParameterTypes); err != nil {
				return Payload{}, fmt.Errorf("hangfire: parameter types: %w", err)
			}
		}
		if legacy.Arguments != "" {
			args, err := decodeArgsArray(legacy.Arguments)
			if err != nil {
				return Payload{}, err
			}
			p.Args = args
		}
	}
	if arguments != "" {
		args, err := decodeArgsArray(arguments)
		if err != nil {
			return Payload{}, err
		}
		p.Args = args
	}
	return p, nil
}

func decodeArgsArray(s string) ([]json.RawMessage, error) {
	var items []*string
	if err := json.Unmarshal([]byte(s), &items); err != nil {
		return nil, fmt.Errorf("hangfire: arguments: %w", err)
	}
	return rawArgs(items), nil
}

func rawArgs(items []*string) []json.RawMessage {
	if items == nil {
		return nil
	}
	out := make([]json.RawMessage, len(items))
	for i, it := range items {
		if it != nil {
			out[i] = json.RawMessage(*it)
		}
	}
	return out
}

// EncodeParameter produces a JobParameter value: the JSON of v, so strings
// are quoted (CoreBackgroundJobFactory.cs:65-68).
func EncodeParameter(v any) (string, error) { return marshalNet(v) }

// DecodeParameter reads a JobParameter value written by EncodeParameter or .NET.
func DecodeParameter(s string, out any) error { return json.Unmarshal([]byte(s), out) }

// Continuation is one entry of a parent job's "Continuations" parameter.
type Continuation struct {
	JobID   JobID
	Options ContinuationOptions
}

var continuationOptionNames = map[string]int{
	"onanyfinishedstate":   int(OnAnyFinishedState),
	"onlyonsucceededstate": int(OnlyOnSucceededState),
	"onlyondeletedstate":   int(OnlyOnDeletedState),
}

// EncodeContinuations writes the "Continuations" parameter value, omitting
// Options when it is 0 (ContinuationsSupportAttribute.cs:349-353).
func EncodeContinuations(cs []Continuation) (string, error) {
	b := []byte{'['}
	for i, c := range cs {
		if i > 0 {
			b = append(b, ',')
		}
		var o jsonObject
		o.str("JobId", string(c.JobID))
		if c.Options != 0 {
			o.raw("Options", strconv.Itoa(int(c.Options)))
		}
		b = append(b, o.String()...)
	}
	return string(append(b, ']')), nil
}

// DecodeContinuations reads the "Continuations" parameter. Empty or null
// values mean no continuations. Keys match case-insensitively, which covers
// the camelCase fallback of ContinuationsSupportAttribute.cs:106-116.
func DecodeContinuations(s string) ([]Continuation, error) {
	if s == "" {
		return nil, nil
	}
	var items []struct {
		JobID   string          `json:"JobId"`
		Options json.RawMessage `json:"Options"`
	}
	if err := json.Unmarshal([]byte(s), &items); err != nil {
		return nil, fmt.Errorf("hangfire: continuations: %w", err)
	}
	out := make([]Continuation, 0, len(items))
	for _, it := range items {
		c := Continuation{JobID: JobID(it.JobID)}
		if len(it.Options) > 0 && string(it.Options) != "null" {
			n, err := netEnumOrInt(it.Options, continuationOptionNames)
			if err != nil {
				return nil, fmt.Errorf("hangfire: continuation %s: %w", it.JobID, err)
			}
			c.Options = ContinuationOptions(n)
		}
		out = append(out, c)
	}
	return out, nil
}
