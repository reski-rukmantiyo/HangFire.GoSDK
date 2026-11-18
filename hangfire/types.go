// Package hangfire holds the storage-independent model of Hangfire jobs:
// invocations, states, and the byte-level codecs .NET Hangfire servers read.
// See docs/golang-sdk-srs.md for the requirements each part implements.
package hangfire

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// JobID is a Hangfire background job id. SQL Server storage uses bigint ids,
// written as decimal strings everywhere Hangfire serializes them.
type JobID string

// RecurringJobID names a recurring job. It is a separate type so it can't be
// passed where a background job id is expected.
type RecurringJobID string

// CompatLevel is the .NET CompatibilityLevel of the oldest server in the
// fleet (SRS CR-02). Only 170 and 180 are supported.
type CompatLevel int

const (
	CompatLevel170 CompatLevel = 170
	CompatLevel180 CompatLevel = 180
)

// Invocation names the method to run and its arguments (SRS DR-02, DR-05).
// Type and Method are opaque strings written exactly as given.
type Invocation struct {
	Type           string   // "MyApp.Jobs.EmailJob, MyApp"
	Method         string   // "Send"
	ParameterTypes []string // ["System.String", "System.Int32"]
	Args           []any    // encoded per DR-05/DR-06; nil encodes as JSON null
}

// ContinuationOptions mirrors JobContinuationOptions (SRS FR-P21).
type ContinuationOptions int

const (
	OnAnyFinishedState   ContinuationOptions = 0
	OnlyOnSucceededState ContinuationOptions = 1
	OnlyOnDeletedState   ContinuationOptions = 2
)

// MisfireHandling mirrors MisfireHandlingMode (SRS FR-P30).
type MisfireHandling int

const (
	MisfireRelaxed   MisfireHandling = 0
	MisfireStrict    MisfireHandling = 1
	MisfireIgnorable MisfireHandling = 2
)

// State is a closed sum type over Hangfire's built-in states (SRS DR-10).
// Each concrete struct carries only its own fields.
type State interface {
	StateName() string
	StateReason() string
	IsFinal() bool
	// SerializeData returns State.Data exactly as the .NET state would.
	SerializeData(level CompatLevel) map[string]string
	isState()
}

const (
	StateEnqueued   = "Enqueued"
	StateScheduled  = "Scheduled"
	StateAwaiting   = "Awaiting"
	StateProcessing = "Processing"
	StateSucceeded  = "Succeeded"
	StateFailed     = "Failed"
	StateDeleted    = "Deleted"
)

type Enqueued struct {
	Queue      string // "" means "default"
	EnqueuedAt time.Time
	Reason     string
}

type Scheduled struct {
	EnqueueAt   time.Time
	ScheduledAt time.Time
	Reason      string
}

type Awaiting struct {
	ParentID   JobID
	NextState  State // Enqueued or Scheduled; nil means Enqueued on the job's queue
	Options    ContinuationOptions
	Expiration time.Duration // 0 means 365 days
	Reason     string
}

type Processing struct {
	ServerID  string
	WorkerID  string
	StartedAt time.Time
	Reason    string
}

type Succeeded struct {
	SucceededAt         time.Time
	Result              json.RawMessage // nil omits the Result key
	PerformanceDuration time.Duration
	Latency             time.Duration
	Reason              string
}

type Failed struct {
	FailedAt         time.Time
	ExceptionType    string
	ExceptionMessage string
	ExceptionDetails string
	ServerID         string
	Reason           string
}

type Deleted struct {
	DeletedAt time.Time
	Exception string // optional serialized exception info
	Reason    string
}

// EnqueueOptions configures job creation.
type EnqueueOptions struct {
	Queue      string         // "" means "default"; validated with DR-08
	Parameters map[string]any // extra job parameters, JSON-encoded per DR-09
}

type Option func(*EnqueueOptions)

func WithQueue(q string) Option { return func(o *EnqueueOptions) { o.Queue = q } }

func WithParameter(name string, v any) Option {
	return func(o *EnqueueOptions) {
		if o.Parameters == nil {
			o.Parameters = map[string]any{}
		}
		o.Parameters[name] = v
	}
}

// RecurringOptions configures a recurring job (SRS FR-P30).
type RecurringOptions struct {
	TimeZone string // IANA or Windows id as the .NET servers resolve it; "" means "UTC"
	Queue    string // "" means "default"
	Misfire  MisfireHandling
}

// Client is the producer API (SRS FR-P01..FR-P42, FR-P50).
type Client interface {
	Enqueue(ctx context.Context, inv Invocation, opts ...Option) (JobID, error)
	Schedule(ctx context.Context, inv Invocation, delay time.Duration, opts ...Option) (JobID, error)
	ScheduleAt(ctx context.Context, inv Invocation, at time.Time, opts ...Option) (JobID, error)
	ContinueJobWith(ctx context.Context, parent JobID, inv Invocation, next State, cont ContinuationOptions, opts ...Option) (JobID, error)
	Create(ctx context.Context, inv Invocation, initial State, opts ...Option) (JobID, error)
	// ChangeState returns false when expected is non-empty and the current state differs.
	ChangeState(ctx context.Context, id JobID, s State, expected string) (bool, error)
	Delete(ctx context.Context, id JobID) (bool, error)
	Requeue(ctx context.Context, id JobID) (bool, error)
	Reschedule(ctx context.Context, id JobID, at time.Time) (bool, error)
}

// RecurringManager is the recurring-job API (SRS FR-P30..FR-P36).
type RecurringManager interface {
	AddOrUpdate(ctx context.Context, id RecurringJobID, inv Invocation, cron string, opts RecurringOptions) error
	RemoveIfExists(ctx context.Context, id RecurringJobID) error
	Trigger(ctx context.Context, id RecurringJobID) (JobID, error)
}

// Sentinel errors (SRS NFR-08). Callers match them with errors.Is.
var (
	ErrParentNotFound  = errors.New("hangfire: parent job not found")
	ErrInvalidQueue    = errors.New("hangfire: invalid queue name")
	ErrInvalidCron     = errors.New("hangfire: invalid cron expression")
	ErrSchemaVersion   = errors.New("hangfire: unsupported schema version")
	ErrStateMismatch   = errors.New("hangfire: job is not in the expected state")
	ErrLockTimeout     = errors.New("hangfire: distributed lock timeout")
	ErrJobNotFound     = errors.New("hangfire: job not found")
	ErrPayloadTooLarge = errors.New("hangfire: argument payload too large")
)
