package server

import (
	"context"
	"time"

	"hangfire-gosdk/hangfire"
	"hangfire-gosdk/sqlserver"
)

// JobContext describes the running job (SRS FR-W07). Handlers get it with FromContext.
type JobContext struct {
	JobID      hangfire.JobID
	ServerID   string
	WorkerID   string
	Queue      string // the queue the job was fetched from
	RetryCount int    // retries done so far; 0 on the first run
	CreatedAt  time.Time
	storage    *sqlserver.Storage
}

type jobContextKey struct{}

// FromContext returns the JobContext of the job a handler is running, or nil.
func FromContext(ctx context.Context) *JobContext {
	jc, _ := ctx.Value(jobContextKey{}).(*JobContext)
	return jc
}

// Parameter decodes the job parameter name into out. It reports false when
// the parameter does not exist.
func (j *JobContext) Parameter(ctx context.Context, name string, out any) (bool, error) {
	raw, ok, err := j.storage.JobParameter(ctx, j.JobID, name)
	if err != nil || !ok {
		return false, err
	}
	return true, hangfire.DecodeParameter(raw, out)
}

// SetParameter stores v as JSON in the job parameter name, as
// PerformContext.SetJobParameter does.
func (j *JobContext) SetParameter(ctx context.Context, name string, v any) error {
	raw, err := hangfire.EncodeParameter(v)
	if err != nil {
		return err
	}
	return j.storage.SetJobParameter(ctx, j.JobID, name, raw)
}
