package sqlserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"hangfire-gosdk/hangfire"
)

// JobData is a job row with its parameters, as IStorageConnection.GetJobData returns it.
type JobData struct {
	ID         hangfire.JobID
	Payload    hangfire.Payload
	PayloadErr error // set when InvocationData can't be decoded; the job can still be deleted
	StateName  string
	CreatedAt  time.Time
	Parameters map[string]string
}

// StateData is a job's current State row (IStorageConnection.GetStateData).
type StateData struct {
	Name   string
	Reason string
	Data   map[string]string
}

func parseJobID(id hangfire.JobID) (int64, bool) {
	n, err := strconv.ParseInt(string(id), 10, 64)
	return n, err == nil
}

// JobData returns nil when the job does not exist.
func (s *Storage) JobData(ctx context.Context, id hangfire.JobID) (*JobData, error) {
	return s.jobData(ctx, s.db, id)
}

func (se *Session) JobData(ctx context.Context, id hangfire.JobID) (*JobData, error) {
	return se.s.jobData(ctx, se.q(), id)
}

func (s *Storage) jobData(ctx context.Context, q querier, id hangfire.JobID) (*JobData, error) {
	n, ok := parseJobID(id)
	if !ok {
		return nil, nil
	}
	var (
		invocation, arguments sql.NullString
		stateName             sql.NullString
		jd                    = JobData{ID: id, Parameters: map[string]string{}}
	)
	err := q.QueryRowContext(ctx, s.sql(`select InvocationData, StateName, Arguments, CreatedAt from [HF].Job with (readcommittedlock, forceseek) where Id = @id`),
		sql.Named("id", n)).Scan(&invocation, &stateName, &arguments, &jd.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	jd.StateName = stateName.String
	jd.Payload, jd.PayloadErr = hangfire.DecodePayload(invocation.String, arguments.String)
	rows, err := q.QueryContext(ctx, s.sql(`select Name, Value from [HF].JobParameter with (forceseek) where JobId = @id`), sql.Named("id", n))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var value sql.NullString
		if err := rows.Scan(&name, &value); err != nil {
			return nil, err
		}
		jd.Parameters[name] = value.String
	}
	return &jd, rows.Err()
}

// StateData returns nil when the job does not exist or has no state yet.
func (s *Storage) StateData(ctx context.Context, id hangfire.JobID) (*StateData, error) {
	return s.stateData(ctx, s.db, id)
}

func (s *Storage) stateData(ctx context.Context, q querier, id hangfire.JobID) (*StateData, error) {
	n, ok := parseJobID(id)
	if !ok {
		return nil, nil
	}
	var sd StateData
	var reason, data sql.NullString
	err := q.QueryRowContext(ctx, s.sql(`select s.Name, s.Reason, s.Data
from [HF].State s with (readcommittedlock, forceseek)
inner join [HF].Job j with (readcommittedlock, forceseek) on j.StateId = s.Id and j.Id = s.JobId
where j.Id = @jobId`), sql.Named("jobId", n)).Scan(&sd.Name, &reason, &data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	sd.Reason = reason.String
	if sd.Data, err = decodeStateData(data.String); err != nil {
		return nil, fmt.Errorf("sqlserver: job %s state data: %w", id, err)
	}
	return &sd, nil
}

func decodeStateData(s string) (map[string]string, error) {
	m := map[string]string{}
	if s == "" {
		return m, nil
	}
	return m, json.Unmarshal([]byte(s), &m)
}

// JobParameter returns the raw (JSON) parameter value; ok is false when it is absent.
func (s *Storage) JobParameter(ctx context.Context, id hangfire.JobID, name string) (value string, ok bool, err error) {
	return s.jobParameter(ctx, s.db, id, name)
}

func (s *Storage) jobParameter(ctx context.Context, q querier, id hangfire.JobID, name string) (string, bool, error) {
	n, valid := parseJobID(id)
	if !valid {
		return "", false, nil
	}
	var v sql.NullString
	err := q.QueryRowContext(ctx, s.sql(`select top (1) Value from [HF].JobParameter with (forceseek) where JobId = @id and Name = @name`),
		sql.Named("id", n), sql.Named("name", name)).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return v.String, err == nil, err
}

// SetJobParameter upserts a raw (JSON) parameter value (SqlServerConnection.SetJobParameter).
func (s *Storage) SetJobParameter(ctx context.Context, id hangfire.JobID, name, value string) error {
	return s.setJobParameter(ctx, s.db, id, name, value)
}

func (s *Storage) setJobParameter(ctx context.Context, q querier, id hangfire.JobID, name, value string) error {
	n, ok := parseJobID(id)
	if !ok {
		return fmt.Errorf("%w: %q", hangfire.ErrJobNotFound, id)
	}
	_, err := q.ExecContext(ctx, s.sql(`set xact_abort off;
begin try
  update [HF].JobParameter set Value = @value where JobId = @jobId and Name = @name;
  if @@ROWCOUNT = 0 insert into [HF].JobParameter (JobId, Name, Value) values (@jobId, @name, @value);
end try
begin catch
  declare @em nvarchar(4000), @es int, @est int;
  select @em=error_message(),@es=error_severity(),@est=error_state();
  IF ERROR_NUMBER() not in (2601, 2627) raiserror(@em, @es, @est);
  update [HF].JobParameter set Value = @value where JobId = @jobId and Name = @name;
end catch`), sql.Named("jobId", n), sql.Named("name", name), sql.Named("value", value))
	return err
}

// NewJob is a job ready to insert: payload and parameters already encoded.
type NewJob struct {
	InvocationData string            // DR-02 payload without arguments
	Arguments      string            // DR-05 array
	Parameters     map[string]string // name -> JSON value (DR-09)
	State          hangfire.State    // initial state; nil leaves the job invisible, as CreateExpiredJob alone does
}

// EncodeJob builds a NewJob from an invocation. queue becomes the payload's
// "q" (Job.Queue); "" omits it. Parameter values are JSON-encoded.
func (s *Storage) EncodeJob(inv hangfire.Invocation, queue string, params map[string]any, initial hangfire.State) (NewJob, error) {
	payload, err := hangfire.EncodePayload(inv, queue, false, s.opts.Level)
	if err != nil {
		return NewJob{}, err
	}
	args, err := hangfire.EncodeArguments(inv.Args)
	if err != nil {
		return NewJob{}, err
	}
	if len(args) > s.opts.MaxArgumentBytes {
		return NewJob{}, fmt.Errorf("%w: %d bytes, limit %d", hangfire.ErrPayloadTooLarge, len(args), s.opts.MaxArgumentBytes)
	}
	if len(args) > warnArgumentBytes {
		s.log.Warn("hangfire: large job arguments; pass ids instead of data", "type", inv.Type, "method", inv.Method, "bytes", len(args))
	}
	nj := NewJob{InvocationData: payload, Arguments: args, Parameters: map[string]string{}, State: initial}
	for name, v := range params {
		if len([]rune(name)) > 40 {
			return NewJob{}, fmt.Errorf("sqlserver: parameter name %q longer than 40 characters", name)
		}
		if nj.Parameters[name], err = hangfire.EncodeParameter(v); err != nil {
			return NewJob{}, fmt.Errorf("sqlserver: parameter %q: %w", name, err)
		}
	}
	return nj, nil
}

const createdJobExpiration = 30 * 24 * time.Hour // CoreBackgroundJobFactory.cs:71

// CreateJob inserts the job, its parameters and its initial state in one
// transaction, merging the two steps of CoreBackgroundJobFactory (SRS FR-P02).
// It returns the state actually applied, which differs from nj.State when a
// continuation's parent has already finished.
func (se *Session) CreateJob(ctx context.Context, nj NewJob) (hangfire.JobID, hangfire.State, error) {
	var (
		id      hangfire.JobID
		applied hangfire.State
	)
	err := se.InTx(ctx, func() error {
		now := time.Now().UTC()
		var n int64
		err := se.q().QueryRowContext(ctx, se.s.sql(`insert into [HF].Job (InvocationData, Arguments, CreatedAt, ExpireAt) output inserted.Id values (@invocationData, @arguments, @createdAt, @expireAt)`),
			sql.Named("invocationData", nj.InvocationData), sql.Named("arguments", nj.Arguments),
			sql.Named("createdAt", sqlTime(now)), sql.Named("expireAt", sqlTime(now.Add(createdJobExpiration)))).Scan(&n)
		if err != nil {
			return err
		}
		id = hangfire.JobID(strconv.FormatInt(n, 10))
		b := se.s.newBatch()
		for name, v := range nj.Parameters {
			b.insertParameter(id, name, v)
		}
		if nj.State != nil {
			payload, _ := hangfire.DecodePayload(nj.InvocationData, "")
			job := jobRef{id: id, queue: payload.Queue}
			if applied, err = se.transition(ctx, b, job, "", nj.State, nil); err != nil {
				return err
			}
		}
		return b.exec(ctx, se.q())
	})
	if err != nil {
		return "", nil, err
	}
	return id, applied, nil
}
