package sqlserver

import (
	"context"
	"fmt"
	"time"

	"github.com/reski-rukmantiyo/HangFire.GoSDK/hangfire"
)

// Client is the SQL Server implementation of hangfire.Client.
type Client struct{ s *Storage }

var _ hangfire.Client = (*Client)(nil)

func NewClient(s *Storage) *Client { return &Client{s: s} }

// Enqueue mirrors BackgroundJobClient.Enqueue(queue, call): the queue goes
// into the payload ("q") and the state keeps the default queue, so the
// Enqueued handler routes the job to the payload's queue.
func (c *Client) Enqueue(ctx context.Context, inv hangfire.Invocation, opts ...hangfire.Option) (hangfire.JobID, error) {
	return c.Create(ctx, inv, hangfire.Enqueued{}, opts...)
}

func (c *Client) Schedule(ctx context.Context, inv hangfire.Invocation, delay time.Duration, opts ...hangfire.Option) (hangfire.JobID, error) {
	return c.ScheduleAt(ctx, inv, time.Now().Add(delay), opts...)
}

func (c *Client) ScheduleAt(ctx context.Context, inv hangfire.Invocation, at time.Time, opts ...hangfire.Option) (hangfire.JobID, error) {
	return c.Create(ctx, inv, hangfire.Scheduled{EnqueueAt: at}, opts...)
}

// ContinueJobWith creates a job that waits for parent. A nil next means
// Enqueued, routed to the job's queue.
func (c *Client) ContinueJobWith(ctx context.Context, parent hangfire.JobID, inv hangfire.Invocation, next hangfire.State, cont hangfire.ContinuationOptions, opts ...hangfire.Option) (hangfire.JobID, error) {
	return c.Create(ctx, inv, hangfire.Awaiting{ParentID: parent, NextState: next, Options: cont}, opts...)
}

// Create is IBackgroundJobClientV2.Create(job, state, parameters).
func (c *Client) Create(ctx context.Context, inv hangfire.Invocation, initial hangfire.State, opts ...hangfire.Option) (hangfire.JobID, error) {
	var o hangfire.EnqueueOptions
	for _, f := range opts {
		f(&o)
	}
	if err := validateState(initial); err != nil {
		return "", err
	}
	nj, err := c.s.EncodeJob(inv, o.Queue, o.Parameters, initial)
	if err != nil {
		return "", err
	}
	var id hangfire.JobID
	err = c.s.withSession(ctx, func(se *Session) error {
		id, _, err = se.CreateJob(ctx, nj)
		return err
	})
	return id, err
}

// validateState rejects states whose data can't be written, at the API boundary.
func validateState(st hangfire.State) error {
	switch s := st.(type) {
	case nil:
		return fmt.Errorf("sqlserver: nil state")
	case hangfire.Enqueued:
		if s.Queue != "" {
			return hangfire.ValidateQueue(s.Queue)
		}
	case hangfire.Scheduled:
		if s.EnqueueAt.IsZero() {
			return fmt.Errorf("sqlserver: scheduled state without EnqueueAt")
		}
	case hangfire.Awaiting:
		if _, ok := parseJobID(s.ParentID); !ok {
			return fmt.Errorf("%w: invalid parent id %q", hangfire.ErrParentNotFound, s.ParentID)
		}
		if _, err := hangfire.EncodeNextState(s.NextState); err != nil {
			return err
		}
	}
	return nil
}

// ChangeState is IBackgroundJobClient.ChangeState: false when the job is
// missing or not in the expected state.
func (c *Client) ChangeState(ctx context.Context, id hangfire.JobID, st hangfire.State, expected string) (bool, error) {
	if err := validateState(st); err != nil {
		return false, err
	}
	var exp []string
	if expected != "" {
		exp = []string{expected}
	}
	applied, err := c.s.ChangeState(ctx, StateChange{JobID: id, State: st, Expected: exp})
	return applied != nil, err
}

// Delete, Requeue and Reschedule are BackgroundJobClientExtensions.cs:686-798.
func (c *Client) Delete(ctx context.Context, id hangfire.JobID) (bool, error) {
	return c.ChangeState(ctx, id, hangfire.Deleted{}, "")
}

func (c *Client) Requeue(ctx context.Context, id hangfire.JobID) (bool, error) {
	return c.ChangeState(ctx, id, hangfire.Enqueued{}, "")
}

func (c *Client) Reschedule(ctx context.Context, id hangfire.JobID, at time.Time) (bool, error) {
	return c.ChangeState(ctx, id, hangfire.Scheduled{EnqueueAt: at}, "")
}
