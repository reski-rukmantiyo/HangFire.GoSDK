// Package server runs Hangfire jobs in Go: a handler registry, workers that
// fetch from Hangfire's SQL Server queues, and the background processes a
// .NET BackgroundJobServer runs (SRS section 5).
//
// Jobs run at least once. A crash, or a shutdown that outlasts
// ShutdownTimeout, makes the job visible again after InvisibilityTimeout and
// it runs again, so handlers must be idempotent.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"hangfire-gosdk/hangfire"
	"hangfire-gosdk/sqlserver"
)

// Options configures a Server. Zero values take the .NET defaults (SRS Appendix A).
type Options struct {
	// Name prefixes the server id; "" uses COMPUTERNAME, HOSTNAME or the host name.
	Name string
	// WorkerCount defaults to min(NumCPU*5, 20) (BackgroundJobServerOptions.cs:29,42).
	WorkerCount int
	// Queues defaults to ["default"]. SQL Server fetches queues in
	// alphanumeric order whatever the slice order, as in .NET.
	Queues []string
	// Retry is the global retry policy; nil means DefaultRetryPolicy.
	Retry *RetryPolicy

	InvisibilityTimeout time.Duration // 5 min (SqlServerStorageOptions.cs:38); keep-alive runs every fifth of it
	QueuePollInterval   time.Duration // 200 ms (SqlServerJobQueue.cs:42)

	SchedulePollingInterval    time.Duration // 15 s, delayed and recurring schedulers (DelayedJobScheduler.cs:73)
	CancellationCheckInterval  time.Duration // 5 s (ServerJobCancellationWatcher.cs:23)
	HeartbeatInterval          time.Duration // 30 s (BackgroundProcessingServerOptions.cs:25)
	ServerCheckInterval        time.Duration // 5 min (ServerWatchdog.cs:23)
	ServerTimeout              time.Duration // 5 min (ServerWatchdog.cs:24)
	JobExpirationCheckInterval time.Duration // 30 min (SqlServerStorageOptions.cs:42)
	CountersAggregateInterval  time.Duration // 5 min (SqlServerStorageOptions.cs:43)

	StopTimeout     time.Duration // 500 ms before handler contexts are cancelled (BackgroundProcessingServerOptions.cs:23)
	ShutdownTimeout time.Duration // 15 s before running jobs are requeued and abandoned (BackgroundProcessingServer.cs:43)

	// Processes on by default (FR-W28). A mixed fleet can leave them to .NET servers.
	DisableDelayedScheduler    bool
	DisableCancellationWatcher bool
	DisableServerWatchdog      bool
	// Processes off by default, because .NET servers usually run them.
	EnableRecurringScheduler bool
	EnableExpirationManager  bool
	EnableCountersAggregator bool

	Logger *slog.Logger
}

func (o Options) withDefaults() (Options, error) {
	def := func(d *time.Duration, v time.Duration) {
		if *d == 0 {
			*d = v
		}
	}
	if o.WorkerCount == 0 {
		o.WorkerCount = min(runtime.NumCPU()*5, 20)
	}
	if o.WorkerCount < 0 {
		return o, fmt.Errorf("server: WorkerCount must be positive, got %d", o.WorkerCount)
	}
	if len(o.Queues) == 0 {
		o.Queues = []string{"default"}
	}
	for _, q := range o.Queues {
		if err := hangfire.ValidateQueue(q); err != nil {
			return o, err
		}
	}
	if o.Retry == nil {
		p := DefaultRetryPolicy()
		o.Retry = &p
	}
	def(&o.InvisibilityTimeout, 5*time.Minute)
	def(&o.QueuePollInterval, 200*time.Millisecond)
	def(&o.SchedulePollingInterval, 15*time.Second)
	def(&o.CancellationCheckInterval, 5*time.Second)
	def(&o.HeartbeatInterval, 30*time.Second)
	def(&o.ServerCheckInterval, 5*time.Minute)
	def(&o.ServerTimeout, 5*time.Minute)
	def(&o.JobExpirationCheckInterval, 30*time.Minute)
	def(&o.CountersAggregateInterval, 5*time.Minute)
	def(&o.StopTimeout, 500*time.Millisecond)
	def(&o.ShutdownTimeout, 15*time.Second)
	if o.InvisibilityTimeout < time.Second {
		return o, fmt.Errorf("server: InvisibilityTimeout must be at least 1s, got %v", o.InvisibilityTimeout)
	}
	if o.Name == "" {
		o.Name = defaultServerName()
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return o, nil
}

// defaultServerName follows BackgroundServerProcess.GetServerId (:179-199).
func defaultServerName() string {
	for _, env := range []string{"COMPUTERNAME", "HOSTNAME"} {
		if v := os.Getenv(env); v != "" {
			return v
		}
	}
	h, _ := os.Hostname()
	return h
}

// Server owns workers and background processes for one storage (FR-W01).
type Server struct {
	storage  *sqlserver.Storage
	opts     Options
	log      *slog.Logger
	id       string
	handlers map[handlerKey]*registration
	started  atomic.Bool

	// Lifecycle contexts, cancelled in order by Shutdown:
	// stopping ends fetching and process loops, stopped cancels handler
	// contexts, aborted ends keep-alives after ShutdownTimeout.
	stopping, stopped, aborted  context.Context
	stop, cancelHandlers, abort context.CancelFunc
	wg                          sync.WaitGroup

	mu       sync.Mutex
	inflight map[*inflight]struct{}

	shutdownOnce sync.Once
	shutdownErr  error
}

// New returns a server bound to storage. Register handlers with Handle, then Start or Run.
func New(storage *sqlserver.Storage, opts Options) (*Server, error) {
	opts, err := opts.withDefaults()
	if err != nil {
		return nil, err
	}
	id := uuid.NewString()
	if strings.TrimSpace(opts.Name) != "" {
		id = strings.ToLower(opts.Name+":"+strconv.Itoa(os.Getpid())) + ":" + id
	}
	return &Server{
		storage:  storage,
		opts:     opts,
		log:      opts.Logger.With("server", id),
		id:       id,
		handlers: map[handlerKey]*registration{},
		inflight: map[*inflight]struct{}{},
	}, nil
}

// ID is the server id written to the Server table and to Processing states.
func (s *Server) ID() string { return s.id }

// Handle registers h for jobs whose payload names typeName and method
// exactly, e.g. ("MyApp.Jobs.EmailJob, MyApp", "Send") (FR-W02). It panics
// when called after Start.
func (s *Server) Handle(typeName, method string, h Handler, opts ...HandlerOption) {
	if s.started.Load() {
		panic("server: Handle called after Start")
	}
	r := &registration{h: h}
	for _, o := range opts {
		o(r)
	}
	s.handlers[handlerKey{typeName, method}] = r
}

// Start registers the server and starts workers and background processes.
// It returns once they run; ctx bounds only the registration.
func (s *Server) Start(ctx context.Context) error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("server: already started")
	}
	if err := s.announce(ctx); err != nil {
		s.started.Store(false)
		return err
	}
	s.stopping, s.stop = context.WithCancel(context.Background())
	s.stopped, s.cancelHandlers = context.WithCancel(context.Background())
	s.aborted, s.abort = context.WithCancel(context.Background())

	for range s.opts.WorkerCount {
		s.goLoop(s.newWorker().run)
	}
	o := s.opts
	s.every(o.HeartbeatInterval, true, s.heartbeat)
	if !o.DisableServerWatchdog {
		s.every(o.ServerCheckInterval, false, func(ctx context.Context) error {
			n, err := s.storage.RemoveTimedOutServers(ctx, o.ServerTimeout)
			if n > 0 {
				s.log.Info("hangfire: removed timed-out servers", "count", n)
			}
			return err
		})
	}
	if !o.DisableCancellationWatcher {
		s.every(o.CancellationCheckInterval, false, s.checkAborted)
	}
	if !o.DisableDelayedScheduler {
		s.every(o.SchedulePollingInterval, false, func(ctx context.Context) error {
			return drain(ctx, s.storage.EnqueueDueScheduledJobs)
		})
	}
	if o.EnableRecurringScheduler {
		s.every(o.SchedulePollingInterval, false, func(ctx context.Context) error {
			return drain(ctx, func(ctx context.Context) (int, error) {
				return s.storage.TriggerDueRecurringJobs(ctx, o.SchedulePollingInterval)
			})
		})
	}
	if o.EnableExpirationManager {
		s.every(o.JobExpirationCheckInterval, false, s.storage.DeleteExpired)
	}
	if o.EnableCountersAggregator {
		s.every(o.CountersAggregateInterval, false, s.storage.AggregateCounters)
	}
	s.log.Info("hangfire: server started", "workers", o.WorkerCount, "queues", o.Queues)
	return nil
}

// Run starts the server and blocks until ctx is done, then shuts down
// gracefully (FR-W32).
func (s *Server) Run(ctx context.Context) error {
	if err := s.Start(ctx); err != nil {
		return err
	}
	<-ctx.Done()
	return s.Shutdown(context.Background())
}

// Shutdown stops fetching, cancels handler contexts after StopTimeout,
// waits up to ShutdownTimeout (or until ctx is done), requeues jobs still
// running, and removes the server row (FR-W30). Later calls return the
// first call's result.
func (s *Server) Shutdown(ctx context.Context) error {
	if !s.started.Load() {
		return nil
	}
	s.shutdownOnce.Do(func() { s.shutdownErr = s.shutdown(ctx) })
	return s.shutdownErr
}

func (s *Server) shutdown(ctx context.Context) error {
	begin := time.Now()
	s.stop()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()

	wait := func(d time.Duration) bool {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-done:
			return true
		case <-t.C:
		case <-ctx.Done():
		}
		return false
	}
	finished := wait(s.opts.StopTimeout)
	s.cancelHandlers()
	if !finished {
		finished = wait(s.opts.ShutdownTimeout - time.Since(begin))
	}
	s.abort()

	cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var errs []error
	if !finished {
		s.mu.Lock()
		stuck := make([]*inflight, 0, len(s.inflight))
		for j := range s.inflight {
			stuck = append(stuck, j)
		}
		s.mu.Unlock()
		for _, j := range stuck {
			if f, ok := j.claim(); ok {
				s.log.Warn("hangfire: job still running after shutdown timeout; requeued", "job", j.jobID)
				errs = append(errs, s.storage.Requeue(cleanup, &f))
			}
		}
	}
	errs = append(errs, s.storage.RemoveServer(cleanup, s.id))
	s.log.Info("hangfire: server stopped", "graceful", finished)
	return errors.Join(errs...)
}

func (s *Server) announce(ctx context.Context) error {
	return s.storage.AnnounceServer(ctx, s.id, sqlserver.ServerData{
		WorkerCount: s.opts.WorkerCount,
		Queues:      s.opts.Queues,
		StartedAt:   time.Now().UTC(),
	})
}

// heartbeat is ServerHeartbeatProcess (:43-95). When the watchdog of another
// server removed this one, .NET restarts the server; re-announcing is the
// part of that restart other servers can see.
func (s *Server) heartbeat(ctx context.Context) error {
	err := s.storage.Heartbeat(ctx, s.id)
	if errors.Is(err, sqlserver.ErrServerGone) {
		s.log.Warn("hangfire: server was considered dead by other servers; announcing again")
		return s.announce(ctx)
	}
	return err
}

// drain repeats a scheduler pass while it handles jobs, as the .NET
// schedulers' Execute loops do.
func drain(ctx context.Context, pass func(context.Context) (int, error)) error {
	for ctx.Err() == nil {
		n, err := pass(ctx)
		if errors.Is(err, hangfire.ErrLockTimeout) {
			return nil // another server did this pass
		}
		if err != nil || n == 0 {
			return err
		}
	}
	return nil
}

func (s *Server) goLoop(fn func()) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		fn()
	}()
}

// every runs fn each interval until the server stops. waitFirst delays the
// first run, as the heartbeat does.
func (s *Server) every(interval time.Duration, waitFirst bool, fn func(context.Context) error) {
	s.goLoop(func() {
		if waitFirst && !sleep(s.stopping, interval) {
			return
		}
		for {
			if err := fn(s.stopping); err != nil && s.stopping.Err() == nil {
				s.log.Error("hangfire: background process failed", "err", err)
			}
			if !sleep(s.stopping, interval) {
				return
			}
		}
	})
}

// sleep waits d and reports false when ctx ended first.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
