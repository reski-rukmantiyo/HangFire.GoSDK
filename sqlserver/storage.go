// Package sqlserver implements Hangfire's SQL Server storage contract
// (Hangfire.SqlServer, schema version 9) for Go producers, monitors and workers.
package sqlserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"hangfire-gosdk/hangfire"
)

// SchemaVersion is the only Hangfire.SqlServer schema version this package writes to (SRS CR-01).
const SchemaVersion = 9

type Options struct {
	Schema string // "" means "HangFire"
	// Level is the compatibility level of the oldest .NET server in the fleet (SRS CR-02). Required.
	Level hangfire.CompatLevel
	// MaxArgumentBytes rejects jobs whose Job.Arguments exceeds it (SRS NFR-04). 0 means 1 MiB.
	MaxArgumentBytes int
	// JobExpiration is how long finished jobs live, as WithJobExpirationTimeout. 0 means 24h (JobStorage.cs:32).
	JobExpiration time.Duration
	Logger        *slog.Logger // nil means slog.Default()
}

type Storage struct {
	db   *sql.DB
	opts Options
	log  *slog.Logger
}

var schemaNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

const warnArgumentBytes = 64 << 10

// NewStorage checks the schema version and returns a storage bound to db.
// It never installs or migrates the schema.
func NewStorage(ctx context.Context, db *sql.DB, opts Options) (*Storage, error) {
	if opts.Schema == "" {
		opts.Schema = "HangFire"
	}
	if !schemaNamePattern.MatchString(opts.Schema) {
		return nil, fmt.Errorf("sqlserver: invalid schema name %q", opts.Schema)
	}
	if opts.Level != hangfire.CompatLevel170 && opts.Level != hangfire.CompatLevel180 {
		return nil, fmt.Errorf("sqlserver: unsupported compatibility level %d", opts.Level)
	}
	if opts.MaxArgumentBytes == 0 {
		opts.MaxArgumentBytes = 1 << 20
	}
	if opts.JobExpiration == 0 {
		opts.JobExpiration = 24 * time.Hour
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	s := &Storage{db: db, opts: opts, log: opts.Logger}
	var version int
	err := db.QueryRowContext(ctx, s.sql(`select top (1) [Version] from [HF].[Schema]`)).Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("%w: reading [%s].[Schema]: %v", hangfire.ErrSchemaVersion, opts.Schema, err)
	}
	if version != SchemaVersion {
		return nil, fmt.Errorf("%w: found %d, want %d", hangfire.ErrSchemaVersion, version, SchemaVersion)
	}
	return s, nil
}

func (s *Storage) Level() hangfire.CompatLevel { return s.opts.Level }
func (s *Storage) Schema() string              { return s.opts.Schema }

// sql binds a query template to the schema. Templates write [HF] for it.
func (s *Storage) sql(template string) string {
	return strings.ReplaceAll(template, "[HF]", "["+s.opts.Schema+"]")
}

type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Session owns one dedicated connection, like SqlServerConnection's
// _dedicatedConnection: Session-owned app locks live as long as the
// connection, so every read and write made under a lock goes through it.
// A Session is not safe for concurrent use.
type Session struct {
	s     *Storage
	conn  *sql.Conn
	tx    *sql.Tx
	locks []string // held resources, schema-prefixed, in acquisition order
}

func (s *Storage) Session(ctx context.Context) (*Session, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	return &Session{s: s, conn: conn}, nil
}

// withSession runs fn on a fresh session and closes it.
func (s *Storage) withSession(ctx context.Context, fn func(*Session) error) error {
	se, err := s.Session(ctx)
	if err != nil {
		return err
	}
	return errors.Join(fn(se), se.Close())
}

func (se *Session) q() querier {
	if se.tx != nil {
		return se.tx
	}
	return se.conn
}

// InTx runs fn in a transaction on the session connection. Nested calls join
// the outer transaction, so a state change that triggers continuations
// commits atomically with them.
func (se *Session) InTx(ctx context.Context, fn func() error) error {
	if se.tx != nil {
		return fn()
	}
	tx, err := se.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	se.tx = tx
	defer func() { se.tx = nil }()
	if err := fn(); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	return tx.Commit()
}

// Close releases every lock the session holds and returns the connection to the pool.
func (se *Session) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var errs []error
	if se.tx != nil {
		errs = append(errs, se.tx.Rollback())
		se.tx = nil
	}
	for i := len(se.locks) - 1; i >= 0; i-- {
		var rs mssqlReturnStatus
		_, err := se.conn.ExecContext(ctx, "sp_releaseapplock",
			sql.Named("Resource", se.locks[i]), sql.Named("LockOwner", "Session"), &rs)
		if err == nil && rs < 0 {
			err = fmt.Errorf("sqlserver: could not release lock %q: result %d", se.locks[i], rs)
		}
		errs = append(errs, err)
	}
	se.locks = nil
	errs = append(errs, se.conn.Close())
	return errors.Join(errs...)
}

const (
	jobLockTimeout          = 15 * time.Minute // BackgroundJobStateChanger.cs:27
	continuationLockTimeout = time.Minute      // ContinuationsSupportAttribute.cs:32
	recurringLockTimeout    = 15 * time.Second // RecurringJobManager.cs:32
	lockAttemptTimeout      = time.Second      // SqlServerDistributedLock.cs:31
)

// Lock takes the Hangfire distributed lock named resource (without the schema
// prefix, as IStorageConnection.AcquireDistributedLock takes it) and holds it
// until Close. Taking a lock the session already holds is a no-op.
func (se *Session) Lock(ctx context.Context, resource string, timeout time.Duration) error {
	full := se.s.opts.Schema + ":" + resource
	for _, l := range se.locks {
		if l == full {
			return nil
		}
	}
	// Short attempts, as SqlServerDistributedLock.Acquire does, so a long wait
	// never pins a SQL Server worker thread.
	attempt := min(lockAttemptTimeout, timeout).Milliseconds()
	deadline := time.Now().Add(timeout)
	for {
		var rs mssqlReturnStatus
		_, err := se.q().ExecContext(ctx, "sp_getapplock",
			sql.Named("Resource", full),
			sql.Named("DbPrincipal", "public"),
			sql.Named("LockMode", "Exclusive"),
			sql.Named("LockOwner", "Session"),
			sql.Named("LockTimeout", attempt),
			&rs)
		if err != nil {
			return fmt.Errorf("sqlserver: lock %q: %w", full, err)
		}
		if rs >= 0 {
			se.locks = append(se.locks, full)
			return nil
		}
		if rs == -999 {
			return fmt.Errorf("sqlserver: lock %q: parameter validation or call error", full)
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("%w: %q", hangfire.ErrLockTimeout, full)
		}
	}
}

// LockJob takes the job's state lock, "job:{id}:state-lock" (StorageConnectionExtensions.cs:34).
func (se *Session) LockJob(ctx context.Context, id hangfire.JobID, timeout time.Duration) error {
	return se.Lock(ctx, "job:"+string(id)+":state-lock", timeout)
}
