// Package sqltest provisions throwaway Hangfire databases for integration
// tests. The schema is installed by the .NET harness (tools/e2e-dotnet), never by Go.
//
// Tests skip unless HANGFIRE_SQL holds an ADO connection string, e.g.
// "Server=localhost,1433;Database=HangfireGoSdk;User Id=sa;Password=...;TrustServerCertificate=True;Encrypt=False".
// The Database part is replaced with a fresh name per call.
package sqltest

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/microsoft/go-mssqldb"
)

// DB is a freshly installed Hangfire database.
type DB struct {
	*sql.DB
	Name    string
	ConnStr string // ADO form, accepted by both go-mssqldb and the .NET harness
}

var databasePart = regexp.MustCompile(`(?i)(^|;)\s*(database|initial catalog)\s*=[^;]*`)

func withDatabase(conn, name string) string {
	conn = databasePart.ReplaceAllString(conn, "")
	return strings.TrimSuffix(conn, ";") + ";Database=" + name
}

// RepoRoot is the directory holding go.mod.
func RepoRoot(t testing.TB) string {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}

// New creates a database for one test and drops it when the test ends,
// unless HANGFIRE_KEEP_DB is set. It skips the test when HANGFIRE_SQL is unset.
func New(t testing.TB) *DB {
	t.Helper()
	base := os.Getenv("HANGFIRE_SQL")
	if base == "" {
		t.Skip("HANGFIRE_SQL not set")
	}
	d, drop, err := Create(base, RepoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if os.Getenv("HANGFIRE_KEEP_DB") != "" {
			t.Logf("kept database %s", d.Name)
			d.Close()
			return
		}
		if err := drop(); err != nil {
			t.Log(err)
		}
	})
	return d
}

var (
	buildOnce sync.Once
	buildErr  error
)

// buildHarness compiles the .NET harness once per process. `go test ./...`
// runs packages as parallel processes, and concurrent `dotnet build`s of one
// project fail on locked obj/ files, so builds are serialized with a lock file.
func buildHarness(repoRoot string) error {
	buildOnce.Do(func() {
		lock := filepath.Join(os.TempDir(), "hangfire-gosdk-e2e-dotnet.lock")
		deadline := time.Now().Add(5 * time.Minute)
		for {
			f, err := os.OpenFile(lock, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
			if err == nil {
				f.Close()
				break
			}
			if info, statErr := os.Stat(lock); statErr == nil && time.Since(info.ModTime()) > 5*time.Minute {
				os.Remove(lock)
				continue
			}
			if time.Now().After(deadline) {
				buildErr = fmt.Errorf("waiting for %s: %w", lock, err)
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
		defer os.Remove(lock)
		cmd := exec.Command("dotnet", "build", "-nologo", "-v", "q", filepath.Join(repoRoot, "tools", "e2e-dotnet"))
		if out, err := cmd.CombinedOutput(); err != nil {
			buildErr = fmt.Errorf("building tools/e2e-dotnet: %v\n%s", err, out)
		}
	})
	return buildErr
}

// Create makes a fresh database next to the one base names and installs the
// schema with the .NET harness. drop closes and deletes it.
func Create(base, repoRoot string) (d *DB, drop func() error, err error) {
	name := fmt.Sprintf("HfGo_%d_%04d", time.Now().Unix(), rand.IntN(10000))
	conn := withDatabase(base, name)
	if err := buildHarness(repoRoot); err != nil {
		return nil, nil, err
	}
	cmd := exec.Command("dotnet", "run", "--no-build", "--project", filepath.Join(repoRoot, "tools", "e2e-dotnet"), "--", "install")
	cmd.Env = append(os.Environ(), "HANGFIRE_SQL="+conn)
	if out, err := cmd.CombinedOutput(); err != nil {
		return nil, nil, fmt.Errorf("installing schema into %s: %v\n%s", name, err, out)
	}
	db, err := sql.Open("sqlserver", conn)
	if err != nil {
		return nil, nil, err
	}
	drop = func() error {
		db.Close()
		master, err := sql.Open("sqlserver", withDatabase(base, "master"))
		if err != nil {
			return err
		}
		defer master.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, err = master.ExecContext(ctx, fmt.Sprintf("alter database [%s] set single_user with rollback immediate; drop database [%s]", name, name))
		return err
	}
	return &DB{DB: db, Name: name, ConnStr: conn}, drop, nil
}

// Harness runs the .NET harness against the database and returns its stdout.
func (d *DB) Harness(t testing.TB, env []string, args ...string) string {
	t.Helper()
	cmd := exec.Command("dotnet", append([]string{"run", "--no-build", "--project", filepath.Join(RepoRoot(t), "tools", "e2e-dotnet"), "--"}, args...)...)
	cmd.Env = append(append(os.Environ(), "HANGFIRE_SQL="+d.ConnStr), env...)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("harness %v: %v\nstderr:\n%s\nstdout:\n%s", args, err, stderr.String(), out)
	}
	return string(out)
}
