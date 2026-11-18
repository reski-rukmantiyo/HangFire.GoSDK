package sqlserver

import (
	"context"
	"database/sql"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"

	mssql "github.com/microsoft/go-mssqldb"

	"hangfire-gosdk/hangfire"
)

type mssqlReturnStatus = mssql.ReturnStatus

// sqlTime sends t as a datetime, the column type Hangfire uses, so equality
// guards such as "FetchedAt = @fetchedAt" compare like for like.
func sqlTime(t time.Time) mssql.DateTime1 { return mssql.DateTime1(t.UTC()) }

// category orders statements inside a batch the way SqlServerWriteOnlyTransaction.Commit
// does (jobs, counters, hashes, lists, sets, queues), so Go and .NET writers
// take row locks in the same order.
type category int

const (
	catJob category = iota
	catCounter
	catHash
	catSet
	catQueue
)

type stmt struct {
	cat  category
	key  string
	text string
	args []sql.NamedArg
}

// batch is one write transaction's statements, sent as a single round trip.
type batch struct {
	s     *Storage
	stmts []stmt
}

func (s *Storage) newBatch() *batch { return &batch{s: s} }

func (b *batch) add(c category, key, text string, args ...sql.NamedArg) {
	b.stmts = append(b.stmts, stmt{c, key, b.s.sql(text), args})
}

var paramRef = regexp.MustCompile(`(^|[^@\w])@([A-Za-z_]\w*)`)

func (b *batch) exec(ctx context.Context, q querier) error {
	if len(b.stmts) == 0 {
		return nil
	}
	sort.SliceStable(b.stmts, func(i, j int) bool {
		if b.stmts[i].cat != b.stmts[j].cat {
			return b.stmts[i].cat < b.stmts[j].cat
		}
		return b.stmts[i].key < b.stmts[j].key
	})
	var text strings.Builder
	text.WriteString("set xact_abort on;set nocount on;\n")
	var args []any
	for i, st := range b.stmts {
		suffix := "_" + strconv.Itoa(i)
		text.WriteString(paramRef.ReplaceAllString(st.text, "${1}@${2}"+suffix))
		text.WriteString(";\n")
		for _, a := range st.args {
			args = append(args, sql.Named(a.Name+suffix, a.Value))
		}
	}
	_, err := q.ExecContext(ctx, text.String(), args...)
	return err
}

func jobKey(id hangfire.JobID) string { return string(id) }

func jobIDArg(name string, id hangfire.JobID) sql.NamedArg {
	n, _ := strconv.ParseInt(string(id), 10, 64)
	return sql.Named(name, n)
}

// truncateUTF16 cuts s to n UTF-16 code units, as String.Substring does.
func truncateUTF16(s string, n int) string {
	u := utf16.Encode([]rune(s))
	if len(u) <= n {
		return s
	}
	return string(utf16.Decode(u[:n]))
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// addJobState and setJobState mirror SqlServerWriteOnlyTransaction.AddJobState/SetJobState.
func (b *batch) addJobState(id hangfire.JobID, st hangfire.State, now time.Time) {
	b.add(catJob, jobKey(id), `insert into [HF].State (JobId, Name, Reason, CreatedAt, Data) values (@jobId, @name, @reason, @createdAt, @data)`,
		b.stateArgs(id, st, now)...)
}

func (b *batch) setJobState(id hangfire.JobID, st hangfire.State, now time.Time) {
	b.add(catJob, jobKey(id), `insert into [HF].State (JobId, Name, Reason, CreatedAt, Data) values (@jobId, @name, @reason, @createdAt, @data);
update [HF].Job set StateId = SCOPE_IDENTITY(), StateName = @name where Id = @jobId`,
		b.stateArgs(id, st, now)...)
}

func (b *batch) stateArgs(id hangfire.JobID, st hangfire.State, now time.Time) []sql.NamedArg {
	return []sql.NamedArg{
		jobIDArg("jobId", id),
		sql.Named("name", st.StateName()),
		sql.Named("reason", nullable(truncateUTF16(st.StateReason(), 99))),
		sql.Named("createdAt", sqlTime(now)),
		sql.Named("data", hangfire.EncodeStateData(st, b.s.opts.Level)),
	}
}

func (b *batch) expireJob(id hangfire.JobID, at time.Time) {
	b.add(catJob, jobKey(id), `update J set ExpireAt = @expireAt from [HF].Job J with (forceseek) where Id = @id`,
		sql.Named("expireAt", sqlTime(at)), jobIDArg("id", id))
}

func (b *batch) persistJob(id hangfire.JobID) {
	b.add(catJob, jobKey(id), `update J set ExpireAt = NULL from [HF].Job J with (forceseek) where Id = @id`, jobIDArg("id", id))
}

func (b *batch) insertParameter(id hangfire.JobID, name, value string) {
	b.add(catJob, jobKey(id), `insert into [HF].JobParameter (JobId, Name, Value) values (@jobId, @name, @value)`,
		jobIDArg("jobId", id), sql.Named("name", name), sql.Named("value", value))
}

// addCounter inserts a Counter row; expireAt zero means no expiry.
func (b *batch) addCounter(key string, delta int, expireAt time.Time) {
	if expireAt.IsZero() {
		b.add(catCounter, key, `insert into [HF].Counter ([Key], [Value]) values (@key, @value)`,
			sql.Named("key", key), sql.Named("value", delta))
		return
	}
	b.add(catCounter, key, `insert into [HF].Counter ([Key], [Value], [ExpireAt]) values (@key, @value, @expireAt)`,
		sql.Named("key", key), sql.Named("value", delta), sql.Named("expireAt", sqlTime(expireAt)))
}

// addToSet is the UseIgnoreDupKeyOption upsert, the default at schema 8+
// (SqlServerWriteOnlyTransaction.cs:316-322, SqlServerStorage.cs:499). Global
// locks are off at schema 6+ (SqlServerStorage.cs:500), so none is taken.
func (b *batch) addToSet(key, value string, score float64) {
	b.add(catSet, key, `insert into [HF].[Set] ([Key], Value, Score) values (@key, @value, @score);
if @@ROWCOUNT = 0 update [HF].[Set] set Score = @score where [Key] = @key and Value = @value`,
		sql.Named("key", key), sql.Named("value", value), sql.Named("score", score))
}

func (b *batch) removeFromSet(key, value string) {
	b.add(catSet, key, `delete S from [HF].[Set] S with (forceseek) where [Key] = @key and Value = @value`,
		sql.Named("key", key), sql.Named("value", value))
}

func (b *batch) setRangeInHash(key string, fields [][2]string) {
	for _, f := range fields {
		b.add(catHash, key, `insert into [HF].Hash ([Key], Field, Value) values (@key, @field, @value);
if @@ROWCOUNT = 0 update [HF].Hash set Value = @value where [Key] = @key and Field = @field`,
			sql.Named("key", key), sql.Named("field", f[0]), sql.Named("value", f[1]))
	}
}

func (b *batch) removeHash(key string) {
	b.add(catHash, key, `delete H from [HF].Hash H with (forceseek) where [Key] = @key`, sql.Named("key", key))
}

func (b *batch) addToQueue(queue string, id hangfire.JobID) {
	b.add(catQueue, queue, `insert into [HF].JobQueue (JobId, Queue) values (@jobId, @queue)`,
		jobIDArg("jobId", id), sql.Named("queue", queue))
}

func (b *batch) removeFromQueue(f *FetchedJob) {
	b.add(catQueue, f.Queue, `delete JQ from [HF].JobQueue JQ with (forceseek, rowlock) where Queue = @queue and Id = @id and FetchedAt = @fetchedAt`,
		sql.Named("queue", f.Queue), sql.Named("id", f.ID), sql.Named("fetchedAt", sqlTime(f.FetchedAt)))
}

// unixSeconds is JobHelper.ToTimestamp: whole seconds since the epoch.
func unixSeconds(t time.Time) float64 { return float64(t.Unix()) }
