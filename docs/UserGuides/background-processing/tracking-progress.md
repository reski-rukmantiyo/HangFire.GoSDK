# Tracking the progress

Hangfire has no built-in progress API. You report progress from the job and read it from wherever your users look. This page shows how a Go handler stores progress in a job parameter, and how a web endpoint reads it back by polling.

## Read the job context

Inside a handler, `server.FromContext(ctx)` returns the `*server.JobContext` of the running job. Outside a handler it returns nil.

| Field | Meaning |
|---|---|
| `JobID` | the Hangfire job id |
| `ServerID` | the id of the server running the job |
| `WorkerID` | the id of the worker running the job |
| `Queue` | the queue the job was fetched from |
| `RetryCount` | retries done before this run. 0 on the first run |
| `CreatedAt` | when the job was created |

`JobContext` also reads and writes job parameters. Parameters are name and value pairs stored with the job in the `JobParameter` table. Values are JSON.

- `Parameter(ctx, name, &out)` decodes a parameter into `out`. It reports false when the parameter does not exist.
- `SetParameter(ctx, name, v)` stores `v` as JSON, as `PerformContext.SetJobParameter` does in .NET.

## Report progress from the handler

The handler below stores a percentage in a `Progress` parameter after each batch.

```go
srv.Handle("MyApp.Jobs.ImportJob, MyApp", "Run",
	server.Handler1(func(ctx context.Context, fileID int64) error {
		job := server.FromContext(ctx)
		batches := loadBatches(fileID)
		for i, b := range batches {
			if err := importBatch(ctx, b); err != nil {
				return err
			}
			percent := (i + 1) * 100 / len(batches)
			if err := job.SetParameter(ctx, "Progress", percent); err != nil {
				return err
			}
		}
		return nil
	}))
```

Each `SetParameter` call is one database write. Update progress per batch or per percent, not per item.

Choose parameter names that Hangfire does not use. The names that Hangfire writes include `RetryCount`, `Continuations`, `CurrentCulture`, `CurrentUICulture`, `RecurringJobId`, and `Time`.

## Poll for progress

Any process with a `*sqlserver.Storage` can read the parameter, including a web service that runs no server. `Monitor.JobDetails` returns the job's parameters as raw JSON and its state history, newest first.

```go
func progressHandler(storage *sqlserver.Storage) http.HandlerFunc {
	monitor := sqlserver.NewMonitor(storage)
	return func(w http.ResponseWriter, r *http.Request) {
		job, err := monitor.JobDetails(r.Context(), hangfire.JobID(r.PathValue("id")))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if job == nil {
			http.NotFound(w, r)
			return
		}
		var percent int
		if raw, ok := job.Properties["Progress"]; ok {
			if err := hangfire.DecodeParameter(raw, &percent); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		state := ""
		if len(job.History) > 0 {
			state = job.History[0].StateName
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"state": state, "progress": percent})
	}
}
```

Register it with `mux.Handle("GET /jobs/{id}/progress", progressHandler(storage))`. The browser polls the endpoint until `state` is `Succeeded`, `Failed`, or `Deleted`.

To read one parameter without the state history, call `storage.JobParameter(ctx, id, "Progress")`. It returns the raw JSON value and whether the parameter exists.

## Push progress instead of polling

Polling costs one query per client per interval. To push progress instead, send it from the handler to the clients over a channel you own. If the server and the web service share one process, the handler can publish to an in-process hub that feeds server-sent events or WebSockets. If they run in different processes, publish through a message broker or a database table that the web service watches.

The handler code is the same in both cases. Replace `job.SetParameter` with a publish keyed on `job.JobID`, and keep the final result in your own table so a client that connects late still sees it.

## Differences from .NET Hangfire

- The .NET page uses SignalR for push. Go has no SignalR equivalent in the standard library, so use server-sent events or WebSockets.
- The SDK does not write the format of Hangfire.Console or other progress extensions. The Dashboard shows no progress bar for Go jobs.

## Related pages

- [Dealing with exceptions](dealing-with-exceptions.md)
- [Processing jobs in a web application](processing-jobs-in-web-app.md)
