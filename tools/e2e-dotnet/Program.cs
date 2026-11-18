using System.Text.Json;
using E2E.Jobs;
using Hangfire;
using Hangfire.Common;
using Hangfire.Storage;
using Hangfire.SqlServer;
using Hangfire.States;
using Microsoft.Data.SqlClient;

const string DefaultConn =
    "Server=localhost,1433;Database=HangfireGoSdk;User Id=sa;Password=Hangfire_Dev_Passw0rd!;TrustServerCertificate=True;Encrypt=False";
var conn = Environment.GetEnvironmentVariable("HANGFIRE_SQL") ?? DefaultConn;
var json = new JsonSerializerOptions { WriteIndented = true };

if (args.Length == 0) return Usage();
var opts = ParseOpts(args.Skip(1).ToArray());

switch (args[0])
{
    case "install":
        Install();
        Console.WriteLine("installed");
        return 0;
    case "server":
        Install();
        RunServer(Queues(opts, "dotnet,default"), Seconds(opts, 30));
        return 0;
    case "enqueue-samples":
        Install();
        EnqueueSamples(opts.GetValueOrDefault("queue", "goq"));
        return 0;
    case "dump-job":
        Install();
        return DumpJob(args.Length > 1 ? args[1] : throw new ArgumentException("job id required"));
    case "smoke":
        return Smoke();
    default:
        return Usage();
}

static int Usage()
{
    Console.Error.WriteLine("commands: install | server --queues a,b --seconds N | enqueue-samples --queue goq | dump-job <id> | smoke");
    return 2;
}

static Dictionary<string, string> ParseOpts(string[] a)
{
    var d = new Dictionary<string, string>();
    for (var i = 0; i + 1 < a.Length; i += 2) d[a[i].TrimStart('-')] = a[i + 1];
    return d;
}

static string[] Queues(Dictionary<string, string> o, string def) => o.GetValueOrDefault("queues", def).Split(',');
static int Seconds(Dictionary<string, string> o, int def) => int.Parse(o.GetValueOrDefault("seconds", def.ToString()));

void Install()
{
    var csb = new SqlConnectionStringBuilder(conn);
    var db = csb.InitialCatalog;
    csb.InitialCatalog = "master";
    using (var c = new SqlConnection(csb.ConnectionString))
    {
        c.Open();
        using var cmd = c.CreateCommand();
        cmd.CommandText = $"IF DB_ID(N'{db}') IS NULL CREATE DATABASE [{db}]";
        cmd.ExecuteNonQuery();
    }
    Configure();
}

void Configure()
{
    GlobalConfiguration.Configuration
        .SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
        .UseSimpleAssemblyNameTypeSerializer()
        .UseRecommendedSerializerSettings()
        .UseSqlServerStorage(conn, new SqlServerStorageOptions
        {
            PrepareSchemaIfNecessary = true,
            QueuePollInterval = TimeSpan.FromSeconds(1),
        });
}

void RunServer(string[] queues, int seconds)
{
    var done = new ManualResetEventSlim();
    Console.CancelKeyPress += (_, e) => { e.Cancel = true; done.Set(); };
    System.Runtime.InteropServices.PosixSignalRegistration.Create(
        System.Runtime.InteropServices.PosixSignal.SIGTERM, ctx => { ctx.Cancel = true; done.Set(); });
    using var server = new BackgroundJobServer(new BackgroundJobServerOptions
    {
        Queues = queues,
        WorkerCount = 2,
        SchedulePollingInterval = TimeSpan.FromSeconds(1),
    });
    Console.Error.WriteLine($"server on [{string.Join(",", queues)}] for {seconds}s");
    done.Wait(TimeSpan.FromSeconds(seconds));
}

void EnqueueSamples(string queue)
{
    var client = new BackgroundJobClient();
    Job Hello(string n) => new(typeof(IGoJobs), typeof(IGoJobs).GetMethod("Hello")!, n);
    Job Add(int a, int b) => new(typeof(IGoJobs), typeof(IGoJobs).GetMethod("Add")!, a, b);

    var enq = client.Create(Hello("world"), new EnqueuedState(queue));
    var sched = client.Create(Add(2, 3), new ScheduledState(TimeSpan.FromSeconds(5)));
    var cont = client.Create(Hello("continuation"), new AwaitingState(enq, new EnqueuedState(queue)));
    Console.WriteLine(JsonSerializer.Serialize(new { enqueued = enq, scheduled = sched, continuation = cont }));
}

int DumpJob(string id)
{
    var d = JobStorage.Current.GetMonitoringApi().JobDetails(id);
    if (d == null) { Console.Error.WriteLine("not found"); return 1; }
    using var c = JobStorage.Current.GetConnection();
    var data = d.Job == null ? null : InvocationData.SerializeJob(d.Job);
    Console.WriteLine(JsonSerializer.Serialize(new
    {
        id,
        d.CreatedAt,
        d.ExpireAt,
        invocation = data == null ? null : new { data.Type, data.Method, data.ParameterTypes, data.Arguments },
        properties = d.Properties,
        history = d.History.Select(h => new { h.StateName, h.Reason, h.CreatedAt, h.Data }),
    }, json));
    return 0;
}

int Smoke()
{
    var rec = Environment.GetEnvironmentVariable("E2E_RECORD_FILE") ?? "/tmp/e2e-record.txt";
    File.Delete(rec);
    Install();
    var tag = "smoke-" + Guid.NewGuid().ToString("N")[..8];
    var id = new BackgroundJobClient().Create(
        Job.FromExpression<Recorder>(r => r.Record(tag)), new EnqueuedState("dotnet"));
    Console.WriteLine($"enqueued {id} tag={tag}");
    RunServer(new[] { "dotnet", "default" }, 10);
    var ok = File.Exists(rec) && File.ReadAllLines(rec).Contains(tag + "|");
    Console.WriteLine(ok ? "SMOKE OK" : "SMOKE FAILED");
    return ok ? 0 : 1;
}
