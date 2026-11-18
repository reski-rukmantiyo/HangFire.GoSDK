using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using Hangfire;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Newtonsoft.Json;

namespace Golden
{
    public class Order
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public DateTime CreatedAt { get; set; }
        public double Amount { get; set; }
        public string[] Tags { get; set; }
        public long Big { get; set; }
        public string Note { get; set; }
    }

    public interface IEmailSender
    {
        void Send(string to);
    }

    public class EmailJob
    {
        public void Send(string to, int count, DateTime at) { }
        public void Ping() { }
        public void Nullable(string s, int? n) { }
        public void Process(Order order) { }
        public void WithToken(string s, CancellationToken token) { }
        public void Text(string s) { }

        public class Nested
        {
            public void Run(List<int> xs) { }
        }
    }

    public static class Program
    {
        static readonly DateTime T1 = new DateTime(2026, 11, 18, 10, 0, 0, 123, DateTimeKind.Utc);
        static readonly DateTime T2 = new DateTime(2026, 11, 19, 8, 30, 0, DateTimeKind.Utc);

        public static readonly string TrickyText = "q\"\\ <a&b> " + (char)0xE9 + " " + char.ConvertFromUtf32(0x1F600) + " " + (char)0x01 + " " + (char)0x7F + " " + (char)0x85 + " " + (char)0x2028 + " " + (char)0x2029 + " \t\n/";

        static readonly SortedDictionary<string, string> Strings = new SortedDictionary<string, string>(StringComparer.Ordinal);
        static readonly SortedDictionary<string, SortedDictionary<string, string>> States = new SortedDictionary<string, SortedDictionary<string, string>>(StringComparer.Ordinal);
        static readonly List<Dictionary<string, object>> Cron = new List<Dictionary<string, object>>();

        public static int Main(string[] args)
        {
            var output = args.Length > 0 ? args[0] : "golden.json";

            GlobalConfiguration.Configuration
                .SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
                .UseSimpleAssemblyNameTypeSerializer();

            EmitJobs();
            EmitDates();
            EmitParameters();
            EmitContinuations();
            foreach (var level in new[] { CompatibilityLevel.Version_170, CompatibilityLevel.Version_180 })
            {
                GlobalConfiguration.Configuration.SetDataCompatibilityLevel(level);
                EmitStates(((int)level).ToString());
            }
            GlobalConfiguration.Configuration.SetDataCompatibilityLevel(CompatibilityLevel.Version_180);
            EmitCron();

            var doc = new Dictionary<string, object>
            {
                ["hangfire"] = typeof(Job).Assembly.GetName().Version.ToString(),
                ["newtonsoft"] = typeof(JsonConvert).Assembly.GetName().Version.ToString(),
                ["trickyText"] = TrickyText,
                ["strings"] = Strings,
                ["states"] = States,
                ["cron"] = Cron,
            };
            File.WriteAllText(output, JsonConvert.SerializeObject(doc, Formatting.Indented) + "\n");
            return 0;
        }

        static void EmitJob(string name, Job job)
        {
            var data = InvocationData.SerializeJob(job);
            Strings[$"job/{name}/payload"] = data.SerializePayload(excludeArguments: true);
            Strings[$"job/{name}/payload_with_args"] = data.SerializePayload(excludeArguments: false);
            Strings[$"job/{name}/arguments"] = data.Arguments;
        }

        static void EmitJobs()
        {
            EmitJob("send", Job.FromExpression<EmailJob>(x => x.Send("a@b.c", 42, T1)));
            EmitJob("send_queue", Job.FromExpression<EmailJob>(x => x.Send("a@b.c", 42, T1), "critical"));
            EmitJob("ping", Job.FromExpression<EmailJob>(x => x.Ping()));
            EmitJob("ping_queue", Job.FromExpression<EmailJob>(x => x.Ping(), "low"));
            EmitJob("nulls", Job.FromExpression<EmailJob>(x => x.Nullable(null, null)));
            EmitJob("nullable_value", Job.FromExpression<EmailJob>(x => x.Nullable("s", 7)));
            EmitJob("token", Job.FromExpression<EmailJob>(x => x.WithToken("s", CancellationToken.None)));
            EmitJob("text", Job.FromExpression<EmailJob>(x => x.Text(TrickyText)));
            EmitJob("interface", Job.FromExpression<IEmailSender>(x => x.Send("x")));
            EmitJob("nested_generic", Job.FromExpression<EmailJob.Nested>(x => x.Run(new List<int> { 1, 2 })));
            EmitJob("poco", Job.FromExpression<EmailJob>(x => x.Process(new Order
            {
                Id = 7,
                Name = "widget",
                CreatedAt = T1,
                Amount = 12.5,
                Tags = new[] { "a", "b" },
                Big = 9007199254740993L,
                Note = null,
            })));
        }

        static void EmitDates()
        {
            var dates = new Dictionary<string, DateTime>
            {
                ["t1"] = T1,
                ["t2"] = T2,
                ["epoch"] = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc),
                ["boundary"] = new DateTime(1978, 1, 11, 21, 31, 40, 799, DateTimeKind.Utc),
                ["after_boundary"] = new DateTime(1978, 1, 11, 21, 31, 40, 800, DateTimeKind.Utc),
                ["ticks"] = new DateTime(2026, 11, 18, 10, 0, 0, DateTimeKind.Utc).AddTicks(1234567),
                ["max"] = DateTime.SpecifyKind(DateTime.MaxValue, DateTimeKind.Utc),
            };
            foreach (var level in new[] { CompatibilityLevel.Version_170, CompatibilityLevel.Version_180 })
            {
                GlobalConfiguration.Configuration.SetDataCompatibilityLevel(level);
                foreach (var kv in dates)
                {
                    Strings[$"date/{(int)level}/{kv.Key}"] = JobHelper.SerializeDateTime(kv.Value);
                }
            }
            GlobalConfiguration.Configuration.SetDataCompatibilityLevel(CompatibilityLevel.Version_180);
        }

        static void EmitParameters()
        {
            var values = new Dictionary<string, object>
            {
                ["string"] = "en-US",
                ["long"] = 1763460000L,
                ["bool"] = true,
                ["date"] = T1,
                ["text"] = TrickyText,
                ["poco"] = new Order { Id = 1, Name = "n", CreatedAt = T2, Amount = 0.25, Tags = new string[0], Big = -5 },
            };
            foreach (var kv in values)
            {
                Strings[$"param/{kv.Key}"] = SerializationHelper.Serialize(kv.Value, SerializationOption.User);
            }
        }

        static void EmitContinuations()
        {
            var type = typeof(ContinuationsSupportAttribute).GetNestedType("Continuation", BindingFlags.NonPublic);
            var listType = typeof(List<>).MakeGenericType(type);

            IList Make(params (string id, JobContinuationOptions options)[] items)
            {
                var list = (IList)Activator.CreateInstance(listType);
                foreach (var (id, options) in items)
                {
                    var c = Activator.CreateInstance(type);
                    type.GetProperty("JobId").SetValue(c, id);
                    type.GetProperty("Options").SetValue(c, options);
                    list.Add(c);
                }
                return list;
            }

            var serialize = typeof(SerializationHelper).GetMethods()
                .Single(m => m.Name == "Serialize" && m.IsGenericMethod && m.GetParameters().Length == 1)
                .MakeGenericMethod(listType);

            Strings["continuations/empty"] = (string)serialize.Invoke(null, new object[] { Make() });
            Strings["continuations/mixed"] = (string)serialize.Invoke(null, new object[]
            {
                Make(("123", JobContinuationOptions.OnAnyFinishedState),
                     ("124", JobContinuationOptions.OnlyOnSucceededState),
                     ("125", JobContinuationOptions.OnlyOnDeletedState))
            });
        }

        static void SetAuto(object target, string property, object value)
        {
            var field = target.GetType().GetField($"<{property}>k__BackingField", BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new InvalidOperationException($"{target.GetType().Name}.{property} has no backing field");
            field.SetValue(target, value);
        }

        static void Put(string level, string name, IState state)
        {
            States[$"{level}/{name}"] = new SortedDictionary<string, string>(state.SerializeData(), StringComparer.Ordinal);
            Strings[$"state/{level}/{name}/data"] = SerializationHelper.Serialize(state.SerializeData());
            Strings[$"state/{level}/{name}/name"] = state.Name;
            Strings[$"state/{level}/{name}/final"] = state.IsFinal ? "true" : "false";
        }

        static void EmitStates(string level)
        {
            var enqueued = new EnqueuedState("critical") { Reason = "r" };
            SetAuto(enqueued, "EnqueuedAt", T1);
            Put(level, "enqueued", enqueued);

            var enqueuedDefault = new EnqueuedState();
            SetAuto(enqueuedDefault, "EnqueuedAt", T1);
            Put(level, "enqueued_default", enqueuedDefault);

            var scheduled = new ScheduledState(T2);
            SetAuto(scheduled, "ScheduledAt", T1);
            Put(level, "scheduled", scheduled);

            Put(level, "awaiting", new AwaitingState("123", new EnqueuedState("critical"), JobContinuationOptions.OnlyOnSucceededState));
            Put(level, "awaiting_default", new AwaitingState("123"));
            Put(level, "awaiting_scheduled", new AwaitingState("123", new ScheduledState(T2) { Reason = "later" },
                JobContinuationOptions.OnlyOnDeletedState, TimeSpan.FromDays(7)));
            Put(level, "awaiting_enqueued_reason", new AwaitingState("9", new EnqueuedState { Reason = "go" },
                JobContinuationOptions.OnAnyFinishedState, TimeSpan.FromHours(36)));

            var processing = (ProcessingState)Activator.CreateInstance(typeof(ProcessingState),
                BindingFlags.Instance | BindingFlags.NonPublic, null, new object[] { "server-1:1234:abc", "8d0f6c1e-0000-0000-0000-000000000001" }, null);
            SetAuto(processing, "StartedAt", T1);
            Put(level, "processing", processing);

            var succeeded = new SucceededState(42, 1500, 250);
            SetAuto(succeeded, "SucceededAt", T1);
            Put(level, "succeeded", succeeded);

            var succeededVoid = new SucceededState(null, 0, 3);
            SetAuto(succeededVoid, "SucceededAt", T1);
            Put(level, "succeeded_void", succeededVoid);

            var failed = new FailedState(new InvalidOperationException("boom"), "server-1");
            SetAuto(failed, "FailedAt", T1);
            Put(level, "failed", failed);

            var failedNoServer = new FailedState(new InvalidOperationException("boom"));
            SetAuto(failedNoServer, "FailedAt", T1);
            Put(level, "failed_noserver", failedNoServer);

            var deleted = new DeletedState();
            SetAuto(deleted, "DeletedAt", T1);
            Put(level, "deleted", deleted);

            var deletedExc = new DeletedState(new ExceptionInfo(new InvalidOperationException("boom")));
            SetAuto(deletedExc, "DeletedAt", T1);
            Put(level, "deleted_exception", deletedExc);
        }

        static void EmitCron()
        {
            var entity = typeof(Job).Assembly.GetType("Hangfire.RecurringJobEntity", throwOnError: true);
            var parse = entity.GetMethod("ParseCronExpression", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic);
            var cronType = typeof(Job).Assembly.GetType("Cronos.CronExpression", throwOnError: true);
            var next = cronType.GetMethod("GetNextOccurrence", new[] { typeof(DateTime), typeof(TimeZoneInfo), typeof(bool) });

            var exprs = new[]
            {
                "* * * * *", "*/15 * * * *", "0 9 * * MON-FRI", "0 0 1 * *", "0 0 L * *", "0 0 L-3 * *",
                "0 0 LW * *", "0 0 15W * *", "0 0 1W * *", "0 0 * * 5L", "0 0 * * 1#2", "0 0 * * 7", "0 12 ? * SUN",
                "0 0 1,15 * 1", "0 0 13 * 5", "5-10/2 3 * * *", "0 0 29 2 *", "0 0 31 * *", "15 10 * JAN,jul *",
                "30 2 * * *", "30 1 * * *", "*/30 1 * * *", "0 */2 * * *", "0 0 * * SAT-SUN",
                "*/10 * * * * *", "0 30 9 * * 1-5", "  0   0  * * *  ",
                "@daily", "@hourly", "@weekly", "@monthly", "@yearly", "@annually", "@midnight",
                "@every_minute", "@every_second", "@DAILY",
                "* * * *", "61 * * * *", "* * * * * * *", "@foo", "", "* * 32 * *", "* * * 13 *", "* * * * 8",
                "0 0 L-31 * *", "0 0 * * 1#6", "1-60 * * * *", "*/0 * * * *", "0 0 ? * ?",
                "0 0 * * FRI-MON", "50-10 * * * *", "0 0 30 2 *", "0 0 1-5/2,20 * *", "0 0 ? * 5", "0 0 1 * ?",
                "0 0 * * 0#1,6L", "0 0 * * L", "0 0 * * FRIL", "0 0 * * MON#1", "0 0 5W,10 * *", "0 0 L,15 * *",
                "*/90 * * * *", "0 0 0/7 * *", "0 0 * */5 *", "0 0 1 1/3 *", "0/20 * * * *", "0 0 L-0 * *",
                "0 0 * * * ?", "0 0 W * *", "0 0 0W * *", "0 0 * * 1#0", "@EVERY_MINUTE", "@ daily", "@daily ",
                "0 0 * * sun,SAT", "0 0 1 jan-mar *", "0 0 1 ? *", "*\t* * * *",
            };
            var zones = new[] { "UTC", "America/New_York", "Asia/Jakarta", "Europe/London", "Australia/Lord_Howe" };
            var froms = new[]
            {
                new DateTime(2026, 11, 18, 10, 0, 0, DateTimeKind.Utc),
                new DateTime(2026, 3, 8, 5, 0, 0, DateTimeKind.Utc),
                new DateTime(2026, 10, 31, 23, 0, 0, DateTimeKind.Utc),
                new DateTime(2026, 3, 29, 0, 0, 0, DateTimeKind.Utc),
                new DateTime(2028, 2, 27, 0, 0, 0, DateTimeKind.Utc),
            };

            foreach (var expr in exprs)
            {
                object cron;
                try
                {
                    cron = parse.Invoke(null, new object[] { expr });
                }
                catch (TargetInvocationException)
                {
                    Cron.Add(new Dictionary<string, object> { ["expr"] = expr, ["error"] = true });
                    continue;
                }

                foreach (var zone in zones)
                {
                    var tz = TimeZoneInfo.FindSystemTimeZoneById(zone);
                    foreach (var from in froms)
                    {
                        var occurrences = new List<string>();
                        DateTime? current = from;
                        for (var i = 0; i < 6 && current != null; i++)
                        {
                            current = (DateTime?)next.Invoke(cron, new object[] { current.Value, tz, false });
                            if (current != null) occurrences.Add(current.Value.ToString("yyyy-MM-ddTHH:mm:ssZ"));
                        }
                        Cron.Add(new Dictionary<string, object>
                        {
                            ["expr"] = expr,
                            ["tz"] = zone,
                            ["from"] = from.ToString("yyyy-MM-ddTHH:mm:ssZ"),
                            ["next"] = string.Join(" ", occurrences),
                        });
                    }
                }
            }
        }
    }
}
