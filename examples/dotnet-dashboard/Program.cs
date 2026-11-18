using Hangfire;
using Hangfire.Dashboard;
using Hangfire.SqlServer;
using HangfireDashboard;

var builder = WebApplication.CreateBuilder(args);

var connectionString = Environment.GetEnvironmentVariable("HANGFIRE_SQL")
    ?? "Server=localhost,1433;Database=HangfireGoSdk;User Id=sa;Password=Hangfire_Dev_Passw0rd!;TrustServerCertificate=True;Encrypt=False";
var runServer = Environment.GetEnvironmentVariable("HANGFIRE_DOTNET_SERVER") != "0";
var allowRemote = Environment.GetEnvironmentVariable("HANGFIRE_DASHBOARD_ALLOW_REMOTE") == "1";

// These settings must match the Go SDK's CompatLevel and type-name format.
builder.Services.AddHangfire(config => config
    .SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
    .UseSimpleAssemblyNameTypeSerializer()
    .UseRecommendedSerializerSettings()
    .UseSqlServerStorage(connectionString, new SqlServerStorageOptions
    {
        PrepareSchemaIfNecessary = true,
    }));

if (runServer)
{
    builder.Services.AddHangfireServer(options =>
    {
        options.ServerName = "dotnet-dashboard";
        options.Queues = new[] { "dotnet" };
    });
}

var app = builder.Build();

app.UseHangfireDashboard("/hangfire", new DashboardOptions
{
    DashboardTitle = "Hangfire (Go SDK + .NET)",
    Authorization = allowRemote
        ? new IDashboardAuthorizationFilter[] { new AllowAllDashboardAuthorizationFilter() }
        : new IDashboardAuthorizationFilter[] { new LocalRequestsOnlyAuthorizationFilter() },
});

app.MapGet("/", () => Results.Redirect("/hangfire"));

app.MapPost("/enqueue", (IBackgroundJobClient client, string? name) =>
{
    var id = client.Enqueue<Greeter>("dotnet", g => g.Hello(name ?? "dashboard"));
    return Results.Ok(new { jobId = id });
});

app.Run();
