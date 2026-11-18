using Hangfire.Dashboard;

namespace HangfireDashboard;

public class Greeter
{
    private readonly ILogger<Greeter> _logger;

    public Greeter(ILogger<Greeter> logger) => _logger = logger;

    public void Hello(string name) => _logger.LogInformation("Hello, {Name}, from .NET", name);
}

// Opt-in only: anyone who can reach the port can delete and requeue jobs.
public class AllowAllDashboardAuthorizationFilter : IDashboardAuthorizationFilter
{
    public bool Authorize(DashboardContext context) => true;
}
