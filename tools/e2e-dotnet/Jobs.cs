namespace E2E.Jobs;

public class Payload
{
    public string Name { get; set; }
    public int Count { get; set; }
}

public interface IGoJobs
{
    void Hello(string name);
    void Add(int a, int b);
}

public class Recorder
{
    private static readonly object Gate = new();

    private static void Append(string tag, string args)
    {
        var path = Environment.GetEnvironmentVariable("E2E_RECORD_FILE") ?? "/tmp/e2e-record.txt";
        lock (Gate) File.AppendAllText(path, $"{tag}|{args}\n");
    }

    public void Record(string tag) => Append(tag, "");
    public void RecordInt(string tag, int n) => Append(tag, n.ToString());
    public void RecordDate(string tag, DateTime at) => Append(tag, at.ToString("o"));
    public void RecordPoco(string tag, Payload p) => Append(tag, $"{p.Name},{p.Count}");

    public void Fail(string tag)
    {
        Append(tag, "fail");
        throw new InvalidOperationException("e2e failure: " + tag);
    }
}
