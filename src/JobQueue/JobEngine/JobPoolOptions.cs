using System.Threading.Channels;
using JobQueue.Domain;

namespace JobQueue.JobEngine;

public sealed class JobPoolOptions
{
    public string Name { get; init; } = "default";
    public string[] Types { get; init; } = [];

    public int ChannelCapacity { get; set; } = 100;
    public int WorkerCount { get; set; } = 3;
    public int PollBatchSize { get; set; } = 10;

    public int LeaseSeconds { get; set; } = 180;
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromMilliseconds(250);
    public TimeSpan RenewSafetyMargin { get; set; } = TimeSpan.FromSeconds(10);
}

public sealed class JobPoolHandle
{
    public string Name { get; }
    public string[] Types { get; }
    public Channel<JobEnvelope> Ch { get; }
    public JobPoolOptions Options { get; }

    public JobPoolHandle(string name, string[] types, JobPoolOptions opts)
    {
        Name = name;
        Types = types;
        Options = opts;

        var bounded = new BoundedChannelOptions(opts.ChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false
        };
        Ch = Channel.CreateBounded<JobEnvelope>(bounded);
    }
}
