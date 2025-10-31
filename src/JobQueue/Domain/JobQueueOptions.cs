namespace JobQueue.Domain;

public sealed class JobQueueOptions
{
    public int ChannelCapacity { get; set; } = 100;
    public int WorkerCount { get; set; } = 3;
    public int PollBatchSize { get; set; } = 6;
    public int LeaseSeconds { get; set; } = 180;
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromMilliseconds(250);
    public TimeSpan RenewSafetyMargin { get; set; } = TimeSpan.FromSeconds(10);
}
