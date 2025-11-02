namespace AutoMealAllocation.Processing;

public interface IJobHandler
{
    string JobType { get; }
    Task HandleAsync(string payloadJson, CancellationToken ct);
}

public interface IJobHandler<T> : IJobHandler { }
