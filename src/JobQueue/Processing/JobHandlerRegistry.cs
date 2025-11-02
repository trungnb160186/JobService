namespace AutoMealAllocation.Processing;

public sealed class JobHandlerRegistry(IEnumerable<IJobHandler> handlers)
{
    private readonly Dictionary<string, IJobHandler> _map = handlers.ToDictionary(h => h.JobType, StringComparer.OrdinalIgnoreCase);

    public IJobHandler? Resolve(string type) => _map.TryGetValue(type, out var h) ? h : null;
}
