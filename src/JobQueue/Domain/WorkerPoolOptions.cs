
namespace AutoMealAllocation.Domain;

public sealed class WorkerPoolOptions
{
    public int MinWorkers { get; set; } = 1;
    public int MaxWorkers { get; set; } = 10;
    public int InnerConcurrency { get; set; } = 1;
    public int ChannelCapacity { get; set; } = 10;
    public TimeSpan ScaleInterval { get; set; } = TimeSpan.FromSeconds(30);
    public int IdleCyclesBeforeScaleDown { get; set; } = 3;
    public TimeSpan ShutdownDrainTimeout { get; set; } = TimeSpan.FromSeconds(30);
    public string Name { get; set; } = "AutoAllocationPool";
}
