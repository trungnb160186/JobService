using AutoMealAllocation.Domain;

namespace AutoMealAllocation.Processing;

public sealed class AllocationHandler(ILogger<AllocationHandler> log) : JobHandler<AllocationRequest>
{
    private readonly ILogger<AllocationHandler> _log = log;

    public override async Task HandleAsync(AllocationRequest m, CancellationToken ct)
    {
        _log.LogInformation("Allocating EventId={EventId}, Guests={Cnt}", m.EventId, m.GuestIds.Count);
        // TODO: Google-OrTools allocation logic here
        await Task.Delay(TimeSpan.FromMinutes(2), ct);
    }
}
