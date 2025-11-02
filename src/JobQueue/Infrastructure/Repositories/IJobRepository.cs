using AutoMealAllocation.Domain;

namespace AutoMealAllocation.Infrastructure.Repositories;

public interface IJobRepository
{
    Task<long> EnqueueAsync(JobRequest jobRequest, CancellationToken ct);

    Task<IReadOnlyList<Job>> ClaimJobsAsync(int batchSize, string instanceId, int leaseSeconds, CancellationToken ct);

    Task<IReadOnlyList<Job>> ClaimJobsByTypesAsync(
        string[] types, int batchSize, string instanceId, int leaseSeconds, CancellationToken ct);

    Task<DateTimeOffset?> TryRenewLeaseAsync(long jobId, string instanceId, int leaseSeconds, CancellationToken ct);

    Task MarkSucceededAsync(long jobId, string instanceId, CancellationToken ct);

    Task MarkFailedOrRetryAsync(long jobId, string instanceId, Exception ex,
                                Func<int, TimeSpan> backoff, CancellationToken ct);

    Task<int> ReviveJobsAsync(string? type, CancellationToken ct);
}
