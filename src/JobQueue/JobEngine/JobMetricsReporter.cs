using Microsoft.ApplicationInsights;
using AutoMealAllocation.Infrastructure.Repositories;

namespace AutoMealAllocation.JobEngine;

public sealed class JobMetricsReporter(TelemetryClient tc, IJobRepository repo) : BackgroundService
{
    private readonly TelemetryClient _tc = tc;
    private readonly IJobRepository _repo = repo;

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var mCount = _tc.GetMetric("jobqueue.db.queued_count");
        var mFlag = _tc.GetMetric("jobqueue.db.queued_flag");

        while (!ct.IsCancellationRequested)
        {
            var count = await _repo.CountQueuedAsync(ct);
            mCount.TrackValue(count);
            mFlag.TrackValue(count > 0 ? 1 : 0);
            await Task.Delay(TimeSpan.FromSeconds(10), ct);
        }
    }
}
