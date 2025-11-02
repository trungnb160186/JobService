using AutoMealAllocation.Domain;
using AutoMealAllocation.Infrastructure.Repositories;

namespace AutoMealAllocation.JobEngine;

public sealed class JobPoller(ILogger<JobPoller> log, IJobRepository repo, TrackableChannel<JobEnvelope> queue, IHostEnvironment env) : BackgroundService
{
    private readonly ILogger<JobPoller> _log = log;
    private readonly IJobRepository _repo = repo;
    private readonly TrackableChannel<JobEnvelope> _queue = queue;
    private readonly string _instanceId = $"{Environment.MachineName}-{Environment.ProcessId}-{env.EnvironmentName}";

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var revived = await _repo.ReviveJobsAsync(_queue.Options.Type, ct);
        if (revived > 0) _log.LogWarning("Revived {Count} stuck jobs", revived);
        _log.LogInformation("[{Pool}] Poller start (types={Types})", _queue.Options.Name, string.Join(",", _queue.Options.Type));

        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (!await _queue.Ch.Writer.WaitToWriteAsync(ct))
                    continue;

                var rows = await _repo.ClaimJobsByTypesAsync([_queue.Options.Type], _queue.Options.BatchSize, _instanceId, _queue.Options.LeaseSeconds, ct);
                foreach (var row in rows)
                    // await _queue.Ch.Writer.WriteAsync(new JobEnvelope(row), ct);
                    await _queue.EnqueueAsync(new JobEnvelope(row), ct);

                if (rows.Count == 0)
                    await Task.Delay(_queue.Options.PollInterval, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
            catch (Exception ex)
            {
                _log.LogError(ex, "[{Pool}] Poller error", _queue.Options.Name);
                await Task.Delay(TimeSpan.FromSeconds(1), ct);
            }
        }

        _queue.Ch.Writer.TryComplete();
        _log.LogInformation("[{Pool}] Poller stop", _queue.Options.Name);
    }
}
