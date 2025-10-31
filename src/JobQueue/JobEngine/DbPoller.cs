using JobQueue.Domain;
using JobQueue.Infrastructure.Repositories;

namespace JobQueue.JobEngine;

public sealed class DbPollerForPool(ILogger<DbPollerForPool> log, IJobRepository repo, JobPoolHandle pool, IHostEnvironment env) : BackgroundService
{
    private readonly ILogger<DbPollerForPool> _log = log;
    private readonly IJobRepository _repo = repo;
    private readonly JobPoolHandle _pool = pool;
    private readonly string _instanceId = $"{Environment.MachineName}-{Environment.ProcessId}-{env.EnvironmentName}";

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var revived = await _repo.ReviveJobsAsync(_pool.Types.FirstOrDefault(), ct);
        if (revived > 0) _log.LogWarning("Revived {Count} stuck jobs", revived);
        _log.LogInformation("[{Pool}] Poller start (types={Types})", _pool.Name, string.Join(",", _pool.Types));

        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (!await _pool.Ch.Writer.WaitToWriteAsync(ct))
                    continue;

                var rows = await _repo.ClaimJobsByTypesAsync(_pool.Types, _pool.Options.PollBatchSize, _instanceId, _pool.Options.LeaseSeconds, ct);
                foreach (var row in rows)
                    await _pool.Ch.Writer.WriteAsync(new JobEnvelope(row), ct);

                if (rows.Count == 0)
                    await Task.Delay(_pool.Options.PollInterval, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
            catch (Exception ex)
            {
                _log.LogError(ex, "[{Pool}] Poller error", _pool.Name);
                await Task.Delay(TimeSpan.FromSeconds(1), ct);
            }
        }

        _pool.Ch.Writer.TryComplete();
        _log.LogInformation("[{Pool}] Poller stop", _pool.Name);
    }
}
