using JobQueue.Infrastructure.Repositories;
using JobQueue.Processing;

namespace JobQueue.JobEngine;

public sealed class ChannelWorkersForPool(ILogger<ChannelWorkersForPool> log, JobPoolHandle pool, IJobRepository repo, JobHandlerRegistry registry, IHostEnvironment env) : BackgroundService
{
    private readonly ILogger<ChannelWorkersForPool> _log = log;
    private readonly JobPoolHandle _pool = pool;
    private readonly IJobRepository _repo = repo;
    private readonly JobHandlerRegistry _registry = registry;
    private readonly string _instanceId = $"{Environment.MachineName}-{Environment.ProcessId}-{env.EnvironmentName}";

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        _log.LogInformation("[{Pool}] Workers start (concurrency={C})", _pool.Name, _pool.Options.WorkerCount);

        var workers = Enumerable.Range(0, _pool.Options.WorkerCount)
            .Select(i => RunWorker(i, ct))
            .ToArray();

        await Task.WhenAll(workers);
        _log.LogInformation("[{Pool}] Workers stop", _pool.Name);
    }

    private async Task RunWorker(int workerId, CancellationToken ct)
    {
        await foreach (var env in _pool.Ch.Reader.ReadAllAsync(ct))
        {
            var initialUntil = env.Job.LockedUntil.HasValue
                ? new DateTimeOffset(DateTime.SpecifyKind(env.Job.LockedUntil.Value, DateTimeKind.Utc))
                : DateTimeOffset.UtcNow.AddSeconds(_pool.Options.LeaseSeconds);

            using var leaseCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var lostLease = false;

            var renewTask = RenewLeaseLoopAsync(env.Job.JobId, initialUntil, () =>
            {
                lostLease = true;
                leaseCts.Cancel();
            }, leaseCts.Token);

            try
            {
                var handler = _registry.Resolve(env.Job.Type)
                              ?? throw new InvalidOperationException($"No handler for type '{env.Job.Type}'");

                await handler.HandleAsync(env.Job.PayloadJson, leaseCts.Token);

                if (!lostLease)
                    await _repo.MarkSucceededAsync(env.Job.JobId, _instanceId, ct);
                else
                    _log.LogWarning("[{Pool}] JobId={JobId} finished locally but lease lost; skip mark.", _pool.Name, env.Job.JobId);
            }
            catch (OperationCanceledException) when (leaseCts.IsCancellationRequested || ct.IsCancellationRequested) { }
            catch (Exception ex)
            {
                if (!lostLease)
                {
                    _log.LogError(ex, "[{Pool}] Worker {Id} failed JobId={JobId}", _pool.Name, workerId, env.Job.JobId);
                    await _repo.MarkFailedOrRetryAsync(env.Job.JobId, _instanceId, ex, Backoff, ct);
                }
                else
                {
                    _log.LogWarning(ex, "[{Pool}] JobId={JobId} lost lease; skip mark fail.", _pool.Name, env.Job.JobId);
                }
            }
            finally
            {
                try { leaseCts.Cancel(); await renewTask; } catch { /* ignore */ }
            }
        }
    }

    private async Task RenewLeaseLoopAsync(long jobId, DateTimeOffset lockedUntilUtc, Action onLostLease, CancellationToken ct)
    {
        var safety = _pool.Options.RenewSafetyMargin;
        var leaseSec = _pool.Options.LeaseSeconds;

        while (!ct.IsCancellationRequested)
        {
            var remaining = lockedUntilUtc - DateTimeOffset.UtcNow;
            var delay = remaining - safety;
            if (delay < TimeSpan.Zero) delay = TimeSpan.Zero;
            if (delay < TimeSpan.FromSeconds(1)) delay = TimeSpan.FromSeconds(1);

            try { await Task.Delay(delay, ct); } catch (OperationCanceledException) { break; }

            var newUntil = await _repo.TryRenewLeaseAsync(jobId, _instanceId, leaseSec, ct);
            if (newUntil is null) { onLostLease(); break; }
            lockedUntilUtc = newUntil.Value;
        }
    }

    private static TimeSpan Backoff(int attempt)
    {
        var seconds = Math.Min(300, (int)Math.Pow(2, Math.Min(8, attempt)) * 2);
        return TimeSpan.FromSeconds(seconds);
    }
}
