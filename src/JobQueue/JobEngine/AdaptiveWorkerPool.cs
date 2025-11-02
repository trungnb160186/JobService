
using AutoMealAllocation.Domain;
using AutoMealAllocation.Processing;
using AutoMealAllocation.Infrastructure.Repositories;

namespace AutoMealAllocation.JobEngine;

public sealed class AdaptiveWorkerPool
{
    private readonly ILogger _log;
    private readonly WorkerPoolOptions _opts;
    private readonly TrackableChannel<JobEnvelope> _q;
    private readonly IJobRepository _repo;
    private readonly JobHandlerRegistry _resolver;
    private readonly CancellationTokenSource _stop = new();
    private readonly Lock _gate = new();
    private readonly List<(Task task, CancellationTokenSource cts)> _workers = [];
    private Task? _controller;

    public TrackableChannel<JobEnvelope> Queue => _q;

    private readonly string _instanceId = string.Empty;

    public AdaptiveWorkerPool(
        ILogger<AdaptiveWorkerPool> log,
        TrackableChannel<JobEnvelope> q,
        WorkerPoolOptions opts,
        IJobRepository repo,
        JobHandlerRegistry resolver,
        IHostEnvironment env)
    {
        _log = log;
        _opts = opts;
        _q = q;
        _repo = repo; 
        _resolver = resolver;

        if (_opts.MinWorkers < 0 || _opts.MaxWorkers < 1 || _opts.MinWorkers > _opts.MaxWorkers)
            throw new ArgumentOutOfRangeException(nameof(opts), "Invalid Min/MaxWorkers");
        if (_opts.InnerConcurrency < 1)
            throw new ArgumentOutOfRangeException(nameof(opts), "InnerConcurrency must be >= 1");

        _instanceId = $"{Environment.MachineName}-{Environment.ProcessId}-{env.EnvironmentName}";
    }

    public async Task StartAsync(CancellationToken ct = default)
    {
        for (int i = 0; i < _opts.MinWorkers; i++)
        {
            StartWorker();
        }
        _controller = Task.Run(ControllerLoopAsync, ct);
        _log.LogInformation("[{Name}] Integrated pool started (min={Min}, max={Max}, inner={Inner})",
            _opts.Name, _opts.MinWorkers, _opts.MaxWorkers, _opts.InnerConcurrency);
        await Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken ct = default)
    {
        _stop.Cancel();
        if (_controller != null) 
        { 
            try { await _controller; } catch { } 
        }
        List<(Task task, CancellationTokenSource cts)> copy;
        lock (_gate) copy = [.. _workers];
        foreach (var (task, cts) in copy)
        {
            cts.Cancel();
        }
        try 
        { 
            await Task.WhenAll(copy.Select(w => w.task)); 
        } 
        catch { }
        _log.LogInformation("[{Name}] Integrated pool stopped", _opts.Name);
    }

    private int ActiveWorkers { get { lock (_gate) return _workers.Count; } }

    private void StartWorker()
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
        var task = Task.Run(() => WorkerLoopAsync(cts.Token), cts.Token);
        lock (_gate) _workers.Add((task, cts));
        _log.LogInformation("[{Name}] Start worker (active={Active})", _opts.Name, ActiveWorkers);
    }

    private void StopOneWorker()
    {
        lock (_gate)
        {
            for (int i = _workers.Count - 1; i >= 0; i--)
            {
                var (task, cts) = _workers[i];
                if (!task.IsCompleted)
                {
                    cts.Cancel();
                    _workers.RemoveAt(i);
                    break;
                }
            }
            _log.LogInformation("[{Name}] Stop worker (active={Active})", _opts.Name, ActiveWorkers);
        }
    }

    private async Task ControllerLoopAsync()
    {
        var idle = 0;
        try
        {
            while (!_stop.IsCancellationRequested)
            {
                var pending = _q.Pending;
                var inProgress = _q.InProgress;
                var active = ActiveWorkers;

                _log.LogInformation("[{Name}]: pending={Pending}, inProgress={InProgress}, active={Active}",
                    _opts.Name, pending, inProgress, active);

                if (pending > 0 && active < _opts.MaxWorkers)
                {
                    StartWorker();
                    idle = 0;
                }
                else if (pending == 0 && inProgress == 0 && active > _opts.MinWorkers)
                {
                    idle++;
                    if (idle >= _opts.IdleCyclesBeforeScaleDown)
                    {
                        StopOneWorker();
                        idle = 0;
                    }
                }
                else
                {
                    idle = 0;
                }

                await Task.Delay(_opts.ScaleInterval, _stop.Token);
            }
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
    }

    private async Task WorkerLoopAsync(CancellationToken ct)
    {
        var inProgress = new HashSet<Task>();
        var reader = _q.Ch.Reader;

        try
        {
            while (await reader.WaitToReadAsync(ct))
            {
                inProgress.RemoveWhere(t => t.IsCompleted);

                if (inProgress.Count >= _opts.InnerConcurrency)
                {
                    var done = await Task.WhenAny(inProgress);
                    inProgress.Remove(done);
                    try { await done; } catch { }
                    continue;
                }
               
                if (reader.TryRead(out var env))
                {
                    _q.MarkStart();
                    _q.MarkEnter();

                    var t = ProcessAsync(env, ct).ContinueWith(static (ante, state) =>
                    {
                        var q = (TrackableChannel<JobEnvelope>)state!;
                        q.MarkLeave();
                    }, _q, TaskScheduler.Default);

                    inProgress.Add(t);
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        finally
        {
            while (inProgress.Count > 0)
            {
                var done = await Task.WhenAny(inProgress);
                inProgress.Remove(done);
                try { await done; } catch { }
            }
        }
    }

    private async Task ProcessAsync(JobEnvelope env, CancellationToken ct)
    {
        using var renewCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var renew = RenewLeaseLoopAsync(env.Job.JobId, renewCts.Token);
        try
        {
            var handler = _resolver.Resolve(env.Job.Type) ??
            throw new InvalidOperationException($"No handler for type '{env.Job.Type}'");

            await _repo.MarkRunningAsync(env.Job.JobId,_instanceId, ct);
            await handler.HandleAsync(env.Job.PayloadJson, ct);
            await _repo.MarkSucceededAsync(env.Job.JobId, _instanceId, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (Exception ex)
        {
            await _repo.MarkFailedOrRetryAsync(env.Job.JobId, _instanceId, ex, Backoff, ct);
            throw;
        }
        finally
        {
            try { renewCts.Cancel(); await renew; } catch { }
        }
    }

    private async Task RenewLeaseLoopAsync(long jobId, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var lease = _q.Options.LeaseSeconds;
                var delay = TimeSpan.FromSeconds(Math.Max(5, Math.Max(1, lease / 2 - 5)));
                await Task.Delay(delay, ct);
                await _repo.TryRenewLeaseAsync(jobId, _instanceId, lease, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
            catch { }
        }
    }

    private static TimeSpan Backoff(int attempt)
    {
        var seconds = Math.Min(300, (int)Math.Pow(2, Math.Min(8, attempt)) * 2);
        return TimeSpan.FromSeconds(seconds);
    }
}

public sealed class AdaptiveWorkerPoolService(AdaptiveWorkerPool pool, ILogger<AdaptiveWorkerPoolService> log) : BackgroundService
{
    private readonly AdaptiveWorkerPool _pool = pool;
    private readonly ILogger<AdaptiveWorkerPoolService> _log = log;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _pool.StartAsync(stoppingToken);
        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        finally
        {
            await _pool.StopAsync(stoppingToken);
        }
    }
}
