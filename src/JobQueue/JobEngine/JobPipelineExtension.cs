using AutoMealAllocation.Domain;
using AutoMealAllocation.Processing;
using AutoMealAllocation.Infrastructure.Repositories;

namespace AutoMealAllocation.JobEngine;

public static class JobPipelineExtension
{
    public static IServiceCollection AddJobPipelineIntegrated(this IServiceCollection services, Action<WorkerPoolOptions> configure)
    {
        var opts = new WorkerPoolOptions();
        configure?.Invoke(opts);
        var queue = new TrackableChannel<JobEnvelope>();
        services.AddSingleton(queue);
        services.AddHostedService(provider =>
        {
            var log = provider.GetRequiredService<ILogger<JobPoller>>();
            var repo = provider.GetRequiredService<IJobRepository>();
            var env = provider.GetRequiredService<IHostEnvironment>();
            return new JobPoller(log, repo, queue, env);
        });

        services.AddSingleton(provider =>
        {
            var log = provider.GetRequiredService<ILogger<AdaptiveWorkerPool>>();
            var repo = provider.GetRequiredService<IJobRepository>();
            var resolver = provider.GetRequiredService<JobHandlerRegistry>();
            var env = provider.GetRequiredService<IHostEnvironment>();
            return new AdaptiveWorkerPool(log, queue, opts, repo, resolver, env);
        });

        services.AddHostedService(sp => new AdaptiveWorkerPoolService(
            sp.GetRequiredService<AdaptiveWorkerPool>(),
            sp.GetRequiredService<ILogger<AdaptiveWorkerPoolService>>()));

        return services;
    }
}
