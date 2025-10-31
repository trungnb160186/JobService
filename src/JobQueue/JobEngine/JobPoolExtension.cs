using JobQueue.Infrastructure.Repositories;
using JobQueue.Processing;

namespace JobQueue.JobEngine;

public static class JobPoolExtension
{
    public static IServiceCollection AddJobPool(this IServiceCollection services, string name, string[] types, Action<JobPoolOptions>? configure = null)
    {
        var opts = new JobPoolOptions { Name = name, Types = types };
        configure?.Invoke(opts);

        var handle = new JobPoolHandle(name, types, opts);
        services.AddSingleton(handle);

        services.AddHostedService(sp =>
        {
            var log = sp.GetRequiredService<ILogger<DbPollerForPool>>();
            var repo = sp.GetRequiredService<IJobRepository>();
            var env = sp.GetRequiredService<IHostEnvironment>();
            return new DbPollerForPool(log, repo, handle, env);
        });

        services.AddHostedService(sp =>
        {
            var log = sp.GetRequiredService<ILogger<ChannelWorkersForPool>>();
            var repo = sp.GetRequiredService<IJobRepository>();
            var reg = sp.GetRequiredService<JobHandlerRegistry>();
            var env = sp.GetRequiredService<IHostEnvironment>();
            return new ChannelWorkersForPool(log, handle, repo, reg, env);
        });

        return services;
    }
}
