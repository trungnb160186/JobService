using AutoMealAllocation.JobEngine;
using AutoMealAllocation.Endpoints;
using AutoMealAllocation.Processing;
using AutoMealAllocation.Infrastructure.Db;
using AutoMealAllocation.Infrastructure.Repositories;

var builder = WebApplication.CreateBuilder(args);

// Infra
builder.Services.AddSingleton<IDbConnectionFactory, SqlConnectionFactory>();
builder.Services.AddSingleton<IJobRepository, JobRepository>();

// Handlers & registry
builder.Services.AddSingleton<IJobHandler, AllocationHandler>();
builder.Services.AddSingleton<JobHandlerRegistry>();

// Hosted services
builder.Services.AddJobPipelineIntegrated(
    configure: opts =>
    {
        opts.MinWorkers = 1;
        opts.MaxWorkers = 5;
        opts.ChannelCapacity = 10;
        opts.InnerConcurrency = 2;
        opts.ShutdownDrainTimeout = TimeSpan.FromSeconds(20);
        opts.ScaleInterval = TimeSpan.FromMilliseconds(5000);
        opts.IdleCyclesBeforeScaleDown = 3;
    });

// Web
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();
app.UseSwagger();
app.UseSwaggerUI();
app.MapJobEndpoints();

app.Run();
