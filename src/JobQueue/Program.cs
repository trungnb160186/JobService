using System.Threading.Channels;
using JobQueue.Domain;
using JobQueue.Infrastructure.Db;
using JobQueue.Infrastructure.Repositories;
using JobQueue.JobEngine;
using JobQueue.Processing;
using JobQueue.Endpoints;

var builder = WebApplication.CreateBuilder(args);

// Infra
builder.Services.AddSingleton<IDbConnectionFactory, SqlConnectionFactory>();
builder.Services.AddSingleton<IJobRepository, JobRepository>();

// Handlers & registry
builder.Services.AddSingleton<IJobHandler, AllocationHandler>();
builder.Services.AddSingleton<JobHandlerRegistry>();

// Hosted services
builder.Services.AddJobPool(
    name: "allocation-pool",
    types: [nameof(AllocationRequest)],
    configure: opts =>
    {
        opts.WorkerCount = 5;
        opts.ChannelCapacity = 20;
        opts.PollBatchSize = 10;
        opts.LeaseSeconds = 300;
        opts.PollInterval = TimeSpan.FromMilliseconds(500);
        opts.RenewSafetyMargin = TimeSpan.FromSeconds(10);
    });

// Web
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();
app.UseSwagger();
app.UseSwaggerUI();
app.MapJobEndpoints();

app.Run();
