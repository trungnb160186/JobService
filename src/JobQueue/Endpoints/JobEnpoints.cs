using System.Text.Json;
using Dapper;
using JobQueue.Domain;
using JobQueue.Infrastructure.Repositories;

namespace JobQueue.Endpoints;

public static class JobEndpoints
{
    public static IEndpointRouteBuilder MapJobEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/job/enqueue", async (JobRequest req, IJobRepository repo, CancellationToken ct) =>
        {
            req.Validate();
            var id = await repo.EnqueueAsync(req, ct);
            return Results.Accepted($"/job/{id}");
        });

        app.MapGet("/job/{id:long}", async (long id, JobQueue.Infrastructure.Db.IDbConnectionFactory dbf, CancellationToken ct) =>
        {
            const string sql = "SELECT * FROM dbo.Jobs WHERE JobId = @id";
            using var con = await dbf.CreateOpenConnectionAsync(ct);
            var row = await con.QuerySingleOrDefaultAsync<Job>(sql, new { id });
            return row is null ? Results.NotFound() : Results.Ok(row);
        });

        return app;
    }
}
