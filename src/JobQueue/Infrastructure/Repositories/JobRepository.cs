using Dapper;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using AutoMealAllocation.Domain;
using AutoMealAllocation.Infrastructure.Db;

namespace AutoMealAllocation.Infrastructure.Repositories;

public sealed class JobRepository(IDbConnectionFactory factory) : IJobRepository
{
    private readonly IDbConnectionFactory _factory = factory;

    public async Task<long> EnqueueAsync(JobRequest req, CancellationToken ct)
    {
        const string sql = """
        INSERT INTO dbo.Jobs(Type, PayloadJson, Status, Attempts, MaxAttempts, Priority, CreatedAt, ScheduledAt, UpdatedAt, IdempotencyKey)
        VALUES(@type, @payload, @status, 0, ISNULL(@maxAttempts, 5), ISNULL(@priority, 0), SYSUTCDATETIME(), ISNULL(@scheduledAt, SYSUTCDATETIME()), SYSUTCDATETIME(), @idem)
        ; SELECT CAST(SCOPE_IDENTITY() AS BIGINT);
        """;
        using var con = await _factory.CreateOpenConnectionAsync(ct);
        try
        {
            return await con.ExecuteScalarAsync<long>(new CommandDefinition(sql, new
            {
                type = req.Type,
                payload = JsonSerializer.Serialize(req.Payload),
                status = JobStatus.Queued,
                maxAttempts = req.MaxAttempts,
                priority = req.Priority,
                scheduledAt = req.ScheduledAt,
                idem = req.IdempotencyKey
            }, cancellationToken: ct));
        }
        catch (SqlException ex) when (ex.Number is 2601 or 2627)
        {
            const string getId = "SELECT TOP 1 JobId FROM dbo.Jobs WHERE IdempotencyKey = @idem";
            var id = await con.ExecuteScalarAsync<long?>(new CommandDefinition(getId, new { idem = req.IdempotencyKey }, cancellationToken: ct));
            return id ?? 0;
        }
    }

    public async Task<IReadOnlyList<Job>> ClaimJobsAsync(int batchSize, string instanceId, int leaseSeconds, CancellationToken ct)
    {
        const string sql = """
        ;WITH cte AS (
          SELECT TOP (@batch) *
          FROM dbo.Jobs WITH (READPAST, UPDLOCK, ROWLOCK)
          WHERE Status = 0
            AND ScheduledAt <= SYSUTCDATETIME()
            AND (NextRunAt IS NULL OR NextRunAt <= SYSUTCDATETIME())
          ORDER BY Priority DESC, ScheduledAt ASC, JobId ASC
        )
        UPDATE cte SET
          Status      = 1,
          LockedBy    = @me,
          LockedUntil = DATEADD(SECOND, @lease, SYSUTCDATETIME()),
          UpdatedAt   = SYSUTCDATETIME()
        OUTPUT INSERTED.JobId, INSERTED.Type, INSERTED.PayloadJson, INSERTED.Status, INSERTED.Attempts, INSERTED.MaxAttempts,
               INSERTED.Priority, INSERTED.CreatedAt, INSERTED.ScheduledAt, INSERTED.NextRunAt,
               INSERTED.LockedBy, INSERTED.LockedUntil, INSERTED.LastError, INSERTED.UpdatedAt, INSERTED.IdempotencyKey;
        """;

        using var con = await _factory.CreateOpenConnectionAsync(ct);
        var rows = await con.QueryAsync<Job>(new CommandDefinition(sql, new { batch = batchSize, me = instanceId, lease = leaseSeconds }, cancellationToken: ct));
        return rows.AsList();
    }

    public async Task<IReadOnlyList<Job>> ClaimJobsByTypesAsync(
    string[] types, int batchSize, string instanceId, int leaseSeconds, CancellationToken ct)
    {
        const string sql = """
    ;WITH cte AS (
      SELECT TOP (@batch) *
      FROM dbo.Jobs WITH (READPAST, UPDLOCK, ROWLOCK)
      WHERE Type IN @types AND (
            (Status = 0
             AND ScheduledAt <= SYSUTCDATETIME()
             AND (NextRunAt IS NULL OR NextRunAt <= SYSUTCDATETIME()))
         OR (Status = 1 AND LockedUntil IS NOT NULL AND LockedUntil <= SYSUTCDATETIME())
      )
      ORDER BY
        CASE WHEN Status = 1 THEN 0 ELSE 1 END,
        Priority DESC, ScheduledAt ASC, JobId ASC
    )
    UPDATE cte SET
      Status      = 1,
      LockedBy    = @me,
      LockedUntil = DATEADD(SECOND, @lease, SYSUTCDATETIME()),
      UpdatedAt   = SYSUTCDATETIME()
    OUTPUT INSERTED.JobId, INSERTED.Type, INSERTED.PayloadJson, INSERTED.Status, INSERTED.Attempts, INSERTED.MaxAttempts,
           INSERTED.Priority, INSERTED.CreatedAt, INSERTED.ScheduledAt, INSERTED.NextRunAt,
           INSERTED.LockedBy, INSERTED.LockedUntil, INSERTED.LastError, INSERTED.UpdatedAt, INSERTED.IdempotencyKey;
    """;

        using var con = await _factory.CreateOpenConnectionAsync(ct);
        var rows = await con.QueryAsync<Job>(new CommandDefinition(
            sql, new { types, batch = batchSize, me = instanceId, lease = leaseSeconds }, cancellationToken: ct));
        return rows.AsList();
    }


    public async Task<DateTimeOffset?> TryRenewLeaseAsync(long jobId, string instanceId, int leaseSeconds, CancellationToken ct)
    {
        const string sql = """
        UPDATE dbo.Jobs
           SET LockedUntil = DATEADD(SECOND, @lease, SYSUTCDATETIME()),
               UpdatedAt   = SYSUTCDATETIME()
         OUTPUT INSERTED.LockedUntil
         WHERE JobId      = @id
           AND Status     = 2
           AND LockedBy   = @me
           AND LockedUntil > SYSUTCDATETIME();
        """;

        using var con = await _factory.CreateOpenConnectionAsync(ct);
        var newUntil = await con.ExecuteScalarAsync<DateTime?>(
            new CommandDefinition(sql, new { id = jobId, me = instanceId, lease = leaseSeconds }, cancellationToken: ct));

        return newUntil is null ? null : new DateTimeOffset(DateTime.SpecifyKind(newUntil.Value, DateTimeKind.Utc));
    }

    public async Task MarkRunningAsync(long jobId, string instanceId, CancellationToken ct)
    {
        const string sql = """
        UPDATE dbo.Jobs
           SET Status = 2, UpdatedAt = SYSUTCDATETIME()
        WHERE JobId = @id AND LockedBy = @me;
        """;
        using var con = await _factory.CreateOpenConnectionAsync(ct);
        await con.ExecuteAsync(new CommandDefinition(sql, new { id = jobId, me = instanceId }, cancellationToken: ct));
    }

    public async Task MarkSucceededAsync(long jobId, string instanceId, CancellationToken ct)
    {
        const string sql = """
        UPDATE dbo.Jobs
           SET Status = 3, LockedBy = NULL, LockedUntil = NULL, UpdatedAt = SYSUTCDATETIME()
         WHERE JobId = @id AND LockedBy = @me;
        """;
        using var con = await _factory.CreateOpenConnectionAsync(ct);
        await con.ExecuteAsync(new CommandDefinition(sql, new { id = jobId, me = instanceId }, cancellationToken: ct));
    }

    public async Task MarkFailedOrRetryAsync(long jobId, string instanceId, Exception ex, Func<int, TimeSpan> backoff, CancellationToken ct)
    {
        const string get = "SELECT Attempts, MaxAttempts FROM dbo.Jobs WHERE JobId=@id";
        const string upd = """
        UPDATE dbo.Jobs
           SET Attempts = Attempts + 1,
               Status   = CASE WHEN Attempts + 1 >= MaxAttempts THEN 4 ELSE 0 END,
               NextRunAt= CASE WHEN Attempts + 1 >= MaxAttempts THEN NULL ELSE DATEADD(SECOND, @delay, SYSUTCDATETIME()) END,
               LockedBy = NULL, LockedUntil = NULL,
               LastError = LEFT(@err, 2000),
               UpdatedAt = SYSUTCDATETIME()
         WHERE JobId = @id AND LockedBy = @me;
        """;

        using var con = await _factory.CreateOpenConnectionAsync(ct);
        var (attempts, _) = await con.QuerySingleAsync<(int Attempts, int MaxAttempts)>(
            new CommandDefinition(get, new { id = jobId }, cancellationToken: ct));

        var delaySec = (int)backoff(attempts + 1).TotalSeconds;
        var err = ex.ToString();

        await con.ExecuteAsync(new CommandDefinition(upd, new { id = jobId, me = instanceId, delay = delaySec, err }, cancellationToken: ct));
    }

    public async Task<int> ReviveJobsAsync(string? type, CancellationToken ct)
    {
        const string sql = """
        UPDATE dbo.Jobs
           SET Status = 0, LockedBy = NULL, LockedUntil = NULL, UpdatedAt = SYSUTCDATETIME()
         WHERE Status = 2 AND Type = @type AND (LockedUntil IS NULL OR LockedUntil < SYSUTCDATETIME());
        SELECT @@ROWCOUNT;
        """;
        using var con = await _factory.CreateOpenConnectionAsync(ct);
        return await con.ExecuteScalarAsync<int>(new CommandDefinition(sql, new { type }, cancellationToken: ct));
    }
}
