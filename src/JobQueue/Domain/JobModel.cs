namespace AutoMealAllocation.Domain;

public enum JobStatus : byte { Queued = 0, Pending = 1, Running = 2, Succeeded = 3, Failed = 4, Dead = 5, Cancel = 6 }

public sealed record Job(
    long JobId,
    string Type,
    string PayloadJson,
    JobStatus Status,
    int Attempts,
    int MaxAttempts,
    int Priority,
    DateTime CreatedAt,
    DateTime ScheduledAt,
    DateTime? NextRunAt,
    string? LockedBy,
    DateTime? LockedUntil,
    string? LastError,
    DateTime UpdatedAt,
    string? IdempotencyKey
);

public sealed record JobEnvelope(Job Job);

public sealed record AllocationRequest(long EventId, IReadOnlyList<long> GuestIds, int MaxGuestsPerHost);
