namespace AutoMealAllocation.Domain;

public enum JobStatus : byte { Pending = 0, Running = 1, Succeeded = 2, Failed = 3, Dead = 4, Cancel = 5 }

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
