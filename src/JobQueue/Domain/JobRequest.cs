using System.Text.Json;

namespace AutoMealAllocation.Domain;

public sealed record JobRequest(
    string Type,
    JsonElement Payload,
    string? IdempotencyKey = null,
    DateTimeOffset? ScheduledAt = null,
    int? MaxAttempts = null,
    int? Priority = null
)
{
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(Type))
            throw new ArgumentException("Type is required.", nameof(Type));
        if (Payload.ValueKind is JsonValueKind.Undefined or JsonValueKind.Null)
            throw new ArgumentException("Payload must be a valid JSON object/value.", nameof(Payload));
    }
}
