using System.Text.Json;

namespace JobQueue.Processing;

public abstract class JobHandler<T> : IJobHandler<T>
{
    public string JobType => typeof(T).Name;

    public async Task HandleAsync(string payloadJson, CancellationToken ct)
    {
        var model = JsonSerializer.Deserialize<T>(payloadJson, new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        })!;
        await HandleAsync(model, ct);
    }

    public abstract Task HandleAsync(T model, CancellationToken ct);
}
