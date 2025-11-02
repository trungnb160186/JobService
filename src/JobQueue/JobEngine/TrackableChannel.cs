
using System.Threading.Channels;

namespace AutoMealAllocation.JobEngine;

public class ChannelOptions
{
    public string Type { get; set; } = "AllocationRequest";
    public string Name { get; set; } = "AllocationRequestChannel";
    public int BatchSize { get; set; } = 500;
    public int LeaseSeconds { get; set; } = 180;
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromMilliseconds(250);
    public TimeSpan RenewSafetyMargin { get; set; } = TimeSpan.FromSeconds(10);
}

public sealed class TrackableChannel<T>
{
    public ChannelOptions Options { get; }
    public Channel<T> Ch { get; }
    private int _pending;
    private int _inProgress;

    public int Pending => Volatile.Read(ref _pending);
    public int InProgress => Volatile.Read(ref _inProgress);

    public TrackableChannel(int capacity, bool singleReader = false, bool singleWriter = false, ChannelOptions? opts = null)
    {
        Options = opts ?? new ChannelOptions();
        {
            var opt = new BoundedChannelOptions(capacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = singleReader,
                SingleWriter = singleWriter
            };
            Ch = Channel.CreateBounded<T>(opt);
        }
    }

    public async ValueTask EnqueueAsync(T item, CancellationToken ct = default)
    {
        Interlocked.Increment(ref _pending);
        try { await Ch.Writer.WriteAsync(item, ct); }
        catch { Interlocked.Decrement(ref _pending); throw; }
    }

    internal void MarkStart() => Interlocked.Decrement(ref _pending);
    internal void MarkEnter() => Interlocked.Increment(ref _inProgress);
    internal void MarkLeave() => Interlocked.Decrement(ref _inProgress);
}
