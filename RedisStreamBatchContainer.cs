using Orleans.Streams;
using StackExchange.Redis;
using System.Text.Json;

namespace Orleans.Stream.Redis;

[GenerateSerializer]
[Immutable]
[Alias("Orleans.Stream.Redis.RedisStreamBatchContainer")]
internal sealed class RedisStreamBatchContainer : IBatchContainer
{
    [Id(0)]
    private readonly String _data;

    [Id(1)]
    private readonly String _actualEventTypeStr;

    public RedisStreamBatchContainer(StreamEntry streamEntry)
    {
        var values = streamEntry.Values;

        var streamNamespace = (String)values[0].Value!;
        var streamKey = (String)values[1].Value!;
        StreamId = StreamId.Create(streamNamespace, streamKey);

        _actualEventTypeStr = (String)values[2].Value!;
        _data = (String)values[3].Value!;

        SequenceToken = new RedisStreamSequenceToken((String)streamEntry.Id!);
        StreamEntryId = streamEntry.Id!;
    }

    [Id(2)]
    public StreamId StreamId { get; }

    [Id(3)]
    public StreamSequenceToken SequenceToken { get; }

    [Id(4)]
    public String StreamEntryId { get; }

    public IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
    {
        var eventType = typeof(T);
        var actualEventType = Type.GetType(_actualEventTypeStr)!;
        if (eventType.IsAssignableFrom(actualEventType) is false)
        {
            return [];
        }
        var @event = (T)JsonSerializer.Deserialize(_data, actualEventType)!;
        var tuple = new Tuple<T, StreamSequenceToken>(@event, SequenceToken);
        return [tuple];
    }

    public Boolean ImportRequestContext() => false;
}
