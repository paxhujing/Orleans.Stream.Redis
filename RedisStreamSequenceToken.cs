using Newtonsoft.Json;
using Orleans.Streams;

namespace Orleans.Stream.Redis;

[Serializable]
[GenerateSerializer]
[Alias("Orleans.Stream.Redis.RedisStreamSequenceToken")]
internal sealed class RedisStreamSequenceToken : StreamSequenceToken
{
    [Id(0)]
    [JsonProperty]
    public override Int64 SequenceNumber { get; protected set; }

    [Id(1)]
    [JsonProperty]
    public override Int32 EventIndex { get; protected set; }

    public RedisStreamSequenceToken(String id)
    {
        var splitIndex = id.IndexOf('-');
        if (splitIndex < 0)
        {
            throw new ArgumentException($"Invalid {nameof(id)}", nameof(id));
        }

        SequenceNumber = Int64.Parse(id.AsSpan(0, splitIndex));
        EventIndex = Int32.Parse(id.AsSpan(splitIndex + 1));
    }

    public RedisStreamSequenceToken(Int64 sequenceNumber, Int32 eventIndex)
    {
        SequenceNumber = sequenceNumber;
        EventIndex = eventIndex;
    }

    [JsonConstructor]
    public RedisStreamSequenceToken()
    { }

    public override Int32 CompareTo(StreamSequenceToken other)
        => other is RedisStreamSequenceToken token
            ? SequenceNumber == token.SequenceNumber
                ? EventIndex.CompareTo(token.EventIndex)
                : SequenceNumber.CompareTo(token.SequenceNumber)
            : throw new ArgumentException("Invalid token type", nameof(other));

    public override Boolean Equals(StreamSequenceToken? other)
        => other is RedisStreamSequenceToken token 
        && SequenceNumber == token.SequenceNumber 
        && EventIndex == token.EventIndex;
}
