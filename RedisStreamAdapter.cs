using Microsoft.Extensions.Logging;
using Orleans.Streams;
using StackExchange.Redis;
using System.Text.Json;

namespace Orleans.Stream.Redis;

internal sealed class RedisStreamAdapter(String providerName,
    RedisStreamOptions redisStreamOptions,
    IStreamQueueMapper queueMapper,
    ILoggerFactory loggerFactory) : IQueueAdapter
{
    private ILogger? _logger;

    public String Name => providerName;

    public Boolean IsRewindable => false;

    public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

    public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
    {
        var database = redisStreamOptions.GetDatabase();
        return new RedisStreamReceiver(database, queueId, loggerFactory.CreateLogger<RedisStreamReceiver>());
    }

    public async Task QueueMessageBatchAsync<T>(StreamId streamId, IEnumerable<T> events, StreamSequenceToken token, Dictionary<String, Object> requestContext)
    {
        var queueId = queueMapper.GetQueueForStream(streamId);
        var database = redisStreamOptions.GetDatabase();

        try
        {
            foreach (var @event in events)
            {
                var @namespace = new NameValueEntry("StreamNamespace", streamId.Namespace);
                var key = new NameValueEntry("StreamKey", streamId.Key);
                var eventType = new NameValueEntry("EventType", @event!.GetType().AssemblyQualifiedName);
                var json = JsonSerializer.Serialize(@event, @event!.GetType());
                var data = new NameValueEntry("Data", json);

                _ = await database.StreamAddAsync(queueId.ToString(), [@namespace, key, eventType, data]);
            }
        }
        catch (Exception ex)
        {
            _logger ??= loggerFactory.CreateLogger<RedisStreamAdapter>();
            _logger.LogError(ex, "Error adding event to stream {StreamId}", streamId);
        }
    }
}
