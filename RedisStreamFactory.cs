using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;

namespace Orleans.Stream.Redis;

internal sealed class RedisStreamFactory(String providerName,
    IStreamFailureHandler streamFailureHandler,
    SimpleQueueCacheOptions simpleQueueCacheOptions,
    HashRingStreamQueueMapperOptions hashRingStreamQueueMapperOptions,
    RedisStreamOptions redisStreamOptions,
    ILoggerFactory loggerFactory)
    : IQueueAdapterFactory
{
    private readonly HashRingBasedStreamQueueMapper _queueMapper = new(hashRingStreamQueueMapperOptions, providerName);

    public static IQueueAdapterFactory Create(IServiceProvider services, String providerName)
    {
        var loggerFactory = services.GetRequiredService<ILoggerFactory>();
        var streamFailureHandler = services.GetRequiredKeyedService<IStreamFailureHandler>(providerName);

        var simpleQueueCacheOptions = services.GetOptionsByName<SimpleQueueCacheOptions>(providerName);
        var hashRingStreamQueueMapperOptions = services.GetOptionsByName<HashRingStreamQueueMapperOptions>(providerName);

        var redisStreamOptions = services.GetOptionsByName<RedisStreamOptions>(providerName);
        redisStreamOptions.InitRedis(providerName);

        return new RedisStreamFactory(providerName, streamFailureHandler, 
            simpleQueueCacheOptions, hashRingStreamQueueMapperOptions,
            redisStreamOptions, loggerFactory);
    }

    public Task<IQueueAdapter> CreateAdapter()
    {
        IQueueAdapter adapter = new RedisStreamAdapter(providerName, redisStreamOptions, _queueMapper, loggerFactory);
        return Task.FromResult(adapter);
    }

    public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId) => Task.FromResult(streamFailureHandler);

    public IQueueAdapterCache GetQueueAdapterCache() => new SimpleQueueAdapterCache(simpleQueueCacheOptions, providerName, loggerFactory);

    public IStreamQueueMapper GetStreamQueueMapper() => _queueMapper;
}
