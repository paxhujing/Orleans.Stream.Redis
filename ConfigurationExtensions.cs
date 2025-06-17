using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Streams;

namespace Orleans.Stream.Redis;

/// <summary>
/// 用于DI配置的扩展类。
/// </summary>
public static class ConfigurationExtensions
{
    private static void ConfigureCore(ISiloPersistentStreamConfigurator c, Action<RedisStreamOptions> configure)
    {
        c.Configure<RedisStreamOptions>(b => b.Configure(configure)
            .Validate(opt => String.IsNullOrWhiteSpace(opt.ConnectionString) is false
                && ((UInt32)opt.QueueCount) > 0)
            .ValidateOnStart());

        c.Configure<HashRingStreamQueueMapperOptions>(b =>
            b.Configure<IOptionsMonitor<RedisStreamOptions>>((c, o) =>
            c.TotalQueueCount = o.Get(b.Name).QueueCount));

        c.Configure<SimpleQueueCacheOptions>(null);

        c.ConfigureDelegate(services => services.AddKeyedSingleton<IStreamFailureHandler, RedisStreamFailureHandler>(c.Name));

    }

    /// <summary>
    /// 添加Redis流提供器。
    /// </summary>
    /// <param name="builder"><see cref="ISiloBuilder"/>。</param>
    /// <param name="providerName">流提供器的名称。</param>
    /// <param name="configure">配置回调。</param>
    /// <returns></returns>
    public static ISiloBuilder AddRedisStreams(this ISiloBuilder builder, String providerName, Action<RedisStreamOptions> configure)
        => builder.AddPersistentStreams(providerName, RedisStreamFactory.Create, c => ConfigureCore(c, configure));

    /// <summary>
    /// 添加Redis流提供器。
    /// </summary>
    /// <param name="builder"><see cref="ISiloBuilder"/>。</param>
    /// <param name="providerName">流提供器的名称。</param>
    /// <param name="configure">配置回调。</param>
    /// <param name="pullInterval">拉取间隔。</param>
    /// <returns></returns>
    public static ISiloBuilder AddRedisStreams(this ISiloBuilder builder, String providerName, Action<RedisStreamOptions> configure, TimeSpan pullInterval)
        => builder.AddPersistentStreams(providerName, RedisStreamFactory.Create, c =>
        {
            ConfigureCore(c, configure);
            c.ConfigurePullingAgent(b => b.Configure(opt => opt.GetQueueMsgsTimerPeriod = pullInterval));
        });
}
