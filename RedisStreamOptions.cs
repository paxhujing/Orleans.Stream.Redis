using StackExchange.Redis;

namespace Orleans.Stream.Redis;

/// <summary>
/// Redis流选项。
/// </summary>
public sealed class RedisStreamOptions
{
    /// <summary>
    /// 连接字符串。
    /// </summary>
    public String? ConnectionString { get; set; }

    /// <summary>
    /// 数据库索引。
    /// </summary>
    public Int32 Database { get; set; }

    /// <summary>
    /// 队列数量。默认8个队列。
    /// </summary>
    public Int32 QueueCount { get; set; } = 8;

    private ConnectionMultiplexer? _redis;

    internal void InitRedis(String providerName)
    {
        if (String.IsNullOrWhiteSpace(ConnectionString))
        {
            throw new ArgumentException($"RedisStreamOptions.ConnectionString is not set for provider {providerName}");
        }
        _redis = ConnectionMultiplexer.Connect(ConnectionString);
    }


    internal IDatabase GetDatabase() => _redis!.GetDatabase(Database);
}
