using Microsoft.Extensions.Logging;
using Orleans.Streams;

namespace Orleans.Stream.Redis;

internal sealed class RedisStreamFailureHandler(ILogger<RedisStreamFailureHandler> logger) : IStreamFailureHandler
{
    public Boolean ShouldFaultSubsriptionOnError => true;

    public Task OnDeliveryFailure(GuidId subscriptionId, String streamProviderName, StreamId streamIdentity, StreamSequenceToken sequenceToken)
    {
        logger.LogError("Delivery failure for subscription {SubscriptionId} on stream {StreamId} with token {Token}", subscriptionId, streamIdentity, sequenceToken);
        return Task.CompletedTask;
    }

    public Task OnSubscriptionFailure(GuidId subscriptionId, String streamProviderName, StreamId streamIdentity, StreamSequenceToken sequenceToken)
    {
        logger.LogError("Subscription failure for subscription {SubscriptionId} on stream {StreamId} with token {Token}", subscriptionId, streamIdentity, sequenceToken);
        return Task.CompletedTask;
    }
}
