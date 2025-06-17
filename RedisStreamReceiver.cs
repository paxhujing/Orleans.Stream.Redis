using Microsoft.Extensions.Logging;
using Orleans.Streams;
using StackExchange.Redis;
using System.Diagnostics;

namespace Orleans.Stream.Redis;

internal sealed class RedisStreamReceiver(IDatabase database, QueueId queueId, ILogger<RedisStreamReceiver> logger) : IQueueAdapterReceiver
{
    private readonly String _queueIdStr = queueId.ToString();
    private String _lastId = "0";
    private Task? _pendingTasks;

    public async Task Initialize(TimeSpan timeout)
    {
        try
        {
            using var cts = new CancellationTokenSource(timeout);
            var task = database.StreamCreateConsumerGroupAsync(_queueIdStr, "consumer", "$", true);
            _ = await task.WaitAsync(timeout, cts.Token);
        }
        catch (Exception ex) when (ex.Message.Contains("name already exists"))
        { }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error initializing stream {QueueId}", queueId);
        }
    }

    public async Task<IList<IBatchContainer>?> GetQueueMessagesAsync(Int32 maxCount)
    {
        try
        {
            var streamTask = database.StreamReadGroupAsync(_queueIdStr, "consumer", _queueIdStr, _lastId, maxCount);
            _pendingTasks = streamTask;
            _lastId = ">";
            var events = await streamTask;
            switch (events.Length)
            {
                case 0:
                    return null;
                case 1:
                    Debug.WriteLine("+");
                    return [new RedisStreamBatchContainer(events[0])];
                default:
                    Debug.WriteLine("++");
                    var tempItems = events.Select(static e => new RedisStreamBatchContainer(e))
                        .ToList<IBatchContainer>();
                    var batch = new BatchContainerBatch(tempItems);
                    return [batch];
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error reading from stream {QueueId}\r\n{Message}\r\n{Stacktrace}",
                _queueIdStr, ex.Message, ex.StackTrace);
            return null;
        }
        finally
        {
            _pendingTasks = null;
        }

    }

    public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
    {
        if (messages.Count is 0)
        {
            return;
        }
        try
        {
            List<String>? idList = null;

            var idEnumerator = messages.Where(x => x is BatchContainerBatch)
                .Cast<BatchContainerBatch>()
                .SelectMany(x => x.BatchContainers)
                .Select(static x=>((RedisStreamBatchContainer)x).StreamEntryId);

            if (idEnumerator.Any())
            {
                idList ??= new(1024);
                idList.AddRange(idEnumerator);
            }

            idEnumerator = messages.Where(static x => x is RedisStreamBatchContainer)
                .Cast<RedisStreamBatchContainer>()
                .Select(static x => x.StreamEntryId);

            if (idEnumerator.Any())
            {
                idList ??= new(1024);
                idList.AddRange(idEnumerator);
            }
            if (idList is null)
            {
                return;
            }
            RedisValue[] ids = [.. idList];
            Debug.WriteLine($"ACK:{String.Join(',', ids)}");
            var batch = database.CreateBatch();
            _ = database.StreamAcknowledgeAsync(_queueIdStr, "consumer", ids);
            Task ackTask = batch.StreamDeleteAsync(_queueIdStr, ids);
            batch.Execute();

            _pendingTasks = ackTask;
            await ackTask;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error acknowledging messages in stream {QueueId}\r\n{Message}\r\n{Stacktrace}",
                _queueIdStr, ex.Message, ex.StackTrace);
        }
        finally
        {
            _pendingTasks = null;
        }
    }

    public async Task Shutdown(TimeSpan timeout)
    {
        using (var cts = new CancellationTokenSource(timeout))
        {
            if (_pendingTasks is not null)
            {
                await _pendingTasks.WaitAsync(timeout, cts.Token);
            }
        }
        logger.LogInformation("Shutting down stream {QueueId}", _queueIdStr);
    }
}
