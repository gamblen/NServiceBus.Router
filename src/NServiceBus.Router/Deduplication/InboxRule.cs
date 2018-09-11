﻿using System;
using System.Data.SqlClient;
using System.Threading.Tasks;
using NServiceBus.Router;
using NServiceBus.Router.Deduplication;
using NServiceBus.Transport;

class InboxRule : IRule<RawContext, RawContext>
{
    InboxPersitence persitence;
    Func<SqlConnection> connectionFactory;
    InboxCleanerCollection cleanerCollection;
    SqlDeduplicationSettings settings;
    Task cleanUpBarrier = Task.CompletedTask;

    public InboxRule(InboxPersitence persitence, InboxCleanerCollection cleanerCollection, SqlDeduplicationSettings settings)
    {
        this.persitence = persitence;
        connectionFactory = settings.ConnFactory;
        this.settings = settings;
        this.cleanerCollection = cleanerCollection;
    }

    public async Task Invoke(RawContext context, Func<RawContext, Task> next)
    {
        await cleanUpBarrier.ConfigureAwait(false);

        if (!settings.IsInboxEnabledFor(context.Interface))
        {
            await next(context).ConfigureAwait(false);
            return;
        }

        if (!context.Headers.TryGetValue("NServiceBus.Router.SequenceKey", out var seqKey))
        {
            await next(context).ConfigureAwait(false);
            return;
        }

        if (!settings.IsInboxEnabledFor(context.Interface, seqKey))
        {
            throw new UnforwardableMessageException($"Deduplication is not enabled for source {seqKey} via interface {context.Interface}");
        }

        if (!context.Headers.TryGetValue("NServiceBus.Router.SequenceNumber", out var seqString))
        {
            throw new UnforwardableMessageException("Missing required sequence value (NServiceBus.Router.SequenceNumber) on the message.");
        }

        var isPlug = context.Headers.ContainsKey("NServiceBus.Router.Plug");
        var seq = int.Parse(seqString);

        using (var conn = connectionFactory())
        {
            await conn.OpenAsync().ConfigureAwait(false);

            using (var trans = conn.BeginTransaction())
            {
                var result = await persitence.Deduplicate(context.MessageId, seq, seqKey, conn, trans).ConfigureAwait(false);
                if (result == InboxDeduplicationResult.Duplicate)
                {
                    return;
                }
                if (result == InboxDeduplicationResult.WatermarkViolation)
                {
                    var (newBarrier, checkResult) = cleanerCollection.CheckAgainsWatermarks(seqKey, seq, cleanUpBarrier);
                    cleanUpBarrier = newBarrier;
                    trans.Rollback();

                    if (checkResult == WatermarkCheckViolationResult.Duplicate)
                    {
                        return;
                    }

                    if (checkResult == WatermarkCheckViolationResult.Retry)
                    {
                        //The watermarks may be outdated (in which case the retry may solve it) or the
                        //Seq number might be too high for the inbox to fit in which case this exception
                        //will repeat triggering the throttled mode untill the table can be closed
                        throw new Exception("Aborting forwarding due to check constraint violation. Re-processing should fix the issue.");
                    }
                }

                cleanerCollection.UpdateReceivedSequence(seqKey, seq);

                if (!isPlug) //If message is only a plug we don't forward it
                {
                    var receivedTransportTransaction = context.Extensions.Get<TransportTransaction>();
                    var sqlTransportTransaction = new TransportTransaction();
                    sqlTransportTransaction.Set(conn);
                    sqlTransportTransaction.Set(trans);

                    context.Extensions.Set(sqlTransportTransaction);

                    await next(context);

                    context.Extensions.Set(receivedTransportTransaction);

                }

                trans.Commit();
            }
        }
    }
}
