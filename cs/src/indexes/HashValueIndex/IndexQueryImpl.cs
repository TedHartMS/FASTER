// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Generic;

namespace FASTER.indexes.HashValueIndex
{
    public partial class HashValueIndex<TKVKey, TKVValue, TPKey> : ISecondaryValueIndex<TKVValue>
    {
        private IEnumerable<QueryRecord<TKVKey, TKVValue>> InternalQuery(SecondaryFasterKV<TPKey>.Input input, SecondaryIndexSessionBroker sessionBroker, QuerySettings querySettings,
                                                                 QueryContinuationToken continuationToken = default, int numRecords = Constants.GetAllRecords)
        {
            var sessions = GetSessions(sessionBroker);
            foreach (var recordId in InternalQuery(sessions.SecondarySession, input, querySettings, continuationToken, numRecords))
            {
                if (querySettings.IsCanceled)
                    yield break;
                if (ResolveRecord(sessions.PrimarySession, recordId, out var record))
                    yield return record;
            }
        }

        private IEnumerable<RecordId> InternalQuery(AdvancedClientSession<TPKey, RecordId, SecondaryFasterKV<TPKey>.Input, SecondaryFasterKV<TPKey>.Output, SecondaryFasterKV<TPKey>.Context, SecondaryFasterKV<TPKey>.Functions> session,
                SecondaryFasterKV<TPKey>.Input input, QuerySettings querySettings, QueryContinuationToken continuationToken, int numRecords)
        {
            var context = new SecondaryFasterKV<TPKey>.Context { Functions = session.functions };
            var recordInfo = new RecordInfo { PreviousAddress = input.PreviousAddress };
            try
            {
                for (var ii = 0; numRecords == Constants.GetAllRecords || ii < numRecords; ++ii)
                {
                    SecondaryFasterKV<TPKey>.Output output = default;
                    if (!QueryNext(session, input, ref output, querySettings, ref recordInfo, context))
                        recordInfo.PreviousAddress = core.Constants.kInvalidAddress;
                    else
                        yield return output.RecordId;
                    if (recordInfo.PreviousAddress == core.Constants.kInvalidAddress)
                        break;
                }

                if (continuationToken is { })
                    input.Serialize(this.keyAccessor, continuationToken, 0, recordInfo.PreviousAddress);
            }
            finally
            {
                input.Dispose();
            }
        }

        private IEnumerable<QueryRecord<TKVKey, TKVValue>> InternalQuery(MultiPredicateQueryIterator<TPKey> queryIter, Func<bool[], bool> lambda, SecondaryIndexSessionBroker sessionBroker, QuerySettings querySettings,
                                                                 QueryContinuationToken continuationToken = default, int numRecords = Constants.GetAllRecords)
        {
            var sessions = GetSessions(sessionBroker);
            foreach (var recordId in InternalQuery(sessions.SecondarySession, queryIter, lambda, querySettings, continuationToken, numRecords))
            {
                if (querySettings.IsCanceled)
                    yield break;
                if (ResolveRecord(sessions.PrimarySession, recordId, out var record))
                    yield return record;
            }
        }

        private IEnumerable<RecordId> InternalQuery(AdvancedClientSession<TPKey, RecordId, SecondaryFasterKV<TPKey>.Input, SecondaryFasterKV<TPKey>.Output, SecondaryFasterKV<TPKey>.Context, SecondaryFasterKV<TPKey>.Functions> session,
                MultiPredicateQueryIterator<TPKey> queryIter, Func<bool[], bool> lambda, QuerySettings querySettings, QueryContinuationToken continuationToken, int numRecords)
        {
            var context = new SecondaryFasterKV<TPKey>.Context { Functions = session.functions };
            try
            {
                // Initialization phase: get the first records by individual key
                var any = false;
                for (var ii = 0; ii < queryIter.Length; ++ii)
                {
                    SecondaryFasterKV<TPKey>.Output output = default;
                    if (!QueryNext(session, queryIter[ii].Input, ref output, querySettings, ref queryIter[ii].RecordInfo, context))
                    {
                        queryIter[ii].RecordInfo.PreviousAddress = core.Constants.kInvalidAddress;
                        continue;
                    }
                    queryIter[ii].RecordId = output.RecordId;
                    any = true;
                }
                if (!any)
                    yield break;

                // Iteration phase: "yield return" the highest RecordId, then retrieve all PreviousAddresses for those predicates.
                for (var count = 0; numRecords == Constants.GetAllRecords || count < numRecords; ++count)
                {
                    if (!queryIter.Next())
                        break;
                    if (lambda(queryIter.matches))
                        yield return queryIter[queryIter.activeIndexes[0]].RecordId;
                    for (var ii = 0; ii < queryIter.activeLength; ++ii)
                    {
                        var activeIndex = queryIter.activeIndexes[ii];
                        queryIter[activeIndex].RecordId = default;
                        if (queryIter[activeIndex].RecordInfo.PreviousAddress != core.Constants.kInvalidAddress)
                        {
                            SecondaryFasterKV<TPKey>.Output output = default;
                            if (!QueryNext(session, queryIter[activeIndex].Input, ref output, querySettings, ref queryIter[activeIndex].RecordInfo, context))
                                queryIter[activeIndex].RecordInfo.PreviousAddress = core.Constants.kInvalidAddress;
                            else
                                queryIter[activeIndex].RecordId = output.RecordId;
                        }
                    }
                }

                if (continuationToken is { })
                {
                    for (var ii = 0; ii < queryIter.Length; ++ii)
                        queryIter[ii].Input.Serialize(this.keyAccessor, continuationToken, 0, queryIter[ii].RecordInfo.PreviousAddress);
                }
            }
            finally
            {
                queryIter.Dispose();
            }
        }

        private bool QueryNext(AdvancedClientSession<TPKey, RecordId, SecondaryFasterKV<TPKey>.Input, SecondaryFasterKV<TPKey>.Output, SecondaryFasterKV<TPKey>.Context, SecondaryFasterKV<TPKey>.Functions> session,
                SecondaryFasterKV<TPKey>.Input input, ref SecondaryFasterKV<TPKey>.Output output, QuerySettings querySettings, ref RecordInfo recordInfo, SecondaryFasterKV<TPKey>.Context context)
        {
            Status status = session.IndexRead(this.secondaryFkv, ref input.QueryKeyRef, ref input, ref output, ref recordInfo, context);
            if (!querySettings.IsCanceled)
            {
                if (status == Status.PENDING)
                {
                    // Because we traverse the chain, we must wait for any pending read operations to complete.
                    session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    if (completedOutputs.Next())
                    {
                        ref var completedOutput = ref completedOutputs.Current;
                        status = completedOutput.Status;
                        output = completedOutput.Output;
                    }
                    completedOutputs.Dispose();
                }

                // ConcurrentReader and SingleReader are not called for tombstoned records, so instead we keep that state in the keyPointer.
                // Thus, Status.NOTFOUND should only be returned if the key was not found.
                if (status == Status.OK)
                {
                    recordInfo.PreviousAddress = output.PreviousAddress;
                    return true;
                }
            }

            recordInfo.PreviousAddress = FASTER.core.Constants.kInvalidAddress;
            return false;
        }

        private async IAsyncEnumerable<RecordId> InternalQueryAsync(AdvancedClientSession<TPKey, RecordId, SecondaryFasterKV<TPKey>.Input, SecondaryFasterKV<TPKey>.Output, SecondaryFasterKV<TPKey>.Context, SecondaryFasterKV<TPKey>.Functions> session,
                SecondaryFasterKV<TPKey>.Input input, SecondaryFasterKV<TPKey>.Output queryOutput, QuerySettings querySettings)
        {
            var context = new SecondaryFasterKV<TPKey>.Context { Functions = session.functions };
            RecordInfo recordInfo = default;
            try
            {
                do
                {
                    // Because we traverse the chain, we must wait for any pending read operations to complete.
                    var readAsyncResult = await session.IndexReadAsync(this.secondaryFkv, ref input.QueryKeyRef, ref input, queryOutput, recordInfo.PreviousAddress, context, session.ctx.serialNum, querySettings);
                    if (querySettings.IsCanceled)
                        yield break;
                    var (status, output) = readAsyncResult.Complete();

                    // ConcurrentReader and SingleReader are not called for tombstoned records, so instead we keep that state in the keyPointer.
                    // Thus, Status.NOTFOUND should only be returned if the key was not found.
                    if (status != Status.OK)
                        yield break;
                    yield return output.RecordId;

                    recordInfo.PreviousAddress = output.PreviousAddress;
                } while (recordInfo.PreviousAddress != core.Constants.kInvalidAddress);
            }
            finally
            {
                input.Dispose();
            }
        }
    }
}
