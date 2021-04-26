// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Generic;

namespace FASTER.indexes.HashValueIndex
{
    public partial class HashValueIndex<TKVKey, TKVValue, TPKey> : ISecondaryValueIndex<TKVValue>
    {
        #region Sync
        private IEnumerable<QueryRecord<TKVKey, TKVValue>> InternalQuery(SecondaryFasterKV<TPKey>.Input input, SecondaryIndexSessionBroker sessionBroker, QuerySettings querySettings,
                                                                 QueryContinuationToken continuationToken = default, int numRecords = Constants.GetAllRecords)
        {
            var sessions = GetSessions(sessionBroker);
            var context = new SecondaryFasterKV<TPKey>.Context { Functions = sessions.SecondarySession.functions };
            var recordInfo = new RecordInfo { PreviousAddress = input.PreviousAddress };
            try
            {
                for (var ii = 0; numRecords == Constants.GetAllRecords || ii < numRecords; ++ii)
                {
                    // Do not yield break here, because we want to serialize the current state into the continuationToken.
                    if (querySettings.IsCanceled)
                        break;
                    SecondaryFasterKV<TPKey>.Output output = default;
                    if (!QueryNext(sessions.SecondarySession, input, ref output, querySettings, ref recordInfo, context))
                        recordInfo.PreviousAddress = core.Constants.kInvalidAddress;
                    else if (ResolveRecord(sessions.PrimarySession, output.RecordId, out var record))
                        yield return record;
                    if (recordInfo.PreviousAddress == core.Constants.kInvalidAddress)
                        break;
                }

                if (continuationToken is { })
                    input.Serialize(continuationToken, 0, recordInfo.PreviousAddress, default);
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
            var context = new SecondaryFasterKV<TPKey>.Context { Functions = sessions.SecondarySession.functions };
            try
            {
                // Initialization phase: get the first records by individual key
                if (continuationToken is null || continuationToken.IsEmpty)
                {
                    var any = false;
                    for (var ii = 0; ii < queryIter.Length; ++ii)
                    {
                        SecondaryFasterKV<TPKey>.Output output = default;
                        if (!QueryNext(sessions.SecondarySession, queryIter[ii].Input, ref output, querySettings, ref queryIter[ii].RecordInfo, context))
                        {
                            queryIter[ii].RecordInfo.PreviousAddress = core.Constants.kInvalidAddress;
                            continue;
                        }
                        if (querySettings.IsCanceled)
                            yield break;
                        queryIter[ii].RecordId = output.RecordId;
                        any = true;
                    }
                    if (!any)
                        yield break;
                }

                // Iteration phase: "yield return" the highest RecordId, then retrieve all PreviousAddresses for those predicates.
                for (var count = 0; numRecords == Constants.GetAllRecords || count < numRecords; /* incremented in loop */)
                {
                    // Do not yield break here, because we want to serialize the current state into the continuationToken.
                    if (querySettings.IsCanceled || !queryIter.Next())
                        break;
                    if (lambda(queryIter.matches) && ResolveRecord(sessions.PrimarySession, queryIter[queryIter.activeIndexes[0]].RecordId, out var record))
                    {
                        yield return record;
                        ++count;
                    }
                    for (var ii = 0; ii < queryIter.activeLength; ++ii)
                    {
                        var activeIndex = queryIter.activeIndexes[ii];
                        queryIter[activeIndex].RecordId = default;
                        if (queryIter[activeIndex].RecordInfo.PreviousAddress != core.Constants.kInvalidAddress)
                        {
                            SecondaryFasterKV<TPKey>.Output output = default;
                            if (!QueryNext(sessions.SecondarySession, queryIter[activeIndex].Input, ref output, querySettings, ref queryIter[activeIndex].RecordInfo, context))
                                queryIter[activeIndex].RecordInfo.PreviousAddress = core.Constants.kInvalidAddress;
                            else
                                queryIter[activeIndex].RecordId = output.RecordId;
                        }
                    }
                }

                if (continuationToken is { })
                {
                    for (var ii = 0; ii < queryIter.Length; ++ii)
                        queryIter[ii].Input.Serialize(continuationToken, ii, queryIter[ii].RecordInfo.PreviousAddress, queryIter[ii].RecordId);
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
        #endregion Sync

        #region Async
        private async IAsyncEnumerable<QueryRecord<TKVKey, TKVValue>> InternalQueryAsync(SecondaryFasterKV<TPKey>.Input input, SecondaryIndexSessionBroker sessionBroker, QuerySettings querySettings,
                                                                    QueryContinuationToken continuationToken = default, int numRecords = Constants.GetAllRecords)
        {
            var sessions = GetSessions(sessionBroker);
            var context = new SecondaryFasterKV<TPKey>.Context { Functions = sessions.SecondarySession.functions };
            var startAddress = input.PreviousAddress;
            try
            {
                for (var ii = 0; numRecords == Constants.GetAllRecords || ii < numRecords; ++ii)
                {
                    if (querySettings.IsCanceled)
                        break;

                    var readAsyncResult = await sessions.SecondarySession.IndexReadAsync(this.secondaryFkv, ref input.QueryKeyRef, ref input, startAddress, context, sessions.SecondarySession.ctx.serialNum, querySettings);
                    var (status, output) = readAsyncResult.Complete();

                    // ConcurrentReader and SingleReader are not called for tombstoned records, so instead we keep that state in the keyPointer.
                    // Thus, Status.NOTFOUND should only be returned if the key was not found.
                    if (status != Status.OK)
                        break;
                    var queryRecord = await ResolveRecordAsync(sessions.PrimarySession, output.RecordId, querySettings);
                    if (queryRecord is { })
                        yield return queryRecord;

                    startAddress = output.PreviousAddress;
                    if (startAddress == core.Constants.kInvalidAddress)
                        break;
                }

                if (continuationToken is { })
                    input.Serialize(continuationToken, 0, startAddress, default);
            }
            finally
            {
                input.Dispose();
            }
        }

        private async IAsyncEnumerable<QueryRecord<TKVKey, TKVValue>> InternalQueryAsync(MultiPredicateQueryIterator<TPKey> queryIter, Func<bool[], bool> lambda, SecondaryIndexSessionBroker sessionBroker, QuerySettings querySettings,
                                                                                         QueryContinuationToken continuationToken = default, int numRecords = Constants.GetAllRecords)
        {
            var sessions = GetSessions(sessionBroker);
            var context = new SecondaryFasterKV<TPKey>.Context { Functions = sessions.SecondarySession.functions };
            try
            {
                // Initialization phase: get the first records by individual key
                if (continuationToken is null || continuationToken.IsEmpty)
                {
                    var any = false;
                    for (var ii = 0; ii < queryIter.Length; ++ii)
                    {
                        var readAsyncResult = await sessions.SecondarySession.IndexReadAsync(this.secondaryFkv, ref queryIter[ii].Input.QueryKeyRef, ref queryIter[ii].Input,
                                                queryIter[ii].RecordInfo.PreviousAddress, context, sessions.SecondarySession.ctx.serialNum, querySettings);
                        var (status, output) = readAsyncResult.Complete();
                        if (status != Status.OK)
                        {
                            queryIter[ii].RecordInfo.PreviousAddress = core.Constants.kInvalidAddress;
                            continue;
                        }
                        if (querySettings.IsCanceled)
                            yield break;
                        queryIter[ii].RecordId = output.RecordId;
                        queryIter[ii].RecordInfo.PreviousAddress = output.PreviousAddress;
                        any = true;
                    }
                    if (!any)
                        yield break;
                }

                // Iteration phase: "yield return" the highest RecordId, then retrieve all PreviousAddresses for those predicates.
                for (var count = 0; numRecords == Constants.GetAllRecords || count < numRecords; /* incremented in loop */)
                {
                    // Do not yield break here, because we want to serialize the current state into the continuationToken.
                    if (querySettings.IsCanceled || !queryIter.Next())
                        break;
                    if (lambda(queryIter.matches))
                    {
                        var queryRecord = await ResolveRecordAsync(sessions.PrimarySession, queryIter[queryIter.activeIndexes[0]].RecordId, querySettings);
                        if (queryRecord is { })
                        {
                            yield return queryRecord;
                            ++count;
                        }
                    }
                    for (var ii = 0; ii < queryIter.activeLength; ++ii)
                    {
                        var activeIndex = queryIter.activeIndexes[ii];
                        queryIter[activeIndex].RecordId = default;
                        if (queryIter[activeIndex].RecordInfo.PreviousAddress != core.Constants.kInvalidAddress)
                        {
                            var readAsyncResult = await sessions.SecondarySession.IndexReadAsync(this.secondaryFkv, ref queryIter[activeIndex].Input.QueryKeyRef, ref queryIter[activeIndex].Input, 
                                                    queryIter[activeIndex].RecordInfo.PreviousAddress, context, sessions.SecondarySession.ctx.serialNum, querySettings);
                            var (status, output) = readAsyncResult.Complete();
                            if (status != Status.OK)
                                queryIter[activeIndex].RecordInfo.PreviousAddress = core.Constants.kInvalidAddress;
                            else
                            {
                                queryIter[activeIndex].RecordId = output.RecordId;
                                queryIter[activeIndex].RecordInfo.PreviousAddress = output.PreviousAddress;
                            }
                        }
                    }
                }

                if (continuationToken is { })
                {
                    for (var ii = 0; ii < queryIter.Length; ++ii)
                        queryIter[ii].Input.Serialize(continuationToken, ii, queryIter[ii].RecordInfo.PreviousAddress, queryIter[ii].RecordId);
                }
            }
            finally
            {
                queryIter.Dispose();
            }
        }

        #endregion Async
    }
}
