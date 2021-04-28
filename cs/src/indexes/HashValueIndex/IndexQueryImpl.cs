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
                                                                 QueryContinuationToken continuationToken = default, int numRecords = Constants.kGetAllRecords)
        {
            var sessions = GetSessions(sessionBroker);
            var context = new SecondaryFasterKV<TPKey>.Context { Functions = sessions.SecondarySession.functions };

            // If continuationToken is default then they did not call segmented, but it simplifies the code to make it something.
            bool isSegmented = continuationToken is { };
            continuationToken ??= new QueryContinuationToken(1);
            try
            {
                int count = 0;
                bool needMoreRecords() => numRecords == Constants.kGetAllRecords || count < numRecords;

                // Primary FKV mutable region scan: start at ReadOnlyAddress.
                if (!continuationToken.IsPrimaryComplete)
                {
                    var scanner = GetPrimaryFkvScanner(continuationToken);
                    while (needMoreRecords() && scanner.GetNext(out var recordInfo) && !querySettings.IsCanceled)
                    {
                        continuationToken.PrimaryStartAddress = scanner.NextAddress;
                        if (IsMatch(scanner, input) && ResolveRecord(sessions.PrimarySession, new RecordId(scanner.CurrentAddress, recordInfo.Version), out var queryRecord))
                        {
                            yield return queryRecord;
                            ++count;
                        }
                    }
                }

                // HashValueIndex Iteration phase: "yield return" all matching records.
                if (needMoreRecords() && !querySettings.IsCanceled)
                {
                    continuationToken.IsSecondaryStarted = true;
                    var recordInfo = new RecordInfo { PreviousAddress = input.PreviousAddress };
                    while (needMoreRecords() && !querySettings.IsCanceled)
                    {
                        SecondaryFasterKV<TPKey>.Output output = default;
                        if (!QueryNext(sessions.SecondarySession, input, ref output, querySettings, ref recordInfo, context))
                            recordInfo.PreviousAddress = core.Constants.kInvalidAddress;
                        else if (output.RecordId.Address <= continuationToken.PrimaryStartAddress && ResolveRecord(sessions.PrimarySession, output.RecordId, out var record))
                        {
                            yield return record;
                            ++count;
                        }
                        input.PreviousAddress = recordInfo.PreviousAddress;
                        if (recordInfo.PreviousAddress == core.Constants.kInvalidAddress)
                            break;
                    }
                }

                if (isSegmented)
                {
                    continuationToken.IsCanceled = querySettings.IsCanceled;
                    input.Serialize(continuationToken, 0, input.PreviousAddress, default);
                }
            }
            finally
            {
                input.Dispose();
            }
        }

        private IEnumerable<QueryRecord<TKVKey, TKVValue>> InternalQuery(MultiPredicateQueryIterator<TPKey> queryIter, Func<bool[], bool> lambda, SecondaryIndexSessionBroker sessionBroker, QuerySettings querySettings,
                                                                 QueryContinuationToken continuationToken = default, int numRecords = Constants.kGetAllRecords)
        {
            var sessions = GetSessions(sessionBroker);
            var context = new SecondaryFasterKV<TPKey>.Context { Functions = sessions.SecondarySession.functions };

            // If continuationToken is default then they did not call segmented, but it simplifies the code to make it something.
            bool isSegmented = continuationToken is { };
            continuationToken ??= new QueryContinuationToken(queryIter.Length);
            try
            {
                int count = 0;
                bool needMoreRecords() => numRecords == Constants.kGetAllRecords || count < numRecords;

                // Primary FKV mutable region scan: start at ReadOnlyAddress.
                if (!continuationToken.IsPrimaryComplete)
                {
                    var scanner = GetPrimaryFkvScanner(continuationToken);
                    while (needMoreRecords() && scanner.GetNext(out var recordInfo))
                    {
                        continuationToken.PrimaryStartAddress = scanner.NextAddress;
                        if (IsMatch(scanner, queryIter, lambda) && ResolveRecord(sessions.PrimarySession, new RecordId(scanner.CurrentAddress, recordInfo.Version), out var queryRecord))
                        {
                            yield return queryRecord;
                            ++count;
                        }
                    }
                }

                // HashValueIndex Initialization phase: get the first records by individual key
                if (needMoreRecords() && !continuationToken.IsSecondaryStarted && !querySettings.IsCanceled)
                {
                    continuationToken.IsSecondaryStarted = true;
                    for (var ii = 0; ii < queryIter.Length; ++ii)
                    {
                        do
                        {
                            SecondaryFasterKV<TPKey>.Output output = default;
                            if (!QueryNext(sessions.SecondarySession, queryIter[ii].Input, ref output, querySettings, ref queryIter[ii].RecordInfo, context))
                            {
                                queryIter[ii].RecordId = default;
                                queryIter[ii].RecordInfo.PreviousAddress = core.Constants.kInvalidAddress;
                                continue;
                            }
                            queryIter[ii].RecordId = output.RecordId;
                        } while (queryIter[ii].RecordId.Address > continuationToken.PrimaryStartAddress && !querySettings.IsCanceled);
                    }
                }

                // HashValueIndex Iteration phase: "yield return" the highest RecordId, then retrieve all PreviousAddresses for those predicates.
                while (needMoreRecords())
                {
                    // Do not yield break here, because we want to serialize the current state into the continuationToken.
                    if (querySettings.IsCanceled || !queryIter.Next())
                        break;
                    if (lambda(queryIter.matches) && ResolveRecord(sessions.PrimarySession, queryIter[queryIter.activeIndexes[0]].RecordId, out var queryRecord))
                    {
                        yield return queryRecord;
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

                if (isSegmented)
                {
                    continuationToken.IsCanceled = querySettings.IsCanceled;
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
            Status status = session.IndexRead(this.secondaryFkv, ref input.AsQueryKeyRef, ref input, ref output, ref recordInfo, context);
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
                                                                    QueryContinuationToken continuationToken = default, int numRecords = Constants.kGetAllRecords)
        {
            var sessions = GetSessions(sessionBroker);
            var context = new SecondaryFasterKV<TPKey>.Context { Functions = sessions.SecondarySession.functions };
            var startAddress = input.PreviousAddress;

            // If continuationToken is default then they did not call segmented, but it simplifies the code to make it something.
            bool isSegmented = continuationToken is { };
            continuationToken ??= new QueryContinuationToken(1);
            try
            {
                int count = 0;
                bool needMoreRecords() => numRecords == Constants.kGetAllRecords || count < numRecords;

                // Primary FKV mutable region scan: start at ReadOnlyAddress.
                if (!continuationToken.IsPrimaryComplete)
                {
                    var scanner = GetPrimaryFkvScanner(continuationToken);
                    while (needMoreRecords() && scanner.GetNext(out var recordInfo))
                    {
                        continuationToken.PrimaryStartAddress = scanner.NextAddress;
                        if (IsMatch(scanner, input))
                        {
                            var queryRecord = await ResolveRecordAsync(sessions.PrimarySession, new RecordId(scanner.CurrentAddress, recordInfo.Version), querySettings);
                            if (queryRecord is { })
                            {
                                yield return queryRecord;
                                ++count;
                            }
                        }
                    }
                }

                // HashValueIndex Iteration phase: "yield return" all matching records.
                if (needMoreRecords() && !querySettings.IsCanceled)
                {
                    continuationToken.IsSecondaryStarted = true;
                    while (needMoreRecords() && !querySettings.IsCanceled)
                    {
                        var readAsyncResult = await sessions.SecondarySession.IndexReadAsync(this.secondaryFkv, ref input.AsQueryKeyRef, ref input, startAddress, context, sessions.SecondarySession.ctx.serialNum, querySettings);
                        var (status, output) = readAsyncResult.Complete();

                        // ConcurrentReader and SingleReader are not called for tombstoned records, so instead we keep that state in the keyPointer.
                        // Thus, Status.NOTFOUND should only be returned if the key was not found.
                        if (status != Status.OK)
                            break;

                        if (output.RecordId.Address <= continuationToken.PrimaryStartAddress)
                        {
                            var queryRecord = await ResolveRecordAsync(sessions.PrimarySession, output.RecordId, querySettings);
                            if (queryRecord is { })
                            {
                                yield return queryRecord;
                                ++count;
                            }
                        }

                        startAddress = output.PreviousAddress;
                        if (startAddress == core.Constants.kInvalidAddress)
                            break;
                    }
                }

                if (isSegmented)
                {
                    continuationToken.IsCanceled = querySettings.IsCanceled;
                    input.Serialize(continuationToken, 0, startAddress, default);
                }
            }
            finally
            {
                input.Dispose();
            }
        }

        private async IAsyncEnumerable<QueryRecord<TKVKey, TKVValue>> InternalQueryAsync(MultiPredicateQueryIterator<TPKey> queryIter, Func<bool[], bool> lambda, SecondaryIndexSessionBroker sessionBroker, QuerySettings querySettings,
                                                                                         QueryContinuationToken continuationToken = default, int numRecords = Constants.kGetAllRecords)
        {
            var sessions = GetSessions(sessionBroker);
            var context = new SecondaryFasterKV<TPKey>.Context { Functions = sessions.SecondarySession.functions };

            // If continuationToken is default then they did not call segmented, but it simplifies the code to make it something.
            bool isSegmented = continuationToken is { };
            continuationToken ??= new QueryContinuationToken(queryIter.Length);
            try
            {
                int count = 0;
                bool needMoreRecords() => numRecords == Constants.kGetAllRecords || count < numRecords;

                // Primary FKV mutable region scan: start at ReadOnlyAddress.
                if (!continuationToken.IsPrimaryComplete)
                {
                    var scanner = GetPrimaryFkvScanner(continuationToken);
                    while (needMoreRecords() && scanner.GetNext(out var recordInfo) && !querySettings.IsCanceled)
                    {
                        continuationToken.PrimaryStartAddress = scanner.NextAddress;
                        if (IsMatch(scanner, queryIter, lambda))
                        {
                            var queryRecord = await ResolveRecordAsync(sessions.PrimarySession, new RecordId(scanner.CurrentAddress, recordInfo.Version), querySettings);
                            if (queryRecord is { })
                            {
                                yield return queryRecord;
                                ++count;
                            }
                        }
                    }
                }

                // HashValueIndex Initialization phase: get the first records by individual key
                if (needMoreRecords() && !continuationToken.IsSecondaryStarted)
                {
                    continuationToken.IsSecondaryStarted = true;
                    for (var ii = 0; ii < queryIter.Length; ++ii)
                    {
                        do {
                            var readAsyncResult = await sessions.SecondarySession.IndexReadAsync(this.secondaryFkv, ref queryIter[ii].Input.AsQueryKeyRef, ref queryIter[ii].Input,
                                                    queryIter[ii].RecordInfo.PreviousAddress, context, sessions.SecondarySession.ctx.serialNum, querySettings);
                            var (status, output) = readAsyncResult.Complete();
                            if (status != Status.OK)
                            {
                                queryIter[ii].RecordId = default;
                                queryIter[ii].RecordInfo.PreviousAddress = core.Constants.kInvalidAddress;
                                break;
                            }
                            queryIter[ii].RecordId = output.RecordId;
                            queryIter[ii].RecordInfo.PreviousAddress = output.PreviousAddress;
                        } while (queryIter[ii].RecordId.Address > continuationToken.PrimaryStartAddress && !querySettings.IsCanceled);
                    }
                }

                // HashValueIndex Iteration phase: "yield return" the highest RecordId, then retrieve all PreviousAddresses for those predicates.
                while (needMoreRecords())
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
                            var readAsyncResult = await sessions.SecondarySession.IndexReadAsync(this.secondaryFkv, ref queryIter[activeIndex].Input.AsQueryKeyRef, ref queryIter[activeIndex].Input, 
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

                if (isSegmented)
                {
                    continuationToken.IsCanceled = querySettings.IsCanceled;
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

        #region Utilities
        private IFasterScanIterator<TKVKey, TKVValue> GetPrimaryFkvScanner(QueryContinuationToken continuationToken)
        {
            long startAddress, endAddress;
            if (continuationToken.IsPrimaryStarted)
            {
                startAddress = continuationToken.PrimaryStartAddress;
                endAddress = continuationToken.PrimaryEndAddress;
            }
            else
            {
                startAddress = this.primaryFkv.Log.ReadOnlyAddress;
                endAddress = this.primaryFkv.Log.TailAddress;
                continuationToken.SetPrimaryAddresses(startAddress, endAddress);
            }
            return this.primaryFkv.Log.Scan(startAddress, endAddress);
        }

        private bool IsMatch(IFasterScanIterator<TKVKey, TKVValue> scanner, SecondaryFasterKV<TPKey>.Input input)
        {
            var key = this.predicates[input.PredicateOrdinal].Execute(ref scanner.GetValue());
            return this.userKeyComparer.Equals(ref input.KeyRef, ref key);
        }

        private bool IsMatch(IFasterScanIterator<TKVKey, TKVValue> scanner, MultiPredicateQueryIterator<TPKey> queryIter, Func<bool[], bool> lambda)
        {
            for (var ii = 0; ii < queryIter.Length; ++ii)
            {
                var activeIndex = queryIter.activeIndexes[ii];
                var key = this.predicates[activeIndex].Execute(ref scanner.GetValue());
                queryIter.matches[activeIndex] = this.userKeyComparer.Equals(ref queryIter[activeIndex].Input.KeyRef, ref key);
            }
            return lambda(queryIter.matches);
        }

        #endregion Utilities
    }
}
