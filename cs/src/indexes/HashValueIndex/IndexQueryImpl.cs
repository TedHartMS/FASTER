﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace FASTER.indexes.HashValueIndex
{
    public partial class HashValueIndex<TKVKey, TKVValue, TPKey> : ISecondaryValueIndex<TKVValue>
    {
        // TODO: compare to NullIndicator using IEquatable (hold a value indicating the key type supports that)

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe FasterKVHVI<TPKey>.Input MakeQueryInput(int predOrdinal, ref TPKey key)
        {
            // Putting the query key in Input is necessary because iterator functions cannot contain unsafe code or have
            // byref args, and bufferPool is needed here because the stack goes away as part of the iterator operation.
            var input = new FasterKVHVI<TPKey>.Input(predOrdinal);
            input.SetQueryKey(this.bufferPool, this.keyAccessor, ref key);
            return input;
        }

        private IEnumerable<long> Query(AdvancedClientSession<TPKey, long, FasterKVHVI<TPKey>.Input, FasterKVHVI<TPKey>.Output, FasterKVHVI<TPKey>.Context, FasterKVHVI<TPKey>.Functions> session,
                FasterKVHVI<TPKey>.Input input, QuerySettings querySettings)
        {
            var context = new FasterKVHVI<TPKey>.Context { Functions = session.functions };
            RecordInfo recordInfo = default;
            try
            {
                do
                {
                    var output = new FasterKVHVI<TPKey>.Output();
                    Status status = session.IndexRead(this.secondaryFkv, ref input.QueryKeyRef, ref input, ref output, ref recordInfo, ref context);
                    if (querySettings.IsCanceled)
                        yield break;
                    if (status == Status.PENDING)
                    {
                        // Because we traverse the chain, we must wait for any pending read operations to complete.
                        session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                        if (completedOutputs.Next())
                            status = output.PendingResultStatus;
                        completedOutputs.Dispose();
                    }

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

        internal unsafe IAsyncEnumerable<long> QueryAsync(AdvancedClientSession<TPKey, long, FasterKVHVI<TPKey>.Input, FasterKVHVI<TPKey>.Output, FasterKVHVI<TPKey>.Context, FasterKVHVI<TPKey>.Functions> session,
                int predOrdinal, ref TPKey key, QuerySettings querySettings)
            => QueryAsync(session, MakeQueryInput(predOrdinal, ref key), querySettings);

        private async IAsyncEnumerable<long> QueryAsync(AdvancedClientSession<TPKey, long, FasterKVHVI<TPKey>.Input, FasterKVHVI<TPKey>.Output, FasterKVHVI<TPKey>.Context, FasterKVHVI<TPKey>.Functions> session,
                FasterKVHVI<TPKey>.Input input, QuerySettings querySettings)
        {
            var context = new FasterKVHVI<TPKey>.Context { Functions = session.functions };
            RecordInfo recordInfo = default;
            try
            {
                do
                {
                    // Because we traverse the chain, we must wait for any pending read operations to complete.
                    var readAsyncResult = await session.IndexReadAsync(this.secondaryFkv, ref input.QueryKeyRef, ref input, recordInfo.PreviousAddress, ref context, session.ctx.serialNum, querySettings);
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
