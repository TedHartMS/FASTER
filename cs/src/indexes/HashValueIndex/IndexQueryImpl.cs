// Copyright (c) Microsoft Corporation. All rights reserved.
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
        private unsafe SecondaryFasterKV<TPKey>.Input MakeQueryInput(int predOrdinal, ref TPKey key)
        {
            // Putting the query key in Input is necessary because iterator functions cannot contain unsafe code or have
            // byref args, and bufferPool is needed here because the stack goes away as part of the iterator operation.
            var input = new SecondaryFasterKV<TPKey>.Input(predOrdinal);
            input.SetQueryKey(this.bufferPool, this.keyAccessor, ref key);
            return input;
        }

        private IEnumerable<RecordId> Query(AdvancedClientSession<TPKey, RecordId, SecondaryFasterKV<TPKey>.Input, SecondaryFasterKV<TPKey>.Output, SecondaryFasterKV<TPKey>.Context, SecondaryFasterKV<TPKey>.Functions> session,
                SecondaryFasterKV<TPKey>.Input input, QuerySettings querySettings)
        {
            var context = new SecondaryFasterKV<TPKey>.Context { Functions = session.functions };
            RecordInfo recordInfo = default;
            try
            {
                do
                {
                    var output = new SecondaryFasterKV<TPKey>.Output();
                    Status status = session.IndexRead(this.secondaryFkv, ref input.QueryKeyRef, ref input, ref output, ref recordInfo, ref context);
                    if (querySettings.IsCanceled)
                        yield break;
                    if (status == Status.PENDING)
                    {
                        // Because we traverse the chain, we must wait for any pending read operations to complete.
                        session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                        if (completedOutputs.Next())
                        {
                            // The local function avoids CS8176: Iterators cannot have by-reference locals
                            void setOutput()
                            {
                                ref var completedOutput = ref completedOutputs.Current;
                                status = completedOutput.Status;
                                output = completedOutput.Output;
                            }
                            setOutput();
                        }
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

        internal unsafe IAsyncEnumerable<RecordId> QueryAsync(AdvancedClientSession<TPKey, RecordId, SecondaryFasterKV<TPKey>.Input, SecondaryFasterKV<TPKey>.Output, SecondaryFasterKV<TPKey>.Context, SecondaryFasterKV<TPKey>.Functions> session,
                int predOrdinal, ref TPKey key, QuerySettings querySettings)
            => QueryAsync(session, MakeQueryInput(predOrdinal, ref key), querySettings);

        private async IAsyncEnumerable<RecordId> QueryAsync(AdvancedClientSession<TPKey, RecordId, SecondaryFasterKV<TPKey>.Input, SecondaryFasterKV<TPKey>.Output, SecondaryFasterKV<TPKey>.Context, SecondaryFasterKV<TPKey>.Functions> session,
                SecondaryFasterKV<TPKey>.Input input, QuerySettings querySettings)
        {
            var context = new SecondaryFasterKV<TPKey>.Context { Functions = session.functions };
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
