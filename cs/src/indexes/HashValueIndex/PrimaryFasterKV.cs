// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace FASTER.indexes.HashValueIndex
{
    public partial class HashValueIndex<TKVKey, TKVValue, TPKey> : ISecondaryValueIndex<TKVValue>
    {
        internal FasterKV<TKVKey, TKVValue> primaryFkv;

        internal struct PrimaryInput
        {
            internal AllocatorBase<TKVKey, TKVValue> hlog;
        }

        internal struct PrimaryOutput : IDisposable
        {
            private IHeapContainer<TKVKey> keyContainer;
            private IHeapContainer<TKVValue> valueContainer;
            internal long currentAddress;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void SetHeapContainers(IHeapContainer<TKVKey> kc, IHeapContainer<TKVValue> vc)
            {
                this.keyContainer = kc;
                this.valueContainer = vc;
            }

            internal ref TKVKey GetKey() => ref this.keyContainer.Get();

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void DetachHeapContainers(out IHeapContainer<TKVKey> kc, out IHeapContainer<TKVValue> vc)
            {
                kc = this.keyContainer;
                this.keyContainer = null;
                vc = this.valueContainer;
                this.valueContainer = null;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void Set(ref PrimaryOutput other)
            {
                this.Dispose();
                other.DetachHeapContainers(out this.keyContainer, out this.valueContainer);
                this.currentAddress = other.currentAddress;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Dispose()
            {
                if (this.keyContainer is { })
                {
                    this.keyContainer.Dispose();
                    this.keyContainer = null;
                }
                if (this.valueContainer is { })
                {
                    this.valueContainer.Dispose();
                    this.valueContainer = null;
                }
            }
        }

        internal class PrimaryFunctions : AdvancedFunctionsBase<TKVKey, TKVValue, PrimaryInput, PrimaryOutput, Empty>
        {
            public override void ConcurrentReader(ref TKVKey key, ref PrimaryInput input, ref TKVValue value, ref PrimaryOutput output, ref RecordInfo recordInfo, long address)
            {
                if (input.hlog is { })
                    output.SetHeapContainers(input.hlog.GetKeyContainer(ref key), input.hlog.GetValueContainer(ref value));
                else
                    // Only currentAddress is needed here; recordInfo is returned via the Read(..., out RecordInfo recordInfo, ...) parameter
                    // because this is only called on in-memory addresses, not on a Read that has gone pending.
                    output.currentAddress = address;
            }

            public override void SingleReader(ref TKVKey key, ref PrimaryInput input, ref TKVValue value, ref PrimaryOutput output, long address)
            {
                if (input.hlog is { })
                    output.SetHeapContainers(input.hlog.GetKeyContainer(ref key), input.hlog.GetValueContainer(ref value));
                else
                    // Only currentAddress is needed here; recordInfo is returned via the Read(..., out RecordInfo recordInfo, ...) parameter
                    // if this is an in-memory call, or via CompletePendingWithOutputs() if the Read has gone pending.
                    output.currentAddress = address;
            }
        }

        private bool ResolveRecord(AdvancedClientSession<TKVKey, TKVValue, PrimaryInput, PrimaryOutput, Empty, PrimaryFunctions> session, long recordId, out QueryRecord<TKVKey, TKVValue> record)
        {
            record = default;
            var output = new PrimaryOutput();

            try
            {
                // Look up logicalAddress in the primary FasterKV by address only, and returns the key and value. The key is needed for
                // the liveness loop, and if the record is live, we'll return the key and value this call obtains.
                var input = new PrimaryInput { hlog = this.primaryFkv.hlog };

                void GetPendingStatus(ref Status status)
                {
                    session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    if (completedOutputs.Next())
                    {
                        status = Status.OK; // TODO completedOutputs.Current.Status;
                        output.Set(ref completedOutputs.Current.Output);
                    }
                    completedOutputs.Dispose();
                }

                Status status = session.ReadAtAddress(recordId, ref input, ref output, ReadFlags.SkipReadCache);
                if (status == Status.PENDING)
                    GetPendingStatus(ref status);
                if (status != Status.OK)
                    return false;

                // Now prepare to confirm liveness: Look up the key and see if the address matches (it must be the highest non-readCache address for the key).
                // Setting input.hlog to null switches Concurrent/SingleReader mode from "get the key and value at this address" to "traverse the liveness chain".
                input.hlog = null;
                RecordInfo recordInfo = default;

                while (true)
                {
                    status = session.Read(ref output.GetKey(), ref input, ref output, ref recordInfo, ReadFlags.SkipReadCache);
                    if (status == Status.PENDING)
                        GetPendingStatus(ref status);

                    if (status != Status.OK || output.currentAddress != recordId)
                        return false;

                    output.DetachHeapContainers(out IHeapContainer<TKVKey> keyContainer, out IHeapContainer<TKVValue> valueContainer);
                    record = new QueryRecord<TKVKey, TKVValue>(keyContainer, valueContainer);
                    return true;
                }
            }
            finally
            {
                output.Dispose();
            }
        }

        private async ValueTask<QueryRecord<TKVKey, TKVValue>> ResolveRecordAsync(AdvancedClientSession<TKVKey, TKVValue, PrimaryInput, PrimaryOutput, Empty, PrimaryFunctions> session, long recordId, QuerySettings querySettings)
        {
            PrimaryOutput initialOutput = default;

            try
            {
                // Look up logicalAddress in the primary FasterKV by address only, and returns the key.
                var input = new PrimaryInput { hlog = this.primaryFkv.hlog };

                //  We ignore the updated previousAddress here; we're just looking for the key.
                Status status;
                var readAsyncResult = await session.ReadAtAddressAsync(recordId, ref input, ReadFlags.SkipReadCache, cancellationToken: querySettings.CancellationToken);
                if (querySettings.IsCanceled)
                    return null;
                (status, initialOutput) = readAsyncResult.Complete();
                if (status != Status.OK)
                    return default;

                // Now prepare to confirm liveness: Look up the key and see if the address matches (it must be the highest non-readCache address for the key).
                // Setting input.hlog to null switches Concurrent/SingleReader mode from "get the key and value at this address" to "traverse the liveness chain".
                input.hlog = null;
                RecordInfo recordInfo = default;

                while (true)
                {
                    readAsyncResult = await session.ReadAsync(ref initialOutput.GetKey(), ref input, recordInfo.PreviousAddress, ReadFlags.SkipReadCache, cancellationToken: querySettings.CancellationToken);
                    if (querySettings.IsCanceled)
                        return null;
                    PrimaryOutput output = default;
                    (status, output) = readAsyncResult.Complete(out recordInfo);

                    if (status != Status.OK || output.currentAddress != recordId)
                        return default;

                    initialOutput.DetachHeapContainers(out IHeapContainer<TKVKey> keyContainer, out IHeapContainer<TKVValue> valueContainer);
                    return new QueryRecord<TKVKey, TKVValue>(keyContainer, valueContainer);
                }
            }
            finally
            {
                initialOutput.Dispose();
            }
        }
    }
}
