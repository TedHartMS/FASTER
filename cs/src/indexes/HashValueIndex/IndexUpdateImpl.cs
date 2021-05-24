// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Runtime.CompilerServices;

namespace FASTER.indexes.HashValueIndex
{
    public partial class HashValueIndex<TKVKey, TKVValue, TPKey> : ISecondaryValueIndex<TKVKey, TKVValue>
    {
        private SectorAlignedBufferPool bufferPool;
        private WorkQueueOrdered<long, OrderedRange> readOnlyQueue;

        private readonly int keyPointerSize = Utility.GetSize(default(KeyPointer<TPKey>));
        private readonly int recordIdSize = Utility.GetSize(default(RecordId));

        private unsafe Status ExecuteAndStore(AdvancedClientSession<TPKey, RecordId, SecondaryFasterKV<TPKey>.Input, SecondaryFasterKV<TPKey>.Output, SecondaryFasterKV<TPKey>.Context, SecondaryFasterKV<TPKey>.Functions> session,
                ref TKVValue kvValue, RecordId recordId)
        {
            // Note: stackalloc is safe because it's copied to a HeapContainer in PendingContext if the operation goes pending.
            var keyMemLen = this.keyPointerSize * this.PredicateCount;
            var keyBytes = stackalloc byte[keyMemLen];
            var anyMatch = false;

            // Keep a local copy in case the list is updated.
            var localPreds = this.predicates;

            for (var ii = 0; ii < this.PredicateCount; ++ii)
            {
                ref KeyPointer<TPKey> keyPointer = ref Unsafe.AsRef<KeyPointer<TPKey>>(keyBytes + ii * this.keyPointerSize);
                keyPointer.PreviousAddress = core.Constants.kInvalidAddress;
                keyPointer.PredicateOrdinal = (byte)ii;

                var pKey = localPreds[ii].Execute(ref kvValue);
                keyPointer.IsNull = userKeyComparer.Equals(ref pKey, ref this.RegistrationSettings.NullIndicator);
                if (!keyPointer.IsNull)
                {
                    keyPointer.Key = pKey;  // TODO: handle the non-blittable cases here
                    anyMatch = true;
                }
            }

            if (!anyMatch)
                return Status.OK;

            ref CompositeKey<TPKey> compositeKey = ref Unsafe.AsRef<CompositeKey<TPKey>>(keyBytes);
            var input = new SecondaryFasterKV<TPKey>.Input();   // TODO remove this if we don't need Input.IsDelete()
            var context = new SecondaryFasterKV<TPKey>.Context { Functions = session.functions };
            return session.IndexInsert(this.secondaryFkv, ref compositeKey.CastToFirstKeyPointerRefAsKeyRef(), recordId, ref input, context);
        }

        /// <inheritdoc/>
        public void ScanReadOnlyPages(IFasterScanIterator<TKVKey, TKVValue> iter, SecondaryIndexSessionBroker indexSessionBroker)
        {
            if (!readOnlyQueue.TryStartWork(iter.BeginAddress, () => new OrderedRange(iter.BeginAddress, iter.EndAddress, createEvent: true), out var waitingRange))
            {
                waitingRange.Wait();
                waitingRange.Dispose();
            }

            while (iter.GetNext(out var recordInfo, out TKVKey key, out TKVValue value))
                this.Upsert(ref key, ref value, new RecordId(recordInfo, iter.CurrentAddress), isMutable: false, indexSessionBroker);

            readOnlyQueue.CompleteWork(iter.BeginAddress, iter.EndAddress);
        }
    }
}
