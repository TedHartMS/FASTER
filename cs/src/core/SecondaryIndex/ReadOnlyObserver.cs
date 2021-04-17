// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.core
{
    class ReadOnlyObserver<TKVKey, TKVValue> : IObserver<IFasterScanIterator<TKVKey, TKVValue>>
    {
        readonly SecondaryIndexBroker<TKVKey, TKVValue> secondaryIndexBroker;

        // We're not operating in the context of a FasterKV session, so we need our own sessionBroker.
        readonly SecondaryIndexSessionBroker indexSessionBroker = new SecondaryIndexSessionBroker();

        internal ReadOnlyObserver(SecondaryIndexBroker<TKVKey, TKVValue> sib) => this.secondaryIndexBroker = sib;

        public void OnCompleted()
        {
            // Called when AllocatorBase is Disposed
            indexSessionBroker.Dispose();
        }

        public void OnError(Exception error)
        {
            // Apparently not called by FASTER
        }

        public void OnNext(IFasterScanIterator<TKVKey, TKVValue> iter)
        {
            while (iter.GetNext(out var recordInfo, out TKVKey key, out TKVValue value))
            {
                secondaryIndexBroker.UpsertReadOnly(ref key, ref value, new RecordId(iter.CurrentAddress, recordInfo), indexSessionBroker);
            }
        }
    }
}
