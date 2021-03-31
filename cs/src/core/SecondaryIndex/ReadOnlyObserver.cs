// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.core
{
    class ReadOnlyObserver<TKVKey, TKVValue> : IObserver<IFasterScanIterator<TKVKey, TKVValue>>
    {
        readonly SecondaryIndexBroker<TKVKey, TKVValue> secondaryIndexBroker;
        readonly SecondaryIndexSessionBroker indexSessionBroker = new SecondaryIndexSessionBroker();

        internal ReadOnlyObserver(SecondaryIndexBroker<TKVKey, TKVValue> sib) => this.secondaryIndexBroker = sib;

        public void OnCompleted()
        {
            // Called when AllocatorBase is Disposed; nothing to do here.
        }

        public void OnError(Exception error)
        {
            // Apparently not called by FASTER
        }

        public void OnNext(IFasterScanIterator<TKVKey, TKVValue> iter)
        {
            while (iter.GetNext(out _, out TKVKey key, out TKVValue value))
            {
                secondaryIndexBroker.UpsertReadOnly(ref key, ref value, iter.CurrentAddress, indexSessionBroker);
            }
        }
    }
}
