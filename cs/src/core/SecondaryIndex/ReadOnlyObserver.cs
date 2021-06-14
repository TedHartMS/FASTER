// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.core
{
    class ReadOnlyObserver<TKVKey, TKVValue> : IObserver<IFasterScanIterator<TKVKey, TKVValue>>
    {
        readonly SecondaryIndexBroker<TKVKey, TKVValue> secondaryIndexBroker;

        internal ReadOnlyObserver(SecondaryIndexBroker<TKVKey, TKVValue> sib) => this.secondaryIndexBroker = sib;

        public void OnCompleted()
        {
            // Called when AllocatorBase is Disposed
        }

        public void OnError(Exception error)
        {
            // Apparently not called by FASTER
        }

        public void OnNext(IFasterScanIterator<TKVKey, TKVValue> iter)
        {
            // We're not operating in the context of a FasterKV session, so we need our own sessionBroker.
            using var indexSessionBroker = new SecondaryIndexSessionBroker();
            secondaryIndexBroker.ScanReadOnlyPages(iter, indexSessionBroker);
        }
    }
}
