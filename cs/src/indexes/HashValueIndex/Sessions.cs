// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace FASTER.indexes.HashValueIndex
{
    public partial class HashValueIndex<TKVKey, TKVValue, TPKey> : ISecondaryValueIndex<TKVKey, TKVValue>
    {
        class Sessions
        {
            private readonly FasterKV<TKVKey, TKVValue> primaryFkv;
            private readonly SecondaryFasterKV<TPKey> secondaryFkv;
            private readonly KeyAccessor<TPKey> keyAccessor;

            private AdvancedClientSession<TKVKey, TKVValue, PrimaryInput, PrimaryOutput, Empty, PrimaryFunctions> primarySession;
            private AdvancedClientSession<TPKey, RecordId, SecondaryFasterKV<TPKey>.Input, SecondaryFasterKV<TPKey>.Output, SecondaryFasterKV<TPKey>.Context, SecondaryFasterKV<TPKey>.Functions> secondarySession;

            internal Sessions (SecondaryIndexSessionBroker sessionBroker, long slot, FasterKV<TKVKey, TKVValue> primaryFkv, SecondaryFasterKV<TPKey> secondaryFkv, KeyAccessor<TPKey> keyAccessor)
            {
                this.primaryFkv = primaryFkv;
                this.secondaryFkv = secondaryFkv;
                this.keyAccessor = keyAccessor;
                sessionBroker.SetSessionObject(slot, this);
            }

            internal AdvancedClientSession<TKVKey, TKVValue, PrimaryInput, PrimaryOutput, Empty, PrimaryFunctions> PrimarySession
                => this.primarySession ??= this.primaryFkv.For(new PrimaryFunctions()).NewSession<PrimaryFunctions>(threadAffinitized: false);

            internal AdvancedClientSession<TPKey, RecordId, SecondaryFasterKV<TPKey>.Input, SecondaryFasterKV<TPKey>.Output, SecondaryFasterKV<TPKey>.Context, SecondaryFasterKV<TPKey>.Functions> SecondarySession
                => this.secondarySession ??= this.secondaryFkv.NewSession(this.keyAccessor);
        }
    }
}
