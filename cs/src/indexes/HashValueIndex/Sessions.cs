// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace FASTER.indexes.HashValueIndex
{
    public partial class HashValueIndex<TKVKey, TKVValue, TPKey> : ISecondaryValueIndex<TKVValue>
    {
        class Sessions
        {
            internal AdvancedClientSession<TKVKey, TKVValue, PrimaryInput, PrimaryOutput, Empty, PrimaryFunctions> PrimarySession;
            internal AdvancedClientSession<TPKey, long, FasterKVHVI<TPKey>.Input, FasterKVHVI<TPKey>.Output, FasterKVHVI<TPKey>.Context, FasterKVHVI<TPKey>.Functions> SecondarySession;

            internal static Sessions CreateNew(SecondaryIndexSessionBroker sessionBroker, long slot, FasterKV<TKVKey, TKVValue> primaryFkv, FasterKVHVI<TPKey> secondaryFkv, KeyAccessor<TPKey> keyAccessor)
            {
                var sessions = new Sessions
                {
                    PrimarySession = primaryFkv.For(new PrimaryFunctions()).NewSession<PrimaryFunctions>(threadAffinitized: false),
                    SecondarySession = secondaryFkv.NewSession(keyAccessor)
                };
                sessionBroker.SetSessionObject(slot, sessions);
                return sessions;
            }
        }
    }
}
