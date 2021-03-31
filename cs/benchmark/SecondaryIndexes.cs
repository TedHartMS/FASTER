// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace FASTER.benchmark
{
    class NullKeyIndex<Key> : ISecondaryKeyIndex<Key>
    {
        public string Name => "KeyIndex";

        public bool IsMutable => true;

        public void SetSessionSlot(long slot) { }

        public void Delete(ref Key key, long recordId, SecondaryIndexSessionBroker indexSessionBroker) { }

        public void Insert(ref Key key, long recordId, SecondaryIndexSessionBroker indexSessionBroker) { }

        public void Upsert(ref Key key, long recordId, bool isMutableRecord, SecondaryIndexSessionBroker indexSessionBroker) { }
    }

    class NullValueIndex<Value> : ISecondaryValueIndex<Value>
    {
        public string Name => "ValueIndex";

        public bool IsMutable => true;

        public void SetSessionSlot(long slot) { }

        public void Delete(long recordId, SecondaryIndexSessionBroker indexSessionBroker) { }

        public void Insert(ref Value value, long recordId, SecondaryIndexSessionBroker indexSessionBroker) { }

        public void Upsert(ref Value value, long recordId, bool isMutableRecord, SecondaryIndexSessionBroker indexSessionBroker) { }
    }
}
