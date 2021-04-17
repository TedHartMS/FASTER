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

        public void Delete(ref Key key, SecondaryIndexSessionBroker indexSessionBroker) { }

        public void Insert(ref Key key, SecondaryIndexSessionBroker indexSessionBroker) { }

        public void Upsert(ref Key key, bool isMutableRecord, SecondaryIndexSessionBroker indexSessionBroker) { }
    }

    class NullValueIndex<Value> : ISecondaryValueIndex<Value>
    {
        public string Name => "ValueIndex";

        public bool IsMutable => true;

        public void SetSessionSlot(long slot) { }

        public void Delete(RecordId recordId, SecondaryIndexSessionBroker indexSessionBroker) { }

        public void Insert(ref Value value, RecordId recordId, SecondaryIndexSessionBroker indexSessionBroker) { }

        public void Upsert(ref Value value, RecordId recordId, bool isMutableRecord, SecondaryIndexSessionBroker indexSessionBroker) { }
    }
}
