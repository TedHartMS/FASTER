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

        public void Delete(ref Key key, RecordId recordId, SecondaryIndexSessionBroker indexSessionBroker) { }

        public void Insert(ref Key key, RecordId recordId, SecondaryIndexSessionBroker indexSessionBroker) { }

        public void Upsert(ref Key key, RecordId recordId, bool isMutableRecord, SecondaryIndexSessionBroker indexSessionBroker) { }

        public void OnPrimaryCheckpoint(int version, long flushedUntilAddress) { }

        public void OnPrimaryRecover(int version, long flushedUntilAddress, out int recoveredToVersion, out long recoveredToAddress)
        {
            recoveredToVersion = default;
            recoveredToAddress = default;
        }

        public void OnPrimaryTruncate(long newBeginAddress) { }
    }

    class NullValueIndex<Key, Value> : ISecondaryValueIndex<Key, Value>
    {
        public string Name => "ValueIndex";

        public bool IsMutable => true;

        public void SetSessionSlot(long slot) { }

        public void Delete(ref Key key, RecordId recordId, SecondaryIndexSessionBroker indexSessionBroker) { }

        public void Insert(ref Key key, ref Value value, RecordId recordId, SecondaryIndexSessionBroker indexSessionBroker) { }

        public void Upsert(ref Key key, ref Value value, RecordId recordId, bool isMutableRecord, SecondaryIndexSessionBroker indexSessionBroker) { }

        public void OnPrimaryCheckpoint(int version, long flushedUntilAddress) { }

        public void OnPrimaryRecover(int version, long flushedUntilAddress, out int recoveredToVersion, out long recoveredToAddress)
        {
            recoveredToVersion = default;
            recoveredToAddress = default;
        }

        public void OnPrimaryTruncate(long newBeginAddress) { }
    }
}
