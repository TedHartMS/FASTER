// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Threading;
using System.Threading.Tasks;

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

        public void OnPrimaryTruncate(long newBeginAddress) { }

        public void ScanReadOnlyPages<TScanValue>(IFasterScanIterator<Key, TScanValue> iter, SecondaryIndexSessionBroker indexSessionBroker) { }

        public void OnPrimaryCheckpointInitiated(PrimaryCheckpointInfo recoveredPci) { }

        public void OnPrimaryCheckpointCompleted(PrimaryCheckpointInfo primaryCheckpointInfo) { }

        public PrimaryCheckpointInfo Recover(PrimaryCheckpointInfo recoveredPci, bool undoNextVersion) => default;

        public Task<PrimaryCheckpointInfo> RecoverAsync(PrimaryCheckpointInfo recoveredPci, bool undoNextVersion, CancellationToken cancellationToken = default) => default;

        public void RecoveryReplay<TScanValue>(IFasterScanIterator<Key, TScanValue> iter, SecondaryIndexSessionBroker indexSessionBroker) { }
    }

    class NullValueIndex<Key, Value> : ISecondaryValueIndex<Key, Value>
    {
        public string Name => "ValueIndex";

        public bool IsMutable => true;

        public void SetSessionSlot(long slot) { }

        public void Delete(ref Key key, RecordId recordId, SecondaryIndexSessionBroker indexSessionBroker) { }

        public void Insert(ref Key key, ref Value value, RecordId recordId, SecondaryIndexSessionBroker indexSessionBroker) { }

        public void Upsert(ref Key key, ref Value value, RecordId recordId, bool isMutableRecord, SecondaryIndexSessionBroker indexSessionBroker) { }

        public void OnPrimaryTruncate(long newBeginAddress) { }

        public void ScanReadOnlyPages(IFasterScanIterator<Key, Value> iter, SecondaryIndexSessionBroker indexSessionBroker) { }

        public void OnPrimaryCheckpointInitiated(PrimaryCheckpointInfo recoveredPci) { }

        public void OnPrimaryCheckpointCompleted(PrimaryCheckpointInfo primaryCheckpointInfo) { }

        public PrimaryCheckpointInfo Recover(PrimaryCheckpointInfo recoveredPci, bool undoNextVersion) => default;

        public Task<PrimaryCheckpointInfo> RecoverAsync(PrimaryCheckpointInfo recoveredPci, bool undoNextVersion, CancellationToken cancellationToken = default) => default; // Not used for this class

        public void RecoveryReplay(IFasterScanIterator<Key, Value> iter, SecondaryIndexSessionBroker indexSessionBroker) { }
    }
}
