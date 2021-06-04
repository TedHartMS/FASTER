// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.indexes.HashValueIndex
{
    public partial class HashValueIndex<TKVKey, TKVValue, TPKey> : ISecondaryValueIndex<TKVKey, TKVValue>
    {
        internal PrimaryCheckpointInfo primaryRecoveredPci;
        internal long recoveredTailAddress;
        internal RecordId recoveredHighWaterRecordId;

        /// <inheritdoc/>
        public PrimaryCheckpointInfo Recover(PrimaryCheckpointInfo primaryRecoveredPci, bool undoNextVersion)
        {
            if (!this.BeginRecovery(primaryRecoveredPci, out var indexToken, out var logToken, out PrimaryCheckpointInfo lastCompletedPci, out PrimaryCheckpointInfo lastStartedPci))
                return default;
            if (logToken != Guid.Empty)
                this.secondaryFkv.Recover(indexToken, logToken, undoNextVersion:undoNextVersion);
            return this.EndRecovery(lastCompletedPci, lastStartedPci);
        }

        /// <inheritdoc/>
        public async Task<PrimaryCheckpointInfo> RecoverAsync(PrimaryCheckpointInfo primaryRecoveredPci, bool undoNextVersion, CancellationToken cancellationToken = default)
        {
            if (!this.BeginRecovery(primaryRecoveredPci, out var indexToken, out var logToken, out PrimaryCheckpointInfo lastCompletedPci, out PrimaryCheckpointInfo lastStartedPci))
                return default;
            if (logToken != Guid.Empty)
                await this.secondaryFkv.RecoverAsync(indexToken, logToken, undoNextVersion: undoNextVersion, cancellationToken: cancellationToken);
            return this.EndRecovery(lastCompletedPci, lastStartedPci);
        }

        private bool BeginRecovery(PrimaryCheckpointInfo primaryRecoveredPci, out Guid indexToken, out Guid logToken, out PrimaryCheckpointInfo lastCompletedPci, out PrimaryCheckpointInfo lastStartedPci)
        {
            this.primaryRecoveredPci = primaryRecoveredPci;
            highWaterRecordId = default;
            return this.checkpointManager.GetRecoveryTokens(primaryRecoveredPci, out indexToken, out logToken, out lastCompletedPci, out lastStartedPci);
        }

        PrimaryCheckpointInfo EndRecovery(PrimaryCheckpointInfo lastCompletedPci, PrimaryCheckpointInfo lastStartedPci)
        {
            this.checkpointManager.SetPCIs(lastCompletedPci, lastStartedPci);
            if (highWaterRecordId.IsDefault())
            {
                RecoverHighWaterRecordId();
            }
            recoveredTailAddress = this.secondaryFkv.Log.TailAddress;
            return this.checkpointManager.secondaryMetadata.lastCompletedPrimaryCheckpointInfo;
        }

        /// <inheritdoc/>
        public void RecoveryReplay(IFasterScanIterator<TKVKey, TKVValue> iter, SecondaryIndexSessionBroker indexSessionBroker)
        {
            // On Replay, we have already done undoNextVersion (if specified) in Primary's recovery path, so there will only be valid records to replay.
            // And because we're in Recovery, we don't need to worry about ReadOnlyAddress changing.
            while (iter.GetNext(out var recordInfo, out TKVKey key, out TKVValue value))
            {
                Debug.Assert(iter.CurrentAddress < this.primaryFkv.Log.ReadOnlyAddress);
                if (iter.CurrentAddress > highWaterRecordId.Address)
                    this.Upsert(ref key, ref value, new RecordId(recordInfo, iter.CurrentAddress), isMutable: false, indexSessionBroker);
            }
        }

        private void RecoverHighWaterRecordId()
        {
            var page = this.secondaryFkv.hlog.GetPage(this.secondaryFkv.Log.TailAddress);
            var startLogicalAddress = this.secondaryFkv.hlog.GetStartLogicalAddress(page);
            var endLogicalAddress = this.secondaryFkv.hlog.GetStartLogicalAddress(page + 1);
            using var iter = this.secondaryFkv.hlog.Scan(startLogicalAddress, endLogicalAddress);
            while (iter.GetNext(out _))
                highWaterRecordId = iter.GetValue();
            recoveredHighWaterRecordId = highWaterRecordId;
        }
    }
}
