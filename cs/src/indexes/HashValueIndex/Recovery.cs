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
            HighWaterRecordId = default;
            return this.checkpointManager.GetRecoveryTokens(primaryRecoveredPci, out indexToken, out logToken, out lastCompletedPci, out lastStartedPci);
        }

        PrimaryCheckpointInfo EndRecovery(PrimaryCheckpointInfo lastCompletedPci, PrimaryCheckpointInfo lastStartedPci)
        {
            this.checkpointManager.SetPCIs(lastCompletedPci, lastStartedPci);

            // There are 3 ways we must ensure consistency for the correct final value of HighWaterRecordId *and* readOnlyQueue.nextKey:
            //  1. RecoverFromPage(), which has completed by the time we get here.
            //  2. RecoverHighWaterRecordIdFromLastPage() if RecoverFromPage() did not set it.
            if (HighWaterRecordId.IsDefault())
                RecoverHighWaterRecordIdFromLastPage();

            //  3. RecoveryReplay() will be called if the stored LastCompletedPCI is < primaryFkv.ReadOnlyAddress. In case it's not, set readOnlyQueue.nextKey here.
            readOnlyQueue.nextKey = this.primaryFkv.Log.ReadOnlyAddress;
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
                if (iter.CurrentAddress > HighWaterRecordId.Address)
                    this.Upsert(ref key, ref value, new RecordId(recordInfo, iter.CurrentAddress), isMutable: false, indexSessionBroker);
            }

            // We set readonlyQueue.nextKey to primaryFkv.ReadOnlyAddress in EndRecovery, and if RecoveryReplay is called it should end at primaryFkv.ReadOnlyAddress also.
            Debug.Assert(this.readOnlyQueue.nextKey == iter.EndAddress);
        }

        private void RecoverHighWaterRecordIdFromLastPage()
        {
            var page = this.secondaryFkv.hlog.GetPage(this.secondaryFkv.Log.TailAddress);
            var startLogicalAddress = this.secondaryFkv.hlog.GetStartLogicalAddress(page);
            var endLogicalAddress = this.secondaryFkv.hlog.GetStartLogicalAddress(page + 1);
            using var iter = this.secondaryFkv.hlog.Scan(startLogicalAddress, endLogicalAddress);
            while (iter.GetNext(out _))
                HighWaterRecordId = iter.GetValue();
            recoveredHighWaterRecordId = HighWaterRecordId;
        }
    }
}
