// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.indexes.HashValueIndex
{
    public partial class HashValueIndex<TKVKey, TKVValue, TPKey> : ISecondaryValueIndex<TKVKey, TKVValue>
    {
        /// <inheritdoc/>
        public PrimaryCheckpointInfo Recover(PrimaryCheckpointInfo recoveredPci, bool undoNextVersion)
        {
            if (!this.checkpointManager.GetRecoveryTokens(recoveredPci, out var indexToken, out var logToken, out PrimaryCheckpointInfo lastCompletedPci, out PrimaryCheckpointInfo lastStartedPci))
                return default;
            this.secondaryFkv.Recover(indexToken, logToken, undoNextVersion:undoNextVersion);
            this.checkpointManager.SetPCIs(lastCompletedPci, lastStartedPci);
            return this.checkpointManager.lastCompletedPrimaryCheckpointInfo;
        }

        /// <inheritdoc/>
        public async Task<PrimaryCheckpointInfo> RecoverAsync(PrimaryCheckpointInfo recoveredPci, bool undoNextVersion, CancellationToken cancellationToken = default)
        {
            if (!this.checkpointManager.GetRecoveryTokens(recoveredPci, out var indexToken, out var logToken, out PrimaryCheckpointInfo lastCompletedPci, out PrimaryCheckpointInfo lastStartedPci))
                return default;
            await this.secondaryFkv.RecoverAsync(indexToken, logToken, undoNextVersion: undoNextVersion, cancellationToken: cancellationToken);
            this.checkpointManager.SetPCIs(lastCompletedPci, lastStartedPci);
            return this.checkpointManager.lastCompletedPrimaryCheckpointInfo;
        }

        /// <inheritdoc/>
        public void RecoveryReplay(IFasterScanIterator<TKVKey, TKVValue> iter, SecondaryIndexSessionBroker indexSessionBroker)
        {
            // On Replay, we have already done undoNextVersion (if specified) in Primary's recovery path, so there will only be valid records to replay.
            // And because we're in Recovery, we don't need to worry about ReadOnlyAddress changing.
            while (iter.GetNext(out var recordInfo, out TKVKey key, out TKVValue value))
            {
                Debug.Assert(iter.CurrentAddress < this.primaryFkv.Log.ReadOnlyAddress);
                this.Upsert(ref key, ref value, new RecordId(recordInfo, iter.CurrentAddress), isMutable: false, indexSessionBroker);
            }
        }
    }
}
