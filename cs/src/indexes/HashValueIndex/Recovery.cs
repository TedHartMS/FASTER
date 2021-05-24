// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.indexes.HashValueIndex
{
    public partial class HashValueIndex<TKVKey, TKVValue, TPKey> : ISecondaryValueIndex<TKVKey, TKVValue>
    {
        void PrepareToRecover() => this.checkpointManager.PrepareToRecover();

        /// <inheritdoc/>
        public PrimaryCheckpointInfo BeginRecover(Guid secondaryLogToken)
        {
            if (!GetCompatibleIndexToken(secondaryLogToken, out var indexToken))
                return default;
            PrepareToRecover();
            this.latestLogCheckpointToken = secondaryLogToken;
            this.secondaryFkv.Recover(indexToken, secondaryLogToken, undoNextVersion: false);  // We will do this differently due to the Fishstore-like chains.
            this.latestPrimaryCheckpointInfo = this.checkpointManager.primaryCheckpointInfo;
            return this.latestPrimaryCheckpointInfo;
        }

        /// <inheritdoc/>
        public async Task<PrimaryCheckpointInfo> BeginRecoverAsync(Guid secondaryLogToken, CancellationToken cancellationToken = default)
        {
            if (!GetCompatibleIndexToken(secondaryLogToken, out var indexToken))
                return default;
            PrepareToRecover();
            this.latestLogCheckpointToken = secondaryLogToken;
            await this.secondaryFkv.RecoverAsync(indexToken, secondaryLogToken, undoNextVersion: false, cancellationToken: cancellationToken);
            this.latestPrimaryCheckpointInfo = this.checkpointManager.primaryCheckpointInfo;
            return this.latestPrimaryCheckpointInfo;
        }

        /// <inheritdoc/>
        public void EndRecover();

        private bool GetCompatibleIndexToken(Guid logToken, out Guid indexToken)
        {
            indexToken = default;
            if (this.checkpointManager is null)
                return false;
            var recoveredHLCInfo = new HybridLogCheckpointInfo();
            try
            {
                recoveredHLCInfo.Recover(logToken, this.checkpointManager, this.secondaryFkv.hlog.LogPageSizeBits);
            }
            catch
            {
                return false;
            }

            // We return true even if the index token is default (compatible index token could not be found), in which case we restore the index from the log.
            var recoveredICInfo = this.secondaryFkv.GetCompatibleIndexCheckpointInfo(recoveredHLCInfo);
            indexToken = recoveredICInfo.info.token;
            return true;
        }
    }
}
