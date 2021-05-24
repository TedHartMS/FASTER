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
        Guid latestLogCheckpointToken;
        PrimaryCheckpointInfo latestPrimaryCheckpointInfo;

        /// <inheritdoc/>
        public Guid GetLatestCheckpointToken() => this.latestLogCheckpointToken;

        /// <inheritdoc/>
        public void PrepareToCheckpoint() => this.checkpointManager.PrepareToCheckpoint(this.latestPrimaryCheckpointInfo);

        /// <inheritdoc/>
        public void OnPrimaryCheckpointCompleted(PrimaryCheckpointInfo primaryCheckpointInfo) => this.latestPrimaryCheckpointInfo = primaryCheckpointInfo;

        /// <summary>
        /// Initiate full checkpoint
        /// </summary>
        /// <param name="token">Checkpoint token</param>
        /// <returns>
        /// Whether we successfully initiated the checkpoint (initiation may
        /// fail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index). Use CompleteCheckpointAsync to wait completion.
        /// </returns>
        public bool TakeFullCheckpoint(out Guid token)
        {
            this.PrepareToCheckpoint();
            bool success = this.secondaryFkv.TakeFullCheckpoint(out token);
            if (success)
                this.latestLogCheckpointToken = token;
            return success;
        }

        /// <summary>
        /// Initiate full checkpoint
        /// </summary>
        /// <param name="token">Checkpoint token</param>
        /// <param name="checkpointType">Checkpoint type</param>
        /// <returns>
        /// Whether we successfully initiated the checkpoint (initiation may
        /// fail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index). Use CompleteCheckpointAsync to wait completion.
        /// </returns>
        public bool TakeFullCheckpoint(out Guid token, CheckpointType checkpointType)
        {
            this.PrepareToCheckpoint();
            bool success = this.secondaryFkv.TakeFullCheckpoint(out token, checkpointType);
            if (success)
                this.latestLogCheckpointToken = token;
            return success;
        }

        /// <summary>
        /// Take full (index + log) checkpoint
        /// </summary>
        /// <param name="checkpointType">Checkpoint type</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>
        /// (bool success, Guid token)
        /// success: Whether we successfully initiated the checkpoint (initiation may
        /// fail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index).
        /// token: Token for taken checkpoint
        /// Await task to complete checkpoint, if initiated successfully
        /// </returns>
        public async ValueTask<(bool success, Guid token)> TakeFullCheckpointAsync(CheckpointType checkpointType, CancellationToken cancellationToken = default)
        {
            this.PrepareToCheckpoint();
            var result = await this.secondaryFkv.TakeFullCheckpointAsync(checkpointType, cancellationToken);
            if (result.success)
                this.latestLogCheckpointToken = result.token;
            return result;
        }

        /// <summary>
        /// Initiate index-only checkpoint
        /// </summary>
        /// <param name="token">Checkpoint token</param>
        /// <returns>Whether we could initiate the checkpoint. Use CompleteCheckpointAsync to wait completion.</returns>
        public bool TakeIndexCheckpoint(out Guid token)
        {
            // We don't track index checkpoint tokens; we'll pick up the right index checkpoint as a normal part of checkpointing
            return this.secondaryFkv.TakeIndexCheckpoint(out token);
        }

        /// <summary>
        /// Take index-only checkpoint
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>
        /// (bool success, Guid token)
        /// success: Whether we successfully initiated the checkpoint (initiation may
        /// fail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index).
        /// token: Token for taken checkpoint
        /// Await task to complete checkpoint, if initiated successfully
        /// </returns>
        public ValueTask<(bool success, Guid token)> TakeIndexCheckpointAsync(CancellationToken cancellationToken = default)
        {
            // We don't track index checkpoint tokens; we'll pick up the right index checkpoint as a normal part of checkpointing
            return this.secondaryFkv.TakeIndexCheckpointAsync(cancellationToken);
        }

        /// <summary>
        /// Initiate log-only checkpoint
        /// </summary>
        /// <param name="token">Checkpoint token</param>
        /// <returns>Whether we could initiate the checkpoint. Use CompleteCheckpointAsync to wait completion.</returns>
        public bool TakeHybridLogCheckpoint(out Guid token)
        {
            this.PrepareToCheckpoint();
            bool success = this.secondaryFkv.TakeHybridLogCheckpoint(out token);
            if (success)
                this.latestLogCheckpointToken = token;
            return success;
        }

        /// <summary>
        /// Initiate log-only checkpoint
        /// </summary>
        /// <param name="token">Checkpoint token</param>
        /// <param name="checkpointType">Checkpoint type</param>
        /// <param name="tryIncremental">For snapshot, try to store as incremental delta over last snapshot</param>
        /// <returns>Whether we could initiate the checkpoint. Use CompleteCheckpointAsync to wait completion.</returns>
        public bool TakeHybridLogCheckpoint(out Guid token, CheckpointType checkpointType, bool tryIncremental = false)
        {
            this.PrepareToCheckpoint();
            bool success = this.secondaryFkv.TakeHybridLogCheckpoint(out token, checkpointType, tryIncremental);
            if (success)
                this.latestLogCheckpointToken = token;
            return success;
        }

        /// <summary>
        /// Take log-only checkpoint
        /// </summary>
        /// <param name="checkpointType">Checkpoint type</param>
        /// <param name="tryIncremental">For snapshot, try to store as incremental delta over last snapshot</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>
        /// (bool success, Guid token)
        /// success: Whether we successfully initiated the checkpoint (initiation may
        /// fail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index).
        /// token: Token for taken checkpoint
        /// Await task to complete checkpoint, if initiated successfully
        /// </returns>
        public async ValueTask<(bool success, Guid token)> TakeHybridLogCheckpointAsync(CheckpointType checkpointType, bool tryIncremental = false, CancellationToken cancellationToken = default)
        {
            this.PrepareToCheckpoint();
            var result = await this.secondaryFkv.TakeHybridLogCheckpointAsync(checkpointType, tryIncremental, cancellationToken);
            if (result.success)
                this.latestLogCheckpointToken = result.token;
            return result;
        }
    }
}
