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
        /// <inheritdoc/>
        public void OnPrimaryCheckpointInitiated(PrimaryCheckpointInfo currentCheckpointInfo) => this.checkpointManager.lastStartedPrimaryCheckpointInfo = currentCheckpointInfo;

        /// <inheritdoc/>
        public void OnPrimaryCheckpointCompleted(PrimaryCheckpointInfo completedCheckpointInfo) => this.checkpointManager.lastCompletedPrimaryCheckpointInfo = completedCheckpointInfo;

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
            => this.secondaryFkv.TakeFullCheckpoint(out token);

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
            => this.secondaryFkv.TakeFullCheckpoint(out token, checkpointType);

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
        public ValueTask<(bool success, Guid token)> TakeFullCheckpointAsync(CheckpointType checkpointType, CancellationToken cancellationToken = default) 
            => this.secondaryFkv.TakeFullCheckpointAsync(checkpointType, cancellationToken);

        /// <summary>
        /// Initiate index-only checkpoint
        /// </summary>
        /// <param name="token">Checkpoint token</param>
        /// <returns>Whether we could initiate the checkpoint. Use CompleteCheckpointAsync to wait completion.</returns>
        public bool TakeIndexCheckpoint(out Guid token) => this.secondaryFkv.TakeIndexCheckpoint(out token);

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
            => this.secondaryFkv.TakeIndexCheckpointAsync(cancellationToken);

        /// <summary>
        /// Initiate log-only checkpoint
        /// </summary>
        /// <param name="token">Checkpoint token</param>
        /// <returns>Whether we could initiate the checkpoint. Use CompleteCheckpointAsync to wait completion.</returns>
        public bool TakeHybridLogCheckpoint(out Guid token) 
            => this.secondaryFkv.TakeHybridLogCheckpoint(out token);

        /// <summary>
        /// Initiate log-only checkpoint
        /// </summary>
        /// <param name="token">Checkpoint token</param>
        /// <param name="checkpointType">Checkpoint type</param>
        /// <param name="tryIncremental">For snapshot, try to store as incremental delta over last snapshot</param>
        /// <returns>Whether we could initiate the checkpoint. Use CompleteCheckpointAsync to wait completion.</returns>
        public bool TakeHybridLogCheckpoint(out Guid token, CheckpointType checkpointType, bool tryIncremental = false) 
            => this.secondaryFkv.TakeHybridLogCheckpoint(out token, checkpointType, tryIncremental);

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
        public ValueTask<(bool success, Guid token)> TakeHybridLogCheckpointAsync(CheckpointType checkpointType, bool tryIncremental = false, CancellationToken cancellationToken = default) 
            => this.secondaryFkv.TakeHybridLogCheckpointAsync(checkpointType, tryIncremental, cancellationToken);
    }
}
