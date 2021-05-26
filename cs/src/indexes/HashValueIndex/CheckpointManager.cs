// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Generic;

namespace FASTER.indexes.HashValueIndex
{
    public partial class HashValueIndex<TKVKey, TKVValue, TPKey> : ISecondaryValueIndex<TKVKey, TKVValue>
    {
        internal class CheckpointManager : ICheckpointManager
        {
            private readonly string indexName;
            internal SecondaryFasterKV<TPKey> secondaryFkv;
            private readonly ICheckpointManager userCheckpointManager;

            internal PrimaryCheckpointInfo lastCompletedPrimaryCheckpointInfo;      // The latest previously-completed PCI
            internal PrimaryCheckpointInfo lastStartedPrimaryCheckpointInfo;        // The latest previously-started PCI

            internal CheckpointManager(string indexName, ICheckpointManager userCheckpointManager)
            {
                this.indexName = indexName;
                this.userCheckpointManager = userCheckpointManager;
            }

            internal void SetSecondaryFkv(SecondaryFasterKV<TPKey> secondaryFkv) => this.secondaryFkv = secondaryFkv;

            internal void PrepareToCheckpoint(PrimaryCheckpointInfo latestPci) => this.lastCompletedPrimaryCheckpointInfo = latestPci;

            internal void PrepareToRecover() => this.lastCompletedPrimaryCheckpointInfo = default;

            private static (PrimaryCheckpointInfo completedPci, PrimaryCheckpointInfo startedPCI) GetPCIs(byte[] metadata) 
                => (new PrimaryCheckpointInfo(metadata.Slice(0, PrimaryCheckpointInfo.SerializedSize)),
                    new PrimaryCheckpointInfo(metadata.Slice(PrimaryCheckpointInfo.SerializedSize, PrimaryCheckpointInfo.SerializedSize)));

            internal void SetPCIs(PrimaryCheckpointInfo completedPci, PrimaryCheckpointInfo startedPCI)
            {
                this.lastCompletedPrimaryCheckpointInfo = completedPci;
                this.lastStartedPrimaryCheckpointInfo = startedPCI;
            }

            private byte[] DetachFrom(byte[] metadata) 
                => metadata.Slice(PrimaryCheckpointInfo.SerializedSize, metadata.Length - PrimaryCheckpointInfo.SerializedSize);

            private byte[] PrependToLogMetadata(byte[] metadata)
            {
                var result = new byte[metadata.Length + PrimaryCheckpointInfo.SerializedSize];
                Array.Copy(this.lastCompletedPrimaryCheckpointInfo.ToByteArray(), 0, result, PrimaryCheckpointInfo.SerializedSize, metadata.Length);
                return result;
            }

            internal bool GetRecoveryTokens(PrimaryCheckpointInfo recoveredPci, out Guid indexToken, out Guid logToken, out PrimaryCheckpointInfo lastCompletedPci, out PrimaryCheckpointInfo lastStartedPci)
            {
                logToken = default;
                var recoveredHLCInfo = new HybridLogCheckpointInfo();
                PrimaryCheckpointInfo completedPci = default, currentPci = default;
                foreach (var token in this.userCheckpointManager.GetLogCheckpointTokens())
                {
                    try
                    {
                        // Find the first secondary log checkpoint with currentPci < recoveredPci, or == if the checkpoint's currentPci is the same as its startedPci.
                        var metadata = this.userCheckpointManager.GetLogCheckpointMetadata(token, deltaLog: default);
                        (completedPci, currentPci) = GetPCIs(metadata);
                        if ((completedPci.CompareTo(recoveredPci) == 0 && currentPci.CompareTo(recoveredPci) == 0)
                            || currentPci.CompareTo(recoveredPci) < 0)
                        {
                            logToken = token;
                            recoveredHLCInfo.Recover(logToken, this, this.secondaryFkv.hlog.LogPageSizeBits);
                            break;
                        }
                    }
                    catch
                    {
                        continue;
                    }
                }

                if (logToken == Guid.Empty)
                    throw new SecondaryIndexException($"Unable to find valid index token when recovering secondary index {this.indexName}");

                // We return true even if the index token is default (compatible index token could not be found), in which case we restore the index from the log.
                var recoveredICInfo = this.secondaryFkv.GetCompatibleIndexCheckpointInfo(recoveredHLCInfo);
                indexToken = recoveredICInfo.info.token;
                lastCompletedPci = completedPci;
                lastStartedPci = currentPci;
                return true;
            }

            #region Wrapped ICheckpointManager methods

            public void CommitLogCheckpoint(Guid logToken, byte[] commitMetadata) 
                => this.userCheckpointManager.CommitLogCheckpoint(logToken, this.PrependToLogMetadata(commitMetadata));

            public void CommitLogIncrementalCheckpoint(Guid logToken, int version, byte[] commitMetadata, DeltaLog deltaLog) 
                => this.userCheckpointManager.CommitLogIncrementalCheckpoint(logToken, version, this.PrependToLogMetadata(commitMetadata), deltaLog);

            public byte[] GetLogCheckpointMetadata(Guid logToken, DeltaLog deltaLog) => DetachFrom(this.userCheckpointManager.GetLogCheckpointMetadata(logToken, deltaLog));

            #endregion Wrapped ICheckpointManager methods

            #region Simple call-through ICheckpointManager methods
            public void CommitIndexCheckpoint(Guid indexToken, byte[] commitMetadata) => this.userCheckpointManager.CommitIndexCheckpoint(indexToken, commitMetadata);

            public IDevice GetDeltaLogDevice(Guid token) => this.userCheckpointManager.GetDeltaLogDevice(token);

            public byte[] GetIndexCheckpointMetadata(Guid indexToken) => this.userCheckpointManager.GetIndexCheckpointMetadata(indexToken);

            public IEnumerable<Guid> GetIndexCheckpointTokens() => this.userCheckpointManager.GetIndexCheckpointTokens();

            public IDevice GetIndexDevice(Guid indexToken) => this.userCheckpointManager.GetIndexDevice(indexToken);

            public IEnumerable<Guid> GetLogCheckpointTokens() => this.userCheckpointManager.GetLogCheckpointTokens();

            public IDevice GetSnapshotLogDevice(Guid token) => this.userCheckpointManager.GetSnapshotLogDevice(token);

            public IDevice GetSnapshotObjectLogDevice(Guid token) => this.userCheckpointManager.GetSnapshotObjectLogDevice(token);

            public void InitializeIndexCheckpoint(Guid indexToken) => this.userCheckpointManager.InitializeIndexCheckpoint(indexToken);

            public void InitializeLogCheckpoint(Guid logToken) => this.userCheckpointManager.InitializeLogCheckpoint(logToken);

            public void OnRecovery(Guid indexToken, Guid logToken) => this.userCheckpointManager.OnRecovery(indexToken, logToken);

            public void PurgeAll() => this.userCheckpointManager.PurgeAll();

            public void Dispose() => this.userCheckpointManager.Dispose();
            #endregion Simple call-through ICheckpointManager methods
        }
    }
}
