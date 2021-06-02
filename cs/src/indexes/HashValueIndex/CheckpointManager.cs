// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace FASTER.indexes.HashValueIndex
{
    internal struct SecondaryCheckpointMetadata
    {
        const int MetadataVersion = 1;

        /// <summary>
        /// Serialized byte size, including two PCIs, MetadataVersion, and long checksum
        /// </summary>
        public const int SerializedSize = PrimaryCheckpointInfo.SerializedSize * 2 + 8 + 4;

        internal PrimaryCheckpointInfo lastCompletedPrimaryCheckpointInfo;      // The latest previously-completed PCI
        internal PrimaryCheckpointInfo lastStartedPrimaryCheckpointInfo;        // The latest previously-started PCI

        /// <summary>
        /// Constructs from a byte array.
        /// </summary>
        public SecondaryCheckpointMetadata(byte[] metadata)
        {
            var offset = 0;

            var slice = metadata.Slice(offset, 4);
            var metaVersion = BitConverter.ToInt32(slice, 0);
            if (metaVersion != MetadataVersion)
                throw new SecondaryIndexException("Unknown metadata version");
            offset += slice.Length;

            slice = metadata.Slice(offset, 8);
            var checksum = BitConverter.ToInt64(slice, 0);
            offset += slice.Length;

            slice = metadata.Slice(offset, PrimaryCheckpointInfo.SerializedSize);
            this.lastCompletedPrimaryCheckpointInfo = new PrimaryCheckpointInfo(slice);
            offset += slice.Length;

            slice = metadata.Slice(offset, PrimaryCheckpointInfo.SerializedSize);
            this.lastStartedPrimaryCheckpointInfo = new PrimaryCheckpointInfo(slice);
            offset += slice.Length;

            Debug.Assert(offset == SerializedSize);
            if (checksum != Checksum())
                throw new SecondaryIndexException("Invalid checksum for checkpoint");
        }

        /// <summary>
        /// Converts to a byte array for serialization.
        /// </summary>
        /// <returns></returns>
        internal byte[] ToByteArray()
        {
            var result = new byte[SerializedSize];
            var bytes = BitConverter.GetBytes(MetadataVersion);
            Array.Copy(bytes, 0, result, 0, bytes.Length);
            var offset = bytes.Length;

            bytes = BitConverter.GetBytes(Checksum());
            Array.Copy(bytes, 0, result, offset, bytes.Length);
            offset += bytes.Length;

            bytes = this.lastCompletedPrimaryCheckpointInfo.ToByteArray();
            Array.Copy(bytes, 0, result, offset, bytes.Length);
            offset += bytes.Length;

            bytes = this.lastStartedPrimaryCheckpointInfo.ToByteArray();
            Array.Copy(bytes, 0, result, offset, bytes.Length);
            offset += bytes.Length;

            Debug.Assert(offset == SerializedSize);
            return result;
        }

        internal readonly long Checksum() => MetadataVersion ^ this.lastCompletedPrimaryCheckpointInfo.Checksum() ^ this.lastStartedPrimaryCheckpointInfo.Checksum();
    }

    internal class CheckpointManager<TPKey> : ICheckpointManager
    {
        private readonly string indexName;
        internal SecondaryFasterKV<TPKey> secondaryFkv;
        private readonly ICheckpointManager userCheckpointManager;

        internal SecondaryCheckpointMetadata secondaryMetadata;

        internal CheckpointManager(string indexName, ICheckpointManager userCheckpointManager)
        {
            this.indexName = indexName;
            this.userCheckpointManager = userCheckpointManager;
        }

        internal void SetSecondaryFkv(SecondaryFasterKV<TPKey> secondaryFkv) => this.secondaryFkv = secondaryFkv;

        internal void PrepareToCheckpoint(PrimaryCheckpointInfo latestPci) => this.secondaryMetadata.lastCompletedPrimaryCheckpointInfo = latestPci;

        internal void PrepareToRecover() => this.secondaryMetadata.lastCompletedPrimaryCheckpointInfo = default;

        private static SecondaryCheckpointMetadata GetSecondaryMetadata(byte[] metadata) 
            => new SecondaryCheckpointMetadata(metadata.Slice(0, SecondaryCheckpointMetadata.SerializedSize));

        internal void SetPCIs(PrimaryCheckpointInfo completedPci, PrimaryCheckpointInfo startedPCI)
        {
            this.secondaryMetadata.lastCompletedPrimaryCheckpointInfo = completedPci;
            this.secondaryMetadata.lastStartedPrimaryCheckpointInfo = startedPCI;
        }

        private byte[] RemoveFrom(byte[] metadata) 
            => metadata.Slice(SecondaryCheckpointMetadata.SerializedSize, metadata.Length - SecondaryCheckpointMetadata.SerializedSize);

        private byte[] PrependToLogMetadata(byte[] metadata)
        {
            var result = new byte[metadata.Length + SecondaryCheckpointMetadata.SerializedSize];
            var offset = 0;
            Array.Copy(this.secondaryMetadata.ToByteArray(), 0, result, offset, SecondaryCheckpointMetadata.SerializedSize);
            offset += SecondaryCheckpointMetadata.SerializedSize;
            Array.Copy(metadata, 0, result, offset, metadata.Length);
            offset += metadata.Length;
            Debug.Assert(offset == result.Length);
            return result;
        }

        internal bool GetRecoveryTokens(PrimaryCheckpointInfo recoveredPci, out Guid indexToken, out Guid logToken, out PrimaryCheckpointInfo lastCompletedPci, out PrimaryCheckpointInfo lastStartedPci)
        {
            logToken = default;
            var recoveredHLCInfo = new HybridLogCheckpointInfo();
            SecondaryCheckpointMetadata secondaryMetadata = default;
            foreach (var token in this.userCheckpointManager.GetLogCheckpointTokens())
            {
                try
                {
                    // Find the first secondary log checkpoint with currentPci < recoveredPci.
                    var metadata = this.userCheckpointManager.GetLogCheckpointMetadata(token, deltaLog: default);
                    secondaryMetadata = GetSecondaryMetadata(metadata);
                    if (secondaryMetadata.lastStartedPrimaryCheckpointInfo.CompareTo(recoveredPci) < 0)
                    {
                        logToken = token;
                        RecoverHLCInfo(ref recoveredHLCInfo, logToken);
                        break;
                    }
                }
                catch
                {
                    continue;
                }
            }

            if (logToken == Guid.Empty)
            {
                // No secondary checkpoints available, so we'll replay from the beginning
                indexToken = logToken = default;
                lastCompletedPci = lastStartedPci = default;
                return true;
            }

            // We return true even if the index token is default (compatible index token could not be found), in which case we restore the index from the log.
            indexToken = GetCompatibleIndexToken(recoveredHLCInfo);
            lastCompletedPci = secondaryMetadata.lastCompletedPrimaryCheckpointInfo;
            lastStartedPci = secondaryMetadata.lastStartedPrimaryCheckpointInfo;
            return true;
        }

        // Virtual for test
        internal virtual void RecoverHLCInfo(ref HybridLogCheckpointInfo recoveredHLCInfo, Guid logToken) => recoveredHLCInfo.Recover(logToken, this, this.secondaryFkv.hlog.LogPageSizeBits);
        internal virtual Guid GetCompatibleIndexToken(HybridLogCheckpointInfo recoveredHLCInfo) => this.secondaryFkv.GetCompatibleIndexCheckpointInfo(recoveredHLCInfo).info.token;

        #region Wrapped ICheckpointManager methods

        public void CommitLogCheckpoint(Guid logToken, byte[] commitMetadata) 
            => this.userCheckpointManager.CommitLogCheckpoint(logToken, this.PrependToLogMetadata(commitMetadata));

        public void CommitLogIncrementalCheckpoint(Guid logToken, int version, byte[] commitMetadata, DeltaLog deltaLog) 
            => this.userCheckpointManager.CommitLogIncrementalCheckpoint(logToken, version, this.PrependToLogMetadata(commitMetadata), deltaLog);

        public byte[] GetLogCheckpointMetadata(Guid logToken, DeltaLog deltaLog) => RemoveFrom(this.userCheckpointManager.GetLogCheckpointMetadata(logToken, deltaLog));

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
