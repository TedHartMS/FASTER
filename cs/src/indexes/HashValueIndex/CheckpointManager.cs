// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace FASTER.indexes.HashValueIndex
{
    public partial class HashValueIndex<TKVKey, TKVValue, TPKey> : ISecondaryValueIndex<TKVKey, TKVValue>
    {
        internal struct HighWatermark
        {
            const int MetadataVersion = 1;
            internal int version;
            internal long address;

            // Above members, plus long checksum
            const int SerializedSize = 24;

            internal HighWatermark(RecordId recordId) : this(recordId.Version, recordId.Address) { }

            internal HighWatermark(int version, long address)
            {
                this.version = version;
                this.address = address;
            }

            private readonly long Checksum() 
                => this.version ^ this.address;

            internal HighWatermark(byte[] metadata)
            {
                var slice = metadata.Slice(0, 4);
                var metaVersion = BitConverter.ToInt32(slice, 0);
                if (metaVersion != MetadataVersion)
                    throw new HashValueIndexException("Unknown metadata version");
                var offset = slice.Length;

                slice = metadata.Slice(offset, 8);
                var checksum = BitConverter.ToInt64(slice, 0);
                offset += slice.Length;

                slice = metadata.Slice(offset, 4);
                this.version = BitConverter.ToInt32(slice, 0);
                offset += slice.Length;

                slice = metadata.Slice(offset, 8);
                this.address = BitConverter.ToInt64(slice, 0);
                offset += slice.Length;

                Debug.Assert(offset == SerializedSize);
                if (checksum != Checksum())
                    throw new FasterException("Invalid checksum for checkpoint");
            }

            internal bool IsDefault() => this.version == 0 && this.address == 0;

        }

        internal class CheckpointManager : ICheckpointManager
        {
            private readonly string indexName;
            internal SecondaryFasterKV<TPKey> secondaryFkv;
            private readonly ICheckpointManager userCheckpointManager;

            internal PrimaryCheckpointInfo primaryCheckpointInfo;

            internal CheckpointManager(string indexName, ICheckpointManager userCheckpointManager)
            {
                this.indexName = indexName;
                this.userCheckpointManager = userCheckpointManager;
            }

            internal void SetSecondaryFkv(SecondaryFasterKV<TPKey> secondaryFkv) => this.secondaryFkv = secondaryFkv;

            internal void PrepareToCheckpoint(PrimaryCheckpointInfo latestPci) => this.primaryCheckpointInfo = latestPci;

            internal void PrepareToRecover() => this.primaryCheckpointInfo = default;

            internal byte[] DetachFrom(byte[] metadata)
            {
                this.primaryCheckpointInfo = new PrimaryCheckpointInfo(metadata.Slice(0, PrimaryCheckpointInfo.SerializedSize));
                return metadata.Slice(PrimaryCheckpointInfo.SerializedSize, metadata.Length - PrimaryCheckpointInfo.SerializedSize);
            }

            private byte[] PrependToLogMetadata(byte[] metadata)
            {
                var result = new byte[metadata.Length + PrimaryCheckpointInfo.SerializedSize];
                Array.Copy(this.primaryCheckpointInfo.ToByteArray(), 0, result, PrimaryCheckpointInfo.SerializedSize, metadata.Length);
                return result;
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
