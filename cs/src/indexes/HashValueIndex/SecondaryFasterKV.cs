// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace FASTER.indexes.HashValueIndex
{
    public partial class HashValueIndex<TKVKey, TKVValue, TPKey> : ISecondaryValueIndex<TKVKey, TKVValue>
    {
        internal SecondaryFasterKV<TPKey> secondaryFkv;
        CheckpointManager checkpointManager; 

        void CreateSecondaryFkv()
        {
            CheckpointSettings checkpointSettings = default;
            if (this.RegistrationSettings.CheckpointSettings is { }) {
                // Because we have to augment the metadata, we need to have a checkpoint manager wrapper, not just a directory.
                this.checkpointManager = new CheckpointManager(this.Name,
                        this.RegistrationSettings.CheckpointSettings.CheckpointManager ?? Utility.CreateDefaultCheckpointManager(this.RegistrationSettings.CheckpointSettings));
                checkpointSettings = new CheckpointSettings {
                    CheckpointManager = this.checkpointManager,
                    CheckPointType = this.RegistrationSettings.CheckpointSettings.CheckPointType,
                    RemoveOutdated = this.RegistrationSettings.CheckpointSettings.RemoveOutdated
                };
            }
            this.secondaryFkv = new SecondaryFasterKV<TPKey>(
                    this.RegistrationSettings.HashTableSize, this.RegistrationSettings.LogSettings, checkpointSettings, null /*SerializerSettings*/,
                    this.keyAccessor,
                    new VariableLengthStructSettings<TPKey, RecordId>
                    {
                        keyLength = new CompositeKey<TPKey>.VarLenLength(this.keyPointerSize, this.PredicateCount)
                    }
                );
            this.checkpointManager?.SetSecondaryFkv(this.secondaryFkv);

            // Now we have the log to use.
            this.keyAccessor.SetLog(this.secondaryFkv.hlog);
            this.bufferPool = this.secondaryFkv.hlog.bufferPool;
        }

        /// <inheritdoc/>
        public void OnPrimaryTruncate(long newBeginAddress) { }
    }

    internal partial class SecondaryFasterKV<TPKey> : FasterKV<TPKey, RecordId>
    {
        internal SecondaryFasterKV(long size, LogSettings logSettings,
            CheckpointSettings checkpointSettings = null, SerializerSettings<TPKey, RecordId> serializerSettings = null,
            IFasterEqualityComparer<TPKey> comparer = null,
            VariableLengthStructSettings<TPKey, RecordId> variableLengthStructSettings = null)
            : base(size, logSettings, checkpointSettings, serializerSettings, comparer, variableLengthStructSettings)
        {
        }
    }
}
