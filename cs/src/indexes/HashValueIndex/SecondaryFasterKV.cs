﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

namespace FASTER.indexes.HashValueIndex
{
    public partial class HashValueIndex<TKVKey, TKVValue, TPKey> : ISecondaryValueIndex<TKVKey, TKVValue>, IDisposable
    {
        internal SecondaryFasterKV<TPKey> secondaryFkv;
        CheckpointManager<TPKey> checkpointManager; 

        void CreateSecondaryFkv()
        {
            CheckpointSettings checkpointSettings = default;
            if (this.RegistrationSettings.CheckpointSettings is { }) {
                // Because we have to augment the metadata, we need to have a checkpoint manager wrapper, not just a directory.
                // For test, we may have a derived wrapper instance already.
                var userCheckpointManager = this.RegistrationSettings.CheckpointSettings.CheckpointManager ?? Utility.CreateDefaultCheckpointManager(this.RegistrationSettings.CheckpointSettings);
                this.checkpointManager = (userCheckpointManager as CheckpointManager<TPKey>) ?? new CheckpointManager<TPKey>(this.Name, userCheckpointManager);
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

        /// <inheritdoc/>
        public void Dispose()
        {
            // Caller disposes the log device, just like it does for primary FKV.
            this.secondaryFkv.Dispose();
        }
    }

    internal partial class SecondaryFasterKV<TPKey> : FasterKV<TPKey, RecordId>
    {
        internal RecordId highWaterRecordId = default;

        internal SecondaryFasterKV(long size, LogSettings logSettings,
            CheckpointSettings checkpointSettings = null, SerializerSettings<TPKey, RecordId> serializerSettings = null,
            IFasterEqualityComparer<TPKey> comparer = null,
            VariableLengthStructSettings<TPKey, RecordId> variableLengthStructSettings = null)
            : base(size, logSettings, checkpointSettings, serializerSettings, comparer, variableLengthStructSettings)
        {
        }
    }
}