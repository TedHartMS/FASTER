﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace FASTER.indexes.HashValueIndex
{
    public partial class HashValueIndex<TKVKey, TKVValue, TPKey> : ISecondaryValueIndex<TKVKey, TKVValue>
    {
        internal SecondaryFasterKV<TPKey> secondaryFkv;

        void CreateSecondaryFkv()
        {
            this.secondaryFkv = new SecondaryFasterKV<TPKey>(
                    this.RegistrationSettings.HashTableSize, this.RegistrationSettings.LogSettings, this.RegistrationSettings.CheckpointSettings, null /*SerializerSettings*/,
                    this.keyAccessor,
                    new VariableLengthStructSettings<TPKey, RecordId>
                    {
                        keyLength = new CompositeKey<TPKey>.VarLenLength(this.keyPointerSize, this.PredicateCount)
                    }
                );

            // Now we have the log to use.
            this.keyAccessor.SetLog(this.secondaryFkv.hlog);
            this.bufferPool = this.secondaryFkv.hlog.bufferPool;
        }

        /// <inheritdoc/>
        public void OnPrimaryCheckpoint(int version, long flushedUntilAddress) { }

        /// <inheritdoc/>
        public void OnPrimaryRecover(int version, long flushedUntilAddress, out int recoveredToVersion, out long recoveredToAddress)
        {
            recoveredToVersion = default;
            recoveredToAddress = default;
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
