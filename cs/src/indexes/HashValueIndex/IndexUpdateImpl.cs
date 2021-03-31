﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Runtime.CompilerServices;

namespace FASTER.indexes.HashValueIndex
{
    public partial class HashValueIndex<TKVKey, TKVValue, TPKey> : ISecondaryValueIndex<TKVValue>
    {
        private SectorAlignedBufferPool bufferPool;        // TODO: Look at SpanByte etc. instead

        private readonly int keyPointerSize = Utility.GetSize(default(KeyPointer<TPKey>));
        private readonly int recordIdSize = Utility.GetSize(sizeof(long));

        void CreateSecondaryFkv()
        {
            this.secondaryFkv = new FasterKVHVI<TPKey>(
                    this.RegistrationSettings.HashTableSize, this.RegistrationSettings.LogSettings, this.RegistrationSettings.CheckpointSettings, null /*SerializerSettings*/,
                    this.keyAccessor,
                    new VariableLengthStructSettings<TPKey, long>
                    {
                        keyLength = new CompositeKey<TPKey>.VarLenLength(this.keyPointerSize, this.PredicateCount)
                    }
                );

            // Now we have the log to use.
            this.keyAccessor.SetLog(this.secondaryFkv.hlog);
            this.bufferPool = this.secondaryFkv.hlog.bufferPool;
        }

        private unsafe Status ExecuteAndStore(ref TKVValue kvValue, long recordId)
        {
            // Note: stackalloc is safe because PendingContext or ChangeTracker will copy it to the bufferPool
            // if needed. On the Insert fast path, we don't want any allocations otherwise; changeTracker is null.
            var keyMemLen = this.keyPointerSize * this.PredicateCount;
            var keyBytes = stackalloc byte[keyMemLen];
            var anyMatch = false;

            // Keep a local copy in case the list is updated.
            var localPreds = this.predicates;

            for (var ii = 0; ii < this.PredicateCount; ++ii)
            {
                ref KeyPointer<TPKey> keyPointer = ref Unsafe.AsRef<KeyPointer<TPKey>>(keyBytes + ii * this.keyPointerSize);
                keyPointer.PreviousAddress = core.Constants.kInvalidAddress;
                keyPointer.PredicateOrdinal = (byte)ii;

                var pKey = localPreds[ii].Execute(ref kvValue);
                keyPointer.IsNull = userKeyComparer.Equals(ref pKey, ref this.RegistrationSettings.NullIndicator);
                if (!keyPointer.IsNull)
                {
                    keyPointer.Key = pKey;  // TODO: handle the non-blittable cases here
                    anyMatch = true;
                }
            }

            if (!anyMatch)
                return Status.OK;

            var session = TODO();
            ref CompositeKey<TPKey> compositeKey = ref Unsafe.AsRef<CompositeKey<TPKey>>(keyBytes);
            var input = new FasterKVHVI<TPKey>.Input(0);
            var value = recordId;
            var context = new FasterKVHVI<TPKey>.Context { Functions = session.functions };
            return session.IndexInsert(this.secondaryFkv, ref compositeKey.CastToFirstKeyPointerRefAsKeyRef(), ref value, ref input, ref context);
        }
    }
}
