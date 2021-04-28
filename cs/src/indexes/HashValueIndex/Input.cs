// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Runtime.CompilerServices;

namespace FASTER.indexes.HashValueIndex
{
    internal unsafe partial class SecondaryFasterKV<TPKey> : FasterKV<TPKey, RecordId>
    {
        internal interface IInputAccessor<TInput>       // TODO needed?
        {
            bool IsDelete(ref TInput input);
            bool SetDelete(ref TInput input, bool value);
        }

        /// <summary>
        /// Input to Read operations on the secondary FasterKV instance
        /// </summary>
        internal unsafe struct Input : IDisposable
        {
            internal SectorAlignedMemory keyPointerMem;

            internal Input(SectorAlignedBufferPool pool, KeyAccessor<TPKey> keyAccessor, int predOrdinal, ref TPKey key)
            {
                // Create a varlen CompositeKey with just one item. This is ONLY used as the query key to Query.
                // Putting the query key in Input is necessary because iterator functions cannot contain unsafe code or have
                // byref args, and bufferPool is needed here because the stack goes away as part of the iterator operation.
                this.keyPointerMem = pool.Get(keyAccessor.KeyPointerSize);
                ref KeyPointer<TPKey> keyPointer = ref Unsafe.AsRef<KeyPointer<TPKey>>(keyPointerMem.GetValidPointer());
                keyPointer.Initialize(predOrdinal, ref key, keyAccessor.KeyPointerSize);
                this.IsDelete = false;
            }

            internal Input(SectorAlignedBufferPool pool, KeyAccessor<TPKey> keyAccessor, QueryContinuationToken continuationToken, int predOrdinal, ref TPKey key, int queryOrdinal)
            {
                ref var serializedState = ref continuationToken.Predicates[queryOrdinal];

                this.keyPointerMem = pool.Get(keyAccessor.KeyPointerSize);
                ref KeyPointer<TPKey> keyPointer = ref Unsafe.AsRef<KeyPointer<TPKey>>(this.keyPointerMem.GetValidPointer());
                keyPointer.Initialize(predOrdinal, ref key, keyAccessor.KeyPointerSize);
                keyPointer.PreviousAddress = serializedState.PreviousAddress;   // TODO: Validate the restored PreviousAddress is good
                this.IsDelete = false;
            }

            internal void Serialize(QueryContinuationToken continuationToken, int queryOrdinal, long previousAddress, RecordId recordId)
            {
                ref var serializedState = ref continuationToken.Predicates[queryOrdinal];
                this.PreviousAddress = previousAddress;
                serializedState.PreviousAddress = previousAddress;
                serializedState.RecordId = recordId;
            }

            internal long PreviousAddress
            {
                get => this.AsQueryKeyPointerRef.PreviousAddress;
                set => this.AsQueryKeyPointerRef.PreviousAddress = value;
            }

            internal int PredicateOrdinal => this.AsQueryKeyPointerRef.PredicateOrdinal;

            internal bool IsDelete { get; set; }    // TODO needed?

            internal ref TPKey KeyRef => ref AsQueryKeyPointerRef.Key;

            internal ref TPKey AsQueryKeyRef => ref Unsafe.AsRef<TPKey>(this.keyPointerMem.GetValidPointer());

            internal ref KeyPointer<TPKey> AsQueryKeyPointerRef => ref Unsafe.AsRef<KeyPointer<TPKey>>(this.keyPointerMem.GetValidPointer());

            public void Dispose()
            {
                this.keyPointerMem?.Return();
                this.keyPointerMem = null;
            }

            public override string ToString() => $"qKeyPtr {this.AsQueryKeyPointerRef}, predOrd {this.PredicateOrdinal}, isDel {this.IsDelete}";
        }
    }
}