// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Runtime.CompilerServices;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FASTER.indexes.HashValueIndex
{
    internal unsafe partial class FasterKVHVI<TPKey> : FasterKV<TPKey, long>
    {
        internal interface IInputAccessor<TInput>
        {
            bool IsDelete(ref TInput input);
            bool SetDelete(ref TInput input, bool value);
        }

        /// <summary>
        /// Input to Read operations on the secondary FasterKV instance
        /// </summary>
        internal unsafe struct Input : IDisposable
        {
            private SectorAlignedMemory keyPointerMem;

            internal Input(int predOrdinal)
            {
                this.keyPointerMem = null;
                this.PredicateOrdinal = predOrdinal;
                this.IsDelete = false;
            }

            internal void SetQueryKey(SectorAlignedBufferPool pool, KeyAccessor<TPKey> keyAccessor, ref TPKey key)
            {
                // Create a varlen CompositeKey with just one item. This is ONLY used as the query key to Query.
                this.keyPointerMem = pool.Get(keyAccessor.KeyPointerSize);
                ref KeyPointer<TPKey> keyPointer = ref Unsafe.AsRef<KeyPointer<TPKey>>(keyPointerMem.GetValidPointer());
                keyPointer.Initialize(this.PredicateOrdinal, ref key, keyAccessor.KeyPointerSize);
            }

            /// <summary>
            /// The ordinal of the <see cref="Predicate{TKVValue, TPKey}"/> in the <see cref="HashValueIndex"/>.
            /// </summary>
            public int PredicateOrdinal { get; set; }

            /// <summary>
            /// Whether this is a Delete (or the Delete part of an RCU)
            /// </summary>
            public bool IsDelete { get; set; }

            /// <summary>
            /// The query key for a Query method
            /// </summary>
            public ref TPKey QueryKeyRef => ref Unsafe.AsRef<TPKey>(this.keyPointerMem.GetValidPointer());

            /// <summary>
            /// The query key for a Query method
            /// </summary>
            public ref KeyPointer<TPKey> QueryKeyPointerRef => ref Unsafe.AsRef<KeyPointer<TPKey>>(this.keyPointerMem.GetValidPointer());

            public void Dispose()
            {
                if (this.keyPointerMem is {})
                {
                    this.keyPointerMem.Return();
                    this.keyPointerMem = null;
                }
            }

            public override string ToString() => $"qKeyPtr {this.QueryKeyPointerRef}, predOrd {this.PredicateOrdinal}, isDel {this.IsDelete}";
        }
    }
}