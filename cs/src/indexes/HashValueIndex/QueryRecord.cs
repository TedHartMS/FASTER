// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FASTER.indexes.HashValueIndex
{
    /// <summary>
    /// The wrapper around the provider data stored in the primary faster instance.
    /// </summary>
    /// <typeparam name="TKVKey">The type of the key in the primary FasterKV instance</typeparam>
    /// <typeparam name="TKVValue">The type of the value in the primary FasterKV instance</typeparam>
    /// <remarks>Having this class enables separation between the LogicalAddress stored in the Index-implementing
    ///     FasterKV instances, and the actual <typeparamref name="TKVKey"/> and <typeparamref name="TKVValue"/>
    ///     types.</remarks>
    public class QueryRecord<TKVKey, TKVValue> : IDisposable
    {
        // C# doesn't allow ref fields and even if it did, if the client held the QueryRecord
        // past the ref lifetime, bad things would happen when accessing the ref key/value.
        // TODOperf: Only varlen needs the container; have TKVKey/TKVValue members to use if not varlen.
        internal IHeapContainer<TKVKey> keyContainer;
        internal IHeapContainer<TKVValue> valueContainer;

        internal QueryRecord(IHeapContainer<TKVKey> keyContainer, IHeapContainer<TKVValue> valueContainer)
        {
            this.keyContainer = keyContainer;
            this.valueContainer = valueContainer;
        }

        public ref TKVKey GetKey() => ref this.keyContainer.Get();

        public ref TKVValue GetValue() => ref this.valueContainer.Get();

        /// <inheritdoc/>
        public void Dispose()
        {
            this.keyContainer.Dispose();
            this.valueContainer.Dispose();
        }

        public override string ToString() => $"Key = {this.keyContainer.Get()}; Value = {this.valueContainer.Get()}";
    }
}
