// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.core
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
        internal IHeapContainer<TKVKey> keyContainer;
        internal IHeapContainer<TKVValue> valueContainer;

        internal QueryRecord(IHeapContainer<TKVKey> keyContainer, IHeapContainer<TKVValue> valueContainer, RecordId recordId)
        {
            this.keyContainer = keyContainer;
            this.valueContainer = valueContainer;
            this.RecordId = recordId;
        }

        /// <summary>
        /// A reference to the record key
        /// </summary>
        public ref TKVKey KeyRef => ref this.keyContainer.Get();

        /// <summary>
        /// A reference to the record value
        /// </summary>
        public ref TKVValue ValueRef => ref this.valueContainer.Get();

        /// <summary>
        /// The ID of the record with this key and value; useful for post-query processing across multiple indexes.
        /// </summary>
        public RecordId RecordId { get; }

        /// <inheritdoc/>
        public void Dispose()
        {
            this.keyContainer.Dispose();
            this.valueContainer.Dispose();
        }

        /// <inheritdoc/>
        public override string ToString() => $"Key = {this.keyContainer.Get()}; Value = {this.valueContainer.Get()}";
    }
}
