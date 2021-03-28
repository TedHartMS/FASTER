// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

#pragma warning disable IDE0056 // Use index operator (^ is not supported on .NET Framework or NETCORE pre-3.0)

namespace FASTER.core
{
    /// <summary>
    /// Manages the list of secondary indexes in the FasterKV.
    /// </summary>
    public class SecondaryIndexBroker<TKVKey, TKVValue>
    {
        private readonly Dictionary<string, ISecondaryIndex> indexes = new Dictionary<string, ISecondaryIndex>();

        // Use arrays for faster traversal.
        private ISecondaryKeyIndex<TKVKey>[] mutableKeyIndexes = Array.Empty<ISecondaryKeyIndex<TKVKey>>();
        internal int MutableKeyIndexCount => mutableKeyIndexes.Length;

        private ISecondaryValueIndex<TKVValue>[] mutableValueIndexes = Array.Empty<ISecondaryValueIndex<TKVValue>>();
        internal int MutableValueIndexCount => mutableValueIndexes.Length;

        readonly object membershipLock = new object();

        /// <summary>
        /// Adds a secondary index to the list.
        /// </summary>
        /// <param name="index"></param>
        public void AddIndex(ISecondaryIndex index)
        {
            bool isMutable = false;

            bool addSpecific<TIndex>(TIndex idx, ref TIndex[] vec)
                where TIndex : ISecondaryIndex
            {
                if (idx is null)
                    return false;
                if (idx.IsMutable)
                {
                    Array.Resize(ref vec, vec.Length + 1);
                    vec[vec.Length - 1] = idx;
                    isMutable = true;
                }
                return true;
            }

            lock (membershipLock)
            {
                if (!addSpecific(index as ISecondaryKeyIndex<TKVKey>, ref mutableKeyIndexes)
                    && !addSpecific(index as ISecondaryValueIndex<TKVValue>, ref mutableValueIndexes))
                    throw new SecondaryIndexException("Object is not a KeyIndex or ValueIndex");
                this.HasMutableIndexes |= isMutable;
                indexes[index.Name] = index;
                index.SetSessionSlot(SecondaryIndexSessionBroker.NextSessionSlot++);
            }
        }

        /// <summary>
        /// The number of indexes registered.
        /// </summary>
        public int Count => indexes.Count;

        /// <summary>
        /// The number of indexes registered.
        /// </summary>
        public bool HasMutableIndexes { get; private set; }

        /// <summary>
        /// Enumerates the list of indexes.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<ISecondaryIndex> GetIndexes() => indexes.Values;

        /// <summary>
        /// Returns the index with the specified name.
        /// </summary>
        /// <param name="name"></param>
        public ISecondaryIndex GetIndex(string name) => indexes[name];

        // On failure of an operation, a SecondaryIndexException is thrown by the Index

        #region Mutable KeyIndexes
        /// <summary>
        /// Inserts a mutable key into all mutable secondary key indexes.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Insert(ref TKVKey key, long recordId, SecondaryIndexSessionBroker indexSessionBroker)
        {
            var mki = this.mutableKeyIndexes;
            foreach (var keyIndex in mki)
                keyIndex.Insert(ref key, recordId, indexSessionBroker);
        }

        /// <summary>
        /// Upserts a mutable key into all mutable secondary key indexes.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Upsert(ref TKVKey key, long recordId, SecondaryIndexSessionBroker indexSessionBroker)
        {
            var mki = this.mutableKeyIndexes;
            foreach (var keyIndex in mki)
                keyIndex.Upsert(ref key, recordId, isMutableRecord: true, indexSessionBroker);
        }

        /// <summary>
        /// Deletes recordId for a key from all mutable secondary key indexes.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Delete(ref TKVKey key, long recordId, SecondaryIndexSessionBroker indexSessionBroker)
        {
            var mki = this.mutableKeyIndexes;
            foreach (var keyIndex in mki)
                keyIndex.Delete(ref key, recordId, indexSessionBroker);
        }
        #endregion Mutable KeyIndexes

        #region Mutable ValueIndexes
        /// <summary>
        /// Inserts a recordId keyed by a mutable value into all mutable secondary value indexes.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Insert(ref TKVValue value, long recordId, SecondaryIndexSessionBroker indexSessionBroker)
        {
            var mvi = this.mutableValueIndexes;
            foreach (var valueIndex in mvi)
                valueIndex.Insert(ref value, recordId, indexSessionBroker);
        }

        /// <summary>
        /// Upserts a recordId keyed by a mutable value into all mutable secondary value indexes.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Upsert(ref TKVValue value, long recordId, SecondaryIndexSessionBroker indexSessionBroker)
        {
            var mvi = this.mutableValueIndexes;
            foreach (var valueIndex in mvi)
                valueIndex.Upsert(ref value, recordId, isMutableRecord:false, indexSessionBroker);
        }

        /// <summary>
        /// Deletes a recordId keyed by a mutable value from all mutable secondary value indexes.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Delete(long recordId, SecondaryIndexSessionBroker indexSessionBroker)
        {
            var mvi = this.mutableValueIndexes;
            foreach (var valueIndex in mvi)
                valueIndex.Delete(recordId, indexSessionBroker);
        }
        #endregion Mutable ValueIndexes

        /// <summary>
        /// Upserts a readonly key into all secondary key indexes and readonly values into secondary value indexes.
        /// </summary>
        public void UpsertReadOnly(ref TKVKey key, ref TKVValue value, long recordId, SecondaryIndexSessionBroker indexSessionBroker)
        {
            var idxs = this.indexes;
            foreach (var index in idxs)
            {
                if (index is ISecondaryKeyIndex<TKVKey> keyIndex)
                    keyIndex.Upsert(ref key, recordId, isMutableRecord: false, indexSessionBroker);
                else if (index is ISecondaryValueIndex<TKVValue> valueIndex)
                    valueIndex.Upsert(ref value, recordId, isMutableRecord: false, indexSessionBroker);
            }
        }
    }
}
