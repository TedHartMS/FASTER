// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

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

        /// <summary>
        /// Adds a secondary index to the list.
        /// </summary>
        /// <param name="index"></param>
        public void AddIndex(ISecondaryIndex index)
        {
            static bool addSpecific<TIndex>(TIndex idx, ref TIndex[] vec)
                where TIndex : ISecondaryIndex
            {
                if (idx is { })
                {
                    if (idx.IsMutable)
                    {
                        Array.Resize(ref vec, vec.Length + 1);
#pragma warning disable IDE0056 // Use index operator (^ is not supported on .NET Framework or NETCORE pre-3.0)
                        vec[vec.Length - 1] = idx;
#pragma warning restore IDE0056 // Use index operator
                    }
                    return true;
                }
                return false;
            }

            if (!addSpecific(index as ISecondaryKeyIndex<TKVKey>, ref mutableKeyIndexes) 
                && !addSpecific(index as ISecondaryValueIndex<TKVValue>, ref mutableValueIndexes))
                throw new SecondaryIndexException("Object is not a KeyIndex or ValueIndex");
            indexes[index.Name] = index;
        }

        /// <summary>
        /// The number of indexes registered.
        /// </summary>
        public int Count => indexes.Count;

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
        public void Insert(ref TKVKey key)
        {
            foreach (var keyIndex in mutableKeyIndexes)
                keyIndex.Insert(ref key);
        }

        /// <summary>
        /// Upserts a mutable key into all mutable secondary key indexes.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Upsert(ref TKVKey key)
        {
            foreach (var keyIndex in mutableKeyIndexes)
                keyIndex.Upsert(ref key, true);
        }

        /// <summary>
        /// Deletes a key from all mutable secondary key indexes.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Delete(ref TKVKey key)
        {
            foreach (var keyIndex in mutableKeyIndexes)
                keyIndex.Delete(ref key);
        }
        #endregion Mutable KeyIndexes

        #region Mutable ValueIndexes
        /// <summary>
        /// Inserts a recordId keyed by a mutable value into all mutable secondary value indexes.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Insert(ref TKVValue value, long recordId)
        {
            foreach (var valueIndex in mutableValueIndexes)
                valueIndex.Insert(ref value, recordId);
        }

        /// <summary>
        /// Upserts a recordId keyed by a mutable value into all mutable secondary value indexes.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Upsert(ref TKVValue value, long recordId)
        {
            foreach (var valueIndex in mutableValueIndexes)
                valueIndex.Upsert(ref value, recordId, isMutable:false);
        }

        /// <summary>
        /// Deletes a recordId keyed by a mutable value from all mutable secondary value indexes.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Delete(long recordId)
        {
            foreach (var valueIndex in mutableValueIndexes)
                valueIndex.Delete(recordId);
        }
        #endregion Mutable ValueIndexes

        /// <summary>
        /// Upserts a readonly key into all secondary key indexes and readonly values into secondary value indexes.
        /// </summary>
        public void UpsertReadOnly(ref TKVKey key, ref TKVValue value, long recordId)
        {
            foreach (var index in indexes)
            {
                if (index is ISecondaryKeyIndex<TKVKey> keyIndex)
                    keyIndex.Upsert(ref key, isMutable: false);
                else if (index is ISecondaryValueIndex<TKVValue> valueIndex)
                    valueIndex.Upsert(ref value, recordId, isMutable: false);
            }
        }

    }
}
