// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// Base, non-generic interface for a SecondaryIndex in FASTER.
    /// </summary>
    public interface ISecondaryIndex
    {
        /// <summary>
        /// The identifier of the index.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// If true, the index is updated immediately on each FasterKV operation; otherwise it is updated only when record pages go ReadOnly.
        /// </summary>
        /// <remarks>Requires <see cref="FasterKV{Key, Value}.SupportsMutableIndexes"/></remarks>
        bool IsMutable { get; }
    }

    /// <summary>
    /// Interface for a FASTER SecondaryIndex that is derived from the FasterKV Key generic parameter.
    /// </summary>
    public interface ISecondaryKeyIndex<TKVKey> : ISecondaryIndex
    {
        /// <summary>
        /// Inserts a key into the secondary index. Called only for mutable indexes, on the initial insert of a Key.
        /// </summary>
        /// <param name="key">The key to be inserted; always mutable</param>
        /// <remarks>
        /// If the index is mutable and the <paramref name="key"/> is already there, this call should be ignored, because it is the result
        /// of a race in which the record in the primary FasterKV was updated after the initial insert but before this method
        /// was called.
        /// </remarks>
        void Insert(ref TKVKey key);

        /// <summary>
        /// Upserts a key into the secondary index. This may be called either immediately during a FasterKV operation, or when the page containing a record goes ReadOnly.
        /// </summary>
        /// <param name="key">The key to be inserted</param>
        /// <param name="isMutable">Whether the recordId was in the mutable region of FASTER. If true, the record may subsequently be Upserted or Deleted.</param>
        /// <remarks>
        /// For an immutable index, this is the only call made on the interface, when the page containing the <paramref name="key"/> has moved to ReadOnly.
        /// In this case, <paramref name="isMutable"/> is false, and the index may move the <paramref name="key"/> to an immutable storage area.
        /// </remarks>
        void Upsert(ref TKVKey key, bool isMutable);

        /// <summary>
        /// Removes a key from the secondary index. Called only for mutable indexes.
        /// </summary>
        /// <param name="key">The key to be removed</param>
        void Delete(ref TKVKey key);
    }

    /// <summary>
    /// Interface for a FASTER SecondaryIndex that is derived from the FasterKV Value generic parameter.
    /// </summary>
    public interface ISecondaryValueIndex<TKVValue> : ISecondaryIndex
    {
        /// <summary>
        /// Inserts a recordId into the secondary index, with the associated value from which the index derives its key(s).
        /// Called only for mutable indexes, on the initial insert of a Key.
        /// </summary>
        /// <param name="value">The value to be inserted; always mutable</param>
        /// <param name="recordId">The identifier of the record containing the <paramref name="value"/></param>
        /// <remarks>
        /// If the index is mutable and the <paramref name="recordId"/> is already there for this <paramref name="value"/>,
        /// this call should be ignored, because it is the result of a race in which the record in the primary FasterKV was
        /// updated after the initial insert but before this method was called, so the <paramref name="value"/> on this call
        /// would overwrite it with an obsolete value.
        /// </remarks>
        void Insert(ref TKVValue value, long recordId);

        /// <summary>
        /// Upserts a recordId into the secondary index, with the associated value from which the index derives its key(s).
        /// This may be called either immediately during a FasterKV operation, or when the page containing a record goes ReadOnly.
        /// </summary>
        /// <param name="value">The value to be upserted</param>
        /// <param name="recordId">The identifier of the record containing the <paramref name="value"/></param>
        /// <param name="isMutable">Whether the recordId was in the mutable region of FASTER; if so, it may subsequently be Upserted or Deleted.</param>
        /// <remarks>
        /// For an immutable index, this is the only call made on the interface, when the page containing the <paramref name="recordId"/> has moved to ReadOnly.
        ///In this case, <paramref name="isMutable"/> is false, and the index may move the <paramref name="recordId"/> to an immutable storage area.
        /// </remarks>
        void Upsert(ref TKVValue value, long recordId, bool isMutable);

        /// <summary>
        /// Removes a recordId from the secondary index's key(s) derived from the <paramref name="value"/>. Called only for mutable indexes.
        /// </summary>
        /// <param name="value">The value from whose derived key(s) the <paramref name="recordId"/> is to be removed</param>
        /// <param name="recordId">The recordId to be removed</param>
        void Delete(ref TKVValue value, long recordId);
    }
}