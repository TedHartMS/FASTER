// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// Base, non-generic interface for a SecondaryIndex in FASTER.
    /// </summary>
    public interface ISecondaryIndex
    {
        /// <summary>
        /// The friendly identifier of the index.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// The unique identifier of the index.
        /// </summary>
        Guid Id { get; }

        /// <summary>
        /// If true, the index is updated immediately on each FasterKV operation; otherwise it is updated only when record pages go ReadOnly.
        /// </summary>
        bool IsMutable { get; }

        /// <summary>
        /// The slot id to be passed to <see cref="SecondaryIndexSessionBroker"/>; called by <see cref="SecondaryIndexBroker{TKVKey, TKVValue}.AddIndex(ISecondaryIndex)"/>.
        /// </summary>
        void SetSessionSlot(long slot);

        /// <summary>
        /// Returns the latest checkpoint token for this index.
        /// </summary>
        /// <remarks>Called when the Primary FKV has begun checkpointing the log (either by itself or as part of a full checkpoint).</remarks>
        Guid GetLatestCheckpointToken();

        /// <summary>
        /// Provides information about a just-completed Primary FasterKV checkpoint.
        /// </summary>
        /// <param name="primaryCheckpointInfo"></param>
        /// <remarks>Called when the Primary FKV has completed checkpointing the log (either by itself or as part of a full checkpoint). The Secondary Index should
        ///     hold this information in-memory so it can store it to and retrieve it from its own checkpoint information, so the <see cref="SecondaryIndexBroker{TKVKey, TKVValue}"/> can replay
        ///     records from that point</remarks>
        void OnPrimaryCheckpointCompleted(PrimaryCheckpointInfo primaryCheckpointInfo);

        /// <summary>
        /// Sychronous function to begin recovery of a secondary index; the index recovers itself here and returns a <see cref="PrimaryCheckpointInfo"/> indicating the last Primary FasterKV
        /// checkpoint to which it recovered. The <see cref="SecondaryIndexBroker{TKVKey, TKVValue}"/> will replay records since then, until the index is up to date with the Primary FasterKV.
        /// </summary>
        /// <param name="secondaryLogToken">A checkpoint token that was returned by <see cref="GetLatestCheckpointToken"/></param>
        /// <param name="undoNextVersion">Whether records with versions beyond checkpoint version need to be undone (and invalidated on log)</param>
        /// <returns>The Primary FasterKV checkpoint info stored in the secondary index checkpoint identified by <paramref name="secondaryLogToken"/>, or default if not found</returns>
        /// <remarks>This is called after the Primary FKV has recovered itself and before it is open for operations.</remarks>
        PrimaryCheckpointInfo BeginRecover(Guid secondaryLogToken, bool undoNextVersion);

        /// <summary>
        /// Asychronous function to begin recovery of a secondary index; the index recovers itself here and returns a <see cref="PrimaryCheckpointInfo"/> indicating the last Primary FasterKV
        /// checkpoint to which it recovered. The <see cref="SecondaryIndexBroker{TKVKey, TKVValue}"/> will replay records since then, until the index is up to date with the Primary FasterKV.
        /// </summary>
        /// <param name="secondaryLogToken">A checkpoint token that was returned by <see cref="GetLatestCheckpointToken"/></param>
        /// <param name="undoNextVersion">Whether records with versions beyond checkpoint version need to be undone (and invalidated on log)</param>
        /// <param name="cancellationToken">Allow cancellation of the operation</param>
        /// <returns>A task wrapping the Primary FasterKV checkpoint info stored in the secondary index checkpoint identified by <paramref name="secondaryLogToken"/>, or default if not found.</returns>
        /// <remarks>This is called after the Primary FKV has recovered itself and before it is open for operations.</remarks>
        Task<PrimaryCheckpointInfo> BeginRecoverAsync(Guid secondaryLogToken, bool undoNextVersion, CancellationToken cancellationToken = default);

        /// <summary>
        /// Called when the <see cref="SecondaryIndexBroker{TKVKey, TKVValue}"/> has completed replay of records (if any).
        /// </summary>
        void EndRecover();

        /// <summary>
        /// Called when the Primary FKV has set the new BeginAddress.
        /// </summary>
        /// <param name="newBeginAddress"></param>
        void OnPrimaryTruncate(long newBeginAddress);
    }

    /// <summary>
    /// Interface for a FASTER SecondaryIndex that is derived from the FasterKV Key generic parameter.
    /// </summary>
    public interface ISecondaryKeyIndex<TKVKey> : ISecondaryIndex
    {
        /// <summary>
        /// Inserts a key into the secondary index. Called only for mutable indexes, on the initial insert of a Key. KeyIndexes do not take RecordIds
        /// because they reflect the current value of the primary FasterKV Key.
        /// </summary>
        /// <param name="key">The key to be inserted; always mutable</param>
        /// <param name="recordId">The identifier of the record containing the <paramref name="key"/>; may be used to generate a list</param>
        /// <param name="indexSessionBroker">The <see cref="SecondaryIndexSessionBroker"/> for the primary FasterKV session making this call</param>
        /// <remarks>
        /// If the index is mutable and the <paramref name="key"/> is already there, this call should be ignored, because it is the result
        /// of a race in which the record in the primary FasterKV was updated after the initial insert but before this method
        /// was called.
        /// </remarks>
        void Insert(ref TKVKey key, RecordId recordId, SecondaryIndexSessionBroker indexSessionBroker);

        /// <summary>
        /// Upserts a key into the secondary index. This may be called either immediately during a FasterKV operation, or during recovery.
        /// </summary>
        /// <param name="key">The key to be upserted</param>
        /// <param name="recordId">The identifier of the record containing the <paramref name="key"/></param>
        /// <param name="isMutableRecord">Whether the recordId was in the mutable region of FASTER. If true, the record may subsequently be Upserted or Deleted.</param>
        /// <param name="indexSessionBroker">The <see cref="SecondaryIndexSessionBroker"/> for the primary FasterKV session making this call</param>
        /// <remarks>
        /// For an immutable index, this is the only call made on the interface, when the page containing the <paramref name="key"/> has moved to ReadOnly.
        /// In this case, <paramref name="isMutableRecord"/> is false, and the index may move the <paramref name="key"/> to an immutable storage area.
        /// </remarks>
        void Upsert(ref TKVKey key, RecordId recordId, bool isMutableRecord, SecondaryIndexSessionBroker indexSessionBroker);

        /// <summary>
        /// Removes a key from the secondary index. Called only for mutable indexes.
        /// </summary>
        /// <param name="key">The key to be removed</param>
        /// <param name="recordId">The identifier of the record to be removed for the <paramref name="key"/></param>
        /// <param name="indexSessionBroker">The <see cref="SecondaryIndexSessionBroker"/> for the primary FasterKV session making this call</param>
        void Delete(ref TKVKey key, RecordId recordId, SecondaryIndexSessionBroker indexSessionBroker);

        /// <summary>
        /// Scans a range from a page that has gone readonly.
        /// </summary>
        /// <param name="iter">The iterator over the region that is going readonly</param>
        /// <param name="indexSessionBroker">The <see cref="SecondaryIndexSessionBroker"/> for the primary FasterKV session making this call</param>
        /// <typeparam name="TKVValue">The value type for the <see cref="FasterKV{Key, Value}"/></typeparam>
        void ScanReadOnlyPages<TKVValue>(IFasterScanIterator<TKVKey, TKVValue> iter, SecondaryIndexSessionBroker indexSessionBroker);
    }

    /// <summary>
    /// Interface for a FASTER SecondaryIndex that is derived from the FasterKV Value generic parameter.
    /// </summary>
    public interface ISecondaryValueIndex<TKVKey, TKVValue> : ISecondaryIndex
    {
        /// <summary>
        /// Inserts a recordId into the secondary index, with the associated value from which the index derives its key(s).
        /// Called only for mutable indexes, on the initial insert of a Key.
        /// </summary>
        /// <param name="key">The key for the <paramref name="value"/> being inserted</param>
        /// <param name="value">The value to be inserted; always mutable</param>
        /// <param name="recordId">The identifier of the record containing the <paramref name="value"/></param>
        /// <param name="indexSessionBroker">The <see cref="SecondaryIndexSessionBroker"/> for the primary FasterKV session making this call</param>
        /// <remarks>
        /// If the index is mutable and the <paramref name="recordId"/> is already there for this <paramref name="value"/>,
        /// this call should be ignored, because it is the result of a race in which the record in the primary FasterKV was
        /// updated after the initial insert but before this method was called, so the <paramref name="value"/> on this call
        /// would overwrite it with an obsolete value.
        /// </remarks>
        void Insert(ref TKVKey key, ref TKVValue value, RecordId recordId, SecondaryIndexSessionBroker indexSessionBroker);

        /// <summary>
        /// Upserts a recordId into the secondary index, with the associated value from which the index derives its key(s).
        /// This may be called either immediately during a FasterKV operation, or during recovery.
        /// </summary>
        /// <param name="key">The key for the <paramref name="value"/> being upserted</param>
        /// <param name="value">The value to be upserted</param>
        /// <param name="recordId">The identifier of the record containing the <paramref name="value"/></param>
        /// <param name="isMutableRecord">Whether the recordId was in the mutable region of FASTER; if so, it may subsequently be Upserted or Deleted.</param>
        /// <param name="indexSessionBroker">The <see cref="SecondaryIndexSessionBroker"/> for the primary FasterKV session making this call</param>
        /// <remarks>
        /// For an immutable index, this is the only call made on the interface, when the page containing the <paramref name="recordId"/> has moved to ReadOnly.
        /// In this case, <paramref name="isMutableRecord"/> is false, and the index may move the <paramref name="recordId"/> to an immutable storage area.
        /// </remarks>
        void Upsert(ref TKVKey key, ref TKVValue value, RecordId recordId, bool isMutableRecord, SecondaryIndexSessionBroker indexSessionBroker);

        /// <summary>
        /// Removes a recordId from the secondary index. Called only for mutable indexes.
        /// </summary>
        /// <param name="key">The key for the <paramref name="recordId"/> being deleted</param>
        /// <param name="recordId">The recordId to be removed</param>
        /// <param name="indexSessionBroker">The <see cref="SecondaryIndexSessionBroker"/> for the primary FasterKV session making this call</param>
        void Delete(ref TKVKey key, RecordId recordId, SecondaryIndexSessionBroker indexSessionBroker);

        /// <summary>
        /// Scans a range from a page that has gone readonly.
        /// </summary>
        /// <param name="iter">The iterator over the region that is going readonly</param>
        /// <param name="indexSessionBroker">The <see cref="SecondaryIndexSessionBroker"/> for the primary FasterKV session making this call</param>
        void ScanReadOnlyPages(IFasterScanIterator<TKVKey, TKVValue> iter, SecondaryIndexSessionBroker indexSessionBroker);
    }
}