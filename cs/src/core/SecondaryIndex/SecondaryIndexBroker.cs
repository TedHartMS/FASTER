// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

#pragma warning disable IDE0056 // Use index operator (^ is not supported on .NET Framework or NETCORE pre-3.0)

namespace FASTER.core
{
    /// <summary>
    /// Manages the list of secondary indexes in the FasterKV.
    /// </summary>
    public class SecondaryIndexBroker<TKVKey, TKVValue>
    {
        private ISecondaryKeyIndex<TKVKey>[] allKeyIndexes;
        private ISecondaryValueIndex<TKVKey, TKVValue>[] allValueIndexes;

        // Use arrays for faster traversal.
        private ISecondaryKeyIndex<TKVKey>[] mutableKeyIndexes = Array.Empty<ISecondaryKeyIndex<TKVKey>>();
        internal int MutableKeyIndexCount => mutableKeyIndexes.Length;

        private ISecondaryValueIndex<TKVKey, TKVValue>[] mutableValueIndexes = Array.Empty<ISecondaryValueIndex<TKVKey, TKVValue>>();
        internal int MutableValueIndexCount => mutableValueIndexes.Length;

        readonly object membershipLock = new object();
        
        readonly FasterKV<TKVKey, TKVValue> primaryFkv;
        IDisposable logSubscribeDisposable; // Used if we implement index removal, if we go to zero indexes; Dispose() and null this then.

        internal SecondaryIndexBroker(FasterKV<TKVKey, TKVValue> pFkv) => this.primaryFkv = pFkv;

        /// <summary>
        /// Adds a secondary index to the list.
        /// </summary>
        /// <param name="index"></param>
        public void AddIndex(ISecondaryIndex index)
        {
            bool isMutable = false;

            void AppendToArray<TIndex>(ref TIndex[] vec, TIndex idx)
            {
                var resizedVec = new TIndex[vec is null ? 1 : vec.Length + 1];
                if (vec is { })
                    Array.Copy(vec, resizedVec, vec.Length);
                resizedVec[resizedVec.Length - 1] = idx;
                vec = resizedVec;
            }

            bool addSpecific<TIndex>(ref TIndex[] allVec, ref TIndex[] mutableVec, TIndex idx)
                where TIndex : ISecondaryIndex
            {
                if (idx is null)
                    return false;
                if (idx.IsMutable)
                {
                    AppendToArray(ref mutableVec, idx);
                    isMutable = true;
                }
                AppendToArray(ref allVec, idx);
                return true;
            }

            lock (membershipLock)
            {
                if (!addSpecific(ref allKeyIndexes, ref mutableKeyIndexes, index as ISecondaryKeyIndex<TKVKey>)
                    && !addSpecific(ref allValueIndexes, ref mutableValueIndexes, index as ISecondaryValueIndex<TKVKey, TKVValue>))
                    throw new SecondaryIndexException("Object is not a KeyIndex or ValueIndex");
                this.HasMutableIndexes |= isMutable;    // Note: removing indexes will have to recalculate this
                index.SetSessionSlot(SecondaryIndexSessionBroker.NextSessionSlot++);

                if (logSubscribeDisposable is null)
                    logSubscribeDisposable = primaryFkv.Log.Subscribe(new ReadOnlyObserver<TKVKey, TKVValue>(primaryFkv.SecondaryIndexBroker));
            }
        }

        /// <summary>
        /// The number of indexes registered.
        /// </summary>
        public int Count => allKeyIndexes.Length + allValueIndexes.Length;

        /// <summary>
        /// The number of indexes registered.
        /// </summary>
        public bool HasMutableIndexes { get; private set; }

        // On failure of an operation, a SecondaryIndexException is thrown by the Index

        #region Mutable KeyIndexes
        /// <summary>
        /// Inserts a mutable key into all mutable secondary key indexes.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Insert(ref TKVKey key, RecordId recordId, SecondaryIndexSessionBroker indexSessionBroker)
        {
            var mki = this.mutableKeyIndexes;
            foreach (var keyIndex in mki)
                keyIndex.Insert(ref key, recordId, indexSessionBroker);
        }

        /// <summary>
        /// Upserts a mutable key into all mutable secondary key indexes.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Upsert(ref TKVKey key, RecordId recordId, SecondaryIndexSessionBroker indexSessionBroker)
        {
            var mki = this.mutableKeyIndexes;
            foreach (var keyIndex in mki)
                keyIndex.Upsert(ref key, recordId, isMutableRecord: false, indexSessionBroker);
        }
        #endregion Mutable KeyIndexes

        #region Mutable ValueIndexes
        /// <summary>
        /// Inserts a recordId keyed by a mutable value into all mutable secondary value indexes.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Insert(ref TKVKey key, ref TKVValue value, RecordId recordId, SecondaryIndexSessionBroker indexSessionBroker)
        {
            var mvi = this.mutableValueIndexes;
            foreach (var valueIndex in mvi)
                valueIndex.Insert(ref key, ref value, recordId, indexSessionBroker);
        }

        /// <summary>
        /// Upserts a recordId keyed by a mutable value into all mutable secondary value indexes.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Upsert(ref TKVKey key, ref TKVValue value, RecordId recordId, SecondaryIndexSessionBroker indexSessionBroker)
        {
            var mvi = this.mutableValueIndexes;
            foreach (var valueIndex in mvi)
                valueIndex.Upsert(ref key, ref value, recordId, isMutableRecord:false, indexSessionBroker);
        }
        #endregion Mutable ValueIndexes

        #region Mutable Key and Value Indexes

        /// <summary>
        /// Deletes recordId for a key from all mutable secondary key indexes.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Delete(ref TKVKey key, RecordId recordId, SecondaryIndexSessionBroker indexSessionBroker)
        {
            var mki = this.mutableKeyIndexes;
            foreach (var keyIndex in mki)
                keyIndex.Delete(ref key, recordId, indexSessionBroker);

            var mvi = this.mutableValueIndexes;
            foreach (var valueIndex in mvi)
                valueIndex.Delete(ref key, recordId, indexSessionBroker);
        }
        #endregion Mutable Key and Value Indexes

        /// <summary>
        /// Upserts a readonly key into all secondary key indexes and readonly values into secondary value indexes.
        /// </summary>
        internal void ScanReadOnlyPages(IFasterScanIterator<TKVKey, TKVValue> iter, SecondaryIndexSessionBroker indexSessionBroker)
        {
            var inputIter = iter;

            IFasterScanIterator<TKVKey, TKVValue> GetIter()
            {
                var localIter = inputIter is null
                                   ? primaryFkv.Log.Scan(iter.BeginAddress, iter.EndAddress, ScanBufferingMode.NoBuffering)
                                   : inputIter;
                inputIter = null;
                return localIter;
            }

            void ReleaseIter(IFasterScanIterator<TKVKey, TKVValue> localIter)
            {
                if (localIter != iter)
                    localIter.Dispose();
            }

            // TODO: Parallelize ScanReadOnlyPages
            var ki = this.allKeyIndexes;
            if (ki is { })
            {
                foreach (var keyIndex in ki)
                {
                    var localIter = GetIter();
                    keyIndex.ScanReadOnlyPages(localIter, indexSessionBroker);
                    ReleaseIter(localIter);
                }
            }
            var vi = this.allValueIndexes;
            if (vi is { })
            {
                foreach (var valueIndex in vi)
                {
                    var localIter = GetIter();
                    valueIndex.ScanReadOnlyPages(localIter, indexSessionBroker);
                    ReleaseIter(localIter);
                }
            }
        }

        internal byte[] GetLatestCheckpointMetadata()
        {
            var checkpointMetadata = new CheckpointMetadata();

            var ki = this.allKeyIndexes;
            if (ki is { })
            {
                foreach (var keyIndex in ki)
                    checkpointMetadata.Append(keyIndex.Id, keyIndex.GetLatestCheckpointToken());
            }
            var vi = this.allValueIndexes;
            if (vi is { })
            {
                foreach (var valueIndex in vi)
                    checkpointMetadata.Append(valueIndex.Id, valueIndex.GetLatestCheckpointToken());
            }

            return checkpointMetadata.Flatten();
        }

        internal void OnPrimaryCheckpointComplete(PrimaryCheckpointInfo pci)
        {
            var ki = this.allKeyIndexes;
            if (ki is { })
            {
                foreach (var keyIndex in ki)
                    keyIndex.OnPrimaryCheckpointCompleted(pci);
            }
            var vi = this.allValueIndexes;
            if (vi is { })
            {
                foreach (var valueIndex in vi)
                    valueIndex.OnPrimaryCheckpointCompleted(pci);
            }
        }

        internal void Recover(byte[] indexMetadata)
        {
            // This is called during recovery, before the PrimaryFKV is open for operations, so we do not have to worry about things changing
            // We're not operating in the context of a FasterKV session, so we need our own sessionBroker.
            using var indexSessionBroker = new SecondaryIndexSessionBroker();

            var checkpointMetadata = new CheckpointMetadata(indexMetadata);
            var tasks = new List<Task>();

            var ki = this.allKeyIndexes;
            if (ki is { })
            {
                foreach (var keyIndex in ki)
                    tasks.Add(Task.Run(() => RecoverIndex(keyIndex, default, checkpointMetadata.GetToken(keyIndex.Id), indexSessionBroker)));
            }
            var vi = this.allValueIndexes;
            if (vi is { })
            {
                foreach (var valueIndex in vi)
                    tasks.Add(Task.Run(() => RecoverIndex(default, valueIndex, checkpointMetadata.GetToken(valueIndex.Id), indexSessionBroker)));
            }

            Task.WaitAll(tasks.ToArray());
        }

        internal Task RecoverAsync(byte[] indexMetadata)
        {
            // This is called during recovery, before the PrimaryFKV is open for operations, so we do not have to worry about things changing
            // We're not operating in the context of a FasterKV session, so we need our own sessionBroker.
            using var indexSessionBroker = new SecondaryIndexSessionBroker();

            var checkpointMetadata = new CheckpointMetadata(indexMetadata);
            var tasks = new List<Task>();

            var ki = this.allKeyIndexes;
            if (ki is { })
            {
                foreach (var keyIndex in ki)
                    tasks.Add(RecoverIndexAsync(keyIndex, default, checkpointMetadata.GetToken(keyIndex.Id), indexSessionBroker));
            }
            var vi = this.allValueIndexes;
            if (vi is { })
            {
                foreach (var valueIndex in vi)
                    tasks.Add(RecoverIndexAsync(default, valueIndex, checkpointMetadata.GetToken(valueIndex.Id), indexSessionBroker));
            }

            return Task.WhenAll(tasks.ToArray());
        }

        void RecoverIndex(ISecondaryKeyIndex<TKVKey> keyIndex, ISecondaryValueIndex<TKVKey, TKVValue> valueIndex, Guid checkpointToken, SecondaryIndexSessionBroker indexSessionBroker)
        {
            var pci = checkpointToken != Guid.Empty ? ((ISecondaryIndex)keyIndex ?? valueIndex).BeginRecover(checkpointToken) : default;
            RollIndexForward(keyIndex, valueIndex, pci, indexSessionBroker);
        }

        async Task RecoverIndexAsync(ISecondaryKeyIndex<TKVKey> keyIndex, ISecondaryValueIndex<TKVKey, TKVValue> valueIndex, Guid checkpointToken, SecondaryIndexSessionBroker indexSessionBroker)
        {
            var pci = checkpointToken != Guid.Empty ? await ((ISecondaryIndex)keyIndex ?? valueIndex).BeginRecoverAsync(checkpointToken) : default;
            await Task.Run(() => RollIndexForward(keyIndex, valueIndex, pci, indexSessionBroker));
        }

        private void RollIndexForward(ISecondaryKeyIndex<TKVKey> keyIndex, ISecondaryValueIndex<TKVKey, TKVValue> valueIndex, PrimaryCheckpointInfo pci, SecondaryIndexSessionBroker indexSessionBroker)
        {
            if (pci.FlushedUntilAddress < primaryFkv.Log.TailAddress)
            {
                using var iter = primaryFkv.Log.Scan(pci.FlushedUntilAddress, primaryFkv.Log.TailAddress, ScanBufferingMode.NoBuffering);
                if (keyIndex is { })
                {
                    while (iter.GetNext(out var recordInfo))
                        keyIndex.Upsert(ref iter.GetKey(), new RecordId(recordInfo, iter.CurrentAddress), isMutableRecord: false, indexSessionBroker);
                }
                else
                {
                    while (iter.GetNext(out var recordInfo))
                        valueIndex.Upsert(ref iter.GetKey(), ref iter.GetValue(), new RecordId(recordInfo, iter.CurrentAddress), isMutableRecord: false, indexSessionBroker);
                }
            }
        }
    }
}
