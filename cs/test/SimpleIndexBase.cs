// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.test.SecondaryIndex.SimpleIndex
{
    class SimpleIndexBase<TKey>
    {
        protected internal long sessionSlot = 0;
        protected readonly string indexType;
        private Guid sessionId = Guid.Empty;

        protected SimpleIndexBase(string name, bool isKeyIndex, bool isMutableIndex)
        {
            this.Name = name;
            this.IsMutable = isMutableIndex;
            this.indexType = isKeyIndex ? "KeyIndex" : "ValueIndex";
        }

        public string Name { get; private set; }

        public bool IsMutable { get; private set; }

        public void SetSessionSlot(long slot) => this.sessionSlot = slot;

        protected void VerifySession(SecondaryIndexSessionBroker indexSessionBroker, bool isMutableRecord = true)
        {
            Assert.IsNotNull(indexSessionBroker);
            var sessionObject = indexSessionBroker.GetSessionObject(this.sessionSlot);

            // For these tests, we will always do all mutable inserts before checkpointing and getting readonly inserts.
            // The readonly inserts come from a different SecondaryIndexSessionBroker (owned by the SecondaryIndexBroker
            // for the ReadOnlyObserver), so we expect not to find a session there on the first call.
            if (isMutableRecord)
                Assert.AreEqual(sessionObject is null, this.sessionId == Guid.Empty);

            if (!(sessionObject is SimpleIndexSession session))
            {
                if (sessionObject is { })
                    Assert.Fail($"Unexpected session object type {sessionObject.GetType().Name} for {indexType}");

                if (this.sessionId == Guid.Empty)
                    this.sessionId = Guid.NewGuid();
                session = new SimpleIndexSession() { Id = this.sessionId };
                indexSessionBroker.SetSessionObject(this.sessionSlot, session);
            }
            Assert.AreEqual(this.sessionId, session.Id);
        }

        public void OnPrimaryCheckpointInitiated(PrimaryCheckpointInfo recoveredPCI) { }

        public void OnPrimaryCheckpointCompleted(PrimaryCheckpointInfo primaryCheckpointInfo) { }

        public PrimaryCheckpointInfo Recover(PrimaryCheckpointInfo recoveredPCI, bool undoNextVersion) => default;

        public Task<PrimaryCheckpointInfo> RecoverAsync(PrimaryCheckpointInfo recoveredPCI, bool undoNextVersion, CancellationToken cancellationToken = default) => default;
    }

    class SimpleKeyIndexBase<TKey> : SimpleIndexBase<TKey>
        where TKey : IComparable
    {
        // Value is IsMutable
        protected internal readonly Dictionary<TKey, bool> keys = new Dictionary<TKey, bool>();

        protected SimpleKeyIndexBase(string name, bool isMutableIndex)
            : base(name, isKeyIndex: true, isMutableIndex)
        {
        }

        // Name methods "BaseXxx" instead of using virtuals so we don't get unwanted implementations, e.g. of Delete taking a key for the ValueIndex.

        internal void BaseDelete(ref TKey key, SecondaryIndexSessionBroker indexSessionBroker)
        {
            VerifySession(indexSessionBroker);

            Assert.IsTrue(this.keys.TryGetValue(key, out bool isMutable));
            Assert.IsFalse(isMutable);
            this.keys.Remove(key);
        }

        private void UpdateKey(ref TKey key, bool isMutable, SecondaryIndexSessionBroker indexSessionBroker)
        {
            VerifySession(indexSessionBroker, isMutable);

            // Key indexes just track the key, and a mutable record for a key may be added after the page for its previous record goes immutable.
            this.keys[key] = isMutable;
        }

        internal void BaseInsert(ref TKey key, SecondaryIndexSessionBroker indexSessionBroker)
            => UpdateKey(ref key, isMutable: true, indexSessionBroker);

        internal void BaseUpsert(ref TKey key, bool isMutableRecord, SecondaryIndexSessionBroker indexSessionBroker)
            => UpdateKey(ref key, isMutableRecord, indexSessionBroker);

        // Pseudo-range index
        internal void Query(TKey from, TKey to, int expectedCount)
        {
            var keysFound = this.keys.Keys.Where(k => k.CompareTo(from) >= 0 && k.CompareTo(to) <= 0).OrderBy(k => k).ToArray();
            Assert.AreEqual(expectedCount, keysFound.Length);
        }

        internal static void VerifyIntIndex(ISecondaryIndex index, PrimaryFasterKV store)
        {
            const int from = 42, to = 101;
            var indexBase = index as SimpleKeyIndexBase<int>;
            Assert.AreEqual(indexBase.IsMutable ? SimpleIndexUtils.NumKeys : 0, indexBase.keys.Count);

            if (store is null)
            {
                Assert.IsFalse(indexBase.keys.Values.Any(isMutable => !isMutable));
                indexBase.Query(from, to, to - from + 1);
                return;
            }

            store.Checkpoint();
            Assert.IsFalse(indexBase.keys.Values.Any(isMutable => isMutable));
            indexBase.Query(from, to, to - from + 1);
        }

        internal static void VerifyMixedIntIndexes(ISecondaryIndex mutableIndex, ISecondaryIndex immutableIndex, PrimaryFasterKV store)
        {
            const int from = 42, to = 101;
            var mutableIndexBase = mutableIndex as SimpleKeyIndexBase<int>;
            var immutableIndexBase = immutableIndex as SimpleKeyIndexBase<int>;
            Assert.AreEqual(SimpleIndexUtils.NumKeys, mutableIndexBase.keys.Count);
            Assert.AreEqual(0, immutableIndexBase.keys.Count);

            store.Checkpoint();
            Assert.AreEqual(SimpleIndexUtils.NumKeys, mutableIndexBase.keys.Count);
            Assert.AreEqual(SimpleIndexUtils.NumKeys, immutableIndexBase.keys.Count);

            mutableIndexBase.Query(from, to, to - from + 1);
            immutableIndexBase.Query(from, to, to - from + 1);
        }
    }

    class SimpleValueIndexBase<TValue> : SimpleIndexBase<TValue>
    {
        protected internal readonly Dictionary<TValue, List<RecordId>> MutableRecords = new Dictionary<TValue, List<RecordId>>();
        protected internal readonly Dictionary<TValue, List<RecordId>> ImmutableRecords = new Dictionary<TValue, List<RecordId>>();
        private readonly Dictionary<RecordId, TValue> reverseLookup = new Dictionary<RecordId, TValue>();
        readonly Func<TValue, TValue> valueIndexKeyFunc;

        protected SimpleValueIndexBase(string name, Func<TValue, TValue> indexKeyFunc, bool isMutableIndex)
            : base(name, isKeyIndex: false, isMutableIndex)
        {
            this.valueIndexKeyFunc = indexKeyFunc;
        }

        // Name methods "BaseXxx" instead of using virtuals so we don't get unwanted implementations, e.g. of Delete taking a key for the ValueIndex.

        internal void BaseDelete(RecordId recordId, SecondaryIndexSessionBroker indexSessionBroker)
        {
            Assert.IsFalse(recordId.IsDefault());
            VerifySession(indexSessionBroker);
            if (!reverseLookup.TryGetValue(recordId, out TValue key))
                Assert.Fail($"RecordId {recordId} not found in revserse lookup for {indexType}");

            VerifyNotImmutable(ref key, recordId);
            if (!MutableRecords.ContainsKey(key))
                Assert.Fail($"{indexType} '{key}' not found as index key");
            MutableRecords.Remove(key);

            Assert.IsTrue(reverseLookup.ContainsKey(recordId));
            reverseLookup.Remove(recordId);
        }

        internal void BaseInsert(ref TValue rawKey, RecordId recordId, SecondaryIndexSessionBroker indexSessionBroker)
        {
            VerifySession(indexSessionBroker);
            Assert.IsFalse(recordId.IsDefault());
            var key = this.valueIndexKeyFunc(rawKey);
            VerifyNotImmutable(ref key, recordId);
            if (!MutableRecords.TryGetValue(key, out var recordIds))
            {
                recordIds = new List<RecordId>();
                MutableRecords[key] = recordIds;
            }
            else if (recordIds.Contains(recordId))
            {
                return;
            }
            recordIds.Add(recordId);
            AddToReverseLookup(ref key, recordId);
        }

        internal void BaseUpsert(ref TValue rawKey, RecordId recordId, bool isMutableRecord, SecondaryIndexSessionBroker indexSessionBroker)
        {
            VerifySession(indexSessionBroker, isMutableRecord);
            if (isMutableRecord)
            {
                BaseInsert(ref rawKey, recordId, indexSessionBroker);
                return;
            }

            // Move from mutable to immutable
            var key = this.valueIndexKeyFunc(rawKey);
            VerifyNotImmutable(ref key, recordId);
            VerifySession(indexSessionBroker, isMutableRecord);
            if (MutableRecords.TryGetValue(key, out var recordIds) && recordIds.Contains(recordId))
            {
                recordIds.Remove(recordId);
                if (recordIds.Count == 0)
                    MutableRecords.Remove(key);
            }

            if (!ImmutableRecords.TryGetValue(key, out recordIds))
            {
                recordIds = new List<RecordId>();
                ImmutableRecords[key] = recordIds;
            }
            recordIds.Add(recordId);
            AddToReverseLookup(ref key, recordId);
        }

        private readonly List<RecordId> emptyRecordList = new List<RecordId>();

        internal RecordId[] Query(TValue rawKey)
        {
            var key = this.valueIndexKeyFunc(rawKey);
            if (!MutableRecords.TryGetValue(key, out var mutableRecordList))
                mutableRecordList = emptyRecordList;
            if (!ImmutableRecords.TryGetValue(key, out var immutableRecordList))
                immutableRecordList = emptyRecordList;

            return mutableRecordList.Concat(immutableRecordList).ToArray();
        }

        private void VerifyNotImmutable(ref TValue key, RecordId recordId)
        {
            if (ImmutableRecords.TryGetValue(key, out var recordIds) && recordIds.Contains(recordId))
                Assert.Fail($"Unexpected update of recordId {recordId} for {indexType} '{key}'");
        }

        private void AddToReverseLookup(ref TValue key, RecordId recordId)
        {
            if (reverseLookup.TryGetValue(recordId, out TValue existingKey))
            {
                if (!existingKey.Equals(key))
                    Assert.Fail($"Unexpected update of recordId {recordId} for {indexType} '{key}'");
                return;
            }
            reverseLookup[recordId] = key;
        }

        internal static void VerifyMutableIntIndex(ISecondaryIndex secondaryIndex, int indexKeyDivisor, int queryKeyOffset)
        {
            var indexBase = secondaryIndex as SimpleValueIndexBase<int>;
            Assert.AreEqual(SimpleIndexUtils.NumKeys / indexKeyDivisor, indexBase.MutableRecords.Count);
            Assert.AreEqual(0, indexBase.ImmutableRecords.Count);

            var records = indexBase.Query(42 + queryKeyOffset);
            Assert.AreEqual(indexKeyDivisor, records.Length);
        }

        internal static void VerifyImmutableIntIndex(ISecondaryIndex secondaryIndex, int indexKeyDivisor, int queryKeyOffset, PrimaryFasterKV store)
        {
            var indexBase = secondaryIndex as SimpleValueIndexBase<int>;
            Assert.AreEqual(0, indexBase.MutableRecords.Count);
            Assert.AreEqual(0, indexBase.ImmutableRecords.Count);

            store.Checkpoint();
            Assert.AreEqual(0, indexBase.MutableRecords.Count);
            Assert.AreEqual(SimpleIndexUtils.NumKeys / indexKeyDivisor, indexBase.ImmutableRecords.Count);

            var records = indexBase.Query(42 + queryKeyOffset);
            Assert.AreEqual(indexKeyDivisor, records.Length);
        }

        internal static void VerifyMixedIntIndexes(ISecondaryIndex mutableIndex, ISecondaryIndex immutableIndex, int indexKeyDivisor, int queryKeyOffset, PrimaryFasterKV store)
        {
            var mutableIndexBase = mutableIndex as SimpleValueIndexBase<int>;
            var immutableIndexBase = immutableIndex as SimpleValueIndexBase<int>;
            Assert.AreEqual(SimpleIndexUtils.NumKeys / indexKeyDivisor, mutableIndexBase.MutableRecords.Count);
            Assert.AreEqual(0, mutableIndexBase.ImmutableRecords.Count);
            Assert.AreEqual(0, immutableIndexBase.MutableRecords.Count);
            Assert.AreEqual(0, immutableIndexBase.ImmutableRecords.Count);

            store.Checkpoint();
            Assert.AreEqual(0, mutableIndexBase.MutableRecords.Count);
            Assert.AreEqual(SimpleIndexUtils.NumKeys / indexKeyDivisor, mutableIndexBase.ImmutableRecords.Count);
            Assert.AreEqual(0, immutableIndexBase.MutableRecords.Count);
            Assert.AreEqual(SimpleIndexUtils.NumKeys / indexKeyDivisor, immutableIndexBase.ImmutableRecords.Count);

            var records = mutableIndexBase.Query(42 + queryKeyOffset);
            Assert.AreEqual(indexKeyDivisor, records.Length);

            records = immutableIndexBase.Query(42 + queryKeyOffset);
            Assert.AreEqual(indexKeyDivisor, records.Length);
        }
    }

    internal class SimpleIndexSession
    {
        public Guid Id { get; set; }
    }

    internal class PrimaryFasterKV
    {
        private string testPath;
        internal FasterKV<int, int> fkv;
        private IDevice log;

        internal void Setup()
        {
            testPath = TestContext.CurrentContext.TestDirectory + "/" + Path.GetRandomFileName();
            if (!Directory.Exists(testPath))
                Directory.CreateDirectory(testPath);

            log = Devices.CreateLogDevice(testPath + $"/{TestContext.CurrentContext.Test.Name}.log", false);

            fkv = new FasterKV<int, int>(SimpleIndexUtils.KeySpace, new LogSettings { LogDevice = log },
                    new CheckpointSettings { CheckpointDir = testPath, CheckPointType = CheckpointType.FoldOver }
            );
        }

        internal void Populate(bool useAdvancedFunctions, bool useRMW, bool isAsync)
        {
            if (useAdvancedFunctions)
                SimpleIndexUtils.PopulateIntsWithAdvancedFunctions(this.fkv, useRMW, isAsync);
            else
                SimpleIndexUtils.PopulateInts(this.fkv, useRMW, isAsync);
        }

        internal void Checkpoint()
        {
            this.fkv.TakeFullCheckpoint(out _);
            this.fkv.CompleteCheckpointAsync().GetAwaiter().GetResult();
        }

        internal void TearDown()
        {
            fkv.Dispose();
            fkv = null;
            log.Dispose();
            TestUtils.DeleteDirectory(testPath);
        }
    }

    static class SimpleIndexUtils
    {
        internal const int NumKeys = 2_000;
        internal const long KeySpace = 1L << 14;

        internal const int ValueStart = 10_000;

        internal static void PopulateInts(FasterKV<int, int> fkv, bool useRMW, bool isAsync)
        {
            using var session = fkv.NewSession(new SimpleFunctions<int, int>());

            // Prpcess the batch of input data
            for (int key = 0; key < NumKeys; key++)
            {
                var value = key + ValueStart;
                if (useRMW)
                {
                    if (isAsync)
                        session.RMWAsync(ref key, ref value).GetAwaiter().GetResult().Complete();
                    else
                        session.RMW(ref key, ref value);
                }
                else
                {
                    if (isAsync)
                        session.UpsertAsync(ref key, ref value).GetAwaiter().GetResult().Complete();
                    else
                        session.Upsert(ref key, ref value);
                }
            }

            // Make sure operations are completed
            session.CompletePending(true);
        }

        internal static void PopulateIntsWithAdvancedFunctions(FasterKV<int, int> fkv, bool useRMW, bool isAsync)
        {
            using var session = fkv.NewSession(new AdvancedSimpleFunctions<int, int>());

            // Prpcess the batch of input data
            for (int key = 0; key < NumKeys; key++)
            {
                var value = key + ValueStart;
                if (useRMW)
                {
                    if (isAsync)
                        session.RMWAsync(ref key, ref value).GetAwaiter().GetResult().Complete();
                    else
                        session.RMW(ref key, ref value);
                }
                else
                {
                    if (isAsync)
                        session.UpsertAsync(ref key, ref value).GetAwaiter().GetResult().Complete();
                    else
                        session.Upsert(ref key, ref value);
                }
            }

            // Make sure operations are completed
            session.CompletePending(true);
        }
    }
}
