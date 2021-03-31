// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace FASTER.test
{
    class SimpleIndexBase<TKey>
    {
        protected readonly internal Dictionary<TKey, List<long>> MutableRecords = new Dictionary<TKey, List<long>>();
        protected readonly internal Dictionary<TKey, List<long>> ImmutableRecords = new Dictionary<TKey, List<long>>();
        private readonly Dictionary<long, TKey> reverseLookup = new Dictionary<long, TKey>();

        protected internal long sessionSlot = 0;
        private readonly bool isKeyIndex;
        private readonly string indexType;
        private Guid sessionId = Guid.Empty;
        readonly Func<TKey, TKey> indexKeyFunc;

        protected SimpleIndexBase(string name, bool isKeyIndex, Func<TKey, TKey> indexKeyFunc, bool isMutableIndex)
        {
            this.Name = name;
            this.IsMutable = isMutableIndex;
            this.isKeyIndex = isKeyIndex;
            this.indexType = isKeyIndex ? "KeyIndex" : "ValueIndex";
            this.indexKeyFunc = indexKeyFunc;
        }

        public string Name { get; private set; }

        public bool IsMutable { get; private set; }

        public void SetSessionSlot(long slot) => this.sessionSlot = slot;

        // Name methods "BaseXxx" instead of using virtuals so we don't get unwanted implementations, e.g. of Delete taking a key for the ValueIndex.

        internal void BaseDelete(long recordId, SecondaryIndexSessionBroker indexSessionBroker)
        {
            Assert.IsFalse(this.isKeyIndex);
            if (!reverseLookup.TryGetValue(recordId, out TKey key))
                Assert.Fail($"RecordId {recordId} not found in revserse lookup for {indexType}");
            DoDelete(ref key, recordId, indexSessionBroker);
        }

        internal void BaseDelete(ref TKey rawKey, long recordId, SecondaryIndexSessionBroker indexSessionBroker)
            => DoDelete(ref rawKey, recordId, indexSessionBroker);

        private void DoDelete(ref TKey rawKey, long recordId, SecondaryIndexSessionBroker indexSessionBroker)
        {
            var key = this.indexKeyFunc(rawKey);
            VerifyNotImmutable(ref key, recordId);
            VerifySession(indexSessionBroker);
            if (!MutableRecords.ContainsKey(key))
                Assert.Fail($"{indexType} '{key}' not found as index key");
            MutableRecords.Remove(key);

            Assert.IsTrue(reverseLookup.ContainsKey(recordId));
            reverseLookup.Remove(recordId);
        }

        internal void BaseInsert(ref TKey rawKey, long recordId, SecondaryIndexSessionBroker indexSessionBroker)
        {
            var key = this.indexKeyFunc(rawKey);
            VerifyNotImmutable(ref key, recordId);
            VerifySession(indexSessionBroker);
            if (!MutableRecords.TryGetValue(key, out var recordIds))
            {
                recordIds = new List<long>();
                MutableRecords[key] = recordIds;
            }
            else if (recordIds.Contains(recordId))
            {
                return;
            }

            recordIds.Add(recordId);
            AddToReverseLookup(ref key, recordId);
        }

        internal void BaseUpsert(ref TKey rawKey, long recordId, bool isMutableRecord, SecondaryIndexSessionBroker indexSessionBroker)
        {
            if (isMutableRecord)
            {
                BaseInsert(ref rawKey, recordId, indexSessionBroker);
                return;
            }

            // Move from mutable to immutable
            var key = this.indexKeyFunc(rawKey);
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
                recordIds = new List<long>();
                ImmutableRecords[key] = recordIds;
            }
            recordIds.Add(recordId);
            AddToReverseLookup(ref key, recordId);
        }

        private void VerifySession(SecondaryIndexSessionBroker indexSessionBroker, bool isMutableRecord = true)
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

        private readonly List<long> emptyRecordList = new List<long>();

        internal long[] Query(TKey rawKey)
        {
            var key = this.indexKeyFunc(rawKey);
            if (!MutableRecords.TryGetValue(key, out var mutableRecordList))
                mutableRecordList = emptyRecordList;
            if (!ImmutableRecords.TryGetValue(key, out var immutableRecordList))
                immutableRecordList = emptyRecordList;

            return mutableRecordList.Concat(immutableRecordList).ToArray();
        }

        private void VerifyNotImmutable(ref TKey key, long recordId)
        {
            if (ImmutableRecords.TryGetValue(key, out var recordIds) && recordIds.Contains(recordId))
                Assert.Fail($"Unexpected update of recordId {recordId} for {indexType} '{key}'");
        }

        private void AddToReverseLookup(ref TKey key, long recordId)
        {
            if (reverseLookup.TryGetValue(recordId, out TKey existingKey))
            {
                if (!existingKey.Equals(key))
                    Assert.Fail($"Unexpected update of recordId {recordId} for {indexType} '{key}'");
                return;
            }
            reverseLookup[recordId] = key;
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
                    // TODO: UpsertAsync
                    //if (isAsync)
                    //    session.UpsertAsync(ref key, ref value).GetAwaiter().GetResult().Complete();
                    //else
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
                    // TODO: UpsertAsync
                    //if (isAsync)
                    //    session.UpsertAsync(ref key, ref value).GetAwaiter().GetResult().Complete();
                    //else
                    session.Upsert(ref key, ref value);
                }
            }

            // Make sure operations are completed
            session.CompletePending(true);
        }

        internal static void VerifyMutableIndex(ISecondaryIndex secondaryIndex, int indexKeyDivisor, int queryKeyOffset)
        {
            var indexBase = secondaryIndex as SimpleIndexBase<int>;
            Assert.AreEqual(NumKeys / indexKeyDivisor, indexBase.MutableRecords.Count);
            Assert.AreEqual(0, indexBase.ImmutableRecords.Count);

            var records = indexBase.Query(42 + queryKeyOffset);
            Assert.AreEqual(indexKeyDivisor, records.Length);
        }

        internal static void VerifyImmutableIndex(ISecondaryIndex secondaryIndex, int indexKeyDivisor, int queryKeyOffset, PrimaryFasterKV store)
        {
            var indexBase = secondaryIndex as SimpleIndexBase<int>;
            Assert.AreEqual(0, indexBase.MutableRecords.Count);
            Assert.AreEqual(0, indexBase.ImmutableRecords.Count);

            store.Checkpoint();
            Assert.AreEqual(0, indexBase.MutableRecords.Count);
            Assert.AreEqual(NumKeys / indexKeyDivisor, indexBase.ImmutableRecords.Count);

            var records = indexBase.Query(42 + queryKeyOffset);
            Assert.AreEqual(indexKeyDivisor, records.Length);
        }

        internal static void VerifyMixedIndexes(ISecondaryIndex mutableIndex, ISecondaryIndex immutableIndex, int indexKeyDivisor, int queryKeyOffset, PrimaryFasterKV store)
        {
            var mutableIndexBase = mutableIndex as SimpleIndexBase<int>;
            var immutableIndexBase = immutableIndex as SimpleIndexBase<int>;
            Assert.AreEqual(NumKeys / indexKeyDivisor, mutableIndexBase.MutableRecords.Count);
            Assert.AreEqual(0, mutableIndexBase.ImmutableRecords.Count);
            Assert.AreEqual(0, immutableIndexBase.MutableRecords.Count);
            Assert.AreEqual(0, immutableIndexBase.ImmutableRecords.Count);

            store.Checkpoint();
            Assert.AreEqual(0, mutableIndexBase.MutableRecords.Count);
            Assert.AreEqual(NumKeys / indexKeyDivisor, mutableIndexBase.ImmutableRecords.Count);
            Assert.AreEqual(0, immutableIndexBase.MutableRecords.Count);
            Assert.AreEqual(NumKeys / indexKeyDivisor, immutableIndexBase.ImmutableRecords.Count);

            var records = mutableIndexBase.Query(42 + queryKeyOffset);
            Assert.AreEqual(indexKeyDivisor, records.Length);

            records = immutableIndexBase.Query(42 + queryKeyOffset);
            Assert.AreEqual(indexKeyDivisor, records.Length);
        }
    }
}
