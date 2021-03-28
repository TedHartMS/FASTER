// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;

namespace FASTER.test
{
    class SimpleIndexBase<TKey>
    {
        protected internal Dictionary<TKey, List<long>> mutableRecords = new Dictionary<TKey, List<long>>();
        protected internal Dictionary<TKey, List<long>> immutableRecords = new Dictionary<TKey, List<long>>();
        protected internal Dictionary<long, TKey> reverseLookup = new Dictionary<long, TKey>();

        protected internal long sessionSlot = 0;
        private bool isKeyIndex;
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
            if (!mutableRecords.ContainsKey(key))
                Assert.Fail($"{indexType} '{key}' not found as index key");
            mutableRecords.Remove(key);

            Assert.IsTrue(reverseLookup.ContainsKey(recordId));
            reverseLookup.Remove(recordId);
        }

        internal void BaseInsert(ref TKey rawKey, long recordId, SecondaryIndexSessionBroker indexSessionBroker)
        {
            var key = this.indexKeyFunc(rawKey);
            VerifyNotImmutable(ref key, recordId);
            VerifySession(indexSessionBroker);
            if (!mutableRecords.TryGetValue(key, out var recordIds))
            {
                recordIds = new List<long>();
                mutableRecords[key] = recordIds;
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
            VerifySession(indexSessionBroker);
            if (mutableRecords.TryGetValue(key, out var recordIds) && recordIds.Contains(recordId))
                recordIds.Remove(recordId);

            if (!immutableRecords.TryGetValue(key, out recordIds))
            {
                recordIds = new List<long>();
                mutableRecords[key] = recordIds;
            }
            recordIds.Add(recordId);
            AddToReverseLookup(ref key, recordId);
        }

        private void VerifySession(SecondaryIndexSessionBroker indexSessionBroker)
        {
            Assert.IsNotNull(indexSessionBroker);
            var sessionObject = indexSessionBroker.GetSessionObject(this.sessionSlot);
            Assert.AreEqual(sessionObject is null, this.sessionId == Guid.Empty);

            if (!(sessionObject is SimpleIndexSession session))
            {
                if (sessionObject is { })
                    Assert.Fail($"Unexpected session object type {sessionObject.GetType().Name} for {indexType}");

                this.sessionId = Guid.NewGuid();
                session = new SimpleIndexSession() { Id = this.sessionId };
                indexSessionBroker.SetSessionObject(this.sessionSlot, session);
            }
            Assert.AreEqual(this.sessionId, session.Id);
        }

        private List<long> emptyRecordList = new List<long>();

        internal long[] Query(TKey rawKey)
        {
            var key = this.indexKeyFunc(rawKey);
            if (!mutableRecords.TryGetValue(key, out var mutableRecordList))
                mutableRecordList = emptyRecordList;
            if (!immutableRecords.TryGetValue(key, out var immutableRecordList))
                immutableRecordList = emptyRecordList;

            return mutableRecordList.Concat(immutableRecordList).ToArray();
        }

        internal int DistinctKeyCount => this.mutableRecords.Count + this.immutableRecords.Count;

        private void VerifyNotImmutable(ref TKey key, long recordId)
        {
            if (immutableRecords.TryGetValue(key, out var recordIds) && recordIds.Contains(recordId))
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

    class SimpleIndexSession
    {
        public Guid Id { get; set; }
    }

    static class SimpleIndexUtils
    {
        internal const long NumKeys = 2_000L;
        internal const long KeySpace = 1L << 14;

        internal const int ValueStart = 10_000;

        public static void PopulateInts(FasterKV<int, int> fkv, bool useRMW, bool isAsync)
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

        public static void PopulateIntsWithAdvancedFunctions(FasterKV<int, int> fkv, bool useRMW, bool isAsync)
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
    }
}
