﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test.SecondaryIndex.SimpleIndex
{
    class SimpleValueIndexTests
    {
        class SimpleValueIndex<TKey, TValue> : SimpleValueIndexBase<TValue>, ISecondaryValueIndex<TKey, TValue>
        {
            internal SimpleValueIndex(string name, Func<TValue, TValue> indexKeyFunc, bool isMutableIndex) : base(name, indexKeyFunc, isMutableIndex: isMutableIndex) { }

            public void Delete(ref TKey key, RecordId recordId, SecondaryIndexSessionBroker indexSessionBroker)
                => BaseDelete(recordId, indexSessionBroker);

            public void Insert(ref TKey key, ref TValue value, RecordId recordId, SecondaryIndexSessionBroker indexSessionBroker)
                => BaseInsert(ref value, recordId, indexSessionBroker);

            public void Upsert(ref TKey key, ref TValue value, RecordId recordId, bool isMutableRecord, SecondaryIndexSessionBroker indexSessionBroker)
                => BaseUpsert(ref value, recordId, isMutableRecord, indexSessionBroker);

            public void OnPrimaryTruncate(long newBeginAddress) { }

            public void ScanReadOnlyPages(IFasterScanIterator<TKey, TValue> iter, SecondaryIndexSessionBroker indexSessionBroker)
            {
                while (iter.GetNext(out var recordInfo))
                    Upsert(ref iter.GetKey(), ref iter.GetValue(), new RecordId(recordInfo.Version, iter.CurrentAddress), isMutableRecord: false, indexSessionBroker);
            }

            public void RecoveryReplay(IFasterScanIterator<TKey, TValue> iter, SecondaryIndexSessionBroker indexSessionBroker) { }
        }

        private const int valueDivisor = 50;
        readonly PrimaryFasterKV store = new PrimaryFasterKV();

        [SetUp]
        public void Setup() => store.Setup();

        [TearDown]
        public void TearDown() => store.TearDown();

        private ISecondaryIndex CreateIndex(bool isMutable, bool isAsync, Func<int, int> indexKeyFunc)
            => new SimpleValueIndex<int, int>($"{TestContext.CurrentContext.Test.Name}_mutable_{(isAsync ? "async" : "sync")}", indexKeyFunc, isMutable);

        [Test]
        [Category(TestUtils.SecondaryIndexCategory)]
        public void MutableInsertTest([Values] bool useAdvancedFunctions, [Values] bool useRMW, [Values] bool isAsync)
        {
            var secondaryIndex = CreateIndex(isMutable: true, isAsync, rawValue => (rawValue - SimpleIndexUtils.ValueStart) / valueDivisor);
            store.fkv.SecondaryIndexBroker.AddIndex(secondaryIndex);
            store.Populate(useAdvancedFunctions, useRMW, isAsync);
            SimpleValueIndexBase<int>.VerifyMutableIntIndex(secondaryIndex, valueDivisor, SimpleIndexUtils.ValueStart);
        }

        [Test]
        [Category(TestUtils.SecondaryIndexCategory)]
        public void ImmutableInsertTest([Values] bool useAdvancedFunctions, [Values] bool useRMW, [Values] bool isAsync)
        {
            var secondaryIndex = CreateIndex(isMutable: false, isAsync, rawValue => (rawValue - SimpleIndexUtils.ValueStart) / valueDivisor);
            store.fkv.SecondaryIndexBroker.AddIndex(secondaryIndex);
            store.Populate(useAdvancedFunctions, useRMW, isAsync);
            SimpleValueIndexBase<int>.VerifyImmutableIntIndex(secondaryIndex, valueDivisor, SimpleIndexUtils.ValueStart, store);
        }

        [Test]
        [Category(TestUtils.SecondaryIndexCategory)]
        public void MixedInsertTest([Values] bool useAdvancedFunctions, [Values] bool useRMW, [Values] bool isAsync)
        {
            var mutableIndex = CreateIndex(isMutable: true, isAsync, rawValue => (rawValue - SimpleIndexUtils.ValueStart) / valueDivisor);
            var immutableIndex = CreateIndex(isMutable: false, isAsync, rawValue => (rawValue - SimpleIndexUtils.ValueStart) / valueDivisor);
            store.fkv.SecondaryIndexBroker.AddIndex(mutableIndex);
            store.fkv.SecondaryIndexBroker.AddIndex(immutableIndex);
            store.Populate(useAdvancedFunctions, useRMW, isAsync);
            SimpleValueIndexBase<int>.VerifyMixedIntIndexes(mutableIndex, immutableIndex, valueDivisor, SimpleIndexUtils.ValueStart, store);
        }
    }
}