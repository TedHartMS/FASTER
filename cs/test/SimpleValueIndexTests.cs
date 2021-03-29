// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test.SubsetIndex.SimpleIndexTests
{
    class SimpleValueIndexTests
    {
        class SimpleValueIndex<TValue> : SimpleIndexBase<TValue>, ISecondaryValueIndex<TValue>
        {
            internal SimpleValueIndex(string name, Func<TValue, TValue> indexKeyFunc, bool isMutableIndex) : base(name, isKeyIndex: false, indexKeyFunc, isMutableIndex: isMutableIndex) { }

            public void Delete(long recordId, SecondaryIndexSessionBroker indexSessionBroker)
                => BaseDelete(recordId, indexSessionBroker);

            public void Insert(ref TValue value, long recordId, SecondaryIndexSessionBroker indexSessionBroker)
                => BaseInsert(ref value, recordId, indexSessionBroker);

            public void Upsert(ref TValue value, long recordId, bool isMutableRecord, SecondaryIndexSessionBroker indexSessionBroker)
                => BaseUpsert(ref value, recordId, isMutableRecord, indexSessionBroker);
        }

        private const int valueDivisor = 50;
        readonly PrimaryFasterKV store = new PrimaryFasterKV();

        [SetUp]
        public void Setup() => store.Setup();

        [TearDown]
        public void TearDown() => store.TearDown();

        private ISecondaryIndex CreateIndex(bool isMutable, bool isAsync, Func<int, int> indexKeyFunc)
            => new SimpleValueIndex<int>($"{TestContext.CurrentContext.Test.Name}_mutable_{(isAsync ? "async" : "sync")}", indexKeyFunc, isMutable);

        [Test]
        [Category("FasterKV")]
        public void MutableInsertTest([Values] bool useAdvancedFunctions, [Values] bool useRMW, [Values] bool isAsync)
        {
            var secondaryIndex = CreateIndex(isMutable: true, isAsync, rawValue => (rawValue - SimpleIndexUtils.ValueStart) / valueDivisor);
            store.fkv.SecondaryIndexBroker.AddIndex(secondaryIndex);
            store.Populate(useAdvancedFunctions, useRMW, isAsync);
            SimpleIndexUtils.VerifyMutableIndex(secondaryIndex, valueDivisor, SimpleIndexUtils.ValueStart);
        }

        [Test]
        [Category("FasterKV")]
        public void ImmutableInsertTest([Values] bool useAdvancedFunctions, [Values] bool useRMW, [Values] bool isAsync)
        {
            var secondaryIndex = CreateIndex(isMutable: false, isAsync, rawValue => (rawValue - SimpleIndexUtils.ValueStart) / valueDivisor);
            store.fkv.SecondaryIndexBroker.AddIndex(secondaryIndex);
            store.Populate(useAdvancedFunctions, useRMW, isAsync);
            SimpleIndexUtils.VerifyImmutableIndex(secondaryIndex, valueDivisor, SimpleIndexUtils.ValueStart, store);
        }

        [Test]
        [Category("FasterKV")]
        public void MixedInsertTest([Values] bool useAdvancedFunctions, [Values] bool useRMW, [Values] bool isAsync)
        {
            var mutableIndex = CreateIndex(isMutable: true, isAsync, rawValue => (rawValue - SimpleIndexUtils.ValueStart) / valueDivisor);
            var immutableIndex = CreateIndex(isMutable: false, isAsync, rawValue => (rawValue - SimpleIndexUtils.ValueStart) / valueDivisor);
            store.fkv.SecondaryIndexBroker.AddIndex(mutableIndex);
            store.fkv.SecondaryIndexBroker.AddIndex(immutableIndex);
            store.Populate(useAdvancedFunctions, useRMW, isAsync);
            SimpleIndexUtils.VerifyMixedIndexes(mutableIndex, immutableIndex, valueDivisor, SimpleIndexUtils.ValueStart, store);
        }
    }
}
