// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test.SubsetIndex.SimpleIndexTests
{
    class SimpleKeyIndexTests
    {
        class SimpleKeyIndex<TKey> : SimpleIndexBase<TKey>, ISecondaryKeyIndex<TKey>
        {
            internal SimpleKeyIndex(string name, Func<TKey, TKey> indexKeyFunc, bool isMutableIndex) : base(name, isKeyIndex: true, indexKeyFunc, isMutableIndex: isMutableIndex) { }

            public void Delete(ref TKey key, long recordId, SecondaryIndexSessionBroker indexSessionBroker)
                => BaseDelete(ref key, recordId, indexSessionBroker);

            public void Insert(ref TKey key, long recordId, SecondaryIndexSessionBroker indexSessionBroker)
                => BaseInsert(ref key, recordId, indexSessionBroker);

            public void Upsert(ref TKey key, long recordId, bool isMutableRecord, SecondaryIndexSessionBroker indexSessionBroker)
                => BaseUpsert(ref key, recordId, isMutableRecord, indexSessionBroker);
        }

        private const int keyDivisor = 20;
        readonly PrimaryFasterKV store = new PrimaryFasterKV();

        [SetUp]
        public void Setup() => store.Setup();

        [TearDown]
        public void TearDown() => store.TearDown();

        private ISecondaryIndex CreateIndex(bool isMutable, bool isAsync, Func<int, int> indexKeyFunc)
            => new SimpleKeyIndex<int>($"{TestContext.CurrentContext.Test.Name}_mutable_{(isAsync ? "async" : "sync")}", indexKeyFunc, isMutable);

        [Test]
        [Category("FasterKV")]
        public void MutableInsertTest([Values] bool useAdvancedFunctions, [Values] bool useRMW, [Values] bool isAsync)
        {
            var secondaryIndex = CreateIndex(isMutable: true, isAsync, rawKey => rawKey / keyDivisor);
            store.fkv.SecondaryIndexBroker.AddIndex(secondaryIndex);
            store.Populate(useAdvancedFunctions, useRMW, isAsync);
            SimpleIndexUtils.VerifyMutableIndex(secondaryIndex, keyDivisor, 0);
        }

        [Test]
        [Category("FasterKV")]
        public void ImmutableInsertTest([Values] bool useAdvancedFunctions, [Values] bool useRMW, [Values] bool isAsync)
        {
            var secondaryIndex = CreateIndex(isMutable: false, isAsync, rawKey => rawKey / keyDivisor);
            store.fkv.SecondaryIndexBroker.AddIndex(secondaryIndex);
            store.Populate(useAdvancedFunctions, useRMW, isAsync);
            SimpleIndexUtils.VerifyImmutableIndex(secondaryIndex, keyDivisor, 0, store);
        }

        [Test]
        [Category("FasterKV")]
        public void MixedInsertTest([Values] bool useAdvancedFunctions, [Values] bool useRMW, [Values] bool isAsync)
        {
            var mutableIndex = CreateIndex(isMutable: true, isAsync, rawKey => rawKey / keyDivisor);
            var immutableIndex = CreateIndex(isMutable: false, isAsync, rawKey => rawKey / keyDivisor);
            store.fkv.SecondaryIndexBroker.AddIndex(mutableIndex);
            store.fkv.SecondaryIndexBroker.AddIndex(immutableIndex);
            store.Populate(useAdvancedFunctions, useRMW, isAsync);
            SimpleIndexUtils.VerifyMixedIndexes(mutableIndex, immutableIndex, keyDivisor, 0, store);
        }
    }
}
