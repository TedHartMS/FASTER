// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test.SubsetIndex.SimpleIndexTests
{
    class SimpleKeyIndexTests
    {
        class SimpleKeyIndex<TKey> : SimpleKeyIndexBase<TKey>, ISecondaryKeyIndex<TKey>
            where TKey : IComparable
        {
            internal SimpleKeyIndex(string name, bool isMutableIndex) : base(name, isMutableIndex: isMutableIndex) { }

            public void Delete(ref TKey key, SecondaryIndexSessionBroker indexSessionBroker)
                => BaseDelete(ref key, indexSessionBroker);

            public void Insert(ref TKey key, SecondaryIndexSessionBroker indexSessionBroker)
                => BaseInsert(ref key, indexSessionBroker);

            public void Upsert(ref TKey key, bool isMutableRecord, SecondaryIndexSessionBroker indexSessionBroker)
                => BaseUpsert(ref key, isMutableRecord, indexSessionBroker);
        }

        readonly PrimaryFasterKV store = new PrimaryFasterKV();

        [SetUp]
        public void Setup() => store.Setup();

        [TearDown]
        public void TearDown() => store.TearDown();

        private ISecondaryIndex CreateIndex(bool isMutable, bool isAsync)
            => new SimpleKeyIndex<int>($"{TestContext.CurrentContext.Test.Name}_mutable_{(isAsync ? "async" : "sync")}", isMutable);

        [Test]
        [Category("FasterKV"), Category("Index")]
        public void MutableInsertTest([Values] bool useAdvancedFunctions, [Values] bool useRMW, [Values] bool isAsync)
        {
            var secondaryIndex = CreateIndex(isMutable: true, isAsync);
            store.fkv.SecondaryIndexBroker.AddIndex(secondaryIndex);
            store.Populate(useAdvancedFunctions, useRMW, isAsync);
            SimpleKeyIndexBase<int>.VerifyIntIndex(secondaryIndex, store: null);
        }

        [Test]
        [Category("FasterKV"), Category("Index")]
        public void ImmutableInsertTest([Values] bool useAdvancedFunctions, [Values] bool useRMW, [Values] bool isAsync)
        {
            var secondaryIndex = CreateIndex(isMutable: false, isAsync);
            store.fkv.SecondaryIndexBroker.AddIndex(secondaryIndex);
            store.Populate(useAdvancedFunctions, useRMW, isAsync);
            SimpleKeyIndexBase<int>.VerifyIntIndex(secondaryIndex, store);
        }

        [Test]
        [Category("FasterKV"), Category("Index")]
        public void MixedInsertTest([Values] bool useAdvancedFunctions, [Values] bool useRMW, [Values] bool isAsync)
        {
            var mutableIndex = CreateIndex(isMutable: true, isAsync);
            var immutableIndex = CreateIndex(isMutable: false, isAsync);
            store.fkv.SecondaryIndexBroker.AddIndex(mutableIndex);
            store.fkv.SecondaryIndexBroker.AddIndex(immutableIndex);
            store.Populate(useAdvancedFunctions, useRMW, isAsync);
            SimpleKeyIndexBase<int>.VerifyMixedIntIndexes(mutableIndex, immutableIndex, store);
        }
    }
}
