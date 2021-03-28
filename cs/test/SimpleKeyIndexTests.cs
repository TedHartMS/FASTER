// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test.SubsetIndex.SimpleKeyIndexTests
{
    class SimpleKeyIndexTests
    {
        class SimpleMutableKeyIndex<TKey> : SimpleIndexBase<TKey>, ISecondaryKeyIndex<TKey>
        {
            internal SimpleMutableKeyIndex(string name, Func<TKey, TKey> indexKeyFunc) : base(name, isKeyIndex: true, indexKeyFunc, isMutableIndex: true) { }

            public void Delete(ref TKey key, long recordId, SecondaryIndexSessionBroker indexSessionBroker)
                => BaseDelete(ref key, recordId, indexSessionBroker);

            public void Insert(ref TKey key, long recordId, SecondaryIndexSessionBroker indexSessionBroker)
                => BaseInsert(ref key, recordId, indexSessionBroker);

            public void Upsert(ref TKey key, long recordId, bool isMutableRecord, SecondaryIndexSessionBroker indexSessionBroker)
                => BaseUpsert(ref key, recordId, isMutableRecord, indexSessionBroker);
        }

        class SimpleImmutableKeyIndex<TKey> : SimpleIndexBase<TKey>, ISecondaryKeyIndex<TKey>
        {
            internal SimpleImmutableKeyIndex(string name, Func<TKey, TKey> indexKeyFunc) : base(name, isKeyIndex: true, indexKeyFunc, isMutableIndex: true) { }

            public void Delete(ref TKey key, long recordId, SecondaryIndexSessionBroker indexSessionBroker)
                => BaseDelete(ref key, recordId, indexSessionBroker);

            public void Insert(ref TKey key, long recordId, SecondaryIndexSessionBroker indexSessionBroker)
                => BaseInsert(ref key, recordId, indexSessionBroker);

            public void Upsert(ref TKey key, long recordId, bool isMutableRecord, SecondaryIndexSessionBroker indexSessionBroker)
                => BaseUpsert(ref key, recordId, isMutableRecord, indexSessionBroker);
        }

        const int keyDivisor = 20;
        private string testPath;
        private FasterKV<int, int> fkv;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            if (testPath == null)
            {
                testPath = TestContext.CurrentContext.TestDirectory + "/" + Path.GetRandomFileName();
                if (!Directory.Exists(testPath))
                    Directory.CreateDirectory(testPath);
            }

            log = Devices.CreateLogDevice(testPath + $"/{TestContext.CurrentContext.Test.Name}.log", false);

            fkv = new FasterKV<int, int>(SimpleIndexUtils.KeySpace, new LogSettings { LogDevice = log },
                    new CheckpointSettings { CheckpointDir = testPath, CheckPointType = CheckpointType.Snapshot }
            );
        }

        [TearDown]
        public void TearDown()
        {
            fkv.Dispose();
            fkv = null;
            log.Dispose();
            TestUtils.DeleteDirectory(testPath);
        }

        private ISecondaryIndex CreateIndex(bool isMutable, bool isAsync, Func<int, int> indexKeyFunc)
            => isMutable
                ? (ISecondaryIndex)new SimpleMutableKeyIndex<int>($"{TestContext.CurrentContext.Test.Name}_mutable_{(isAsync ? "async" : "sync")}", indexKeyFunc)
                : new SimpleImmutableKeyIndex<int>($"{TestContext.CurrentContext.Test.Name}_mutable_{(isAsync ? "async" : "sync")}", indexKeyFunc);

        [Test]
        [Category("FasterKV")]
        public void MutableInsertTest([Values] bool useRMW, [Values] bool useAdvancedFunctions, [Values] bool isAsync)
        {
            var secondaryIndex = CreateIndex(isMutable: true, isAsync, rawKey => rawKey / keyDivisor);
            fkv.SecondaryIndexBroker.AddIndex(secondaryIndex);
            if (useAdvancedFunctions)
                SimpleIndexUtils.PopulateIntsWithAdvancedFunctions(fkv, useRMW, isAsync);
            else
                SimpleIndexUtils.PopulateInts(fkv, useRMW, isAsync);

            var indexBase = secondaryIndex as SimpleIndexBase<int>;
            Assert.IsNotNull(indexBase);
            var records = indexBase.Query(42);
            Assert.AreEqual(SimpleIndexUtils.NumKeys / keyDivisor, indexBase.DistinctKeyCount);
            Assert.AreEqual(keyDivisor, records.Length);
        }
    }
}
