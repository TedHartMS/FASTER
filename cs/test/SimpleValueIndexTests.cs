// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test.SubsetIndex.SimpleValueIndexTests
{
    class SimpleValueIndexTests
    {
        class SimpleMutableValueIndex<TValue> : SimpleIndexBase<TValue>, ISecondaryValueIndex<TValue>
        {
            internal SimpleMutableValueIndex(string name, Func<TValue, TValue> indexKeyFunc) : base(name, isKeyIndex: false, indexKeyFunc, isMutableIndex: true) { }

            public void Delete(long recordId, SecondaryIndexSessionBroker indexSessionBroker)
                => BaseDelete(recordId, indexSessionBroker);

            public void Insert(ref TValue value, long recordId, SecondaryIndexSessionBroker indexSessionBroker)
                => BaseInsert(ref value, recordId, indexSessionBroker);

            public void Upsert(ref TValue value, long recordId, bool isMutableRecord, SecondaryIndexSessionBroker indexSessionBroker)
                => BaseUpsert(ref value, recordId, isMutableRecord, indexSessionBroker);
        }

        class SimpleImmutableValueIndex<TValue> : SimpleIndexBase<TValue>, ISecondaryValueIndex<TValue>
        {
            internal SimpleImmutableValueIndex(string name, Func<TValue, TValue> indexKeyFunc) : base(name, isKeyIndex: false, indexKeyFunc, isMutableIndex: true) { }

            public void Delete(long recordId, SecondaryIndexSessionBroker indexSessionBroker)
                => BaseDelete(recordId, indexSessionBroker);

            public void Insert(ref TValue value, long recordId, SecondaryIndexSessionBroker indexSessionBroker)
                => BaseInsert(ref value, recordId, indexSessionBroker);

            public void Upsert(ref TValue value, long recordId, bool isMutableRecord, SecondaryIndexSessionBroker indexSessionBroker)
                => BaseUpsert(ref value, recordId, isMutableRecord, indexSessionBroker);
        }

        const int valueDivisor = 50;
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
                ? (ISecondaryIndex)new SimpleMutableValueIndex<int>($"{TestContext.CurrentContext.Test.Name}_mutable_{(isAsync ? "async" : "sync")}", indexKeyFunc)
                : new SimpleImmutableValueIndex<int>($"{TestContext.CurrentContext.Test.Name}_mutable_{(isAsync ? "async" : "sync")}", indexKeyFunc);

        [Test]
        [Category("FasterKV")]
        public void MutableInsertTest([Values] bool useRMW, [Values] bool useAdvancedFunctions, [Values] bool isAsync)
        {
            var secondaryIndex = CreateIndex(isMutable: true, isAsync, rawValue => (rawValue - SimpleIndexUtils.ValueStart) / valueDivisor);
            fkv.SecondaryIndexBroker.AddIndex(secondaryIndex);
            if (useAdvancedFunctions)
                SimpleIndexUtils.PopulateIntsWithAdvancedFunctions(fkv, useRMW, isAsync);
            else
                SimpleIndexUtils.PopulateInts(fkv, useRMW, isAsync);

            var indexBase = secondaryIndex as SimpleIndexBase<int>;
            Assert.IsNotNull(indexBase);
            var records = indexBase.Query(SimpleIndexUtils.ValueStart + 42);
            Assert.AreEqual(SimpleIndexUtils.NumKeys / valueDivisor, indexBase.DistinctKeyCount);
            Assert.AreEqual(valueDivisor, records.Length);
        }
    }
}
