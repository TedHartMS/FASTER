// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System;

namespace FASTER.test
{
    [TestFixture]
    class BasicObserverTests
    {
        internal IObserver<IFasterScanIterator<int, int>>[] TestObservers;
        readonly object lockObject = new object();

        class TestObserver : IObserver<IFasterScanIterator<int, int>>
        {
            internal int Id;
            internal bool IsCompleted = false;
            internal int OnNextCalls = 0;

            internal TestObserver(int id) => this.Id = id;

            public void OnCompleted() => this.IsCompleted = true;

            public void OnError(Exception error) { /* Apparently not called by FASTER */ }

            public void OnNext(IFasterScanIterator<int, int> iter) { ++this.OnNextCalls; }
        }

        [SetUp]
        public void Setup()
        {
            this.TestObservers = Array.Empty<IObserver<IFasterScanIterator<int, int>>>();
        }

        [TearDown]
        public void TearDown()
        {
        }

        [Test]
        [Category("FasterLog")]
        public void ObserverMembershipTests()
        {
            const int NumObservers = 5;
            for (var ii = 0; ii < NumObservers; ++ii)
            {
                AllocatorBase<int, int>.AddObserver(ref TestObservers, new TestObserver(ii), lockObject);
            }

            Assert.AreEqual(NumObservers, TestObservers.Length);

            // We don't actually use the iterator in this test.
            IFasterScanIterator<int, int> iter = null;

            foreach (var observer in this.TestObservers)
                observer.OnNext(iter);
            foreach (var observer in this.TestObservers)
                Assert.AreEqual(1, (observer as TestObserver).OnNextCalls);

            const int toRemove = 1;
            AllocatorBase<int, int>.RemoveObserver(ref TestObservers, TestObservers[toRemove], lockObject);
            Assert.AreEqual(NumObservers - 1, TestObservers.Length);

            for (var ii = 0; ii < NumObservers - 1; ++ii)
            {
                int comparand = ii < toRemove ? ii : ii + 1;
                Assert.AreEqual(comparand, (TestObservers[ii] as TestObserver).Id);
            }

            AllocatorBase<int, int>.RemoveObserver(ref TestObservers, TestObservers[0], lockObject);
            Assert.AreEqual(NumObservers - 2, TestObservers.Length);
            AllocatorBase<int, int>.RemoveObserver(ref TestObservers, TestObservers[TestObservers.Length - 1], lockObject);
            Assert.AreEqual(NumObservers - 3, TestObservers.Length);

            foreach (var observer in this.TestObservers)
                observer.OnNext(iter);
            foreach (var observer in this.TestObservers)
                Assert.AreEqual(2, (observer as TestObserver).OnNextCalls);

            while (TestObservers.Length > 0)
            {
                AllocatorBase<int, int>.RemoveObserver(ref TestObservers, TestObservers[0], lockObject);
            }
            Assert.AreEqual(0, TestObservers.Length);
        }
    }
}
