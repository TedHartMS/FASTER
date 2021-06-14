// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.indexes.HashValueIndex;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace FASTER.test.HashValueIndex.WorkQueueOrdered
{
    [TestFixture]
    class WorkQueueOrderedTests
    {
        const int NumItems = 3000;
        const int MaxRangeSize = 2000;
        const int MaxSleepMs = 500;
        const long initialAddress = 64;     // The usual initial FASTER BeginAddress

        WorkQueueOrdered<long, OrderedRange> queue;
        Random rng;
        List<OrderedRange> resultRanges;
        OrderedRange[] initialRanges;
        int numWaits;

        [SetUp]
        public void Setup()
        {
            this.queue = new WorkQueueOrdered<long, OrderedRange>(initialAddress);
            this.rng = new Random(1701);
            this.resultRanges = new List<OrderedRange>();
            this.initialRanges = new OrderedRange[NumItems];
            this.numWaits = 0;

            long nextStart = initialAddress;
            for (int ii = 0; ii < NumItems; ++ii)
            {
                var size = rng.Next(MaxRangeSize) + 1;  // Don't allow 0 size
                this.initialRanges[ii] = new OrderedRange (nextStart, nextStart + size);
                nextStart += size;
            }
        }

        [TearDown]
        public void TearDown() { }

        OrderedRange CreateRange(int ordinal, bool createEvent = false) 
            => new OrderedRange(initialRanges[ordinal].startAddress, initialRanges[ordinal].endAddress, createEvent);

        void CompleteWork(int ordinal)
            => queue.CompleteWork(initialRanges[ordinal].startAddress, initialRanges[ordinal].endAddress);

        void Run(Thread[] workers, bool checkWaits)
        {
            Assert.AreEqual(0, resultRanges.Count);
            Assert.AreEqual(NumItems, workers.Length);
            foreach (var worker in workers)
                worker.Start();
            foreach (var worker in workers)
                worker.Join();
            VerifyResults();

            // Make sure we tested waiting
            if (checkWaits)
                Assert.AreNotEqual(0, this.numWaits);
        }

        void Insert(int ordinal, int sleepMs = 0)
        {
            if (sleepMs > 0)
                Thread.Sleep(sleepMs);

            if (!queue.TryStartWork(initialRanges[ordinal].startAddress, () => CreateRange(ordinal, createEvent: true), out var waitingRange))
            {
                Assert.Greater(queue.workItems.Count, 0);
                waitingRange.Wait();
                waitingRange.Dispose();
                ++this.numWaits;
            }
            Assert.AreEqual(ordinal, resultRanges.Count);
            Assert.AreEqual(initialRanges[ordinal].startAddress, queue.nextKey);
            resultRanges.Add(initialRanges[ordinal]);
            CompleteWork(ordinal);
        }

        void VerifyResults()
        {
            Assert.AreEqual(resultRanges.Count, NumItems);
            for (int ii = 0; ii < NumItems; ++ii)
            {
                Assert.AreEqual(initialRanges[ii].startAddress, resultRanges[ii].startAddress);
                Assert.AreEqual(initialRanges[ii].endAddress, resultRanges[ii].endAddress);
            }
        }

        int[] Shuffle(int[] array)
        {
            var max = array.Length;

            // Walk through the array swapping an element ahead of us with the current element.
            for (int ii = 0; ii < max - 1; ii++)
            {
                // rng.Next's parameter is exclusive, so idx stays within the array bounds.
                int idx = ii + rng.Next(max - ii);
                var temp = array[idx];
                array[idx] = array[ii];
                array[ii] = temp;
            }
            return array;
        }

        [Test]
        [Category(TestUtils.SecondaryIndexCategory), Category(TestUtils.HashValueIndexCategory)]
        public void WQOInsertForwardSequenceTest()
        {
            // Don't sleep here as it's sequential
            for (int ii = 0; ii < NumItems; ++ii)
                Insert(ii);
            VerifyResults();

            // We should never Wait in this test as it's strictly sequential.
            Assert.AreEqual(0, numWaits);
        }

        [Test]
        [Category(TestUtils.SecondaryIndexCategory), Category(TestUtils.HashValueIndexCategory)]
        public void WQOInsertReverseSequenceTest([Values]bool sleep)
        {
            Thread[] workers = new Thread[NumItems];
            for (int ii = 0; ii < NumItems; ++ii)
            {
                int jj = NumItems - ii - 1;
                workers[ii] = new Thread(() => Insert(jj, sleep ? rng.Next(MaxSleepMs) : 0));
            }
            Run(workers, checkWaits: true);
        }

        [Test]
        [Category(TestUtils.SecondaryIndexCategory), Category(TestUtils.HashValueIndexCategory)]
        public void WQOInsertRandomWaitTest([Values]bool sleep)
        {
            Thread[] workers = new Thread[NumItems];
            for (int ii = 0; ii < NumItems; ++ii)
            {
                int jj = ii;
                workers[ii] = new Thread(() => Insert(jj, sleep ? rng.Next(MaxSleepMs) : 0));
            }
            
            // If we did not randomly sleep, we may have had sequential insert.
            Run(workers, checkWaits: sleep);
        }

        [Test]
        [Category(TestUtils.SecondaryIndexCategory), Category(TestUtils.HashValueIndexCategory)]
        public void WQOInsertRandomSequenceTest([Values] bool sleep)
        {
            var ordinals = Shuffle(Enumerable.Range(0, NumItems).ToArray());

            Thread[] workers = new Thread[NumItems];
            for (int ii = 0; ii < NumItems; ++ii)
            {
                int jj = ordinals[ii];
                workers[ii] = new Thread(() => Insert(jj, sleep ? rng.Next(MaxSleepMs) : 0));
            }
            Run(workers, checkWaits: true);
        }
    }
}
