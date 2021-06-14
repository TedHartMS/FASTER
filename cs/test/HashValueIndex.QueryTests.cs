// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using FASTER.indexes.HashValueIndex;
using System;
using NUnit.Framework;
using System.Threading;
using System.Linq;
using System.Threading.Tasks;
using System.Diagnostics;

namespace FASTER.test.HashValueIndex.QueryTests
{
    [TestFixture]
    class QueryTests
    {
        HashValueIndexTestBase testBase;

        [SetUp]
        public void Setup() { /* Nothing here as we don't know numPreds yet */}

        private void Setup(int numPreds) => this.testBase = new HashValueIndexTestBase(numPreds);

        [TearDown]
        public void TearDown() => TearDown(deleteDir: true);

        public void TearDown(bool deleteDir)
        {
            testBase.TearDown(deleteDir);
            testBase = null;
        }

        private void PrepareToRecover(int numPreds)
        {
            TearDown(deleteDir: false);
            Setup(numPreds);
        }

        [Test]
        [Category(TestUtils.SecondaryIndexCategory), Category(TestUtils.HashValueIndexCategory)]
        public async Task ThreadedInsertQueryTest([Values(1, 10)] int numThreads,
                                            [Values(HashValueIndexTestBase.MinRecs, HashValueIndexTestBase.MidRecs, HashValueIndexTestBase.MaxRecs)] int numRecs,
                                            [Values(1, 3)] int numPreds)
        {
            await RunTest(numThreads, numRecs, numPreds, isAsync: false);
        }

        [Test]
        [Category(TestUtils.SecondaryIndexCategory), Category(TestUtils.HashValueIndexCategory)]
        public async Task QueryAsyncTest([Values(1, 10)] int numThreads,
                                            [Values(HashValueIndexTestBase.MinRecs, HashValueIndexTestBase.MidRecs)] int numRecs,
                                            [Values(1, 3)] int numPreds)
        {
            await RunTest(numThreads, numRecs, numPreds, isAsync: true);
        }

        private async Task RunTest(int numThreads, int numRecs, int numPreds, bool isAsync)
        {
            Setup(numPreds);
            var half = numRecs / 2;

            // Insert the first half of the records
            var sw = Stopwatch.StartNew();
            Insert(numThreads, 0, half);
            Console.WriteLine($"ThreadInserter with {half} records: {sw.ElapsedMilliseconds} ms");

            // Tests query over mutable portion of primary log
            sw.Restart();
            var numQueryRecs = await QueryAsync(numThreads, half, isAsync);
            Console.WriteLine($"Query with {half} primary, 0 SI records, {numQueryRecs} queried records: {sw.ElapsedMilliseconds} ms");

            // Tests query across records in secondary index; these went readonly in the primary log.
            testBase.primaryFkv.Log.FlushAndEvict(wait: true);
            Console.WriteLine($"FlushAndEvict Primary");

            sw.Restart();
            numQueryRecs = await QueryAsync(numThreads, half, isAsync);
            Console.WriteLine($"Query with {0} primary, {half} SI records, {numQueryRecs} queried records: {sw.ElapsedMilliseconds} ms");

            // Insert the second half of the records
            sw.Restart();
            Insert(numThreads, half, numRecs);
            Console.WriteLine($"ThreadInserter with {half} records: {sw.ElapsedMilliseconds} ms");

            // Tests query over mutable portion of primary log, then across secondary index
            sw.Restart();
            numQueryRecs = await QueryAsync(numThreads, numRecs, isAsync);
            Console.WriteLine($"Query with {half} primary, {half} SI records, {numQueryRecs} queried records: {sw.ElapsedMilliseconds} ms");

            // Tests query across records in secondary index.
            testBase.primaryFkv.Log.FlushAndEvict(wait: true);
            Console.WriteLine($"FlushAndEvict Primary");
            numQueryRecs = await QueryAsync(numThreads, numRecs, isAsync);
            Console.WriteLine($"Query with 0 primary, {numRecs} SI records, {numQueryRecs} queried records: {sw.ElapsedMilliseconds} ms");

            // We need to complete one primary checkpoint before the secondary, then another primary checkpoint after secondary--the latter primary is what we'll
            // recover, which will be able to recover the secondary checkpoint.
            sw.Restart();
            Assert.IsTrue(testBase.primaryFkv.TakeFullCheckpoint(out Guid primaryToken1, CheckpointType.FoldOver));
            await testBase.primaryFkv.CompleteCheckpointAsync();
            Assert.IsTrue(testBase.index.TakeFullCheckpoint(out Guid secondaryToken));
            await testBase.index.CompleteCheckpointAsync();
            Assert.IsTrue(testBase.primaryFkv.TakeFullCheckpoint(out Guid primaryToken2, CheckpointType.FoldOver));
            await testBase.primaryFkv.CompleteCheckpointAsync();
            Console.WriteLine($"Checkpoints in {sw.ElapsedMilliseconds} ms");

            PrepareToRecover(numPreds);
            sw.Restart();
            testBase.primaryFkv.Recover(primaryToken2);
            Console.WriteLine($"Recover in {sw.ElapsedMilliseconds} ms");

            // Now verify correct recovery.
            sw.Restart();
            numQueryRecs = await QueryAsync(numThreads, numRecs, isAsync);
            Console.WriteLine($"Query with 0 primary, {numRecs} SI records, {numQueryRecs} queried records: {sw.ElapsedMilliseconds} ms");
        }

        void Insert(int numThreads, int startRec, int numRecs)
        {
            void insert(int threadId)
            {
                using var session = testBase.primaryFkv.For(new SimpleFunctions<int, int>()).NewSession<SimpleFunctions<int, int>>();
                for (var ii = startRec; ii < numRecs; ++ii)
                    session.Upsert(HashValueIndexTestBase.MaxRecs * threadId + ii, ii);
            }

            Thread[] workers = new Thread[numThreads];
            for (int ii = 0; ii < numThreads; ++ii)
            {
                var threadId = ii + 1;
                workers[ii] = new Thread(() => insert(threadId));
            }
            foreach (var worker in workers)
                worker.Start();
            foreach (var worker in workers)
                worker.Join();
        }

        private async ValueTask<int> QueryAsync(int numThreads, int numRecs, bool isAsync)
        {
            var expectedTotalRecs = numRecs * numThreads * testBase.intPredicates.Length;
            var expectedRecsPerPred = (numRecs * numThreads) / HashValueIndexTestBase.PredMod;

            int predKeysPerThread = (HashValueIndexTestBase.PredMod + numThreads - 1) / numThreads;

            // Verify the test retrieved all records
            int actualTotalRecs = 0;

            async Task verifyResults(int startPredKey)
            {
                using var session = testBase.primaryFkv.For(new SimpleFunctions<int, int>()).NewSession<SimpleFunctions<int, int>>();
                foreach (var pred in testBase.intPredicates.Select((predicate, ordinal) => new { predicate, ordinal }))
                {
                    var maxPredKey = Math.Min(HashValueIndexTestBase.PredMod, startPredKey + predKeysPerThread);
                    for (int predKey = startPredKey; predKey < maxPredKey; ++predKey)
                    {
                        var records = isAsync
                            ? await session.QueryAsync(pred.predicate, HashValueIndexTestBase.PredicateShiftKey(predKey, pred.ordinal), new QuerySettings()).ToArrayAsync()
                            : session.Query(pred.predicate, HashValueIndexTestBase.PredicateShiftKey(predKey, pred.ordinal), new QuerySettings()).ToArray();
                        Assert.AreEqual(expectedRecsPerPred, records.Length);
                        Array.Sort(records, (ll, rr) => ll.KeyRef.CompareTo(rr.KeyRef));
                        var prevKey = records[0].KeyRef - 1;
                        for (var ii = 0; ii < records.Length; ++ii)
                        {
                            var record = records[ii];

                            // Make sure the key and value are consistent
                            Assert.AreEqual(record.KeyRef % HashValueIndexTestBase.PredMod, predKey);

                            // Make sure we didn't return the same key twice. The combination of verifying expectedCount and uniqueness
                            // also verifies we got all keys.
                            Assert.AreNotEqual(prevKey, record.KeyRef);
                            record.Dispose();
                        }
                        Interlocked.Add(ref actualTotalRecs, records.Length);
                    }
                }
            }

            if (isAsync)
            {
                var tasks = Enumerable.Range(0, numThreads).Select(threadOrd => verifyResults(threadOrd * predKeysPerThread));
                await Task.WhenAll(tasks);
            }
            else
            {
                Thread[] workers = new Thread[numThreads];
                for (int ii = 0; ii < numThreads; ++ii)
                {
                    var threadOrd = ii;
                    workers[ii] = new Thread(async () => await verifyResults(threadOrd * predKeysPerThread));
                }
                foreach (var worker in workers)
                    worker.Start();
                foreach (var worker in workers)
                    worker.Join();
            }
            Assert.AreEqual(expectedTotalRecs, actualTotalRecs);
            return actualTotalRecs;
        }
    }
}
