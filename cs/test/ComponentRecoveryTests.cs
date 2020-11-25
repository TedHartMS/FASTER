﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System;
using System.Threading.Tasks;

namespace FASTER.test.recovery
{

    [TestFixture]
    internal class ComponentRecoveryTests
    {
        private static unsafe void Setup_MallocFixedPageSizeRecoveryTest(out int seed, out IDevice device, out int numBucketsToAdd, out long[] logicalAddresses, out ulong numBytesWritten)
        {
            seed = 123;
            var rand1 = new Random(seed);
            device = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\MallocFixedPageSizeRecoveryTest.dat", deleteOnClose: true);
            var allocator = new MallocFixedPageSize<HashBucket>();

            //do something
            numBucketsToAdd = 16 * allocator.GetPageSize();
            logicalAddresses = new long[numBucketsToAdd];
            for (int i = 0; i < numBucketsToAdd; i++)
            {
                long logicalAddress = allocator.Allocate();
                logicalAddresses[i] = logicalAddress;
                var bucket = (HashBucket*)allocator.GetPhysicalAddress(logicalAddress);
                for (int j = 0; j < Constants.kOverflowBucketIndex; j++)
                {
                    bucket->bucket_entries[j] = rand1.Next();
                }
            }

            //issue call to checkpoint
            allocator.BeginCheckpoint(device, 0, out numBytesWritten);
            //wait until complete
            allocator.IsCheckpointCompletedAsync().AsTask().Wait();

            allocator.Dispose();
        }

        private static unsafe void Finish_MallocFixedPageSizeRecoveryTest(int seed, int numBucketsToAdd, long[] logicalAddresses, ulong numBytesWritten, MallocFixedPageSize<HashBucket> recoveredAllocator, ulong numBytesRead)
        {
            Assert.IsTrue(numBytesWritten == numBytesRead);

            var rand2 = new Random(seed);
            for (int i = 0; i < numBucketsToAdd; i++)
            {
                var logicalAddress = logicalAddresses[i];
                var bucket = (HashBucket*)recoveredAllocator.GetPhysicalAddress(logicalAddress);
                for (int j = 0; j < Constants.kOverflowBucketIndex; j++)
                {
                    Assert.IsTrue(bucket->bucket_entries[j] == rand2.Next());
                }
            }

            recoveredAllocator.Dispose();
        }

        [Test]
        public void MallocFixedPageSizeRecoveryTest()
        {
            Setup_MallocFixedPageSizeRecoveryTest(out int seed, out IDevice device, out int numBucketsToAdd, out long[] logicalAddresses, out ulong numBytesWritten);

            var recoveredAllocator = new MallocFixedPageSize<HashBucket>();
            //issue call to recover
            recoveredAllocator.BeginRecovery(device, 0, numBucketsToAdd, numBytesWritten, out ulong numBytesRead);
            //wait until complete
            recoveredAllocator.IsRecoveryCompleted(true);

            Finish_MallocFixedPageSizeRecoveryTest(seed, numBucketsToAdd, logicalAddresses, numBytesWritten, recoveredAllocator, numBytesRead);
        }

        [Test]
        public async Task MallocFixedPageSizeRecoveryAsyncTest()
        {
            Setup_MallocFixedPageSizeRecoveryTest(out int seed, out IDevice device, out int numBucketsToAdd, out long[] logicalAddresses, out ulong numBytesWritten);

            var recoveredAllocator = new MallocFixedPageSize<HashBucket>();
            ulong numBytesRead = await recoveredAllocator.RecoverAsync(device, 0, numBucketsToAdd, numBytesWritten, cancellationToken: default);

            Finish_MallocFixedPageSizeRecoveryTest(seed, numBucketsToAdd, logicalAddresses, numBytesWritten, recoveredAllocator, numBytesRead);
        }

        private static unsafe void Setup_FuzzyIndexRecoveryTest(out int seed, out int size, out long numAdds, out IDevice ht_device, out IDevice ofb_device, out FasterBase hash_table1, out ulong ht_num_bytes_written, out ulong ofb_num_bytes_written, out int num_ofb_buckets)
        {
            seed = 123;
            size = 1 << 16;
            numAdds = 1 << 18;
            ht_device = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\TestFuzzyIndexRecoveryht.dat", deleteOnClose: true);
            ofb_device = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\TestFuzzyIndexRecoveryofb.dat", deleteOnClose: true);
            hash_table1 = new FasterBase();
            hash_table1.Initialize(size, 512);

            //do something
            var bucket = default(HashBucket*);
            var slot = default(int);

            var keyGenerator1 = new Random(seed);
            var valueGenerator = new Random(seed + 1);
            for (int i = 0; i < numAdds; i++)
            {
                long key = keyGenerator1.Next();
                var hash = Utility.GetHashCode(key);
                var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

                var entry = default(HashBucketEntry);
                hash_table1.FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry, 0);

                hash_table1.UpdateSlot(bucket, slot, entry.word, valueGenerator.Next(), out long found_word);
            }

            //issue checkpoint call
            hash_table1.TakeIndexFuzzyCheckpoint(0, ht_device, out ht_num_bytes_written,
                ofb_device, out ofb_num_bytes_written, out num_ofb_buckets);

            //wait until complete
            hash_table1.IsIndexFuzzyCheckpointCompletedAsync().AsTask().Wait();
        }

        private static unsafe void Finish_FuzzyIndexRecoveryTest(int seed, long numAdds, FasterBase hash_table1, FasterBase hash_table2)
        {
            var keyGenerator2 = new Random(seed);

            var bucket1 = default(HashBucket*);
            var bucket2 = default(HashBucket*);
            var slot1 = default(int);
            var slot2 = default(int);

            var entry1 = default(HashBucketEntry);
            var entry2 = default(HashBucketEntry);
            for (int i = 0; i < 2 * numAdds; i++)
            {
                long key = keyGenerator2.Next();
                var hash = Utility.GetHashCode(key);
                var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

                var exists1 = hash_table1.FindTag(hash, tag, ref bucket1, ref slot1, ref entry1);
                var exists2 = hash_table2.FindTag(hash, tag, ref bucket2, ref slot2, ref entry2);

                Assert.IsTrue(exists1 == exists2);

                if (exists1)
                {
                    Assert.IsTrue(entry1.word == entry2.word);
                }
            }

            hash_table1.Free();
            hash_table2.Free();
        }

        [Test]
        public unsafe void FuzzyIndexRecoveryTest()
        {
            Setup_FuzzyIndexRecoveryTest(out int seed, out int size, out long numAdds, out IDevice ht_device, out IDevice ofb_device, out FasterBase hash_table1,
                                         out ulong ht_num_bytes_written, out ulong ofb_num_bytes_written, out int num_ofb_buckets);

            var hash_table2 = new FasterBase();
            hash_table2.Initialize(size, 512);

            //issue recover call
            hash_table2.RecoverFuzzyIndex(0, ht_device, ht_num_bytes_written, ofb_device, num_ofb_buckets, ofb_num_bytes_written);
            //wait until complete
            hash_table2.IsFuzzyIndexRecoveryComplete(true);

            Finish_FuzzyIndexRecoveryTest(seed, numAdds, hash_table1, hash_table2);
        }

        [Test]
        public async Task FuzzyIndexRecoveryAsyncTest()
        {
            Setup_FuzzyIndexRecoveryTest(out int seed, out int size, out long numAdds, out IDevice ht_device, out IDevice ofb_device, out FasterBase hash_table1,
                                         out ulong ht_num_bytes_written, out ulong ofb_num_bytes_written, out int num_ofb_buckets);

            var hash_table2 = new FasterBase();
            hash_table2.Initialize(size, 512);

            await hash_table2.RecoverFuzzyIndexAsync(0, ht_device, ht_num_bytes_written, ofb_device, num_ofb_buckets, ofb_num_bytes_written, cancellationToken: default);

            Finish_FuzzyIndexRecoveryTest(seed, numAdds, hash_table1, hash_table2);
        }
    }
}
