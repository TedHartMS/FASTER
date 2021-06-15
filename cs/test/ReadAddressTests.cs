﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using FASTER.core;
using System.IO;
using NUnit.Framework;
using System.Threading.Tasks;
using System.Threading;

namespace FASTER.test.readaddress
{
    [TestFixture]
    public class ReadAddressTests
    {
        const int numKeys = 1000;
        const int keyMod = 100;
        const int maxLap = numKeys / keyMod;
        const int deleteLap = maxLap / 2;
        const int defaultKeyToScan = 42;

        private static int LapOffset(int lap) => lap * numKeys * 100;

        public struct Key
        {
            public long key;

            public Key(long first) => key = first;

            public override string ToString() => key.ToString();

            internal class Comparer : IFasterEqualityComparer<Key>
            {
                public long GetHashCode64(ref Key key) => Utility.GetHashCode(key.key);

                public bool Equals(ref Key k1, ref Key k2) => k1.key == k2.key;
            }
        }

        public struct Value
        {
            public long value;

            public Value(long value) => this.value = value;

            public override string ToString() => value.ToString();
        }

        public class Context
        {
            public Value output;
            public RecordInfo recordInfo;
            public Status status;

            public void Reset()
            {
                this.output = default;
                this.recordInfo = default;
                this.status = Status.OK;
            }
        }

        private class InsertValueIndex : ISecondaryValueIndex<Key, Value>
        {
            public long lastWriteAddress;

            public string Name => nameof(InsertValueIndex);

            public bool IsMutable => true;

            public void SetSessionSlot(long slot) { }

            public void Delete(ref Key key, RecordId recordId, SecondaryIndexSessionBroker indexSessionBroker) { }

            public void Insert(ref Key key, ref Value value, RecordId recordId, SecondaryIndexSessionBroker indexSessionBroker) => lastWriteAddress = recordId.Address;

            public void Upsert(ref Key key, ref Value value, RecordId recordId, bool isMutableRecord, SecondaryIndexSessionBroker indexSessionBroker) { }

            public void OnPrimaryTruncate(long newBeginAddress) { }

            public void ScanReadOnlyPages(IFasterScanIterator<Key, Value> iter, SecondaryIndexSessionBroker indexSessionBroker) { }

            public void OnPrimaryCheckpointInitiated(PrimaryCheckpointInfo recoveredPCI) { }

            public void OnPrimaryCheckpointCompleted(PrimaryCheckpointInfo primaryCheckpointInfo) { }

            public PrimaryCheckpointInfo Recover(PrimaryCheckpointInfo recoveredPCI, bool undoNextVersion) => default;

            public Task<PrimaryCheckpointInfo> RecoverAsync(PrimaryCheckpointInfo recoveredPCI, bool undoNextVersion, CancellationToken cancellationToken = default) => default;

            public void RecoveryReplay(IFasterScanIterator<Key, Value> iter, SecondaryIndexSessionBroker indexSessionBroker) { }
        }

        private static long SetReadOutput(long key, long value) => (key << 32) | value;

        internal class Functions : AdvancedSimpleFunctions<Key, Value, Context>
        {
            public override void ConcurrentReader(ref Key key, ref Value input, ref Value value, ref Value dst, ref RecordInfo recordInfo, long address) 
                => dst.value = SetReadOutput(key.key, value.value);

            public override void SingleReader(ref Key key, ref Value input, ref Value value, ref Value dst, long address) 
                => dst.value = SetReadOutput(key.key, value.value);

            // Return false to force a chain of values.
            public override bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst, ref RecordInfo recordInfo, long address) => false;

            public override bool InPlaceUpdater(ref Key key, ref Value input, ref Value value, ref RecordInfo recordInfo, long address) => false;

            // Track the recordInfo for its PreviousAddress.
            public override void ReadCompletionCallback(ref Key key, ref Value input, ref Value output, Context ctx, Status status, RecordInfo recordInfo)
            {
                if (ctx is not null)
                {
                    ctx.output = output;
                    ctx.recordInfo = recordInfo;
                    ctx.status = status;
                }
            }

            public override void RMWCompletionCallback(ref Key key, ref Value input, Context ctx, Status status)
            {
                if (ctx is not null)
                {
                    ctx.output = input;
                    ctx.recordInfo = default;
                    ctx.status = status;
                }
                base.RMWCompletionCallback(ref key, ref input, ctx, status);
            }
        }

        private class TestStore : IDisposable
        {
            internal FasterKV<Key, Value> fkv;
            internal IDevice logDevice;
            internal string testDir;
            private readonly bool flush;
            readonly InsertValueIndex insertValueIndex = new InsertValueIndex();

            internal long[] InsertAddresses = new long[numKeys];

            internal TestStore(bool useReadCache, CopyReadsToTail copyReadsToTail, bool flush)
            {
                this.testDir = $"{TestContext.CurrentContext.TestDirectory}/{TestContext.CurrentContext.Test.Name}";
                this.logDevice = Devices.CreateLogDevice($"{testDir}/hlog.log");
                this.flush = flush;

                var logSettings = new LogSettings
                {
                    LogDevice = logDevice,
                    ObjectLogDevice = new NullDevice(),
                    ReadCacheSettings = useReadCache ? new ReadCacheSettings() : null,
                    CopyReadsToTail = copyReadsToTail,
                    // Use small-footprint values
                    PageSizeBits = 12, // (4K pages)
                    MemorySizeBits = 20 // (1M memory for main log)
                };

                this.fkv = new FasterKV<Key, Value>(
                    size: 1L << 20,
                    logSettings: logSettings,
                    checkpointSettings: new CheckpointSettings { CheckpointDir = $"{this.testDir}/CheckpointDir" },
                    serializerSettings: null,
                    comparer: new Key.Comparer()
                    );

                this.fkv.SecondaryIndexBroker.AddIndex(insertValueIndex);
            }

            internal async ValueTask Flush()
            {
                if (this.flush)
                {
                    if (!this.fkv.UseReadCache)
                        await this.fkv.TakeFullCheckpointAsync(CheckpointType.FoldOver);
                    this.fkv.Log.FlushAndEvict(wait: true);
                }
            }

            internal async Task Populate(bool useRMW, bool useAsync)
            {
                var functions = new Functions();
                using var session = this.fkv.For(functions).NewSession<Functions>();
                var context = new Context();

                var prevLap = 0;
                for (int ii = 0; ii < numKeys; ii++)
                {
                    // lap is used to illustrate the changing values
                    var lap = ii / keyMod;

                    if (lap != prevLap)
                    {
                        await Flush();
                        prevLap = lap;
                    }

                    var key = new Key(ii % keyMod);
                    var value = new Value(key.key + LapOffset(lap));

                    var status = useRMW
                        ? useAsync
                            ? (await session.RMWAsync(ref key, ref value, context, serialNo: lap)).Complete()
                            : session.RMW(ref key, ref value, serialNo: lap)
                        : session.Upsert(ref key, ref value, serialNo: lap);

                    if (status == Status.PENDING)
                        await session.CompletePendingAsync();

                    Assert.IsTrue(insertValueIndex.lastWriteAddress > 0);
                    InsertAddresses[ii] = insertValueIndex.lastWriteAddress;
                    //Assert.IsTrue(session.ctx.HasNoPendingRequests);

                    // Illustrate that deleted records can be shown as well (unless overwritten by in-place operations, which are not done here)
                    if (lap == deleteLap)
                        session.Delete(ref key, serialNo: lap);
                }

                await Flush();
            }

            internal bool ProcessChainRecord(Status status, RecordInfo recordInfo, int lap, ref Value actualOutput, ref int previousVersion)
            {
                Assert.GreaterOrEqual(lap, 0);
                long expectedValue = SetReadOutput(defaultKeyToScan, LapOffset(lap) + defaultKeyToScan);

                Assert.AreEqual(status == Status.NOTFOUND, recordInfo.Tombstone, $"status({status}) == NOTFOUND != Tombstone ({recordInfo.Tombstone})");
                Assert.AreEqual(lap == deleteLap, recordInfo.Tombstone, $"lap({lap}) == deleteLap({deleteLap}) != Tombstone ({recordInfo.Tombstone})");
                Assert.GreaterOrEqual(previousVersion, recordInfo.Version);
                if (!recordInfo.Tombstone)
                    Assert.AreEqual(expectedValue, actualOutput.value);

                // Check for end of loop
                previousVersion = recordInfo.Version;
                return recordInfo.PreviousAddress >= this.fkv.Log.BeginAddress;
            }

            internal void ProcessNoKeyRecord(Status status, ref Value actualOutput, int keyOrdinal)
            {
                if (status != Status.NOTFOUND)
                {
                    var keyToScan = keyOrdinal % keyMod;
                    var lap = keyOrdinal / keyMod;
                    long expectedValue = SetReadOutput(keyToScan, LapOffset(lap) + keyToScan);
                    Assert.AreEqual(expectedValue, actualOutput.value);
                }
            }

            public void Dispose()
            {
                if (this.fkv is not null)
                    this.fkv.Dispose();
                if (this.logDevice is not null)
                    this.logDevice.Dispose();
                if (!string.IsNullOrEmpty(this.testDir))
                    new DirectoryInfo(this.testDir).Delete(true);
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(false, CopyReadsToTail.None, false, false)]
        [TestCase(false, CopyReadsToTail.FromStorage, true, true)]
        [TestCase(true, CopyReadsToTail.None, false, true)]
        [Category("FasterKV")]
        public void VersionedReadSyncTests(bool useReadCache, CopyReadsToTail copyReadsToTail, bool useRMW, bool flush)
        {
            using var testStore = new TestStore(useReadCache, copyReadsToTail, flush);
            testStore.Populate(useRMW, useAsync:false).GetAwaiter().GetResult();
            using var session = testStore.fkv.For(new Functions()).NewSession<Functions>();

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var output = default(Value);
                var input = default(Value);
                var key = new Key(defaultKeyToScan);
                var context = new Context();
                RecordInfo recordInfo = default;
                int version = int.MaxValue;

                for (int lap = maxLap - 1; /* tested in loop */; --lap)
                {
                    var status = session.Read(ref key, ref input, ref output, ref recordInfo, userContext: context, serialNo: maxLap + 1);
                    if (status == Status.PENDING)
                    {
                        // This will spin CPU for each retrieved record; not recommended for performance-critical code or when retrieving chains for multiple records.
                        session.CompletePending(wait: true);
                        output = context.output;
                        recordInfo = context.recordInfo;
                        status = context.status;
                        context.Reset();
                    }
                    if (!testStore.ProcessChainRecord(status, recordInfo, lap, ref output, ref version))
                        break;
                }
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(false, CopyReadsToTail.None, false, false)]
        [TestCase(false, CopyReadsToTail.FromStorage, true, true)]
        [TestCase(true, CopyReadsToTail.None, false, true)]
        [Category("FasterKV")]
        public async Task VersionedReadAsyncTests(bool useReadCache, CopyReadsToTail copyReadsToTail, bool useRMW, bool flush)
        {
            using var testStore = new TestStore(useReadCache, copyReadsToTail, flush);
            await testStore.Populate(useRMW, useAsync: true);
            using var session = testStore.fkv.For(new Functions()).NewSession<Functions>();

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var input = default(Value);
                var key = new Key(defaultKeyToScan);
                RecordInfo recordInfo = default;
                int version = int.MaxValue;

                for (int lap = maxLap - 1; /* tested in loop */; --lap)
                {
                    var readAsyncResult = await session.ReadAsync(ref key, ref input, recordInfo.PreviousAddress, default, serialNo: maxLap + 1);
                    var (status, output) = readAsyncResult.Complete(out recordInfo);
                    if (!testStore.ProcessChainRecord(status, recordInfo, lap, ref output, ref version))
                        break;
                }
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(false, CopyReadsToTail.None, false, false)]
        [TestCase(false, CopyReadsToTail.FromStorage, true, true)]
        [TestCase(true, CopyReadsToTail.None, false, true)]
        [Category("FasterKV")]
        public void ReadAtAddressSyncTests(bool useReadCache, CopyReadsToTail copyReadsToTail, bool useRMW, bool flush)
        {
            using var testStore = new TestStore(useReadCache, copyReadsToTail, flush);
            testStore.Populate(useRMW, useAsync: false).GetAwaiter().GetResult();
            using var session = testStore.fkv.For(new Functions()).NewSession<Functions>();

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var output = default(Value);
                var input = default(Value);
                var key = new Key(defaultKeyToScan);
                var context = new Context();
                RecordInfo recordInfo = default;
                int version = int.MaxValue;

                for (int lap = maxLap - 1; /* tested in loop */; --lap)
                {
                    var readAtAddress = recordInfo.PreviousAddress;

                    var status = session.Read(ref key, ref input, ref output, ref recordInfo, userContext: context, serialNo: maxLap + 1);
                    if (status == Status.PENDING)
                    {
                        // This will spin CPU for each retrieved record; not recommended for performance-critical code or when retrieving chains for multiple records.
                        session.CompletePending(wait: true);
                        output = context.output;
                        recordInfo = context.recordInfo;
                        status = context.status;
                        context.Reset();
                    }
                    if (!testStore.ProcessChainRecord(status, recordInfo, lap, ref output, ref version))
                        break;

                    if (readAtAddress >= testStore.fkv.Log.BeginAddress)
                    {
                        var saveOutput = output;
                        var saveRecordInfo = recordInfo;

                        status = session.ReadAtAddress(readAtAddress, ref input, ref output, userContext: context, serialNo: maxLap + 1);
                        if (status == Status.PENDING)
                        {
                            // This will spin CPU for each retrieved record; not recommended for performance-critical code or when retrieving chains for multiple records.
                            session.CompletePending(wait: true);
                            output = context.output;
                            recordInfo = context.recordInfo;
                            status = context.status;
                            context.Reset();
                        }

                        Assert.AreEqual(saveOutput, output);
                        Assert.AreEqual(saveRecordInfo, recordInfo);
                    }
                }
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(false, CopyReadsToTail.None, false, false)]
        [TestCase(false, CopyReadsToTail.FromStorage, true, true)]
        [TestCase(true, CopyReadsToTail.None, false, true)]
        [Category("FasterKV")]
        public async Task ReadAtAddressAsyncTests(bool useReadCache, CopyReadsToTail copyReadsToTail, bool useRMW, bool flush)
        {
            using var testStore = new TestStore(useReadCache, copyReadsToTail, flush);
            await testStore.Populate(useRMW, useAsync: true);
            using var session = testStore.fkv.For(new Functions()).NewSession<Functions>();

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var input = default(Value);
                var key = new Key(defaultKeyToScan);
                RecordInfo recordInfo = default;
                int version = int.MaxValue;

                for (int lap = maxLap - 1; /* tested in loop */; --lap)
                {
                    var readAtAddress = recordInfo.PreviousAddress;

                    var readAsyncResult = await session.ReadAsync(ref key, ref input, recordInfo.PreviousAddress, default, serialNo: maxLap + 1);
                    var (status, output) = readAsyncResult.Complete(out recordInfo);
                    if (!testStore.ProcessChainRecord(status, recordInfo, lap, ref output, ref version))
                        break;

                    if (readAtAddress >= testStore.fkv.Log.BeginAddress)
                    {
                        var saveOutput = output;
                        var saveRecordInfo = recordInfo;

                        readAsyncResult = await session.ReadAtAddressAsync(readAtAddress, ref input, default, serialNo: maxLap + 1);
                        (status, output) = readAsyncResult.Complete(out recordInfo);

                        Assert.AreEqual(saveOutput, output);
                        Assert.AreEqual(saveRecordInfo, recordInfo);
                    }
                }
            }
        }

        // Test is similar to others but tests the Overload where ReadFlag.none is set -- probably don't need all combinations of test but doesn't hurt 
        [TestCase(false, CopyReadsToTail.None, false, false)]
        [TestCase(false, CopyReadsToTail.FromStorage, true, true)]
        [TestCase(true, CopyReadsToTail.None, false, true)]
        [Category("FasterKV")]
        public async Task ReadAtAddressAsyncReadFlagsNoneTests(bool useReadCache, CopyReadsToTail copyReadsToTail, bool useRMW, bool flush)
        {
            using var testStore = new TestStore(useReadCache, copyReadsToTail, flush);
            await testStore.Populate(useRMW, useAsync: true);
            using var session = testStore.fkv.For(new Functions()).NewSession<Functions>();

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var input = default(Value);
                var key = new Key(defaultKeyToScan);
                RecordInfo recordInfo = default;
                int version = int.MaxValue;

                for (int lap = maxLap - 1; /* tested in loop */; --lap)
                {
                    var readAtAddress = recordInfo.PreviousAddress;

                    var readAsyncResult = await session.ReadAsync(ref key, ref input, recordInfo.PreviousAddress, default, serialNo: maxLap + 1);
                    var (status, output) = readAsyncResult.Complete(out recordInfo);
                    if (!testStore.ProcessChainRecord(status, recordInfo, lap, ref output, ref version))
                        break;

                    if (readAtAddress >= testStore.fkv.Log.BeginAddress)
                    {
                        var saveOutput = output;
                        var saveRecordInfo = recordInfo;

                        readAsyncResult = await session.ReadAtAddressAsync(readAtAddress, ref input, ReadFlags.None, default, serialNo: maxLap + 1);
                        (status, output) = readAsyncResult.Complete(out recordInfo);

                        Assert.AreEqual(saveOutput, output);
                        Assert.AreEqual(saveRecordInfo, recordInfo);
                    }
                }
            }
        }

        // Test is similar to others but tests the Overload where RadFlag.SkipReadCache is set
        [TestCase(false, CopyReadsToTail.None, false, false)]
        [TestCase(false, CopyReadsToTail.FromStorage, true, true)]
        [TestCase(true, CopyReadsToTail.None, false, true)]
        [Category("FasterKV")]
        public async Task ReadAtAddressAsyncReadFlagsSkipCacheTests(bool useReadCache, CopyReadsToTail copyReadsToTail, bool useRMW, bool flush)
        {
            using var testStore = new TestStore(useReadCache, copyReadsToTail, flush);
            await testStore.Populate(useRMW, useAsync: true);
            using var session = testStore.fkv.For(new Functions()).NewSession<Functions>();

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var input = default(Value);
                var key = new Key(defaultKeyToScan);
                RecordInfo recordInfo = default;
                int version = int.MaxValue;

                for (int lap = maxLap - 1; /* tested in loop */; --lap)
                {
                    var readAtAddress = recordInfo.PreviousAddress;

                    var readAsyncResult = await session.ReadAsync(ref key, ref input, recordInfo.PreviousAddress, default, serialNo: maxLap + 1);
                    var (status, output) = readAsyncResult.Complete(out recordInfo);
                    if (!testStore.ProcessChainRecord(status, recordInfo, lap, ref output, ref version))
                        break;

                    if (readAtAddress >= testStore.fkv.Log.BeginAddress)
                    {
                        var saveOutput = output;
                        var saveRecordInfo = recordInfo;

                        readAsyncResult = await session.ReadAtAddressAsync(readAtAddress, ref input, ReadFlags.SkipReadCache, default, maxLap + 1);
                        (status, output) = readAsyncResult.Complete(out recordInfo);

                        Assert.AreEqual(saveOutput, output);
                        Assert.AreEqual(saveRecordInfo, recordInfo);
                    }
                }
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(false, CopyReadsToTail.None, false, false)]
        [TestCase(false, CopyReadsToTail.FromStorage, true, true)]
        [TestCase(true, CopyReadsToTail.None, false, true)]
        [Category("FasterKV")]
        public void ReadNoKeySyncTests(bool useReadCache, CopyReadsToTail copyReadsToTail, bool useRMW, bool flush)        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        {
            using var testStore = new TestStore(useReadCache, copyReadsToTail, flush);
            testStore.Populate(useRMW, useAsync: false).GetAwaiter().GetResult();
            using var session = testStore.fkv.For(new Functions()).NewSession<Functions>();

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var rng = new Random(101);
                var output = default(Value);
                var input = default(Value);
                var context = new Context();

                for (int ii = 0; ii < numKeys; ++ii)
                {
                    var keyOrdinal = rng.Next(numKeys);
                    var status = session.ReadAtAddress(testStore.InsertAddresses[keyOrdinal], ref input, ref output, userContext: context, serialNo: maxLap + 1);
                    if (status == Status.PENDING)
                    {
                        // This will spin CPU for each retrieved record; not recommended for performance-critical code or when retrieving chains for multiple records.
                        session.CompletePending(wait: true);
                        output = context.output;
                        status = context.status;
                        context.Reset();
                    }

                    testStore.ProcessNoKeyRecord(status, ref output, keyOrdinal);
                }

                testStore.Flush().GetAwaiter().GetResult();
            }
        }

        // readCache and copyReadsToTail are mutually exclusive and orthogonal to populating by RMW vs. Upsert.
        [TestCase(false, CopyReadsToTail.None, false, false)]
        [TestCase(false, CopyReadsToTail.FromStorage, true, true)]
        [TestCase(true, CopyReadsToTail.None, false, true)]
        [Category("FasterKV")]
        public async Task ReadNoKeyAsyncTests(bool useReadCache, CopyReadsToTail copyReadsToTail, bool useRMW, bool flush)
        {
            using var testStore = new TestStore(useReadCache, copyReadsToTail, flush);
            await testStore.Populate(useRMW, useAsync: true);
            using var session = testStore.fkv.For(new Functions()).NewSession<Functions>();

            // Two iterations to ensure no issues due to read-caching or copying to tail.
            for (int iteration = 0; iteration < 2; ++iteration)
            {
                var rng = new Random(101);
                var input = default(Value);
                RecordInfo recordInfo = default;

                for (int ii = 0; ii < numKeys; ++ii)
                {
                    var keyOrdinal = rng.Next(numKeys);
                    var readAsyncResult = await session.ReadAtAddressAsync(testStore.InsertAddresses[keyOrdinal], ref input, default, serialNo: maxLap + 1);
                    var (status, output) = readAsyncResult.Complete(out recordInfo);
                    testStore.ProcessNoKeyRecord(status, ref output, keyOrdinal);
                }
            }

            await testStore.Flush();
        }
    }
}
