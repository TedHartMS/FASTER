// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using FASTER.indexes.HashValueIndex;
using FASTER.test.HashValueIndex.CheckpointMetadata;
using System;
using NUnit.Framework;
using System.IO;

namespace FASTER.test.HashValueIndex.CompatibleRecoveryTests
{
    class HashValueIndexCheckpointManagerTest : CheckpointManager<int>
    {
        internal PrimaryCheckpointInfo recoveredPci;

        internal HashValueIndexCheckpointManagerTest(string indexName, ICheckpointManager userCheckpointManager)
            : base(indexName, userCheckpointManager)
        { }

        internal override void RecoverHLCInfo(ref HybridLogCheckpointInfo recoveredHLCInfo, Guid logToken)
        {
            base.RecoverHLCInfo(ref recoveredHLCInfo, logToken);
            this.recoveredPci = new PrimaryCheckpointInfo(recoveredHLCInfo.info.version,
                                                          recoveredHLCInfo.info.useSnapshotFile != 0? recoveredHLCInfo.info.finalLogicalAddress : recoveredHLCInfo.info.flushedLogicalAddress);
        }

        internal override Guid GetCompatibleIndexToken(ref HybridLogCheckpointInfo recoveredHLCInfo) => base.GetCompatibleIndexToken(ref recoveredHLCInfo);
    }

    class HashValueIndexCheckpointRecoveryTester : CheckpointRecoveryTester
    {
        private readonly FasterKV<int, int> primaryFkv;
        internal HashValueIndex<int, int, int> index;

        internal HashValueIndexCheckpointRecoveryTester(CheckpointManager<int> outerCheckpointManager, ICheckpointManager innerCheckpointManager, FasterKV<int, int> primaryFkv, HashValueIndex<int, int, int> index)
            : base(outerCheckpointManager,  innerCheckpointManager)
        {
            this.primaryFkv = primaryFkv;
            this.index = index;
        }

        internal override void Recover(Guid primaryToken, ExpectedRecoveryState expectedRecoveryState)
        {
            // We recover from the last Primary Checkpoint, and we always take full checkpoints in this test.
            this.primaryFkv.Recover(primaryToken);
            var hviCheckpointManager = base.outerCheckpointManager as HashValueIndexCheckpointManagerTest;

            var recoveredPci = hviCheckpointManager.recoveredPci;

            Assert.IsTrue(this.outerCheckpointManager.GetRecoveryTokens(recoveredPci, out _ /* indexToken */, out var logToken, out var lastCompletedPci, out var lastStartedPci));

            // We don't support inserts with this default implementation.
            if (logToken == Guid.Empty)
            {
                Assert.AreEqual(0, lastCompletedPci.Version);
                Assert.AreEqual(0, lastCompletedPci.FlushedUntilAddress);
                Assert.AreEqual(0, lastStartedPci.Version);
                return;
            }
            Assert.AreEqual(expectedRecoveryState.completedPci.Version, lastCompletedPci.Version);
            Assert.AreEqual(expectedRecoveryState.completedPci.FlushedUntilAddress, lastCompletedPci.FlushedUntilAddress);
            Assert.AreEqual(expectedRecoveryState.startedPci.Version, lastStartedPci.Version);
        }

        internal override Guid CommitPrimary(out int lastPrimaryVer)
        {
            // We always take full checkpoints in this test. TODO: extend this to show separate start/completed intermixing
            this.primaryFkv.TakeFullCheckpoint(out Guid primaryLogToken);
            this.primaryFkv.CompleteCheckpointAsync().GetAwaiter().GetResult(); // Do not do this in production

            var hlci = new HybridLogCheckpointInfo();
            hlci.Recover(primaryLogToken, primaryFkv.checkpointManager, default);

            var hviCheckpointManager = base.outerCheckpointManager as HashValueIndexCheckpointManagerTest;
            lastPrimaryVer = hlci.info.version;
            Assert.AreEqual(lastPrimaryVer, hviCheckpointManager.secondaryMetadata.lastCompletedPrimaryCheckpointInfo.Version);
            return primaryLogToken;
        }

        internal override void CommitSecondary()
        {
            // We always take full checkpoints in this test.
            this.index.TakeFullCheckpoint(out _);
        }

        internal override (long ptail, long stail) DoInserts()
        {
            using var session = this.primaryFkv.For(new SimpleFunctions<int, int>()).NewSession<SimpleFunctions<int, int>>();
            for (int ii = 0; ii < CheckpointRecoveryTester.RecordsPerChunk; ++ii)
            {
                session.Upsert(ii, ii);
            }
            session.CompletePending(true);
            return (this.primaryFkv.Log.TailAddress, this.index.secondaryFkv.Log.TailAddress);
        }
    }

    internal class RecoveryTests
    {
        // Hash and log sizes
        internal const int HashSizeBits = 20;
        private const int MemorySizeBits = 29;
        private const int SegmentSizeBits = 25;
        private const int PageSizeBits = 20;
        private const string IndexName = "IntIndex";

        private string testDir;
        private IDevice primaryLog;
        private IDevice secondaryLog;
        private FasterKV<int, int> primaryFkv;
        internal HashValueIndex<int, int, int> index;
        internal IPredicate intPred;

        CheckpointManager<int> outerCheckpointManager;
        ICheckpointManager innerCheckpointManager;
        CheckpointRecoveryTester tester;

        [SetUp]
        public void Setup()
        {
            this.testDir = $"{TestContext.CurrentContext.TestDirectory}/{TestContext.CurrentContext.Test.Name}";
            var primaryDir = Path.Combine(this.testDir, "PrimaryFKV");
            var secondaryDir = Path.Combine(this.testDir, "SecondaryFKV");

            this.primaryLog = Devices.CreateLogDevice(Path.Combine(primaryDir, "hlog.log"));

            var primaryLogSettings = new LogSettings
            {
                LogDevice = primaryLog,
                ObjectLogDevice = default,
                MemorySizeBits = MemorySizeBits,
                SegmentSizeBits = SegmentSizeBits,
                PageSizeBits = PageSizeBits,
                CopyReadsToTail = CopyReadsToTail.None,
                ReadCacheSettings = null
            };

            this.secondaryLog = Devices.CreateLogDevice(Path.Combine(secondaryDir, "hlog.log"));

            var secondaryLogSettings = new LogSettings
            {
                LogDevice = secondaryLog,
                MemorySizeBits = MemorySizeBits,
                SegmentSizeBits = SegmentSizeBits,
                PageSizeBits = PageSizeBits,
                CopyReadsToTail = CopyReadsToTail.None,
                ReadCacheSettings = null
            };

            this.primaryFkv = new FasterKV<int, int>(
                                1L << 20, primaryLogSettings,
                                new CheckpointSettings { CheckpointDir = primaryDir, CheckPointType = CheckpointType.FoldOver });


            var secondaryCheckpointSettings = new CheckpointSettings { CheckpointDir = secondaryDir, CheckPointType = CheckpointType.FoldOver };
            innerCheckpointManager = Utility.CreateDefaultCheckpointManager(secondaryCheckpointSettings);
            this.outerCheckpointManager = new HashValueIndexCheckpointManagerTest(IndexName, innerCheckpointManager);
            secondaryCheckpointSettings.CheckpointManager = this.outerCheckpointManager;

            var secondaryRegSettings = new RegistrationSettings<int>
            {
                HashTableSize = 1L << HashSizeBits,
                LogSettings = secondaryLogSettings,
                CheckpointSettings = secondaryCheckpointSettings
            };

            this.index = new HashValueIndex<int, int, int>(IndexName, this.primaryFkv, secondaryRegSettings, nameof(this.intPred), v => v);
            this.primaryFkv.SecondaryIndexBroker.AddIndex(this.index);
            this.intPred = this.index.GetPredicate(nameof(this.intPred));

            this.tester = new HashValueIndexCheckpointRecoveryTester(this.outerCheckpointManager, this.innerCheckpointManager, this.primaryFkv, this.index);
        }

        [TearDown]
        public void TearDown()
        {
            this.primaryFkv?.Dispose();
            this.primaryFkv = null;
            this.index?.Dispose();
            this.index = null;
            this.primaryLog.Dispose();
            this.primaryLog = null;
            this.secondaryLog.Dispose();
            this.secondaryLog = null;
            
            this.outerCheckpointManager?.Dispose();
            this.outerCheckpointManager = null;
            this.innerCheckpointManager?.Dispose();
            this.innerCheckpointManager = null;
        }

        [Test]
        [Category(TestUtils.SecondaryIndexCategory), Category(TestUtils.HashValueIndexCategory)]
        public void NoDataRestoreTest()
        {
            tester.Run(TestContext.CurrentContext.Test.Name);
        }

        [Test]
        [Category(TestUtils.SecondaryIndexCategory), Category(TestUtils.HashValueIndexCategory)]
        public void NoSecondaryCheckpointRestoreTest()
        {
            tester.Run(TestContext.CurrentContext.Test.Name);
        }

        [Test]
        [Category(TestUtils.SecondaryIndexCategory), Category(TestUtils.HashValueIndexCategory)]
        public void RestoreOneChunkTest()
        {
            tester.Run(TestContext.CurrentContext.Test.Name);
        }

        [Test]
        [Category(TestUtils.SecondaryIndexCategory), Category(TestUtils.HashValueIndexCategory)]
        public void RestoreOneChunkButNotTheOtherTest1()
        {
            tester.Run(TestContext.CurrentContext.Test.Name);
        }

        [Test]
        [Category(TestUtils.SecondaryIndexCategory), Category(TestUtils.HashValueIndexCategory)]
        public void RestoreOneChunkButNotTheOtherTest2()
        {
            tester.Run(TestContext.CurrentContext.Test.Name);
        }

        [Test]
        [Category(TestUtils.SecondaryIndexCategory), Category(TestUtils.HashValueIndexCategory)]
        public void RestoreOneChunkButNotTheOther2Test1()
        {
            tester.Run(TestContext.CurrentContext.Test.Name);
        }

        [Test]
        [Category(TestUtils.SecondaryIndexCategory), Category(TestUtils.HashValueIndexCategory)]
        public void RestoreOneChunkButNotTheOther2Test2()
        {
            tester.Run(TestContext.CurrentContext.Test.Name);
        }

        [Test]
        [Category(TestUtils.SecondaryIndexCategory), Category(TestUtils.HashValueIndexCategory)]
        public void RestoreTwoChunksTest()
        {
            tester.Run(TestContext.CurrentContext.Test.Name);
        }
    }
}
