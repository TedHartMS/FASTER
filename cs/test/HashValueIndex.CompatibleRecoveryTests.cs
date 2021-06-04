// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using FASTER.indexes.HashValueIndex;
using FASTER.test.HashValueIndex.CheckpointMetadata;
using System;
using NUnit.Framework;
using System.IO;
using System.Linq;

namespace FASTER.test.HashValueIndex.CompatibleRecoveryTests
{
    class HashValueIndexCheckpointRecoveryTester : CheckpointRecoveryTester
    {
        private readonly RecoveryTests testContainer;
        bool usePrimarySnapshot;

        internal HashValueIndexCheckpointRecoveryTester(RecoveryTests tester)
            : base(tester.outerCheckpointManager,  tester.innerCheckpointManager)
        {
            this.testContainer = tester;
        }

        internal override void Run(string testName, bool usePrimarySnapshot)
        {
            this.usePrimarySnapshot = usePrimarySnapshot;
            base.Run(testName);
        }

        internal override void Recover(Guid primaryToken, ExpectedRecoveryState expectedRecoveryState)
        {
            Assert.AreEqual(expectedRecoveryState.PrePTail, testContainer.primaryFkv.Log.TailAddress);
            Assert.AreEqual(expectedRecoveryState.PreSTail, testContainer.index.secondaryFkv.Log.TailAddress);

            // We recover from the last Primary Checkpoint, and we always take full checkpoints in this test.
            testContainer.PrepareToRecover();
            base.ResetCheckpointManagers(testContainer.outerCheckpointManager, testContainer.innerCheckpointManager);
            testContainer.primaryFkv.Recover(primaryToken);

            bool expectedToRecoverSecondary = expectedRecoveryState.PostSTail > testContainer.index.secondaryFkv.Log.BeginAddress;
            if (expectedToRecoverSecondary)
            {
                // Verify the expected primaryRecoveredPci was passed to secondary Recover().
                Assert.AreEqual(base.lastCompletedPci.Version, testContainer.index.primaryRecoveredPci.Version);
                Assert.AreEqual(base.lastCompletedPci.FlushedUntilAddress, testContainer.index.primaryRecoveredPci.FlushedUntilAddress);
            }

            // Verify primary recovery.
            Assert.AreEqual(expectedRecoveryState.PostPTail, testContainer.primaryFkv.Log.TailAddress);

            // Secondary recovery is verified in two parts:
            // 1. How far did we recover during the FasterKV Recover phase. recoveryTailAddress is 0 if no checkpoint was found.
            Assert.AreEqual(expectedRecoveryState.PostSTail, 
                            testContainer.index.recoveredTailAddress != 0 ? testContainer.index.recoveredTailAddress : testContainer.index.secondaryFkv.Log.BeginAddress);
            // 2. How far did we roll forward. Note the use of rowCount here, because PostSTail is the pre-rollback record count.
            Assert.AreEqual(base.rowCount != 0 ? base.secondaryTailAddresses[base.rowCount] : testContainer.index.secondaryFkv.Log.BeginAddress,
                            testContainer.index.secondaryFkv.Log.TailAddress);

            // This is similar to the foregoing, to verify the expected handling of primaryRecoveredPci -> secondaryLogToken was correct and expectedRecoveryState is correct.
            Assert.IsTrue(this.outerCheckpointManager.GetRecoveryTokens(testContainer.index.primaryRecoveredPci, out _ /* indexToken */, out var logToken, out var localLastCompletedPci, out var localLastStartedPci));

            if (logToken == Guid.Empty)
            {
                Assert.AreEqual(0, localLastCompletedPci.Version);
                Assert.AreEqual(0, localLastCompletedPci.FlushedUntilAddress);
                Assert.AreEqual(0, localLastStartedPci.Version);
                return;
            }
            Assert.AreEqual(expectedRecoveryState.completedPci.Version, localLastCompletedPci.Version);
            Assert.AreEqual(expectedRecoveryState.completedPci.FlushedUntilAddress, localLastCompletedPci.FlushedUntilAddress);
            Assert.AreEqual(expectedRecoveryState.startedPci.Version, localLastStartedPci.Version);
        }

        internal override Guid CommitPrimary(out int lastPrimaryVer)
        {
            // We always take full checkpoints in this test. TODO: extend this to show separate start/completed intermixing
            testContainer.primaryFkv.TakeFullCheckpoint(out Guid primaryLogToken, this.usePrimarySnapshot ? CheckpointType.Snapshot : CheckpointType.FoldOver);
            testContainer.primaryFkv.CompleteCheckpointAsync().GetAwaiter().GetResult(); // Do not do this in production

            var hlci = new HybridLogCheckpointInfo();
            hlci.Recover(primaryLogToken, testContainer.primaryFkv.checkpointManager, default);

            base.lastCompletedPci = outerCheckpointManager.secondaryMetadata.lastCompletedPrimaryCheckpointInfo;
            lastPrimaryVer = hlci.info.version;
            Assert.AreEqual(lastPrimaryVer, base.lastCompletedPci.Version);
            return primaryLogToken;
        }

        internal override void CommitSecondary()
        {
            // We always take full checkpoints in this test.
            testContainer.index.TakeFullCheckpoint(out var logToken);
            testContainer.index.CompleteCheckpointAsync().GetAwaiter().GetResult(); // Do not do this in production

            // Return the metadata from the inner checkpoint manager so it is not stripped of the wrapper metadata.
            var metadata = this.innerCheckpointManager.GetLogCheckpointMetadata(logToken, default);
            var secondaryMetadata = CheckpointManager<int>.GetSecondaryMetadata(metadata);
            Assert.AreEqual(base.lastPrimaryVersion, secondaryMetadata.lastCompletedPrimaryCheckpointInfo.Version);
        }

        internal override (long ptail, long stail) DoInserts()
        {
            using var session = testContainer.primaryFkv.For(new SimpleFunctions<int, int>()).NewSession<SimpleFunctions<int, int>>();
            for (int ii = 0; ii < RecordsPerChunk; ++ii)
            {
                session.Upsert(ii, ii);
            }
            session.CompletePending(true);
            testContainer.primaryFkv.Log.FlushAndEvict(wait: true);
            return (testContainer.primaryFkv.Log.TailAddress, testContainer.index.secondaryFkv.Log.TailAddress);
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

        private string testBaseDir, testDir;
        private IDevice primaryLog;
        private IDevice secondaryLog;

        // Used by HashValueIndexCheckpointRecoveryTester
        internal FasterKV<int, int> primaryFkv;
        internal HashValueIndex<int, int, int> index;
        internal IPredicate intPred;
        internal CheckpointManager<int> outerCheckpointManager;
        internal ICheckpointManager innerCheckpointManager;
        internal CheckpointRecoveryTester tester;

        [SetUp]
        public void Setup()
        {
            this.testBaseDir = Path.Combine(TestContext.CurrentContext.TestDirectory, TestContext.CurrentContext.Test.ClassName.Split('.').Last());
            this.testDir = Path.Combine(this.testBaseDir, TestContext.CurrentContext.Test.MethodName);
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
            this.outerCheckpointManager = new CheckpointManager<int>(IndexName, innerCheckpointManager);
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

            this.tester = new HashValueIndexCheckpointRecoveryTester(this);
        }

        [TearDown]
        public void TearDown() => TearDown(deleteDir: true);

        public void TearDown(bool deleteDir)
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

            if (deleteDir)
                TestUtils.DeleteDirectory(this.testBaseDir);
        }

        internal void PrepareToRecover()
        {
            TearDown(deleteDir:false);
            Setup();
        }

        [Test]
        [Category(TestUtils.SecondaryIndexCategory), Category(TestUtils.HashValueIndexCategory)]
        public void NoCheckpointsTest([Values]bool usePrimarySnapshot)
        {
            tester.Run(TestContext.CurrentContext.Test.MethodName, usePrimarySnapshot);
        }

        [Test]
        [Category(TestUtils.SecondaryIndexCategory), Category(TestUtils.HashValueIndexCategory)]
        public void PrimaryOnlyCheckpointTest([Values] bool usePrimarySnapshot)
        {
            tester.Run(TestContext.CurrentContext.Test.MethodName, usePrimarySnapshot);
        }

        [Test]
        [Category(TestUtils.SecondaryIndexCategory), Category(TestUtils.HashValueIndexCategory)]
        public void NoDataRecoveredTest([Values] bool usePrimarySnapshot)
        {
            tester.Run(TestContext.CurrentContext.Test.MethodName, usePrimarySnapshot);
        }

        [Test]
        [Category(TestUtils.SecondaryIndexCategory), Category(TestUtils.HashValueIndexCategory)]
        public void RecoverPrimaryWithNoSecondaryTest([Values] bool usePrimarySnapshot)
        {
            tester.Run(TestContext.CurrentContext.Test.MethodName, usePrimarySnapshot);
        }

        [Test]
        [Category(TestUtils.SecondaryIndexCategory), Category(TestUtils.HashValueIndexCategory)]
        public void RecoverOneChunkTest([Values] bool usePrimarySnapshot)
        {
            tester.Run(TestContext.CurrentContext.Test.MethodName, usePrimarySnapshot);
        }

        [Test]
        [Category(TestUtils.SecondaryIndexCategory), Category(TestUtils.HashValueIndexCategory)]
        public void RecoverOneChunkButNotTheOtherTest([Values] bool usePrimarySnapshot)
        {
            tester.Run(TestContext.CurrentContext.Test.MethodName, usePrimarySnapshot);
        }

        [Test]
        [Category(TestUtils.SecondaryIndexCategory), Category(TestUtils.HashValueIndexCategory)]
        public void RecoverTwoChunksButReplayTheSecondTest([Values] bool usePrimarySnapshot)
        {
            tester.Run(TestContext.CurrentContext.Test.MethodName, usePrimarySnapshot);
        }

        [Test]
        [Category(TestUtils.SecondaryIndexCategory), Category(TestUtils.HashValueIndexCategory)]
        public void RecoverOneChunkButNotTheOther2Test1([Values] bool usePrimarySnapshot)
        {
            tester.Run(TestContext.CurrentContext.Test.MethodName, usePrimarySnapshot);
        }

        [Test]
        [Category(TestUtils.SecondaryIndexCategory), Category(TestUtils.HashValueIndexCategory)]
        public void RecoverOneChunkButNotTheOther2Test2([Values] bool usePrimarySnapshot)
        {
            tester.Run(TestContext.CurrentContext.Test.MethodName, usePrimarySnapshot);
        }

        [Test]
        [Category(TestUtils.SecondaryIndexCategory), Category(TestUtils.HashValueIndexCategory)]
        public void RecoverTwoChunksTest([Values] bool usePrimarySnapshot)
        {
            tester.Run(TestContext.CurrentContext.Test.MethodName, usePrimarySnapshot);
        }
    }
}
