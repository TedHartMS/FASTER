// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using FASTER.indexes.HashValueIndex;
using FASTER.test.HashValueIndex.CheckpointMetadata;
using System;
using NUnit.Framework;

namespace FASTER.test.HashValueIndex.CompatibleRecoveryTests
{
    class HashValueIndexCheckpointRecoveryTester : CheckpointRecoveryTester
    {
        private HashValueIndexTestBase testBase;
        private readonly RecoveryTests testContainer;
        bool usePrimarySnapshot;

        internal HashValueIndexCheckpointRecoveryTester(HashValueIndexTestBase testBase, RecoveryTests tester)
            : base(testBase.outerCheckpointManager,  testBase.innerCheckpointManager)
        {
            this.testBase = testBase;
            this.testContainer = tester;
        }

        internal override void Run(string testName, bool usePrimarySnapshot)
        {
            this.usePrimarySnapshot = usePrimarySnapshot;
            base.Run(testName);
        }

        internal override void Recover(Guid primaryToken, ExpectedRecoveryState expectedRecoveryState)
        {
            Assert.AreEqual(expectedRecoveryState.PrePTail, testBase.primaryFkv.Log.TailAddress);
            Assert.AreEqual(expectedRecoveryState.PreSTail, testBase.index.secondaryFkv.Log.TailAddress);

            // We recover from the last Primary Checkpoint, and we always take full checkpoints in this test.
            testBase = testContainer.PrepareToRecover();
            base.ResetCheckpointManagers(testBase.outerCheckpointManager, testBase.innerCheckpointManager);
            testBase.primaryFkv.Recover(primaryToken);

            bool expectedToRecoverSecondary = expectedRecoveryState.PostSTail > testBase.index.secondaryFkv.Log.BeginAddress;
            if (expectedToRecoverSecondary)
            {
                // Verify the expected primaryRecoveredPci was passed to secondary Recover().
                Assert.AreEqual(base.lastCompletedPci.Version, testBase.index.primaryRecoveredPci.Version);
                Assert.AreEqual(base.lastCompletedPci.FlushedUntilAddress, testBase.index.primaryRecoveredPci.FlushedUntilAddress);
            }

            // Verify primary recovery.
            Assert.AreEqual(expectedRecoveryState.PostPTail, testBase.primaryFkv.Log.TailAddress);

            // Secondary recovery is verified in two parts:
            // 1. How far did we recover during the FasterKV Recover phase. recoveryTailAddress is 0 if no checkpoint was found.
            Assert.AreEqual(expectedRecoveryState.PostSTail,
                            testBase.index.recoveredTailAddress != 0 ? testBase.index.recoveredTailAddress : testBase.index.secondaryFkv.Log.BeginAddress);
            // 2. How far did we roll forward. Note the use of rowCount here, because PostSTail is the pre-rollback record count.
            Assert.AreEqual(base.rowCount != 0 ? base.secondaryTailAddresses[base.rowCount] : testBase.index.secondaryFkv.Log.BeginAddress,
                            testBase.index.secondaryFkv.Log.TailAddress);

            // This is similar to the foregoing, to verify the expected handling of primaryRecoveredPci -> secondaryLogToken was correct and expectedRecoveryState is correct.
            Assert.IsTrue(this.outerCheckpointManager.GetRecoveryTokens(testBase.index.primaryRecoveredPci, out _ /* indexToken */, out var logToken, out var localLastCompletedPci, out var localLastStartedPci));

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
            testBase.primaryFkv.TakeFullCheckpoint(out Guid primaryLogToken, this.usePrimarySnapshot ? CheckpointType.Snapshot : CheckpointType.FoldOver);
            testBase.primaryFkv.CompleteCheckpointAsync().GetAwaiter().GetResult(); // Do not do this in production

            var hlci = new HybridLogCheckpointInfo();
            hlci.Recover(primaryLogToken, testBase.primaryFkv.checkpointManager, default);

            base.lastCompletedPci = outerCheckpointManager.secondaryMetadata.lastCompletedPrimaryCheckpointInfo;
            lastPrimaryVer = hlci.info.version;
            Assert.AreEqual(lastPrimaryVer, base.lastCompletedPci.Version);
            return primaryLogToken;
        }

        internal override void CommitSecondary()
        {
            // We always take full checkpoints in this test.
            testBase.index.TakeFullCheckpoint(out var logToken);
            testBase.index.CompleteCheckpointAsync().GetAwaiter().GetResult(); // Do not do this in production

            // Return the metadata from the inner checkpoint manager so it is not stripped of the wrapper metadata.
            var metadata = this.innerCheckpointManager.GetLogCheckpointMetadata(logToken, default);
            var secondaryMetadata = CheckpointManager<int>.GetSecondaryMetadata(metadata);
            Assert.AreEqual(base.lastPrimaryVersion, secondaryMetadata.lastCompletedPrimaryCheckpointInfo.Version);
        }

        internal override (long ptail, long stail) DoInserts()
        {
            using var session = testBase.primaryFkv.For(new SimpleFunctions<int, int>()).NewSession<SimpleFunctions<int, int>>();
            for (int ii = 0; ii < RecordsPerChunk; ++ii)
            {
                session.Upsert(ii, ii);
            }
            session.CompletePending(true);
            testBase.primaryFkv.Log.FlushAndEvict(wait: true);
            return (testBase.primaryFkv.Log.TailAddress, testBase.index.secondaryFkv.Log.TailAddress);
        }
    }

    [TestFixture]
    internal class RecoveryTests
    {
        HashValueIndexTestBase testBase;
        internal CheckpointRecoveryTester tester;

        [SetUp]
        public void Setup()
        {
            this.testBase = new HashValueIndexTestBase(1);
            this.tester = new HashValueIndexCheckpointRecoveryTester(this.testBase, this);
        }

        [TearDown]
        public void TearDown() => TearDown(deleteDir: true);

        public void TearDown(bool deleteDir)
        {
            testBase.TearDown(deleteDir);
            testBase = null;
            tester = null;
        }

        internal HashValueIndexTestBase PrepareToRecover()
        {
            TearDown(deleteDir:false);
            Setup();
            return this.testBase;
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
