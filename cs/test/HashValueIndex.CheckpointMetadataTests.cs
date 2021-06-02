// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using FASTER.indexes.HashValueIndex;
using System;
using System.Collections.Generic;
using NUnit.Framework;
using System.Linq;

namespace FASTER.test.HashValueIndex.CheckpointMetadata
{
    internal class CheckpointRecoveryTester
    {
        internal enum Op
        {
            I,              // Insert records
            P1, P2, P3,     // Primary checkpoints
            S1, S2, S3      // Secondary checkpoints
        }

        internal struct ExpectedRecoveryState
        {
            internal long PrePTail, PreSTail, PostPTail, PostSTail;     // These are rowcount placeholders, to be mapped to actual tail addresses
            internal PrimaryCheckpointInfo completedPci, startedPci;
        }

        internal struct TestSequence
        {
            internal Op[] opSequence;
            internal ExpectedRecoveryState expected;
        }

        internal const int RecordsPerChunk = 1000;
        internal const int BA = 64; // BeginAddress
        protected readonly CheckpointManager<int> outerCheckpointManager;
        protected readonly ICheckpointManager innerCheckpointManager;

        // Per-TestSequence variables.
        private Guid lastPrimaryCommitToken;
        int lastPrimaryVersion = 0;
        long rowCount = 0;

        // For the default Test implementation; used by methods that are overridden by the HashValueIndex implementation.
        int defaultPrimaryVersion = 0;
        readonly Dictionary<long, long> primaryTailAddresses = new Dictionary<long, long>() { [0] = 0, [BA] = BA};
        readonly Dictionary<long, long> secondaryTailAddresses = new Dictionary<long, long>() { [0] = 0, [BA] = BA };
        PrimaryCheckpointInfo lastCompletedPci;
        PrimaryCheckpointInfo lastStartedPci;

        // Recovery is done following the last Op here. The structures are initialized using rowcounts for addresses; these are mapped in the test (because the
        // simple test does not do inserts; see HashValueIndex for that).
        internal Dictionary<string, TestSequence> testSequences = new Dictionary<string, TestSequence>{
            [nameof(CheckpointMetadataTests.NoDataRestoreTest)] = 
                new TestSequence { opSequence = new Op[] { Op.S1, Op.P1 },
                                   expected = new ExpectedRecoveryState {PrePTail = BA, PreSTail = BA, PostPTail = BA, PostSTail = BA,
                                                                         completedPci = new PrimaryCheckpointInfo (0, 0), startedPci = new PrimaryCheckpointInfo(0, 0)} },
            [nameof(CheckpointMetadataTests.NoSecondaryCheckpointRestoreTest)] =
                new TestSequence { opSequence = new Op[] { Op.I, Op.S1, Op.P1 },
                                   expected = new ExpectedRecoveryState {PrePTail = 1000, PreSTail = 1000, PostPTail = 1000, PostSTail = BA,
                                                                         completedPci = new PrimaryCheckpointInfo (0, 0), startedPci = new PrimaryCheckpointInfo(0, 0)} },
            [nameof(CheckpointMetadataTests.RestoreOneChunkTest)] = 
                new TestSequence { opSequence = new Op[] { Op.I, Op.P1, Op.S1, Op.P2 },
                                   expected = new ExpectedRecoveryState {PrePTail = 1000, PreSTail = 1000, PostPTail = 1000, PostSTail = 1000,
                                                                         completedPci = new PrimaryCheckpointInfo (1, 1000), startedPci = new PrimaryCheckpointInfo(1, 0)} },
            [nameof(CheckpointMetadataTests.RestoreOneChunkButNotTheOtherTest1)] =
                new TestSequence { opSequence = new Op[] { Op.I, Op.P1, Op.I, Op.S1, Op.P2 },
                                   expected = new ExpectedRecoveryState {PrePTail = 2000, PreSTail = 2000, PostPTail = 2000, PostSTail = 1000,
                                                                         completedPci = new PrimaryCheckpointInfo (1, 1000), startedPci = new PrimaryCheckpointInfo(1, 0)} },
            [nameof(CheckpointMetadataTests.RestoreOneChunkButNotTheOtherTest2)] =
                new TestSequence { opSequence = new Op[] { Op.I, Op.P1, Op.S1, Op.I, Op.P2 },
                                   expected = new ExpectedRecoveryState {PrePTail = 2000, PreSTail = 2000, PostPTail = 2000, PostSTail = 1000,
                                                                         completedPci = new PrimaryCheckpointInfo (1, 1000), startedPci = new PrimaryCheckpointInfo(1, 0)} },
            [nameof(CheckpointMetadataTests.RestoreOneChunkButNotTheOther2Test1)] =
                new TestSequence { opSequence = new Op[] { Op.I, Op.P1, Op.I, Op.S1, Op.I, Op.P2 },
                                   expected = new ExpectedRecoveryState {PrePTail = 3000, PreSTail = 3000, PostPTail = 3000, PostSTail = 2000,
                                                                         completedPci = new PrimaryCheckpointInfo (1, 1000), startedPci = new PrimaryCheckpointInfo(1, 0)} },
            [nameof(CheckpointMetadataTests.RestoreOneChunkButNotTheOther2Test2)] =
                new TestSequence { opSequence = new Op[] { Op.I, Op.P1, Op.I, Op.S1, Op.I, Op.P2, Op.S2 },
                                   expected = new ExpectedRecoveryState {PrePTail = 3000, PreSTail = 3000, PostPTail = 3000, PostSTail = 2000,
                                                                         completedPci = new PrimaryCheckpointInfo (1, 1000), startedPci = new PrimaryCheckpointInfo(1, 0)} },
            [nameof(CheckpointMetadataTests.RestoreTwoChunksTest)] =
                new TestSequence { opSequence = new Op[] { Op.I, Op.P1, Op.I, Op.S1, Op.I, Op.P2, Op.S2, Op.P3 },
                                   expected = new ExpectedRecoveryState {PrePTail = 3000, PreSTail = 3000, PostPTail = 3000, PostSTail = 3000,
                                                                         completedPci = new PrimaryCheckpointInfo (2, 3000), startedPci = new PrimaryCheckpointInfo(2, 0)} },
        };

        internal CheckpointRecoveryTester(CheckpointManager<int> outerCheckpointManager, ICheckpointManager innerCheckpointManager)
        {
            this.outerCheckpointManager = outerCheckpointManager;
            this.innerCheckpointManager = innerCheckpointManager;
        }

        internal void Run(string testName)
        {
            // This needs testName to take advantage of setup/teardown by running one test at a time
            var testSeq = this.testSequences[testName];
            foreach (var op in testSeq.opSequence)
            {
                switch (op) {
                    case Op.I:
                        var (ptail, stail) = DoInserts();
                        rowCount += RecordsPerChunk;
                        primaryTailAddresses[rowCount] = ptail;
                        secondaryTailAddresses[rowCount] = stail;
                        break;
                    case Op.P1: case Op.P2: case Op.P3:
                        this.lastPrimaryCommitToken = CommitPrimary(out this.lastPrimaryVersion);
                        break;
                    case Op.S1: case Op.S2: case Op.S3:
                        CommitSecondary();
                        break;
                }
            }

            var expectedRecoveryState = new ExpectedRecoveryState
            {
                PrePTail = this.primaryTailAddresses[testSeq.expected.PrePTail],
                PreSTail = this.secondaryTailAddresses[testSeq.expected.PreSTail],
                PostPTail = this.primaryTailAddresses[testSeq.expected.PostPTail],
                PostSTail = this.secondaryTailAddresses[testSeq.expected.PostSTail],
                completedPci = new PrimaryCheckpointInfo(testSeq.expected.completedPci.Version, this.primaryTailAddresses[testSeq.expected.completedPci.FlushedUntilAddress]),
                startedPci = new PrimaryCheckpointInfo(testSeq.expected.startedPci.Version, this.primaryTailAddresses[testSeq.expected.startedPci.FlushedUntilAddress])
            };

            Recover(this.lastPrimaryCommitToken, expectedRecoveryState);
        }

        internal virtual void Recover(Guid primaryToken, ExpectedRecoveryState expectedRecoveryState)
        {
            // We recover from the last Primary Checkpoint
            var recoveredPci = new PrimaryCheckpointInfo(this.lastPrimaryVersion, this.primaryTailAddresses[this.rowCount]);

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

        internal virtual Guid CommitPrimary(out int lastPrimaryVer)
        {
            // TODO: extend this to show separate start/completed intermixing
            lastPrimaryVer = ++this.defaultPrimaryVersion;
            this.lastCompletedPci = new PrimaryCheckpointInfo(lastPrimaryVer, primaryTailAddresses[rowCount]);
            this.lastStartedPci = new PrimaryCheckpointInfo(lastPrimaryVer, primaryTailAddresses[rowCount]);
            return Guid.NewGuid();
        }

        internal virtual void CommitSecondary()
        {
            var simpleCheckpointManager = (SimpleCheckpointManager)this.innerCheckpointManager;
            simpleCheckpointManager.LastCompletedPci = this.lastCompletedPci;
            simpleCheckpointManager.LastStartedPci = this.lastStartedPci;
            this.outerCheckpointManager.SetPCIs(this.lastCompletedPci, this.lastStartedPci);
            this.outerCheckpointManager.CommitLogCheckpoint(Guid.NewGuid(), SimpleCheckpointManager.userMetadata);
        }

        internal virtual (long ptail, long stail) DoInserts() => (BA, BA);
    }


    internal class SimpleCheckpointManager : ICheckpointManager
    {
        internal PrimaryCheckpointInfo LastCompletedPci;
        internal PrimaryCheckpointInfo LastStartedPci;
        internal static byte[] userMetadata = new byte[] { 11, 22, 33, 44, 55, 66, 77, 88 };

        private readonly List<Guid> indexTokens = new List<Guid>();
        private readonly List<Guid> logTokens = new List<Guid>();
        private readonly Dictionary<Guid, byte[]> indexMetaDict = new Dictionary<Guid, byte[]>();
        private readonly Dictionary<Guid, byte[]> logMetaDict = new Dictionary<Guid, byte[]>();

        internal static void VerifyIsUserMetadata(byte[] commitMetadata)
        {
            Assert.AreEqual(userMetadata.Length, commitMetadata.Length);
            for (var ii = 0; ii < commitMetadata.Length; ++ii)
                Assert.AreEqual(userMetadata[ii], commitMetadata[ii]);
        }

        private void RegisterLogMetadata(Guid logToken, byte[] commitMetadata)
        {
            Assert.AreEqual(SecondaryCheckpointMetadata.SerializedSize + userMetadata.Length, commitMetadata.Length);
            VerifyIsUserMetadata(commitMetadata.Slice(SecondaryCheckpointMetadata.SerializedSize, userMetadata.Length));

            // Verify the expected SecondaryMetadata was prepended
            var secondaryMetadata = new SecondaryCheckpointMetadata(commitMetadata.Slice(0, SecondaryCheckpointMetadata.SerializedSize));
            Assert.AreEqual(0, secondaryMetadata.lastCompletedPrimaryCheckpointInfo.Version.CompareTo(LastCompletedPci.Version));
            Assert.AreEqual(0, secondaryMetadata.lastCompletedPrimaryCheckpointInfo.FlushedUntilAddress.CompareTo(LastCompletedPci.FlushedUntilAddress));
            Assert.AreEqual(0, secondaryMetadata.lastStartedPrimaryCheckpointInfo.Version.CompareTo(LastStartedPci.Version));
            Assert.AreEqual(0, secondaryMetadata.lastStartedPrimaryCheckpointInfo.FlushedUntilAddress.CompareTo(LastStartedPci.FlushedUntilAddress));

            logTokens.Add(logToken);
            logMetaDict[logToken] = commitMetadata;
        }

        #region Exercised in these tests

        public void CommitIndexCheckpoint(Guid indexToken, byte[] commitMetadata)
        {
            Assert.AreEqual(userMetadata.Length, commitMetadata.Length);
            for (var ii = 0; ii < commitMetadata.Length; ++ii)
                Assert.AreEqual(userMetadata[ii], commitMetadata[ii]);

            indexTokens.Add(indexToken);
            indexMetaDict[indexToken] = commitMetadata;
        }

        public void CommitLogCheckpoint(Guid logToken, byte[] commitMetadata) => RegisterLogMetadata(logToken, commitMetadata);

        public void CommitLogIncrementalCheckpoint(Guid logToken, int version, byte[] commitMetadata, DeltaLog deltaLog) => RegisterLogMetadata(logToken, commitMetadata); // Not tracking version or deltaLog for these tests

        public void Dispose() { }

        public byte[] GetIndexCheckpointMetadata(Guid indexToken) => indexMetaDict[indexToken];

        public IEnumerable<Guid> GetIndexCheckpointTokens() => ((IEnumerable<Guid>)indexTokens).Reverse();  // Reverse order of adding, to simulate elapsed time

        public byte[] GetLogCheckpointMetadata(Guid logToken, DeltaLog deltaLog) => logMetaDict[logToken]; // deltaLog is not used in these tests

        public IEnumerable<Guid> GetLogCheckpointTokens() => ((IEnumerable<Guid>)logTokens).Reverse();  // Reverse order of adding, to simulate elapsed time

        #endregion Exercised in these tests

        #region not used in these tests

        public IDevice GetDeltaLogDevice(Guid token) => throw new NotImplementedException();

        public IDevice GetIndexDevice(Guid indexToken) => throw new NotImplementedException();

        public IDevice GetSnapshotLogDevice(Guid token) => throw new NotImplementedException();

        public IDevice GetSnapshotObjectLogDevice(Guid token) => throw new NotImplementedException();

        public void InitializeIndexCheckpoint(Guid indexToken) => throw new NotImplementedException();

        public void InitializeLogCheckpoint(Guid logToken) => throw new NotImplementedException();

        public void OnRecovery(Guid indexToken, Guid logToken) => throw new NotImplementedException();

        public void PurgeAll() => throw new NotImplementedException();
        #endregion not used in these tests
    }

    class OuterCheckpointManager : CheckpointManager<int>
    {
        internal OuterCheckpointManager(string indexName, ICheckpointManager userCheckpointManager)
            : base(indexName, userCheckpointManager)
        { }

        // We don't exercise these here.
        internal override void RecoverHLCInfo(ref HybridLogCheckpointInfo recoveredHLCInfo, Guid logToken) { }
        internal override Guid GetCompatibleIndexToken(ref HybridLogCheckpointInfo recoveredHLCInfo) => default;
    }

    internal class CheckpointMetadataTests
    {
        CheckpointManager<int> outerCheckpointManager;
        SimpleCheckpointManager innerCheckpointManager;
        CheckpointRecoveryTester tester;

        [SetUp]
        public void Setup()
        {
            this.innerCheckpointManager = new SimpleCheckpointManager();
            this.outerCheckpointManager = new OuterCheckpointManager("tests", innerCheckpointManager);
            this.tester = new CheckpointRecoveryTester(this.outerCheckpointManager, this.innerCheckpointManager);
        }

        [TearDown]
        public void TearDown()
        {
            this.outerCheckpointManager?.Dispose();
            this.outerCheckpointManager = null;
            this.innerCheckpointManager?.Dispose();
            this.innerCheckpointManager = null;
        }

        [Test]
        [Category(TestUtils.SecondaryIndexCategory), Category(TestUtils.HashValueIndexCategory)]
        public void MetadataWrappingTest()
        {
            var localTokens = Enumerable.Range(0, 2).Select(_ => Guid.NewGuid()).ToArray();
            for (var ii = 0; ii < localTokens.Length; ++ii)
                this.outerCheckpointManager.CommitIndexCheckpoint(localTokens[ii], SimpleCheckpointManager.userMetadata);
            var chkptTokens = this.outerCheckpointManager.GetIndexCheckpointTokens().ToArray();
            for (var ii = 0; ii < localTokens.Length; ++ii)
            {
                Assert.AreEqual(localTokens[localTokens.Length - 1 - ii], chkptTokens[ii]);
                var metadata = this.outerCheckpointManager.GetIndexCheckpointMetadata(chkptTokens[ii]);
                SimpleCheckpointManager.VerifyIsUserMetadata(metadata);
            }

            localTokens = Enumerable.Range(0, 4).Select(_ => Guid.NewGuid()).ToArray();
            for (var ii = 0; ii < localTokens.Length; ++ii)
            {
                // This lets us ensure the appropriate SecondaryMetadata is prepended
                this.innerCheckpointManager.LastCompletedPci = new PrimaryCheckpointInfo(ii + 1, (ii + 1) * 10000);
                this.innerCheckpointManager.LastStartedPci = new PrimaryCheckpointInfo(ii + 100, (ii + 100) * 10000);
                this.outerCheckpointManager.SetPCIs(this.innerCheckpointManager.LastCompletedPci, this.innerCheckpointManager.LastStartedPci);
                if ((ii & 1) == 0)
                    this.outerCheckpointManager.CommitLogCheckpoint(localTokens[ii], SimpleCheckpointManager.userMetadata);
                else
                    this.outerCheckpointManager.CommitLogIncrementalCheckpoint(localTokens[ii], 42, SimpleCheckpointManager.userMetadata, default);
            }
            chkptTokens = this.outerCheckpointManager.GetLogCheckpointTokens().ToArray();
            for (var ii = 0; ii < localTokens.Length; ++ii)
            {
                Assert.AreEqual(localTokens[localTokens.Length - 1 - ii], chkptTokens[ii]);
                var metadata = this.outerCheckpointManager.GetLogCheckpointMetadata(chkptTokens[ii], default);
                SimpleCheckpointManager.VerifyIsUserMetadata(metadata);
            }
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
