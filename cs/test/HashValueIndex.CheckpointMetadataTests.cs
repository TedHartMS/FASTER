// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using FASTER.indexes.HashValueIndex;
using System;
using System.Collections.Generic;
using NUnit.Framework;
using System.Linq;

namespace FASTER.test.HashValueIndex.CheckpointMetadataTests
{
    internal class SimpleCheckpointManager : ICheckpointManager
    {
        internal PrimaryCheckpointInfo lastCompletedPci;
        internal PrimaryCheckpointInfo lastStartedPci;
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
            Assert.AreEqual(0, secondaryMetadata.lastCompletedPrimaryCheckpointInfo.Version.CompareTo(lastCompletedPci.Version));
            Assert.AreEqual(0, secondaryMetadata.lastCompletedPrimaryCheckpointInfo.FlushedUntilAddress.CompareTo(lastCompletedPci.FlushedUntilAddress));
            Assert.AreEqual(0, secondaryMetadata.lastStartedPrimaryCheckpointInfo.Version.CompareTo(lastStartedPci.Version));
            Assert.AreEqual(0, secondaryMetadata.lastStartedPrimaryCheckpointInfo.FlushedUntilAddress.CompareTo(lastStartedPci.FlushedUntilAddress));

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

    internal class CheckpointMetadataTests
    {
        CheckpointManager<int> outerCheckpointManager;
        SimpleCheckpointManager innerCheckpointManager;

        [SetUp]
        public void Setup()
        {
            this.innerCheckpointManager = new SimpleCheckpointManager();
            this.outerCheckpointManager = new CheckpointManager<int>("tests", innerCheckpointManager);
        }

        [TearDown]
        public void TearDown()
        {
            this.outerCheckpointManager?.Dispose();
            this.outerCheckpointManager = null;
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
                this.innerCheckpointManager.lastCompletedPci = new PrimaryCheckpointInfo(ii + 1, (ii + 1) * 10000);
                this.innerCheckpointManager.lastStartedPci = new PrimaryCheckpointInfo(ii + 100, (ii + 100) * 10000);
                this.outerCheckpointManager.SetPCIs(this.innerCheckpointManager.lastCompletedPci, this.innerCheckpointManager.lastStartedPci);
                this.outerCheckpointManager.SetPCIs(new PrimaryCheckpointInfo(ii + 1, (ii + 1) * 10000), new PrimaryCheckpointInfo(ii + 100, (ii + 100) * 10000));
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
    }
}
