﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.
using System;
using System.IO;
using System.Threading.Tasks;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    [TestFixture]
    internal class EnqWaitCommitTest
    {
        const int entryLength = 500;
        const int numEntries = 100;

        public FasterLog log;
        public IDevice device;
        static readonly byte[] entry = new byte[entryLength];
        static readonly ReadOnlySpanBatch spanBatch = new ReadOnlySpanBatch(numEntries);
        private string commitPath;

        public enum EnqueueIteratorType
        {
            Byte,
            SpanBatch,
            SpanByte
        }

        private struct ReadOnlySpanBatch : IReadOnlySpanBatch
        {
            private readonly int batchSize;
            public ReadOnlySpanBatch(int batchSize) => this.batchSize = batchSize;
            public ReadOnlySpan<byte> Get(int index) => entry;
            public int TotalEntries() => batchSize;
        }

        [SetUp]
        public void Setup()
        {
            commitPath = TestContext.CurrentContext.TestDirectory + "/" + TestContext.CurrentContext.Test.Name + "/";

            // Clean up log files from previous test runs in case they weren't cleaned up
            try { new DirectoryInfo(commitPath).Delete(true); }
            catch { }

            // Create devices \ log for test
            device = Devices.CreateLogDevice(commitPath + "EnqueueAndWaitForCommit.log", deleteOnClose: true);
            log = new FasterLog(new FasterLogSettings { LogDevice = device });
        }

        [TearDown]
        public void TearDown()
        {
            log.Dispose();
            device.Dispose();

            // Clean up log files
            try { new DirectoryInfo(commitPath).Delete(true); }
            catch { }
        }

        [Test]
        [Category("FasterLog")]
        public async ValueTask EnqueueWaitCommitBasicTest([Values] EnqueueIteratorType iteratorType)
        {
            // Set Default entry data
            for (int i = 0; i < entryLength; i++)
            {
                entry[i] = (byte)i;
            }

            // Add to FasterLog on a separate thread, which will wait for the commit from this thread
            var currentTask = Task.Run(() => LogWriter(log, entry, iteratorType));

            // Delay so LogWriter's call to EnqueueAndWaitForCommit gets into its spinwait for the Commit.
            await Task.Delay(100);

            // Commit to the log and wait for task to finish
            log.Commit(true);
            await currentTask;

            // Read the log - Look for the flag so know each entry is unique
            using var iter = log.Scan(0, 1000);
            int currentEntry = 0;
            while (iter.GetNext(out byte[] result, out _, out _))
            {
                Assert.IsTrue(currentEntry < entryLength);
                Assert.IsTrue(result[currentEntry] == (byte)currentEntry, "Fail - Result[" + currentEntry.ToString() + "]:" + result[0].ToString() + " not match expected:" + currentEntry);
                currentEntry++;
            }

            Assert.AreNotEqual(0, currentEntry, "Failure -- data loop after log.Scan never entered so wasn't verified.");
        }

        public static void LogWriter(FasterLog log, byte[] entry, EnqueueIteratorType iteratorType)
        {
            try
            {
                long returnLogicalAddress = iteratorType switch
                {
                    EnqueueIteratorType.Byte => log.EnqueueAndWaitForCommit(entry),
                    EnqueueIteratorType.SpanByte => // Could slice the span but for basic test just pass span of full entry - easier verification
                                                    log.EnqueueAndWaitForCommit((Span<byte>)entry),
                    EnqueueIteratorType.SpanBatch => log.EnqueueAndWaitForCommit(spanBatch),
                    _ => throw new ApplicationException("Test failure: Unknown EnqueueIteratorType")
                };

                Assert.AreNotEqual(0, returnLogicalAddress, "LogWriter: Returned Logical Address = 0");
            }
            catch (Exception ex)
            {
                Assert.Fail("EnqueueAndWaitForCommit had exception:" + ex.Message);
            }
        }

    }
}
