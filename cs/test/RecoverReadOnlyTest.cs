﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;
using NUnit.Framework;
using System.Text;


//** Note - this test is based on FasterLogPubSub sample found in the samples directory.

namespace FASTER.test
{

    [TestFixture]
    internal class BasicRecoverReadOnly
    {
        private FasterLog log;
        private IDevice device;
        private FasterLog logReadOnly;
        private IDevice deviceReadOnly;

        private static string path = Path.GetTempPath() + "BasicRecoverAsyncReadOnly/";
        const int commitPeriodMs = 2000;
        const int restorePeriodMs = 1000;

        [SetUp]
        public void Setup()
        {

            // Clean up log files from previous test runs in case they weren't cleaned up
            try {  new DirectoryInfo(path).Delete(true);  }
            catch {}

            // Create devices \ log for test
            device = Devices.CreateLogDevice(path + "Recover", deleteOnClose: true);
            log = new FasterLog(new FasterLogSettings { LogDevice = device, MemorySizeBits = 11, PageSizeBits = 9, MutableFraction = 0.5, SegmentSizeBits = 9 });
            deviceReadOnly = Devices.CreateLogDevice(path + "RecoverReadOnly");
            logReadOnly = new FasterLog(new FasterLogSettings { LogDevice = device, ReadOnlyMode = true, PageSizeBits = 9, SegmentSizeBits = 9 });
        }

        [TearDown]
        public void TearDown()
        {
            log.Dispose();
            device.Dispose();
            logReadOnly.Dispose();
            deviceReadOnly.Dispose();

            // Clean up log files
            try { new DirectoryInfo(path).Delete(true); }
            catch { }
        }


        [Test]
        [Category("FasterLog")]
        public void RecoverReadOnlyAsyncBasicTest()
        {
            using var cts = new CancellationTokenSource();

            var producer = ProducerAsync(log, cts.Token);
            var commiter = CommitterAsync(log, cts.Token);

            // Run consumer on SEPARATE read-only FasterLog instance
            var consumer = SeparateConsumerAsync(cts.Token);

            // Give it some time to run a bit - similar to waiting for things to run before hitting cancel
            Thread.Sleep(3000);
            cts.Cancel();

            producer.Wait();
            // commiter.Wait();  // cancel token took care of this one
            // consumer.Wait();  // cancel token took care of this one

        }


        //**** Helper Functions - based off of FasterLogPubSub sample ***
        static async Task CommitterAsync(FasterLog log, CancellationToken cancellationToken)
        {

            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(commitPeriodMs), cancellationToken);
                await log.CommitAsync(cancellationToken);
            }
        }

        static async Task ProducerAsync(FasterLog log, CancellationToken cancellationToken)
        {
            var i = 0L;
            while (!cancellationToken.IsCancellationRequested)
            {
                log.Enqueue(Encoding.UTF8.GetBytes(i.ToString()));
                log.RefreshUncommitted(true);

                i++;

                await Task.Delay(TimeSpan.FromMilliseconds(10));
            }
        }

        // This creates a separate FasterLog over the same log file, using RecoverReadOnly to continuously update
        // to the primary FasterLog's commits.
        public async Task SeparateConsumerAsync(CancellationToken cancellationToken)
        {

            var _ = BeginRecoverReadOnlyLoop(logReadOnly, cancellationToken);

            // This enumerator waits asynchronously when we have reached the committed tail of the duplicate FasterLog. When RecoverReadOnly
            // reads new data committed by the primary FasterLog, it signals commit completion to let iter continue to the new tail.
            using var iter = logReadOnly.Scan(logReadOnly.BeginAddress, long.MaxValue);
            await foreach (var (result, length, currentAddress, nextAddress) in iter.GetAsyncEnumerable(cancellationToken))
            {
                iter.CompleteUntil(nextAddress);
            }
        }


        static async Task BeginRecoverReadOnlyLoop(FasterLog log, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // Delay for a while before checking again.
                await Task.Delay(TimeSpan.FromMilliseconds(restorePeriodMs), cancellationToken);
                await log.RecoverReadOnlyAsync(cancellationToken);
            }
        }

    }
}


