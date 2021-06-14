﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Threading.Tasks;
using FASTER.core;
using System.IO;
using NUnit.Framework;
using System.Threading;
using System.Text;
using System;

namespace FASTER.test.recovery
{
    [TestFixture]
    public class FasterLogRecoverReadOnlyTests
    {
        const int ProducerPauseMs = 1;
        const int CommitPeriodMs = 20;
        const int RestorePeriodMs = 5;
        const int NumElements = 100;

        string path;
        string deviceName;
        CancellationTokenSource cts;
        SemaphoreSlim done;

        [SetUp]
        public void Setup()
        {
            path = Path.GetTempPath() + "RecoverReadOnlyTest/";
            deviceName = path + "testlog";
            if (Directory.Exists(path))
                TestUtils.DeleteDirectory(path);
            cts = new CancellationTokenSource();
            done = new SemaphoreSlim(0);
        }

        [TearDown]
        public void TearDown()
        {
            TestUtils.DeleteDirectory(path);
            cts.Dispose();
        }

        [Test]
        [Category("FasterLog")]
        public async Task RecoverReadOnlyCheck1([Values] bool isAsync)
        {
            using var device = Devices.CreateLogDevice(deviceName);
            var logSettings = new FasterLogSettings { LogDevice = device, MemorySizeBits = 11, PageSizeBits = 9, MutableFraction = 0.5, SegmentSizeBits = 9 };
            using var log = isAsync ? await FasterLog.CreateAsync(logSettings) : new FasterLog(logSettings);

            await Task.WhenAll(ProducerAsync(log, cts),
                               CommitterAsync(log, cts.Token),
                               ReadOnlyConsumerAsync(deviceName, cts.Token, isAsync));
        }

        private async Task ProducerAsync(FasterLog log, CancellationTokenSource cts)
        {
            for (var i = 0L; i < NumElements; ++i)
            {
                log.Enqueue(Encoding.UTF8.GetBytes(i.ToString()));
                log.RefreshUncommitted();
                await Task.Delay(TimeSpan.FromMilliseconds(ProducerPauseMs));
            }
            // Ensure the reader had time to see all data
            await done.WaitAsync();
            cts.Cancel();
        }

        private async Task CommitterAsync(FasterLog log, CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(CommitPeriodMs), cancellationToken);
                    await log.CommitAsync(cancellationToken);
                }
            } catch (OperationCanceledException) { }
        }

        // This creates a separate FasterLog over the same log file, using RecoverReadOnly to continuously update
        // to the primary FasterLog's commits.
        private async Task ReadOnlyConsumerAsync(string deviceName, CancellationToken cancellationToken, bool isAsync)
        {
            using var device = Devices.CreateLogDevice(deviceName);
            var logSettings = new FasterLogSettings { LogDevice = device, ReadOnlyMode = true, PageSizeBits = 9, SegmentSizeBits = 9 };
            using var log = isAsync ? await FasterLog.CreateAsync(logSettings) : new FasterLog(logSettings);

            var _ = BeginRecoverAsyncLoop();

            // This enumerator waits asynchronously when we have reached the committed tail of the duplicate FasterLog. When RecoverReadOnly
            // reads new data committed by the primary FasterLog, it signals commit completion to let iter continue to the new tail.
            using var iter = log.Scan(log.BeginAddress, long.MaxValue);
            var prevValue = -1L;
            try
            {
                await foreach (var (result, _, _, nextAddress) in iter.GetAsyncEnumerable(cancellationToken))
                {
                    var value = long.Parse(Encoding.UTF8.GetString(result));
                    Assert.AreEqual(prevValue + 1, value);
                    prevValue = value;
                    iter.CompleteUntil(nextAddress);
                    if (prevValue == NumElements - 1)
                        done.Release();
                }
            } catch (OperationCanceledException) { }
            Assert.AreEqual(NumElements - 1, prevValue);

            async Task BeginRecoverAsyncLoop()
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    // Delay for a while before recovering to the last commit by the primary FasterLog instance.
                    await Task.Delay(TimeSpan.FromMilliseconds(RestorePeriodMs), cancellationToken);
                    if (cancellationToken.IsCancellationRequested)
                        break;
                    if (isAsync)
                        await log.RecoverReadOnlyAsync();
                    else
                        log.RecoverReadOnly();
                }
            }
        }
    }
}
