﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;
using NUnit.Framework;


namespace FASTER.test
{

    [TestFixture]
    internal class ManageLocalStorageTests
    {
        private FasterLog log;
        private IDevice device;
        private FasterLog logFullParams;
        private IDevice deviceFullParams;
        static readonly byte[] entry = new byte[100];
        private string commitPath;


        [SetUp]
        public void Setup()
        {
            commitPath = TestContext.CurrentContext.TestDirectory + "/" + TestContext.CurrentContext.Test.Name + "/";

            // Clean up log files from previous test runs in case they weren't cleaned up
            // We loop to ensure clean-up as deleteOnClose does not always work for MLSD
            while (Directory.Exists(commitPath))
                Directory.Delete(commitPath, true);

            // Create devices \ log for test
            device = new ManagedLocalStorageDevice(commitPath + "ManagedLocalStore.log", deleteOnClose: true);
            log = new FasterLog(new FasterLogSettings { LogDevice = device, PageSizeBits = 12, MemorySizeBits = 14 });

            deviceFullParams = new ManagedLocalStorageDevice(commitPath + "ManagedLocalStoreFullParams.log", deleteOnClose: false, recoverDevice: true, preallocateFile: true, capacity: 1 << 30);
            logFullParams = new FasterLog(new FasterLogSettings { LogDevice = device, PageSizeBits = 12, MemorySizeBits = 14 });

        }

        [TearDown]
        public void TearDown()
        {
            log.Dispose();
            device.Dispose();
            logFullParams.Dispose();
            deviceFullParams.Dispose();

            // Clean up log files
            if (Directory.Exists(commitPath))
                Directory.Delete(commitPath, true);
        }


        [Test]
        [Category("FasterLog")]
        public void ManagedLocalStoreBasicTest()
        {
            int entryLength = 20;
            int numEntries = 1000;
            int numEnqueueThreads = 1;
            int numIterThreads = 1;

            // Set Default entry data
            for (int i = 0; i < entryLength; i++)
            {
                entry[i] = (byte)i;
            }

            Thread[] th = new Thread[numEnqueueThreads];
            for (int t = 0; t < numEnqueueThreads; t++)
            {
                th[t] =
                new Thread(() =>
                {
                    // Enqueue but set each Entry in a way that can differentiate between entries
                    for (int i = 0; i < numEntries; i++)
                    {
                        // Flag one part of entry data that corresponds to index
                        entry[0] = (byte)i;

                        // Default is add bytes so no need to do anything with it
                        log.Enqueue(entry);
                    }
                });
            }
            for (int t = 0; t < numEnqueueThreads; t++)
                th[t].Start();
            for (int t = 0; t < numEnqueueThreads; t++)
                th[t].Join();

            // Commit to the log
            log.Commit(true);
            
            // flag to make sure data has been checked 
            bool datacheckrun = false;

            Thread[] th2 = new Thread[numIterThreads];
            for (int t = 0; t < numIterThreads; t++)
            {
                th2[t] =
                    new Thread(() =>
                    {
                        // Read the log - Look for the flag so know each entry is unique
                        int currentEntry = 0;
                        using (var iter = log.Scan(0, long.MaxValue))
                        {
                            while (iter.GetNext(out byte[] result, out _, out _))
                            {
                                // set check flag to show got in here
                                datacheckrun = true;

                                if (numEnqueueThreads == 1)
                                    Assert.IsTrue(result[0] == (byte)currentEntry, "Fail - Result[" + currentEntry.ToString() + "]:" + result[0].ToString());
                                currentEntry++;
                            }
                        }

                        Assert.IsTrue(currentEntry == numEntries * numEnqueueThreads);
                    });
            }

            for (int t = 0; t < numIterThreads; t++)
                th2[t].Start();
            for (int t = 0; t < numIterThreads; t++)
                th2[t].Join();

            // if data verification was skipped, then pop a fail
            if (datacheckrun == false)
                Assert.Fail("Failure -- data loop after log.Scan never entered so wasn't verified. ");
        }

        [Test]
        [Category("FasterLog")]
        public void ManagedLocalStoreFullParamsTest()
        {

            int entryLength = 10;

            // Set Default entry data
            for (int i = 0; i < entryLength; i++)
            {
                entry[i] = (byte)i;
                logFullParams.Enqueue(entry);
            }

            // Commit to the log
            logFullParams.Commit(true);

            // Verify  
            Assert.IsTrue(File.Exists(commitPath + "/log-commits/commit.0.0"));
            Assert.IsTrue(File.Exists(commitPath + "/ManagedLocalStore.log.0"));

            // Read the log just to verify can actually read it
            int currentEntry = 0;
            using (var iter = logFullParams.Scan(0, 100_000_000))
            {
                while (iter.GetNext(out byte[] result, out _, out _))
                {
                    Assert.IsTrue(result[currentEntry] == currentEntry, "Fail - Result[" + currentEntry.ToString() + "]: is not same as " + currentEntry.ToString());

                    currentEntry++;
                }
            }
        }



    }
}


