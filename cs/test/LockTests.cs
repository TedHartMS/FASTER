﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.test
{
    [TestFixture]
    internal class LockTests
    {
        internal class Functions : AdvancedSimpleFunctions<int, int>
        {
            public override void ConcurrentReader(ref int key, ref int input, ref int value, ref int dst, ref RecordInfo recordInfo, long address)
            {
                dst = value;
            }

            bool Increment(ref int dst)
            {
                ++dst;
                return true;
            }

            public override bool ConcurrentWriter(ref int key, ref int src, ref int dst, ref RecordInfo recordInfo, long address) => Increment(ref dst);

            public override bool InPlaceUpdater(ref int key, ref int input, ref int value, ref RecordInfo recordInfo, long address) => Increment(ref value);

            public override bool SupportsLocks => true;
            public override void Lock(ref RecordInfo recordInfo, ref int key, ref int value, OperationType opType, ref long context) => recordInfo.SpinLock();
            public override bool Unlock(ref RecordInfo recordInfo, ref int key, ref int value, OperationType opType, long context)
            {
                recordInfo.Unlock();
                return true;
            }
        }

        private FasterKV<int, int> fkv;
        private AdvancedClientSession<int, int, int, int, Empty, Functions> session;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "/GenericStringTests.log", deleteOnClose: true);
            fkv = new FasterKV<int, int>( 1L << 20, new LogSettings { LogDevice = log, ObjectLogDevice = null } );
            session = fkv.For(new Functions()).NewSession<Functions>();
        }

        [TearDown]
        public void TearDown()
        {
            session.Dispose();
            session = null;
            fkv.Dispose();
            fkv = null;
            log.Dispose();
            log = null;
        }

        [Test]
        public unsafe void RecordInfoLockTest()
        {
            for (var ii = 0; ii < 10; ++ii)
            {
                RecordInfo recordInfo = new RecordInfo();
                RecordInfo* ri = &recordInfo;

                XLockTest(() => ri->SpinLock(), () => ri->Unlock());
            }
        }

        private void XLockTest(Action locker, Action unlocker)
        {
            long lockTestValue = 0;
            const int numThreads = 50;
            const int numIters = 5000;

            var tasks = Enumerable.Range(0, numThreads).Select(ii => Task.Factory.StartNew(XLockTestFunc)).ToArray();
            try
            {
                Task.WaitAll(tasks);
            }
            catch (AggregateException ex)
            {
                Assert.Fail(ex.InnerExceptions.First().Message);
            }

            Assert.AreEqual(numThreads * numIters, lockTestValue);

            void XLockTestFunc()
            {
                for (int ii = 0; ii < numIters; ++ii)
                {
                    locker();
                    var temp = lockTestValue;
                    Thread.Yield();
                    lockTestValue = temp + 1;
                    unlocker();
                }
            }
        }

        [Test]
        public void IntExclusiveLockerTest()
        {
            int lockTestValue = 0;
            XLockTest(() => IntExclusiveLocker.SpinLock(ref lockTestValue), () => IntExclusiveLocker.Unlock(ref lockTestValue));
        }

        [Test]
        public void AdvancedFunctionsLockTest()
        {
            // Populate
            const int numRecords = 100;
            const int valueMult = 1000000;
            for (int key = 0; key < numRecords; key++)
            {
                // For this test we should be in-memory, so no pending
                Assert.AreNotEqual(Status.PENDING, session.Upsert(key, key * valueMult));
            }

            // Update
            const int numThreads = 20;
            const int numIters = 500;
            var tasks = Enumerable.Range(0, numThreads).Select(ii => Task.Factory.StartNew(() => UpdateFunc((ii & 1) == 0, numRecords, numIters))).ToArray();
            Task.WaitAll(tasks);

            // Verify
            for (int key = 0; key < numRecords; key++)
            {
                var expectedValue = key * valueMult + numThreads * numIters;
                Assert.AreNotEqual(Status.PENDING, session.Read(key, out int value));
                Assert.AreEqual(expectedValue, value);
            }
        }

        void UpdateFunc(bool useRMW, int numRecords, int numIters)
        {
            for (var key = 0; key < numRecords; ++key)
            {
                for (int iter = 0; iter < numIters; iter++)
                {
                    if ((iter & 7) == 7)
                        Assert.AreNotEqual(Status.PENDING, session.Read(key));

                    // These will both just increment the stored value, ignoring the input argument.
                    if (useRMW)
                        session.RMW(key, default);
                    else
                        session.Upsert(key, default);
                }
            }
        }
    }
}
