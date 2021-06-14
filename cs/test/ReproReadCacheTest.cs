﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    [TestFixture]
    internal class RandomReadCacheTest
    {
        public class Context
        {
            public Status Status { get; set; }
        }

        class Functions : FunctionsBase<SpanByte, long, long, long, Context>
        {
            public override void ConcurrentReader(ref SpanByte key, ref long input, ref long value, ref long dst)
                => dst = value;

            public override void SingleReader(ref SpanByte key, ref long input, ref long value, ref long dst)
                => dst = value;

            public override void ReadCompletionCallback(ref SpanByte key, ref long input, ref long output, Context context, Status status)
            {
                Assert.AreEqual(status, Status.OK);
                Assert.AreEqual(input, output);
                context.Status = status;
            }
        }

        [Test]
        public unsafe void RandomReadCacheTest1()
        {
            var log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "/BasicFasterTests.log", deleteOnClose: true);
            var fht = new FasterKV<SpanByte, long>(
                size: 1L << 20,
                new LogSettings
                {
                    LogDevice = log,
                    MemorySizeBits = 15,
                    PageSizeBits = 12,

                    ReadCacheSettings = new ReadCacheSettings
                    {
                        MemorySizeBits = 15,
                        PageSizeBits = 12,
                        SecondChanceFraction = 0.1,
                    }
                });

            var session = fht.For(new Functions()).NewSession<Functions>();

            void Read(int i)
            {
                var keyString = $"{i}";
                var key = MemoryMarshal.Cast<char, byte>(keyString.AsSpan());
                fixed (byte* _ = key)
                {
                    var context = new Context();
                    var sb = SpanByte.FromFixedSpan(key);
                    long input = i * 2;
                    long output = 0;
                    var status = session.Read(ref sb, ref input, ref output, context);

                    if (status == Status.OK)
                    {
                        Assert.AreEqual(input, output);
                        return;
                    }

                    Assert.AreEqual(Status.PENDING, status, $"was not OK or PENDING: {keyString}");

                    session.CompletePending(wait: true);
                }
            }

            var num = 30000;

            // write the values
            for (int i = 0; i < num; i++)
            {
                var keyString = $"{i}";
                var key = MemoryMarshal.Cast<char, byte>(keyString.AsSpan());
                fixed (byte* _ = key)
                {
                    var sb = SpanByte.FromFixedSpan(key);
                    Assert.AreEqual(Status.OK, session.Upsert(sb, i * 2));
                }
            }

            // read through the keys in order (works)
            for (int i = 0; i < num; i++)
            {
                Read(i);
            }

            // pick random keys to read
            var r = new Random(2115);
            for (int i = 0; i < num; i++)
            {
                Read(r.Next(num));
            }
        }
    }
}
