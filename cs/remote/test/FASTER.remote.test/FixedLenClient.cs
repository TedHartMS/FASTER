﻿using FASTER.client;
using NUnit.Framework;
using System;

namespace FASTER.remote.test
{
    class FixedLenClient<Key, Value> : IDisposable
        where Key : unmanaged
        where Value : unmanaged

    {
        readonly FasterKVClient<long, long> client;

        public FixedLenClient(string address = "127.0.0.1", int port = 33278)
        {
            client = new FasterKVClient<long, long>(address, port);
        }

        public void Dispose()
        {
            client.Dispose();
        }

        public ClientSession<long, long, long, long, long, FixedLenClientFunctions, FixedLenSerializer<long, long, long, long>> GetSession()
            => client.NewSession<long, long, long, FixedLenClientFunctions, FixedLenSerializer<long, long, long, long>>(new FixedLenClientFunctions());
    }

    /// <summary>
    /// Callback functions
    /// </summary>
    sealed class FixedLenClientFunctions : CallbackFunctionsBase<long, long, long, long, long>
    {
        public override void ReadCompletionCallback(ref long key, ref long input, ref long output, long ctx, Status status)
        {
            Assert.IsTrue(status == Status.OK);
            Assert.IsTrue(output == ctx);
        }
    }
}
