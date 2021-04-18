// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Linq;
using FASTER.core;
using FASTER.indexes.HashValueIndex;
using HashValueIndexSampleCommon;

namespace BasicPredicateSample
{
    class BasicPredicateApp
    {
        private static Store store;

        static void Main()
        {
            store = new Store();
            store.RunInitialInserts();
            store.FlushAndEvict();
            store.index.FlushAndEvict(wait: true);

            QueryPredicate();
            Console.WriteLine("Press <enter> to exit");
            Console.ReadLine();
        }

        internal static void QueryPredicate()
        {
            using var session = store.FasterKV.For(new Functions()).NewSession<Functions>();

            QueryRecord<Key, Value>[] results = session.Query(store.PetPred, (int)Species.Cat).ToArray();
            Console.WriteLine($"{results.Length} cats retrieved");

            results = session.Query(store.PetPred, (int)Species.Dog).ToArray();
            Console.WriteLine($"{results.Length} dogs retrieved");
        }
    }
}
