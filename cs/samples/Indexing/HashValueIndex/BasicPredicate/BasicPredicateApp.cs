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
            using (store = new Store())
            {
                store.RunInitialInserts();
                store.FlushAndEvict();
                store.index.FlushAndEvict(wait: true);

                QueryPredicate();
            }

            Console.WriteLine("Press <enter> to exit");
            Console.ReadLine();
        }

        internal static void QueryPredicate()
        {
            using var session = store.FasterKV.For(new Functions()).NewSession<Functions>();
            RunQueries(session, Species.Cat);
            RunQueries(session, Species.Dog);
        }

        private static void RunQueries(ClientSession<Key, Value, Value, Value, Empty, Functions> session, Species species)
        {
            var results = session.Query(store.PetPred, (int)species).ToArray();
            Console.WriteLine($"{results.Length} {species}s retrieved");
            Dispose(results);

            string continuationToken = string.Empty;
            int count = 0;

            while (true)
            {
                using var segment = session.QuerySegmented(store.PetPred, (int)species, continuationToken, 100);
                if (segment.Results.Count == 0)
                    break;
                count += segment.Results.Count;
                continuationToken = segment.ContinuationToken;
                Console.WriteLine($"  CT: {count} {species}s retrieved");
            }
        }

        private static void Dispose(QueryRecord<Key, Value>[] records) => Array.ForEach(records, record => record.Dispose());
    }
}
