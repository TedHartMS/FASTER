// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Linq;
using FASTER.core;
using FASTER.indexes.HashValueIndex;
using HashValueIndexSampleCommon;

namespace MultiPredicateSample
{
    class MultiPredicateApp
    {
        private static Store store;

        static void Main()
        {
            using (store = new Store())
            {
                store.RunInitialInserts();
                store.FlushAndEvict();

                var catsOfAge = QueryPredicates(store);
                store.UpdateCats(catsOfAge);

                /* TODO: remove to test mutable scan: */
                store.FlushAndEvict();

                QueryPredicates(store);
            }

            Console.WriteLine("Press <enter> to exit");
            Console.ReadLine();
        }

        internal static QueryRecord<Key, Value>[] QueryPredicates(Store store)
        {
            Console.WriteLine();
            using var session = store.FasterKV.For(new Functions()).NewSession<Functions>();

            QueryRecord<Key, Value>[] results = session.Query(store.CombinedPetPred, new AgeOrPetKey(Species.Cat)).ToArray();
            Console.WriteLine($"{results.Length} cats retrieved");

            results = session.Query(store.CombinedPetPred, new AgeOrPetKey(Species.Dog)).ToArray();
            Console.WriteLine($"{results.Length} dogs retrieved");

            var catsOfAge = RunQueries(session, Species.Cat, Constants.CatAge, vec => vec[0] && vec[1], $"cats age {Constants.CatAge} retrieved");
            RunQueries(session, Species.Cat, Constants.CatAge + Constants.CatAgeIncrement, vec => vec[0] && vec[1], $"cats age {Constants.CatAge + Constants.CatAgeIncrement} retrieved");
            RunQueries(session, Species.Dog, Constants.DogAge, vec => vec[0] && vec[1], $"dogs age {Constants.DogAge} retrieved");
            RunQueries(session, Species.Dog, Constants.CatAge, vec => vec[0] || vec[1], $"dogs or any pet age {Constants.CatAge} retrieved");
            return catsOfAge;
        }

        private static QueryRecord<Key, Value>[] RunQueries(ClientSession<Key, Value, Value, Value, Empty, Functions> session, Species species, int age, Func<bool[], bool> matchPredicate, string message)
        {
            (IPredicate, AgeOrPetKey)[] queryPredicates = new[] { (store.CombinedPetPred, new AgeOrPetKey(species)),
                                                                  (store.CombinedAgePred, new AgeOrPetKey(age)) };
            var results = session.Query(queryPredicates, matchPredicate).ToArray();
            Console.WriteLine($"{results.Length} {message}");

            string continuationToken = string.Empty;
            int count = 0;

            while (true)
            {
                QuerySegment<Key, Value> segment = session.QuerySegmented(queryPredicates, matchPredicate, continuationToken, 100);
                if (segment.Results.Count == 0)
                    break;
                count += segment.Results.Count;
                continuationToken = segment.ContinuationToken;
                Console.WriteLine($"  CT: {count} {species}s retrieved");
            }

            return results;
        }
    }
}
