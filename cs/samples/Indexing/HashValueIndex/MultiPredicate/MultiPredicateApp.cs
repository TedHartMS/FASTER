// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Threading.Tasks;
using FASTER.core;
using FASTER.indexes.HashValueIndex;
using HashValueIndexSampleCommon;

namespace MultiPredicateSample
{
    class MultiPredicateApp
    {
        private static Store store;

        static async Task Main()
        {
            using (store = new Store())
            {
                store.RunInitialInserts();
                store.FlushAndEvict();

                var catsOfAge = await QueryPredicates(store, isAsync: false);
                store.UpdateCats(catsOfAge);

                /* TODO: remove to test mutable scan: */ store.FlushAndEvict();

                await QueryPredicates(store, isAsync: true);
            }

            Console.WriteLine("Press <enter> to exit");
            Console.ReadLine();
        }

        internal static async ValueTask<QueryRecord<Key, Value>[]> QueryPredicates(Store store, bool isAsync)
        {
            Console.WriteLine();
            using var session = store.FasterKV.For(new Functions()).NewSession<Functions>();

            await SinglePredicateQuery(session, Species.Cat, isAsync);
            await SinglePredicateQuery(session, Species.Dog, isAsync);

            var catsOfAge = await MultiPredicateQuery(session, Species.Cat, Constants.CatAge, vec => vec[0] && vec[1], $"cats age {Constants.CatAge} retrieved", isAsync);
            await MultiPredicateQuery(session, Species.Cat, Constants.CatAge + Constants.CatAgeIncrement, vec => vec[0] && vec[1], $"cats age {Constants.CatAge + Constants.CatAgeIncrement} retrieved", isAsync);
            await MultiPredicateQuery(session, Species.Dog, Constants.DogAge, vec => vec[0] && vec[1], $"dogs age {Constants.DogAge} retrieved", isAsync);
            await MultiPredicateQuery(session, Species.Dog, Constants.CatAge, vec => vec[0] || vec[1], $"dogs or any pet age {Constants.CatAge} retrieved", isAsync);
            return catsOfAge;
        }

        private static async ValueTask SinglePredicateQuery(ClientSession<Key, Value, Value, Value, Empty, Functions> session, Species species, bool isAsync)
        {
            var results = isAsync
                ? await session.QueryAsync(store.CombinedPetPred, new AgeOrPetKey(species)).ToArrayAsync()
                : session.Query(store.CombinedPetPred, new AgeOrPetKey(species)).ToArray();
            Console.WriteLine($"{results.Length} {species}s retrieved");
        }

        private static async ValueTask<QueryRecord<Key, Value>[]> MultiPredicateQuery(ClientSession<Key, Value, Value, Value, Empty, Functions> session, Species species, int age, Func<bool[], bool> matchPredicate, string message, bool isAsync)
        {
            (IPredicate, AgeOrPetKey)[] queryPredicates = new[] { (store.CombinedPetPred, new AgeOrPetKey(species)),
                                                                  (store.CombinedAgePred, new AgeOrPetKey(age)) };
            var results = isAsync
                ? await session.QueryAsync(queryPredicates, matchPredicate).ToArrayAsync()
                : session.Query(queryPredicates, matchPredicate).ToArray();
            Console.WriteLine($"{results.Length} {message}");

            string continuationToken = string.Empty;
            int count = 0;

            while (true)
            {
                var segment = isAsync
                    ? await session.QuerySegmentedAsync(queryPredicates, matchPredicate, continuationToken, 100)
                    : session.QuerySegmented(queryPredicates, matchPredicate, continuationToken, 100);
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
