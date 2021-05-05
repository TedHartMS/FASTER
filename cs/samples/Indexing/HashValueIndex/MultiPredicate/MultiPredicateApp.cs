// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
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
        private static bool isAsync;
        const int SegmentSize = 100;

        static async Task Main(string[] args)
        {
            foreach (var arg in args.Select(a => a.ToLower()))
            {
                if (arg == "--async")
                {
                    isAsync = true;
                    continue;
                }
                throw new ArgumentException($"Unknown arg: {arg}");
            }

            using (store = new Store())
            {
                store.RunInitialInserts();

                await QueryPredicates("Mutable scan only", store);

                store.FlushAndEvict();
                var catsOfAge = await QueryPredicates("Index query only", store);

                // Update generates new mutable records
                store.UpdateCats(catsOfAge);
                Dispose(catsOfAge);

                await QueryPredicates("Mutable scan followed by index query", store);
            }

            Console.WriteLine("Press <enter> to exit");
            Console.ReadLine();
        }

        internal static async ValueTask<QueryRecord<Key, Value>[]> QueryPredicates(string message, Store store)
        {
            Console.WriteLine();
            Console.WriteLine($"*** {message}");
            using var session = store.FasterKV.For(new Functions()).NewSession<Functions>();

            await SinglePredicateQuery(session, Species.Cat);
            await SinglePredicateQuery(session, Species.Dog);

            var catsOfAge = await MultiPredicateQuery(session, Species.Cat, Constants.CatAge, vec => vec[0] && vec[1], $"cats age {Constants.CatAge} retrieved");
            Dispose(await MultiPredicateQuery(session, Species.Cat, Constants.CatAge + Constants.CatAgeIncrement, vec => vec[0] && vec[1], $"cats age {Constants.CatAge + Constants.CatAgeIncrement} retrieved"));
            Dispose(await MultiPredicateQuery(session, Species.Dog, Constants.DogAge, vec => vec[0] && vec[1], $"dogs age {Constants.DogAge} retrieved"));
            Dispose(await MultiPredicateQuery(session, Species.Dog, Constants.CatAge, vec => vec[0] || vec[1], $"dogs or any pet age {Constants.CatAge} retrieved"));
            return catsOfAge;
        }

        private static async ValueTask SinglePredicateQuery(ClientSession<Key, Value, Value, Value, Empty, Functions> session, Species species)
        {
            var queryKey = new AgeOrPetKey(species);

            var results = isAsync
                ? await session.QueryAsync(store.CombinedPetPred, queryKey).ToArrayAsync()
                : session.Query(store.CombinedPetPred, queryKey).ToArray();
            Console.WriteLine($"{results.Length} {species}s retrieved");
            Dispose(results);

            string continuationToken = string.Empty;
            int count = 0;
            bool isComplete = false;

            while (true)
            {
                var segment = isAsync
                    ? await session.QuerySegmentedAsync(store.CombinedPetPred, queryKey, continuationToken, SegmentSize)
                    : session.QuerySegmented(store.CombinedPetPred, queryKey, continuationToken, SegmentSize);
                if (!ProcessSegment(species, segment, ref count, ref continuationToken, ref isComplete))
                    break;
            }
        }

        private static async ValueTask<QueryRecord<Key, Value>[]> MultiPredicateQuery(ClientSession<Key, Value, Value, Value, Empty, Functions> session, Species species, int age, Func<bool[], bool> matchPredicate, string message)
        {
            (IPredicate, AgeOrPetKey)[] queryPredicates = new[] { (store.CombinedPetPred, new AgeOrPetKey(species)),
                                                                  (store.CombinedAgePred, new AgeOrPetKey(age)) };
            var results = isAsync
                ? await session.QueryAsync(queryPredicates, matchPredicate).ToArrayAsync()
                : session.Query(queryPredicates, matchPredicate).ToArray();
            Console.WriteLine($"{results.Length} {message}");

            string continuationToken = string.Empty;
            int count = 0;
            bool isComplete = false;

            while (true)
            {
                var segment = isAsync
                    ? await session.QuerySegmentedAsync(queryPredicates, matchPredicate, continuationToken, SegmentSize)
                    : session.QuerySegmented(queryPredicates, matchPredicate, continuationToken, SegmentSize);
                if (!ProcessSegment(species, segment, ref count, ref continuationToken, ref isComplete))
                    break;
            }

            return results;
        }

        private static bool ProcessSegment(Species species, QuerySegment<Key, Value> segment, ref int count, ref string continuationToken, ref bool isComplete)
        {
            using (segment)
            {
                if (isComplete)
                {
                    // This is to show that sending a completed continuation token is handled properly, by returning no rows.
                    Debug.Assert(segment.Results.Count == 0);
                    Debug.Assert(continuationToken == segment.ContinuationToken);
                    return false;
                }
                if (segment.Results.Count < SegmentSize)
                {
                    if (segment.Results.Count > 0 || count == 0)
                        Console.WriteLine($"  CT: {count + segment.Results.Count} {species}s retrieved");
                    continuationToken = segment.ContinuationToken;
                    isComplete = true;
                    return true;
                }
                count += segment.Results.Count;
                continuationToken = segment.ContinuationToken;
                Console.WriteLine($"  CT: {count} {species}s retrieved");
                return true;
            }
        }

        private static void Dispose(QueryRecord<Key, Value>[] records) => Array.ForEach(records, record => record.Dispose());
    }
}
