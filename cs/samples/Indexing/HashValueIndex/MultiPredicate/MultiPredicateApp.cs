// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Linq;
using FASTER.indexes.HashValueIndex;
using HashValueIndexSampleCommon;

namespace MultiPredicateSample
{
    class MultiPredicateApp
    {
        static void Main()
        {
            using var store = new Store();
            store.RunInitialInserts();
            store.FlushAndEvict();

            var catsOfAge = QueryPredicates(store);
            store.UpdateCats(catsOfAge);
            QueryPredicates(store);
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

            results = session.Query(new[] { (store.CombinedPetPred, new AgeOrPetKey(Species.Cat)),
                                            (store.CombinedAgePred, new AgeOrPetKey(Constants.CatAge)) },
                                    vec => vec[0] && vec[1]).ToArray();
            Console.WriteLine($"{results.Length} cats age {Constants.CatAge} retrieved");
            var catsOfAge = results;

            results = session.Query(new[] { (store.CombinedPetPred, new AgeOrPetKey(Species.Cat)),
                                            (store.CombinedAgePred, new AgeOrPetKey(Constants.CatAge + Constants.CatAgeIncrement)) },
                                    vec => vec[0] && vec[1]).ToArray();
            Console.WriteLine($"{results.Length} cats age {Constants.CatAge + Constants.CatAgeIncrement} retrieved");

            results = session.Query(new[] { (store.CombinedPetPred, new AgeOrPetKey(Species.Dog)),
                                            (store.CombinedAgePred, new AgeOrPetKey(Constants.DogAge)) },
                                    vec => vec[0] && vec[1]).ToArray();
            Console.WriteLine($"{results.Length} dogs age {Constants.DogAge} retrieved");

            results = session.Query(new[] { (store.CombinedPetPred, new AgeOrPetKey(Species.Dog)),
                                            (store.CombinedAgePred, new AgeOrPetKey(Constants.CatAge)) },
                                    vec => vec[0] || vec[1]).ToArray();
            Console.WriteLine($"{results.Length} dogs or any pet age {Constants.CatAge} retrieved");
            return catsOfAge;
        }
    }
}
