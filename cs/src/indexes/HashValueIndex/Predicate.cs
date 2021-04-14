// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

namespace FASTER.indexes.HashValueIndex
{
    /// <summary>
    /// The implementation of the Predicate Subset Function.
    /// </summary>
    /// <typeparam name="TKVKey">The type of the key in the <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</typeparam>
    /// <typeparam name="TKVValue">The type of the value in the <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</typeparam>
    /// <typeparam name="TPKey">The type of the key returned by the Predicate and store in the secondary FasterKV instance. TODO: What is the return type if this is varlen?</typeparam>
    internal class Predicate<TKVKey, TKVValue, TPKey> : IPredicate
     {
        /// <summary>
        /// The definition of the delegate used to obtain a new key matching the Value for this Predicate definition.
        /// </summary>
        /// <param name="kvValue">The value sent to FasterKV on Upsert or RMW</param>
        /// <remarks>This must be a delegate instead of a lambda to allow ref parameters</remarks>
        /// <returns>Null if the value does not match the predicate, else a key for the value in the Index hash table</returns>
        private delegate TPKey PredicateFunction(ref TKVValue kvValue);

        /// <summary>
        /// The predicate function that will be called by FasterKV on Upsert or RMW.
        /// </summary>
        private readonly PredicateFunction PredicateFunc;

        internal int Ordinal { get; }           // ordinal of the Predicate in the Index

        // Predicates are passed by the caller to the session Query functions, so make sure they don't send
        // a Predicate from a different Index.
        internal Guid Id { get; }

        // The containing index
        internal HashValueIndex<TKVKey, TKVValue, TPKey> Index { get; }

        /// <inheritdoc/>
        public string Name { get; }

        internal Predicate(HashValueIndex<TKVKey, TKVValue, TPKey> index, int predOrdinal, string name, Func<TKVValue, TPKey> predicate)
        {
            TPKey wrappedPredicate(ref TKVValue value) => predicate(value);

            this.Index = index;
            this.Ordinal = predOrdinal;
            this.Name = name;
            this.Id = Guid.NewGuid();
            this.PredicateFunc = wrappedPredicate;
        }

        // This return is checked against RegistrationSettings.NullIndicator to determine if the predicate matches the value.
        internal TPKey Execute(ref TKVValue value) => this.PredicateFunc(ref value);
    }
}
