// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace FASTER.indexes.HashValueIndex
{
    /// <summary>
    /// Provides extension methods for FasterKV sessions; external methods are provided for index queries.
    /// </summary>
    public static class SessionExtensions
    {
        #region External Query API - Session extension methods

        #region Single Predicate
        #region IFunctions

        /// <summary>
        /// Query records from a single predicate and resolve them on the primary <see cref="FasterKV{Key, Value}"/> instance.
        /// </summary>
        /// <typeparam name="TKVKey">Key type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TKVValue">Value type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TInput">Input type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TOutput">Output type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TContext">Context </typeparam>
        /// <typeparam name="TFunctions">Functions type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TPKey">Key type of the <paramref name="predicate"/></typeparam>
        /// <param name="clientSession">The session on the primary <see cref="FasterKV{Key, Value}"/></param>
        /// <param name="predicate">The Predicate to query</param>
        /// <param name="queryKey">The key to query on the predicate</param>
        /// <param name="querySettings">Optional settings for the query</param>
        /// <returns></returns>
        public static IEnumerable<QueryRecord<TKVKey, TKVValue>> Query<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions, TPKey>(this ClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions> clientSession,
                            IPredicate predicate, ref TPKey queryKey, QuerySettings querySettings = null)
            where TFunctions : IFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>
            => Query<TKVKey, TKVValue, TPKey>(clientSession.SecondaryIndexSessionBroker, predicate, ref queryKey, querySettings);

        /// <summary>
        /// Query records from a single predicate and resolve them on the primary <see cref="FasterKV{Key, Value}"/> instance.
        /// </summary>
        /// <typeparam name="TKVKey">Key type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TKVValue">Value type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TInput">Input type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TOutput">Output type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TContext">Context </typeparam>
        /// <typeparam name="TFunctions">Functions type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TPKey">Key type of the <paramref name="predicate"/></typeparam>
        /// <param name="clientSession">The session on the primary <see cref="FasterKV{Key, Value}"/></param>
        /// <param name="predicate">The Predicate to query</param>
        /// <param name="queryKey">The key to query on the predicate</param>
        /// <param name="continuationToken"></param>
        /// <param name="numberOfRecords"></param>
        /// <param name="querySettings">Optional settings for the query</param>
        /// <returns></returns>
        public static QuerySegment<TKVKey, TKVValue> QuerySegmented<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions, TPKey>(this ClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions> clientSession,
                            IPredicate predicate, ref TPKey queryKey, string continuationToken, int numberOfRecords, QuerySettings querySettings = null)
            where TFunctions : IFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>
            => QuerySegmented<TKVKey, TKVValue, TPKey>(clientSession.SecondaryIndexSessionBroker, predicate, ref queryKey, continuationToken, numberOfRecords, querySettings);

        /// <summary>
        /// Query records from a single predicate and resolve them on the primary <see cref="FasterKV{Key, Value}"/> instance.
        /// </summary>
        /// <typeparam name="TKVKey">Key type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TKVValue">Value type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TInput">Input type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TOutput">Output type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TContext">Context </typeparam>
        /// <typeparam name="TFunctions">Functions type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TPKey">Key type of the <paramref name="predicate"/></typeparam>
        /// <param name="clientSession">The session on the primary <see cref="FasterKV{Key, Value}"/></param>
        /// <param name="predicate">The Predicate to query</param>
        /// <param name="queryKey">The key to query on the predicate</param>
        /// <param name="querySettings">Optional settings for the query</param>
        /// <returns></returns>
        public static IEnumerable<QueryRecord<TKVKey, TKVValue>> Query<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions, TPKey>(this ClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions> clientSession,
                            IPredicate predicate, TPKey queryKey, QuerySettings querySettings = null)
            where TFunctions : IFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>
            => Query<TKVKey, TKVValue, TPKey>(clientSession.SecondaryIndexSessionBroker, predicate, ref queryKey, querySettings);

        /// <summary>
        /// Query records from a single predicate and resolve them on the primary <see cref="FasterKV{Key, Value}"/> instance.
        /// </summary>
        /// <typeparam name="TKVKey">Key type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TKVValue">Value type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TInput">Input type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TOutput">Output type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TContext">Context </typeparam>
        /// <typeparam name="TFunctions">Functions type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TPKey">Key type of the <paramref name="predicate"/></typeparam>
        /// <param name="clientSession">The session on the primary <see cref="FasterKV{Key, Value}"/></param>
        /// <param name="predicate">The Predicate to query</param>
        /// <param name="queryKey">The key to query on the predicate</param>
        /// <param name="continuationToken"></param>
        /// <param name="numberOfRecords"></param>
        /// <param name="querySettings">Optional settings for the query</param>
        /// <returns></returns>
        public static QuerySegment<TKVKey, TKVValue> QuerySegmented<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions, TPKey>(this ClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions> clientSession,
                            IPredicate predicate, TPKey queryKey, string continuationToken, int numberOfRecords, QuerySettings querySettings = null)
            where TFunctions : IFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>
            => QuerySegmented<TKVKey, TKVValue, TPKey>(clientSession.SecondaryIndexSessionBroker, predicate, ref queryKey, continuationToken, numberOfRecords, querySettings);

        /// <summary>
        /// Query records from a single predicate and resolve them on the primary <see cref="FasterKV{Key, Value}"/> instance.
        /// </summary>
        /// <typeparam name="TKVKey">Key type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TKVValue">Value type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TInput">Input type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TOutput">Output type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TContext">Context </typeparam>
        /// <typeparam name="TFunctions">Functions type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TPKey">Key type of the <paramref name="predicate"/></typeparam>
        /// <param name="clientSession">The session on the primary <see cref="FasterKV{Key, Value}"/></param>
        /// <param name="predicate">The Predicate to query</param>
        /// <param name="queryKey">The key to query on the predicate</param>
        /// <param name="querySettings">Optional settings for the query</param>
        /// <returns></returns>
        public static IAsyncEnumerable<QueryRecord<TKVKey, TKVValue>> QueryAsync<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions, TPKey>(this ClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions> clientSession,
                            IPredicate predicate, ref TPKey queryKey, QuerySettings querySettings = null)
            where TFunctions : IFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>
            => QueryAsync<TKVKey, TKVValue, TPKey>(clientSession.SecondaryIndexSessionBroker, predicate, ref queryKey, querySettings);

        /// <summary>
        /// Query records from a single predicate and resolve them on the primary <see cref="FasterKV{Key, Value}"/> instance.
        /// </summary>
        /// <typeparam name="TKVKey">Key type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TKVValue">Value type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TInput">Input type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TOutput">Output type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TContext">Context </typeparam>
        /// <typeparam name="TFunctions">Functions type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TPKey">Key type of the <paramref name="predicate"/></typeparam>
        /// <param name="clientSession">The session on the primary <see cref="FasterKV{Key, Value}"/></param>
        /// <param name="predicate">The Predicate to query</param>
        /// <param name="queryKey">The key to query on the predicate</param>
        /// <param name="querySettings">Optional settings for the query</param>
        /// <returns></returns>
        public static IAsyncEnumerable<QueryRecord<TKVKey, TKVValue>> QueryAsync<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions, TPKey>(this ClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions> clientSession,
                            IPredicate predicate, TPKey queryKey, QuerySettings querySettings = null)
            where TFunctions : IFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>
            => QueryAsync<TKVKey, TKVValue, TPKey>(clientSession.SecondaryIndexSessionBroker, predicate, ref queryKey, querySettings);

        #endregion IFunctions
        #region IAdvancedFunctions

        /// <summary>
        /// Query records from a single predicate and resolve them on the primary <see cref="FasterKV{Key, Value}"/> instance.
        /// </summary>
        /// <typeparam name="TKVKey">Key type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TKVValue">Value type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TInput">Input type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TOutput">Output type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TContext">Context </typeparam>
        /// <typeparam name="TFunctions">Functions type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TPKey">Key type of the <paramref name="predicate"/></typeparam>
        /// <param name="clientSession">The session on the primary <see cref="FasterKV{Key, Value}"/></param>
        /// <param name="predicate">The Predicate to query</param>
        /// <param name="queryKey">The key to query on the predicate</param>
        /// <param name="querySettings">Optional settings for the query</param>
        /// <returns></returns>
        public static IEnumerable<QueryRecord<TKVKey, TKVValue>> Query<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions, TPKey>(this AdvancedClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions> clientSession,
                            IPredicate predicate, ref TPKey queryKey, QuerySettings querySettings = null)
            where TFunctions : IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>
            => Query<TKVKey, TKVValue, TPKey>(clientSession.SecondaryIndexSessionBroker, predicate, ref queryKey, querySettings);

        /// <summary>
        /// Query records from a single predicate and resolve them on the primary <see cref="FasterKV{Key, Value}"/> instance.
        /// </summary>
        /// <typeparam name="TKVKey">Key type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TKVValue">Value type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TInput">Input type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TOutput">Output type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TContext">Context </typeparam>
        /// <typeparam name="TFunctions">Functions type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TPKey">Key type of the <paramref name="predicate"/></typeparam>
        /// <param name="clientSession">The session on the primary <see cref="FasterKV{Key, Value}"/></param>
        /// <param name="predicate">The Predicate to query</param>
        /// <param name="queryKey">The key to query on the predicate</param>
        /// <param name="querySettings">Optional settings for the query</param>
        /// <returns></returns>
        public static IEnumerable<QueryRecord<TKVKey, TKVValue>> Query<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions, TPKey>(this AdvancedClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions> clientSession,
                            IPredicate predicate, TPKey queryKey, QuerySettings querySettings = null)
            where TFunctions : IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>
            => Query<TKVKey, TKVValue, TPKey>(clientSession.SecondaryIndexSessionBroker, predicate, ref queryKey, querySettings);

        /// <summary>
        /// Query records from a single predicate and resolve them on the primary <see cref="FasterKV{Key, Value}"/> instance.
        /// </summary>
        /// <typeparam name="TKVKey">Key type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TKVValue">Value type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TInput">Input type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TOutput">Output type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TContext">Context </typeparam>
        /// <typeparam name="TFunctions">Functions type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TPKey">Key type of the <paramref name="predicate"/></typeparam>
        /// <param name="clientSession">The session on the primary <see cref="FasterKV{Key, Value}"/></param>
        /// <param name="predicate">The Predicate to query</param>
        /// <param name="queryKey">The key to query on the predicate</param>
        /// <param name="querySettings">Optional settings for the query</param>
        /// <returns></returns>
        public static IAsyncEnumerable<QueryRecord<TKVKey, TKVValue>> QueryAsync<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions, TPKey>(this AdvancedClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions> clientSession,
                            IPredicate predicate, ref TPKey queryKey, QuerySettings querySettings = null)
            where TFunctions : IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>
            => QueryAsync<TKVKey, TKVValue, TPKey>(clientSession.SecondaryIndexSessionBroker, predicate, ref queryKey, querySettings);

        /// <summary>
        /// Query records from a single predicate and resolve them on the primary <see cref="FasterKV{Key, Value}"/> instance.
        /// </summary>
        /// <typeparam name="TKVKey">Key type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TKVValue">Value type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TInput">Input type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TOutput">Output type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TContext">Context </typeparam>
        /// <typeparam name="TFunctions">Functions type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TPKey">Key type of the <paramref name="predicate"/></typeparam>
        /// <param name="clientSession">The session on the primary <see cref="FasterKV{Key, Value}"/></param>
        /// <param name="predicate">The Predicate to query</param>
        /// <param name="queryKey">The key to query on the predicate</param>
        /// <param name="querySettings">Optional settings for the query</param>
        /// <returns></returns>
        public static IAsyncEnumerable<QueryRecord<TKVKey, TKVValue>> QueryAsync<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions, TPKey>(this AdvancedClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions> clientSession,
                            IPredicate predicate, TPKey queryKey, QuerySettings querySettings = null)
            where TFunctions : IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>
            => QueryAsync<TKVKey, TKVValue, TPKey>(clientSession.SecondaryIndexSessionBroker, predicate, ref queryKey, querySettings);

        #endregion IAdvancedFunctions
        #endregion Single Predicate

        #region Vector of Predicates
        #region IFunctions

        /// <summary>
        /// Query records from multiple predicates and resolve them on the primary <see cref="FasterKV{Key, Value}"/> instance.
        /// </summary>
        /// <typeparam name="TKVKey">Key type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TKVValue">Value type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TInput">Input type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TOutput">Output type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TContext">Context </typeparam>
        /// <typeparam name="TFunctions">Functions type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TPKey">Key type of the <paramref name="queryPredicates"/> predicates</typeparam>
        /// <param name="clientSession">The session on the primary <see cref="FasterKV{Key, Value}"/></param>
        /// <param name="queryPredicates">A vector of tuples consisting of the Predicate to query and the key to search for</param>
        /// <param name="matchPredicate">A function that receives a vector of bool in the order of the predicates in <paramref name="queryPredicates"/>, 
        ///     indicating whether the RecordId currently being processed by the query matched the queryPredicate at that position.</param>
        /// <param name="querySettings">Optional settings for the query</param>
        /// <returns></returns>
        public static IEnumerable<QueryRecord<TKVKey, TKVValue>> Query<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions, TPKey>(this ClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions> clientSession,
                            (IPredicate predicate, TPKey queryKey)[] queryPredicates, Func<bool[], bool> matchPredicate, QuerySettings querySettings = null)
            where TFunctions : IFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>
            => Query<TKVKey, TKVValue, TPKey>(clientSession.SecondaryIndexSessionBroker, queryPredicates, matchPredicate, querySettings);

        /// <summary>
        /// Query records from multiple predicates and resolve them on the primary <see cref="FasterKV{Key, Value}"/> instance.
        /// </summary>
        /// <typeparam name="TKVKey">Key type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TKVValue">Value type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TInput">Input type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TOutput">Output type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TContext">Context </typeparam>
        /// <typeparam name="TFunctions">Functions type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TPKey">Key type of the <paramref name="queryPredicates"/> predicate</typeparam>
        /// <param name="clientSession">The session on the primary <see cref="FasterKV{Key, Value}"/></param>
        /// <param name="queryPredicates">A vector of tuples consisting of the Predicate to query and the key to search for</param>
        /// <param name="matchPredicate">A function that receives a vector of bool in the order of the predicates in <paramref name="queryPredicates"/>, 
        ///     indicating whether the RecordId currently being processed by the query matched the queryPredicate at that position.</param>
        /// <param name="querySettings">Optional settings for the query</param>
        /// <returns></returns>
        public static IAsyncEnumerable<QueryRecord<TKVKey, TKVValue>> QueryAsync<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions, TPKey>(this ClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions> clientSession,
                            (IPredicate predicate, TPKey queryKey)[] queryPredicates, Func<bool[], bool> matchPredicate, QuerySettings querySettings = null)
            where TFunctions : IFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>
            => QueryAsync<TKVKey, TKVValue, TPKey>(clientSession.SecondaryIndexSessionBroker, queryPredicates, matchPredicate, querySettings);

        #endregion IFunctions
        #region IAdvancedFunctions

        /// <summary>
        /// Query records from multiple predicates and resolve them on the primary <see cref="FasterKV{Key, Value}"/> instance.
        /// </summary>
        /// <typeparam name="TKVKey">Key type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TKVValue">Value type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TInput">Input type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TOutput">Output type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TContext">Context </typeparam>
        /// <typeparam name="TFunctions">Functions type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TPKey">Key type of the <paramref name="queryPredicates"/> predicates</typeparam>
        /// <param name="clientSession">The session on the primary <see cref="FasterKV{Key, Value}"/></param>
        /// <param name="queryPredicates">A vector of tuples consisting of the Predicate to query and the key to search for</param>
        /// <param name="matchPredicate">A function that receives a vector of bool in the order of the predicates in <paramref name="queryPredicates"/>, 
        ///     indicating whether the RecordId currently being processed by the query matched the queryPredicate at that position.</param>
        /// <param name="querySettings">Optional settings for the query</param>
        /// <returns></returns>
        public static IEnumerable<QueryRecord<TKVKey, TKVValue>> Query<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions, TPKey>(this AdvancedClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions> clientSession,
                            (IPredicate predicate, TPKey queryKey)[] queryPredicates, Func<bool[], bool> matchPredicate, QuerySettings querySettings = null)
            where TFunctions : IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>
            => Query<TKVKey, TKVValue, TPKey>(clientSession.SecondaryIndexSessionBroker, queryPredicates, matchPredicate, querySettings);

        /// <summary>
        /// Query records from multiple predicates and resolve them on the primary <see cref="FasterKV{Key, Value}"/> instance.
        /// </summary>
        /// <typeparam name="TKVKey">Key type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TKVValue">Value type of the primary <see cref="FasterKV{Key, Value}"/></typeparam>
        /// <typeparam name="TInput">Input type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TOutput">Output type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TContext">Context </typeparam>
        /// <typeparam name="TFunctions">Functions type of the <paramref name="clientSession"/></typeparam>
        /// <typeparam name="TPKey">Key type of the <paramref name="queryPredicates"/> predicate</typeparam>
        /// <param name="clientSession">The session on the primary <see cref="FasterKV{Key, Value}"/></param>
        /// <param name="queryPredicates">A vector of tuples consisting of the Predicate to query and the key to search for</param>
        /// <param name="matchPredicate">A function that receives a vector of bool in the order of the predicates in <paramref name="queryPredicates"/>, 
        ///     indicating whether the RecordId currently being processed by the query matched the queryPredicate at that position.</param>
        /// <param name="querySettings">Optional settings for the query</param>
        /// <returns></returns>
        public static IAsyncEnumerable<QueryRecord<TKVKey, TKVValue>> QueryAsync<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions, TPKey>(this AdvancedClientSession<TKVKey, TKVValue, TInput, TOutput, TContext, TFunctions> clientSession,
                            (IPredicate predicate, TPKey queryKey)[] queryPredicates, Func<bool[], bool> matchPredicate, QuerySettings querySettings = null)
            where TFunctions : IAdvancedFunctions<TKVKey, TKVValue, TInput, TOutput, TContext>
            => QueryAsync<TKVKey, TKVValue, TPKey>(clientSession.SecondaryIndexSessionBroker, queryPredicates, matchPredicate, querySettings);

        #endregion IAdvancedFunctions
        #endregion Vector of Predicates

        #endregion External Query API - Session extension methods

        #region Internal API implementations

        private static HashValueIndex<TKVKey, TKVValue, TPKey> GetIndex<TKVKey, TKVValue, TPKey>(IPredicate predicate)
        {
            if (predicate is null)
                throw new ArgumentNullException(nameof(predicate));
            if (predicate is Predicate<TKVKey, TKVValue, TPKey> fullPredicate)
                return fullPredicate.Index;
            throw new HashValueIndexException($"Predicate {predicate.Name} is not of the expected implementation type");
        }

        private static HashValueIndex<TKVKey, TKVValue, TPKey> GetIndex<TKVKey, TKVValue, TPKey>((IPredicate predicate, TPKey queryKey)[] queryPredicates)
        {
            if (queryPredicates is null)
                throw new ArgumentNullException(nameof(queryPredicates));
            return GetIndex<TKVKey, TKVValue, TPKey>(queryPredicates[0].predicate);
        }

        private static IEnumerable<QueryRecord<TKVKey, TKVValue>> Query<TKVKey, TKVValue, TPKey>(SecondaryIndexSessionBroker secondaryIndexSessionBroker,
                            IPredicate predicate, ref TPKey queryKey, QuerySettings querySettings) 
            => GetIndex<TKVKey, TKVValue, TPKey>(predicate).Query(predicate, ref queryKey, secondaryIndexSessionBroker, querySettings);

        private static QuerySegment<TKVKey, TKVValue> QuerySegmented<TKVKey, TKVValue, TPKey>(SecondaryIndexSessionBroker secondaryIndexSessionBroker,
                            IPredicate predicate, ref TPKey queryKey, string continuationToken, int numRecords, QuerySettings querySettings)
            => GetIndex<TKVKey, TKVValue, TPKey>(predicate).QuerySegmented(predicate, ref queryKey, secondaryIndexSessionBroker, continuationToken, numRecords, querySettings);

        private static IAsyncEnumerable<QueryRecord<TKVKey, TKVValue>> QueryAsync<TKVKey, TKVValue, TPKey>(SecondaryIndexSessionBroker secondaryIndexSessionBroker,
                            IPredicate predicate, ref TPKey queryKey, QuerySettings querySettings)
            => GetIndex<TKVKey, TKVValue, TPKey>(predicate).QueryAsync(predicate, ref queryKey, secondaryIndexSessionBroker, querySettings);

        private static IEnumerable<QueryRecord<TKVKey, TKVValue>> Query<TKVKey, TKVValue, TPKey>(SecondaryIndexSessionBroker secondaryIndexSessionBroker,
                            (IPredicate predicate, TPKey queryKey)[] queryPredicates, Func<bool[], bool> matchPredicate, QuerySettings querySettings)
            => GetIndex<TKVKey, TKVValue, TPKey>(queryPredicates).Query(queryPredicates, matchPredicate, secondaryIndexSessionBroker, querySettings);

        private static IAsyncEnumerable<QueryRecord<TKVKey, TKVValue>> QueryAsync<TKVKey, TKVValue, TPKey>(SecondaryIndexSessionBroker secondaryIndexSessionBroker,
                            (IPredicate predicate, TPKey queryKey)[] queryPredicates, Func<bool[], bool> matchPredicate, QuerySettings querySettings)
            => GetIndex<TKVKey, TKVValue, TPKey>(queryPredicates).QueryAsync(queryPredicates, matchPredicate, secondaryIndexSessionBroker, querySettings);

        #endregion Internal API implementations

        #region Internal Context operations

        internal static Status IndexRead<TPKey>(this AdvancedClientSession<TPKey, RecordId, SecondaryFasterKV<TPKey>.Input, SecondaryFasterKV<TPKey>.Output, SecondaryFasterKV<TPKey>.Context, SecondaryFasterKV<TPKey>.Functions> session,
                                                SecondaryFasterKV<TPKey> fkv, ref TPKey key, ref SecondaryFasterKV<TPKey>.Input input, ref SecondaryFasterKV<TPKey>.Output output, ref RecordInfo recordInfo,
                                                SecondaryFasterKV<TPKey>.Context context)
        {
            if (session.SupportAsync) session.UnsafeResumeThread();
            try
            {
                return fkv.ContextIndexRead(ref key, ref input, ref output, ref recordInfo, context, session.FasterSession, session.ctx);
            }
            finally
            {
                if (session.SupportAsync) session.UnsafeSuspendThread();
            }
        }

        internal static ValueTask<SecondaryFasterKV<TPKey>.ReadAsyncResult<SecondaryFasterKV<TPKey>.Input, SecondaryFasterKV<TPKey>.Output, SecondaryFasterKV<TPKey>.Context>> IndexReadAsync<TPKey>(
                                    this AdvancedClientSession<TPKey, RecordId, SecondaryFasterKV<TPKey>.Input, SecondaryFasterKV<TPKey>.Output, SecondaryFasterKV<TPKey>.Context, SecondaryFasterKV<TPKey>.Functions> session,
                                    SecondaryFasterKV<TPKey> fkv, ref TPKey key, ref SecondaryFasterKV<TPKey>.Input input, SecondaryFasterKV<TPKey>.Output output, long startAddress,
                                    SecondaryFasterKV<TPKey>.Context context, long serialNo, QuerySettings querySettings)
            => fkv.ContextIndexReadAsync(session.FasterSession, session.ctx, ref key, ref input, output, startAddress, ref context, serialNo, querySettings);

        internal static Status IndexInsert<TPKey>(this AdvancedClientSession<TPKey, RecordId, SecondaryFasterKV<TPKey>.Input, SecondaryFasterKV<TPKey>.Output, SecondaryFasterKV<TPKey>.Context, SecondaryFasterKV<TPKey>.Functions> session,
                                    SecondaryFasterKV<TPKey> fkv, ref TPKey key, RecordId recordId, ref SecondaryFasterKV<TPKey>.Input input, SecondaryFasterKV<TPKey>.Context context)
        {
            // Called on the secondary FasterKV
            if (session.SupportAsync) session.UnsafeResumeThread();
            try
            {
                return fkv.ContextIndexInsert(ref key, recordId, ref input, context, session.FasterSession, session.ctx);
            }
            finally
            {
                if (session.SupportAsync) session.UnsafeSuspendThread();
            }
        }
        #endregion Internal Context operations
    }
}
