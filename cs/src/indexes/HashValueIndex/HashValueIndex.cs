// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

// TODO: Remove PackageId and PackageOutputPath from csproj when this is folded into master

namespace FASTER.indexes.HashValueIndex
{
    /// <summary>
    /// Implementation of the FASTER HashValueIndex.
    /// </summary>
    /// <typeparam name="TKVKey">The type of the key in the primary <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</typeparam>
    /// <typeparam name="TKVValue">The type of the value in the primary <see cref="FasterKV{TKVKey, TKVValue}"/> instance.</typeparam>
    /// <typeparam name="TPKey">The type of the key returned from the <see cref="IPredicate"/>, which is the key of the secondary <see cref="FasterKV{TPKey, Long}"/>.</typeparam>
    public partial class HashValueIndex<TKVKey, TKVValue, TPKey> : ISecondaryValueIndex<TKVKey, TKVValue>
    {
        /// <inheritdoc/>
        public string Name { get; private set; }

        /// <inheritdoc/>
        public bool IsMutable => false;

        /// <inheritdoc/>
        public void SetSessionSlot(long slot) => this.sessionSlot = slot;

        private readonly RegistrationSettings<TPKey> RegistrationSettings;
        private long sessionSlot;

        internal Predicate<TKVKey, TKVValue, TPKey>[] predicates;
        private int PredicateCount => this.predicates is { } ? this.predicates.Length : 0;
        private readonly ConcurrentDictionary<string, Guid> predicateNames = new ConcurrentDictionary<string, Guid>();

        private readonly IFasterEqualityComparer<TPKey> userKeyComparer;
        private KeyAccessor<TPKey> keyAccessor;

        /// <summary>
        /// Retrieves the key container for the key from the underlying hybrid log
        /// </summary>
        public IHeapContainer<TPKey> GetKeyContainer(ref TPKey key) => this.secondaryFkv.hlog.GetKeyContainer(ref key);

        /// <summary>
        /// Constructs a HashValueIndex with a single Predicate
        /// </summary>
        /// <param name="name">Name of the index</param>
        /// <param name="fkv">The primary FasterKV</param>
        /// <param name="registrationSettings">Settings for the index</param>
        /// <param name="predName">Name of the Predicate</param>
        /// <param name="predFunc">Function of the predicate, mapping the <typeparamref name="TKVValue"/> to <typeparamref name="TPKey"/></param>
        public HashValueIndex(string name, FasterKV<TKVKey, TKVValue> fkv, RegistrationSettings<TPKey> registrationSettings, 
                              string predName, Func<TKVValue, TPKey> predFunc)
            : this(name, fkv, registrationSettings)
        {
            UpdatePredicates(new[] { new Predicate<TKVKey, TKVValue, TPKey>(this, 0, predName, predFunc) });
            CreateSecondaryFkv();
        }

        /// <summary>
        /// Constructs a HashValueIndex with multiple Predicates
        /// </summary>
        /// <param name="name">Name of the index</param>
        /// <param name="fkv">The primary FasterKV</param>
        /// <param name="registrationSettings">Settings for the index</param>
        /// <param name="predFuncs">A vector of tuples of predicate name and the function implementing the Predicate</param>
        public HashValueIndex(string name, FasterKV<TKVKey, TKVValue> fkv, RegistrationSettings<TPKey> registrationSettings,
                              params (string name, Func<TKVValue, TPKey> func)[] predFuncs)
            : this(name, fkv, registrationSettings)
        {
            UpdatePredicates(predFuncs.Select((tup, ord) => new Predicate<TKVKey, TKVValue, TPKey>(this, ord, tup.name, tup.func)).ToArray());
            CreateSecondaryFkv();
        }

        private HashValueIndex(string name, FasterKV<TKVKey, TKVValue> fkv, RegistrationSettings<TPKey> registrationSettings)
        {
            this.Name = name;
            this.primaryFkv = fkv;
            this.RegistrationSettings = registrationSettings;
            VerifyRegistrationSettings();
            this.userKeyComparer = GetUserKeyComparer();
            this.readOnlyQueue = new WorkQueueOrdered<long, OrderedRange>(primaryFkv.Log.BeginAddress); // Note: readOnlyQueue's nextAddress is updated by Recovery and reading of existing data
        }

        private IFasterEqualityComparer<TPKey> GetUserKeyComparer()
        {
            if (this.RegistrationSettings.KeyComparer is { })
                return this.RegistrationSettings.KeyComparer;
            if (typeof(IFasterEqualityComparer<TPKey>).IsAssignableFrom(typeof(TPKey)))
                return default(TPKey) as IFasterEqualityComparer<TPKey>;

            Console.WriteLine(
                $"***WARNING*** Creating default FASTER key equality comparer based on potentially slow {nameof(EqualityComparer<TPKey>)}." +
                $" To avoid this, provide a comparer in {nameof(RegistrationSettings<TPKey>)}.{nameof(RegistrationSettings<TPKey>.KeyComparer)}," +
                $" or make {typeof(TPKey).Name} implement the interface {nameof(IFasterEqualityComparer<TPKey>)}");
            return FasterEqualityComparer.Get<TPKey>();
        }

        private void VerifyRegistrationSettings()
        {
            if (this.RegistrationSettings is null)
                throw new HashValueIndexArgumentException("RegistrationSettings is required");
            if (this.RegistrationSettings.LogSettings is null)
                throw new HashValueIndexArgumentException("RegistrationSettings.LogSettings is required");
            if (this.RegistrationSettings.CheckpointSettings is null)
                throw new HashValueIndexArgumentException("RegistrationSettings.CheckpointSettings is required");

            // TODOdcr: Support ReadCache and CopyReadsToTail for HashValueIndex
            if (this.RegistrationSettings.LogSettings.ReadCacheSettings is { } || this.RegistrationSettings.LogSettings.CopyReadsToTail != CopyReadsToTail.None)
                throw new HashValueIndexArgumentException("HashValueIndex does not support ReadCache or CopyReadsToTail");
        }

        void UpdatePredicates(Predicate<TKVKey, TKVValue, TPKey>[] newPredicates)
        {
            // This is a very rare operation and unlikely to have any contention, and locking the dictionary
            // makes it much easier to avoid problems with concurrent update/query operations.
            // TODO: ensure that update/query operations take a local copy of this.predicates as it may change during the operation
            lock (this.predicateNames)
            {
                if (newPredicates.Length > 0 && this.predicateNames.Count > 0)
                {
                    var dupSet = newPredicates.Select(p => p.Name).Aggregate(new HashSet<string>(), (set, name) => { if (this.predicateNames.ContainsKey(name)) { set.Add(name); } return set; });
                    if (dupSet.Count > 0)
                        throw new HashValueIndexArgumentException($"Duplicate predicate name(s): {string.Join(", ", dupSet)}");
                }

                if (this.predicates is null)
                {
                    this.predicates = newPredicates;
                } 
                else
                {
                    var extendedPredicates = new Predicate<TKVKey, TKVValue, TPKey>[this.predicates.Length + newPredicates.Length];
                    Array.Copy(this.predicates, extendedPredicates, this.predicates.Length);
                    Array.Copy(newPredicates, 0, extendedPredicates, this.predicates.Length, newPredicates.Length);
                    this.predicates = extendedPredicates;
                }

                foreach (var pred in newPredicates)
                    this.predicateNames[pred.Name] = pred.Id;
            }

            // Now we have a predicate count. Note: If we allow adding/removing predicates, we'll have to update this.
            this.keyAccessor = new KeyAccessor<TPKey>(this.userKeyComparer, this.PredicateCount, this.keyPointerSize);
        }

        /// <summary>
        /// Find a named predicate.
        /// </summary>
        public IPredicate GetPredicate(string name) => this.predicates.First(pred => pred.Name == name);

        private Sessions GetSessions(SecondaryIndexSessionBroker sessionBroker) 
            => sessionBroker.GetSessionObject(this.sessionSlot) as Sessions ?? new Sessions(sessionBroker, this.sessionSlot, this.primaryFkv, this.secondaryFkv, this.keyAccessor);

        /// <inheritdoc/>
        public void Insert(ref TKVKey key, ref TKVValue value, RecordId recordId, SecondaryIndexSessionBroker sessionBroker) { /* Currently unsupported for HashValueIndex */ }

        /// <inheritdoc/>
        public void Upsert(ref TKVKey key, ref TKVValue value, RecordId recordId, bool isMutable, SecondaryIndexSessionBroker sessionBroker)
        {
            // key is ignored for HashValueIndex
            if (isMutable)  // Currently unsupported for HashValueIndex
                return;
            ExecuteAndStore(GetSessions(sessionBroker).SecondarySession, ref value, recordId);
        }

        /// <inheritdoc/>
        public void Delete(ref TKVKey key, RecordId recordId, SecondaryIndexSessionBroker sessionBroker) { /* Currently unsupported for HashValueIndex */ }

        /// <summary>
        /// Obtains a list of registered Predicate names organized by the groups defined in previous Register calls. TODO: Replace with GetMetadata()
        /// </summary>
        /// <returns>A list of registered Predicate names organized by the groups defined in previous Register calls.</returns>
        public string[][] GetRegisteredPredicateNames() => throw new NotImplementedException("TODO");

        private Predicate<TKVKey, TKVValue, TPKey> GetImplementingPredicate(IPredicate iPred)
        {
            if (iPred is null)
                throw new HashValueIndexArgumentException($"The Predicate cannot be null.");
            var pred = iPred as Predicate<TKVKey, TKVValue, TPKey>;
            Guid id = default;
            if (pred is null || !this.predicateNames.TryGetValue(pred.Name, out id) || id != pred.Id)
                throw new HashValueIndexArgumentException($"The Predicate {iPred.Name} with Id {(pred is null ? "(unavailable)" : id.ToString())} is not registered with this FasterKV.");
            return pred;
        }

        #region Query Utilities
        private int GetPredicateOrdinal(IPredicate iPred) => this.GetImplementingPredicate(iPred).Ordinal;

        private SecondaryFasterKV<TPKey>.Input MakeQueryInput(IPredicate iPred, ref TPKey key) 
            => new SecondaryFasterKV<TPKey>.Input(this.bufferPool, this.keyAccessor, this.GetPredicateOrdinal(iPred), ref key);

        private SecondaryFasterKV<TPKey>.Input MakeQueryInput(IPredicate iPred, ref TPKey key, QueryContinuationToken continuationToken, int queryOrdinal) 
            => new SecondaryFasterKV<TPKey>.Input(this.bufferPool, this.keyAccessor, continuationToken, this.GetPredicateOrdinal(iPred), ref key, queryOrdinal);

        private bool MakeQueryIterator(IPredicate predicate, ref TPKey key, string continuationString, out QueryContinuationToken continuationToken, out SecondaryFasterKV<TPKey>.Input input)
        {
            if (string.IsNullOrEmpty(continuationString))
            {
                input = this.MakeQueryInput(predicate, ref key);
                continuationToken = new QueryContinuationToken(1);
                return true;
            }

            continuationToken = QueryContinuationToken.FromString(continuationString);
            if (continuationToken.Predicates.Length != 1)
                throw new HashValueIndexArgumentException($"Continuation token does not match number of predicates in the query");
            if (continuationToken.IsComplete)
            {
                input = default;
                return false;
            }
            input = this.MakeQueryInput(predicate, ref key, continuationToken, 0);
            return true;
        }

        private bool MakeQueryIterator(IEnumerable<(IPredicate pred, TPKey key)> queryPredicates, string continuationString, out QueryContinuationToken continuationToken, out MultiPredicateQueryIterator<TPKey> queryIterator)
        {
            if (string.IsNullOrEmpty(continuationString))
            {
                queryIterator = new MultiPredicateQueryIterator<TPKey>(queryPredicates.Select(qp => this.MakeQueryInput(qp.pred, ref qp.key)));
                continuationToken = new QueryContinuationToken(queryIterator.Length);
                return true;
            }

            continuationToken = QueryContinuationToken.FromString(continuationString);
            if (continuationToken.IsComplete)
            {
                queryIterator = default;
                return false;
            }
            var localToken = continuationToken;
            queryIterator = new MultiPredicateQueryIterator<TPKey>(queryPredicates.Select((qp, ii) => this.MakeQueryInput(qp.pred, ref qp.key, localToken, ii)), continuationToken);
            return true;
        }

        internal QuerySegment<TKVKey, TKVValue> CreateSegment(IEnumerable<QueryRecord<TKVKey, TKVValue>> recordsEnum, QueryContinuationToken continuationToken)
            => new QuerySegment<TKVKey, TKVValue>(recordsEnum.ToList(), continuationToken.ToString());

        internal async ValueTask<QuerySegment<TKVKey, TKVValue>> CreateSegmentAsync(IAsyncEnumerable<QueryRecord<TKVKey, TKVValue>> recordsEnum, QueryContinuationToken continuationToken, QuerySettings querySettings)
            => new QuerySegment<TKVKey, TKVValue>(await recordsEnum.ToListAsync(querySettings.CancellationToken), continuationToken.ToString());

        #endregion Query Utilities

        #region Single Predicate Sync
        internal IEnumerable<QueryRecord<TKVKey, TKVValue>> Query(IPredicate predicate, ref TPKey key, SecondaryIndexSessionBroker sessionBroker, QuerySettings querySettings)
            => InternalQuery(this.MakeQueryInput(predicate, ref key), sessionBroker, querySettings ?? QuerySettings.Default);

        internal QuerySegment<TKVKey, TKVValue> QuerySegmented(IPredicate predicate, ref TPKey key, SecondaryIndexSessionBroker sessionBroker, string continuationString, int numRecords, QuerySettings querySettings) 
            => MakeQueryIterator(predicate, ref key, continuationString, out QueryContinuationToken continuationToken, out SecondaryFasterKV<TPKey>.Input input)
                ? CreateSegment(InternalQuery(input, sessionBroker, querySettings ?? QuerySettings.Default, continuationToken, numRecords), continuationToken)
                : new QuerySegment<TKVKey, TKVValue>(new List<QueryRecord<TKVKey, TKVValue>>(), continuationString);
        #endregion Single Predicate Sync

        #region Single Predicate Async
        internal IAsyncEnumerable<QueryRecord<TKVKey, TKVValue>> QueryAsync(IPredicate predicate, ref TPKey key, SecondaryIndexSessionBroker sessionBroker, QuerySettings querySettings)
            => InternalQueryAsync(this.MakeQueryInput(predicate, ref key), sessionBroker, querySettings ?? QuerySettings.Default);

        internal ValueTask<QuerySegment<TKVKey, TKVValue>> QuerySegmentedAsync(IPredicate predicate, ref TPKey key, SecondaryIndexSessionBroker sessionBroker, string continuationString, int numRecords, QuerySettings querySettings)
        {
            querySettings ??= QuerySettings.Default;
            return MakeQueryIterator(predicate, ref key, continuationString, out QueryContinuationToken continuationToken, out SecondaryFasterKV<TPKey>.Input input)
                           ? CreateSegmentAsync(InternalQueryAsync(input, sessionBroker, querySettings, continuationToken, numRecords), continuationToken, querySettings)
                           : new ValueTask<QuerySegment<TKVKey, TKVValue>>(new QuerySegment<TKVKey, TKVValue>(new List<QueryRecord<TKVKey, TKVValue>>(), continuationString));
        }
        #endregion Single Predicate Async

        #region Multi Predicate Sync
        internal IEnumerable<QueryRecord<TKVKey, TKVValue>> Query(IEnumerable<(IPredicate pred, TPKey key)> queryPredicates,
                    Func<bool[], bool> matchPredicate, SecondaryIndexSessionBroker sessionBroker, QuerySettings querySettings)
            => MakeQueryIterator(queryPredicates, string.Empty, out QueryContinuationToken _, out MultiPredicateQueryIterator<TPKey> queryIterator)
                ? InternalQuery(queryIterator, matchPredicate, sessionBroker, querySettings ?? QuerySettings.Default)
                : new List<QueryRecord<TKVKey, TKVValue>>();

        internal QuerySegment<TKVKey, TKVValue> QuerySegmented(IEnumerable<(IPredicate pred, TPKey key)> queryPredicates,
                    Func<bool[], bool> matchPredicate, SecondaryIndexSessionBroker sessionBroker, string continuationString, int numRecords, QuerySettings querySettings) 
            => MakeQueryIterator(queryPredicates, continuationString, out QueryContinuationToken continuationToken, out MultiPredicateQueryIterator<TPKey> queryIterator)
                ? CreateSegment(InternalQuery(queryIterator, matchPredicate, sessionBroker, querySettings ?? QuerySettings.Default, continuationToken, numRecords), continuationToken)
                : new QuerySegment<TKVKey, TKVValue>(new List<QueryRecord<TKVKey, TKVValue>>(), continuationString);
        #endregion Multi Predicate Sync

        #region Multi Predicate Async
        internal IAsyncEnumerable<QueryRecord<TKVKey, TKVValue>> QueryAsync(IEnumerable<(IPredicate pred, TPKey key)> queryPredicates, Func<bool[], bool> matchPredicate,
                    SecondaryIndexSessionBroker sessionBroker, QuerySettings querySettings = null)
            => MakeQueryIterator(queryPredicates, string.Empty, out QueryContinuationToken _, out MultiPredicateQueryIterator<TPKey> queryIterator)
                ? InternalQueryAsync(queryIterator, matchPredicate, sessionBroker, querySettings ?? QuerySettings.Default)
                : new List<QueryRecord<TKVKey, TKVValue>>().ToAsyncEnumerable();

        internal ValueTask<QuerySegment<TKVKey, TKVValue>> QuerySegmentedAsync(IEnumerable<(IPredicate pred, TPKey key)> queryPredicates, Func<bool[], bool> matchPredicate,
                    SecondaryIndexSessionBroker sessionBroker, string continuationString, int numRecords, QuerySettings querySettings = null)
        {
            querySettings ??= QuerySettings.Default;
            return MakeQueryIterator(queryPredicates, continuationString, out QueryContinuationToken continuationToken, out MultiPredicateQueryIterator<TPKey> queryIterator)
                           ? CreateSegmentAsync(InternalQueryAsync(queryIterator, matchPredicate, sessionBroker, querySettings, continuationToken, numRecords), continuationToken, querySettings)
                           : new ValueTask<QuerySegment<TKVKey, TKVValue>>(new QuerySegment<TKVKey, TKVValue>(new List<QueryRecord<TKVKey, TKVValue>>(), continuationString));
        }
        #endregion Multi Predicate Async

        /// <summary>
        /// Flush data in the secondary FasterKV.
        /// </summary>
        /// <param name="wait"></param>
        public void Flush(bool wait) => this.secondaryFkv.Log.Flush(wait);

        /// <summary>
        /// Flush data in the secondary FasterKV and evict from memory.
        /// </summary>
        /// <param name="wait"></param>
        public void FlushAndEvict(bool wait) => this.secondaryFkv.Log.FlushAndEvict(wait);
    }
}
