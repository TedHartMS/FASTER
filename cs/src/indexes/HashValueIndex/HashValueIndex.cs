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
    public partial class HashValueIndex<TKVKey, TKVValue, TPKey> : ISecondaryValueIndex<TKVValue>
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
            UpdatePredicates(this.predicates = predFuncs.Select((tup, ord) => new Predicate<TKVKey, TKVValue, TPKey>(this, ord, tup.name, tup.func)).ToArray());
            CreateSecondaryFkv();
        }

        private HashValueIndex(string name, FasterKV<TKVKey, TKVValue> fkv, RegistrationSettings<TPKey> registrationSettings)
        {
            this.Name = name;
            this.primaryFkv = fkv;
            this.RegistrationSettings = registrationSettings;
            VerifyRegistrationSettings();
            this.userKeyComparer = GetUserKeyComparer();
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

            // TODOdcr: Support ReadCache and CopyReadsToTail for SubsetIndex
            if (this.RegistrationSettings.LogSettings.ReadCacheSettings is { } || this.RegistrationSettings.LogSettings.CopyReadsToTail != CopyReadsToTail.None)
                throw new HashValueIndexArgumentException("SubsetIndex does not support ReadCache or CopyReadsToTail");
        }

        void UpdatePredicates(Predicate<TKVKey, TKVValue, TPKey>[] newPredicates)
        {
            // This is a very rare operation and unlikely to have any contention, and locking the dictionary
            // makes it much easier to avoid problems with concurrent update/query operations.
            // TODO: ensure that update/query operations take a local copy of this.predicates as it may change during the operation
            lock (this.predicateNames)
            {
                if (newPredicates.Length + this.predicateNames.Count > 1)
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
        public void Insert(ref TKVValue value, RecordId recordId, SecondaryIndexSessionBroker sessionBroker) { /* Currently unsupported for HashValueIndex */ }

        /// <inheritdoc/>
        public void Upsert(ref TKVValue value, RecordId recordId, bool isMutable, SecondaryIndexSessionBroker sessionBroker)
        {
            if (isMutable)  // Currently unsupported for HashValueIndex
                return;
            ExecuteAndStore(GetSessions(sessionBroker).SecondarySession, ref value, recordId);
        }

        /// <inheritdoc/>
        public void Delete(RecordId recordId, SecondaryIndexSessionBroker sessionBroker) { /* Currently unsupported for HashValueIndex */ }

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

        private SecondaryFasterKV<TPKey>.Input MakeQueryInput(IPredicate iPred, ref TPKey key) => new SecondaryFasterKV<TPKey>.Input(this.bufferPool, this.keyAccessor, ref key, this.GetPredicateOrdinal(iPred));

        private SecondaryFasterKV<TPKey>.Input MakeQueryInput(QueryContinuationToken continuationToken, int queryOrdinal) => new SecondaryFasterKV<TPKey>.Input(continuationToken, this.bufferPool, queryOrdinal);

        private bool MakeQueryIterator(IPredicate predicate, ref TPKey key, string continuationString, out QueryContinuationToken continuationToken, out SecondaryFasterKV<TPKey>.Input input)
        {
            if (string.IsNullOrEmpty(continuationString))
            {
                continuationToken = default;
                input = this.MakeQueryInput(predicate, ref key);
                return true;
            }

            continuationToken = QueryContinuationToken.FromString(continuationString);
            input = this.MakeQueryInput(continuationToken, 0);
            return input.PreviousAddress != FASTER.core.Constants.kInvalidAddress;
        }


        // TODO: Add RecordId to conttok!



        private bool MakeQueryIterator(IEnumerable<(IPredicate pred, TPKey key)> queryPredicates, string continuationString, out QueryContinuationToken continuationToken, out MultiPredicateQueryIterator<TPKey> queryIterator)
        {
            if (string.IsNullOrEmpty(continuationString))
            {
                continuationToken = default;
                queryIterator = new MultiPredicateQueryIterator<TPKey>(queryPredicates.Select(qp => this.MakeQueryInput(qp.pred, ref qp.key)));
                return true;
            }

            continuationToken = QueryContinuationToken.FromString(continuationString);
            var localToken = continuationToken;
            queryIterator = new MultiPredicateQueryIterator<TPKey>(queryPredicates.Select((qp, ii) => this.MakeQueryInput(localToken, ii)));
            return queryIterator.states.Any(state => state.RecordInfo.PreviousAddress != FASTER.core.Constants.kInvalidAddress);
        }
        #endregion Query Utilities

        #region Single Predicate Sync
        internal IEnumerable<QueryRecord<TKVKey, TKVValue>> Query(IPredicate predicate, ref TPKey key, SecondaryIndexSessionBroker sessionBroker, QuerySettings querySettings)
            => InternalQuery(this.MakeQueryInput(predicate, ref key), sessionBroker, querySettings ?? QuerySettings.Default);

        internal QuerySegment<TKVKey, TKVValue> QuerySegmented(IPredicate predicate, ref TPKey key, SecondaryIndexSessionBroker sessionBroker, string continuationString, int numRecords, QuerySettings querySettings)
        {
            if (MakeQueryIterator(predicate, ref key, continuationString, out QueryContinuationToken continuationToken, out SecondaryFasterKV<TPKey>.Input input))
            {
                continuationToken ??= new QueryContinuationToken(1);
                var records = InternalQuery(input, sessionBroker, querySettings ?? QuerySettings.Default, continuationToken, numRecords).ToList();
                return new QuerySegment<TKVKey, TKVValue>(records, continuationToken.ToString());
            }
            return new QuerySegment<TKVKey, TKVValue>(new List<QueryRecord<TKVKey, TKVValue>>(), continuationString);
        }
        #endregion Single Predicate Sync

        #region Single Predicate Async
        internal IAsyncEnumerable<QueryRecord<TKVKey, TKVValue>> QueryAsync(IPredicate predicate, ref TPKey key, SecondaryIndexSessionBroker sessionBroker, QuerySettings querySettings)
            => QueryAsync(this.MakeQueryInput(predicate, ref key), sessionBroker, querySettings ?? QuerySettings.Default);

        private async IAsyncEnumerable<QueryRecord<TKVKey, TKVValue>> QueryAsync(SecondaryFasterKV<TPKey>.Input input, SecondaryIndexSessionBroker sessionBroker, QuerySettings querySettings)
        {
#if false
            var sessions = GetSessions(sessionBroker);
            await foreach (var recordId in QueryAsync(sessions.SecondarySession, input, querySettings))
            {
                if (querySettings.IsCanceled)
                    yield break;
                var record = await ResolveRecordAsync(sessions.PrimarySession, recordId, querySettings);
                if (record is { })
                    yield return record;
            }
#else
            yield break;
#endif
        }
        #endregion Single Predicate Async

        #region Multi Predicate Sync
        internal IEnumerable<QueryRecord<TKVKey, TKVValue>> Query(IEnumerable<(IPredicate pred, TPKey key)> queryPredicates,
                    Func<bool[], bool> matchPredicate, SecondaryIndexSessionBroker sessionBroker, QuerySettings querySettings)
        {
            return MakeQueryIterator(queryPredicates, string.Empty, out QueryContinuationToken continuationToken, out MultiPredicateQueryIterator<TPKey> queryIterator)
                ? InternalQuery(queryIterator, matchPredicate, sessionBroker, querySettings ??= QuerySettings.Default)
                : new QuerySegment<TKVKey, TKVValue>(new List<QueryRecord<TKVKey, TKVValue>>(), string.Empty);
        }

        internal IEnumerable<QueryRecord<TKVKey, TKVValue>> QuerySegmented(IEnumerable<(IPredicate pred, TPKey key)> queryPredicates,
                    Func<bool[], bool> matchPredicate, SecondaryIndexSessionBroker sessionBroker, string continuationString, int numRecords, QuerySettings querySettings)
        {
            if (!MakeQueryIterator(queryPredicates, continuationString, out QueryContinuationToken continuationToken, out MultiPredicateQueryIterator<TPKey> queryIterator))
            {
                continuationToken ??= new QueryContinuationToken(1);
                var records = InternalQuery(queryIterator, matchPredicate, sessionBroker, querySettings ?? QuerySettings.Default, continuationToken, numRecords).ToList();
                return new QuerySegment<TKVKey, TKVValue>(records, continuationToken.ToString());
            }
            return new QuerySegment<TKVKey, TKVValue>(new List<QueryRecord<TKVKey, TKVValue>>(), continuationString);
        }
        #endregion Multi Predicate Sync

        #region Multi Predicate Async
        internal IAsyncEnumerable<QueryRecord<TKVKey, TKVValue>> QueryAsync(
                    IEnumerable<(IPredicate pred, TPKey key)> queryPredicates, Func<bool[], bool> matchPredicate,
                    SecondaryIndexSessionBroker sessionBroker, QuerySettings querySettings = null)
        {
            var inputs = queryPredicates.Select(pk => (GetImplementingPredicate(pk.pred), pk.key)).ToArray();
            querySettings ??= QuerySettings.Default;
#if false   // TODO
            return new AsyncQueryRecordIterator<RecordId>(new[] { predicatesAndKeys.Select(tup => ((IPredicate)tup.pred, this.QueryAsync(tup.pred, tup.key, querySettings))) },
                                                      matchIndicators => matchPredicate(matchIndicators[0]), querySettings).Run();
#else
            return null;
#endif
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
