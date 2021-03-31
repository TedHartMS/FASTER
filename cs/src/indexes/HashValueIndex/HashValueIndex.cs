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

        private readonly RegistrationSettings<TPKey> RegistrationSettings;

        internal Predicate<TKVValue, TPKey>[] predicates;
        private int PredicateCount => this.predicates.Length;
        private readonly ConcurrentDictionary<string, Guid> predicateNames = new ConcurrentDictionary<string, Guid>();

        internal FasterKVHVI<TPKey> secondaryFkv;
        internal FasterKV<TKVKey, TKVValue> primaryFkv;

        private readonly IFasterEqualityComparer<TPKey> userKeyComparer;
        private readonly KeyAccessor<TPKey> keyAccessor;

#if false   // TODO session pooling
        private readonly Dictionary<long, ClientSessionSI<TProviderData, TRecordId>> indexSessions = new Dictionary<long, ClientSessionSI<TProviderData, TRecordId>>();

        /// <summary>
        /// Create a new session for SubsetIndex operations.
        /// </summary>
        public AdvancedClientSession<TProviderData, long> NewSession()
        {
            var sId = Interlocked.Increment(ref NextSessionId) - 1;
            var session = new ClientSessionSI<TProviderData, long>(this, sId);
            foreach (var group in this.groups.Values)
                session.AddGroup(group);
            lock (this.indexSessions)
                this.indexSessions.Add(sId, session);
            return session;
        }

        private IDisposable GetGroupSession<TPKey>(ClientSessionSI<TProviderData, long> indexSession, Predicate<TPKey> pred)
            => indexSession.GetGroupSession(this.groups[pred.GroupId]);

        internal void ReleaseSession(ClientSessionSI<TProviderData, long> session)
        {
            lock (this.indexSessions)
                this.indexSessions.Remove(session.Id);
        }
#endif

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
            UpdatePredicates(new[] { new Predicate<TKVValue, TPKey>(0, predName, predFunc) });
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
            UpdatePredicates(this.predicates = predFuncs.Select((tup, ord) => new Predicate<TKVValue, TPKey>(ord, tup.name, tup.func)).ToArray());
            CreateSecondaryFkv();
        }

        private HashValueIndex(string name, FasterKV<TKVKey, TKVValue> fkv, RegistrationSettings<TPKey> registrationSettings)
        {
            this.Name = name;
            this.primaryFkv = fkv;
            this.RegistrationSettings = registrationSettings;
            VerifyRegistrationSettings();
            this.userKeyComparer = GetUserKeyComparer();
            this.keyAccessor = new KeyAccessor<TPKey>(this.userKeyComparer, this.PredicateCount, this.keyPointerSize);
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
                throw new ArgumentExceptionHVI("RegistrationSettings is required");
            if (this.RegistrationSettings.LogSettings is null)
                throw new ArgumentExceptionHVI("RegistrationSettings.LogSettings is required");
            if (this.RegistrationSettings.CheckpointSettings is null)
                throw new ArgumentExceptionHVI("RegistrationSettings.CheckpointSettings is required");

            // TODOdcr: Support ReadCache and CopyReadsToTail for SubsetIndex
            if (this.RegistrationSettings.LogSettings.ReadCacheSettings is { } || this.RegistrationSettings.LogSettings.CopyReadsToTail != CopyReadsToTail.None)
                throw new ArgumentExceptionHVI("SubsetIndex does not support ReadCache or CopyReadsToTail");
        }

        void UpdatePredicates(Predicate<TKVValue, TPKey>[] newPredicates)
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
                        throw new ArgumentExceptionHVI($"Duplicate predicate name(s): {string.Join(", ", dupSet)}");
                }
                if (this.predicates is null)
                    this.predicates = newPredicates;
                else
                {
                    var extendedPredicates = new Predicate<TKVValue, TPKey>[this.predicates.Length + newPredicates.Length];
                    Array.Copy(this.predicates, extendedPredicates, this.predicates.Length);
                    Array.Copy(newPredicates, 0, extendedPredicates, this.predicates.Length, newPredicates.Length);
                    this.predicates = extendedPredicates;
                }
            }
        }

        /// <inheritdoc/>
        public void Insert(ref TKVValue value, long recordId) { /* Currently unsupported for HVI */ }

        /// <inheritdoc/>
        public void Upsert(ref TKVValue value, long recordId, bool isMutable)
        {
            if (isMutable)  // Currently unsupported for HVI
                return;
            ExecuteAndStore(ref value, recordId);
        }

        /// <inheritdoc/>
        public void Delete(long recordId) { /* Currently unsupported for HVI */ }

        private async ValueTask WhenAll(IEnumerable<ValueTask> tasks)
        {
            // Sequential to avoid allocating Tasks as there is no Task.WhenAll for ValueTask
            foreach (var task in tasks.Where(task => !task.IsCompletedSuccessfully))
                await task;
        }

        /// <summary>
        /// Obtains a list of registered Predicate names organized by the groups defined in previous Register calls. TODO: Replace with GetMetadata()
        /// </summary>
        /// <returns>A list of registered Predicate names organized by the groups defined in previous Register calls.</returns>
        public string[][] GetRegisteredPredicateNames() => throw new NotImplementedException("TODO");

        private Predicate<TKVValue, TPKey> GetImplementingPredicate(IPredicate iPred)
        {
            if (iPred is null)
                throw new ArgumentExceptionHVI($"The Predicate cannot be null.");
            var pred = iPred as Predicate<TKVValue, TPKey>;
            Guid id = default;
            if (pred is null || !this.predicateNames.TryGetValue(pred.Name, out id) || id != pred.Id)
                throw new ArgumentExceptionHVI($"The Predicate {iPred.Name} with Id {(pred is null ? "(unavailable)" : id.ToString())} is not registered with this FasterKV.");
            return pred;
        }

        internal IEnumerable<long> Query(IPredicate predicate, TPKey key, QuerySettings querySettings)
        {
            var predImpl = this.GetImplementingPredicate(predicate);
            querySettings ??= QuerySettings.Default;
            foreach (var recordId in predImpl.Query(key, querySettings))
            {
                if (querySettings.IsCanceled)
                    yield break;
                yield return recordId;
            }
        }

        internal async IAsyncEnumerable<long> QueryAsync(IPredicate predicate, TPKey key, QuerySettings querySettings)
        {
            var predImpl = this.GetImplementingPredicate(predicate);
            querySettings ??= QuerySettings.Default;
            await foreach (var recordId in predImpl.QueryAsync(key, querySettings))
            {
                if (querySettings.IsCanceled)
                    yield break;
                yield return recordId;
            }
        }

        internal IEnumerable<long> Query(
                    IEnumerable<(IPredicate pred, TPKey key)> predicatesAndKeys,
                    Func<bool[], bool> matchPredicate,
                    QuerySettings querySettings = null)
        {
            var predicates = predicatesAndKeys.Select(pk => (GetImplementingPredicate(pk.pred), pk.key)).ToArray();
            querySettings ??= QuerySettings.Default;

            return new QueryRecordIterator<long>(new[] { predicatesAndKeys.Select(tup => ((IPredicate)tup.pred, this.Query(tup.pred, tup.key, querySettings))) },
                                                      matchIndicators => matchPredicate(matchIndicators[0]), querySettings).Run();
        }

        internal IAsyncEnumerable<long> QueryAsync(
                    IEnumerable<(IPredicate pred, TPKey key)> predicatesAndKeys,
                    Func<bool[], bool> matchPredicate,
                    QuerySettings querySettings = null)
        {
            var predicates = predicatesAndKeys.Select(pk => (GetImplementingPredicate(pk.pred), pk.key)).ToArray();
            querySettings ??= QuerySettings.Default;

            return new AsyncQueryRecordIterator<long>(new[] { predicatesAndKeys.Select(tup => ((IPredicate)tup.pred, this.QueryAsync(tup.pred, tup.key, querySettings))) },
                                                      matchIndicators => matchPredicate(matchIndicators[0]), querySettings).Run();
        }
    }
}
