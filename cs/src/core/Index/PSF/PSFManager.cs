// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core.Index.PSF;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

// TODO: Remove PackageId and PackageOutputPath from csproj when this is folded into master
// TODO: Make a new FASTER.PSF.dll

namespace FASTER.core
{
    /// <summary>
    /// The class that manages PSFs. Called internally by the primary FasterKV.
    /// </summary>
    /// <typeparam name="TProviderData">The type of the provider data returned by PSF queries; for the primary FasterKV, it is <see cref="FasterKVProviderData{TKVKey, TKVValue}"/></typeparam>
    /// <typeparam name="TRecordId">The type of the Record identifier in the data provider; for the primary FasterKV it is the record's logical address</typeparam>
    public class PSFManager<TProviderData, TRecordId> where TRecordId : struct, IComparable<TRecordId>
    {
        private readonly ConcurrentDictionary<long, IExecutePSF<TProviderData, TRecordId>> psfGroups 
            = new ConcurrentDictionary<long, IExecutePSF<TProviderData, TRecordId>>();

        private readonly ConcurrentDictionary<string, Guid> psfNames = new ConcurrentDictionary<string, Guid>();

        internal bool HasPSFs => this.psfGroups.Count > 0;

        /// <summary>
        /// Inserts a new PSF key/RecordId, or adds the RecordId to an existing chain
        /// </summary>
        /// <param name="data">The provider's data; will be passed to the PSF execution</param>
        /// <param name="recordId">The record Id to be stored for any matching PSFs</param>
        /// <param name="changeTracker">Tracks changes if this is an existing Key/RecordId entry</param>
        /// <returns>A status code indicating the result of the operation</returns>
        public Status Upsert(TProviderData data, TRecordId recordId, PSFChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            // TODO: RecordId locking, to ensure consistency of multiple PSFs if the same record is updated
            // multiple times; possibly a single Array<CacheLine>[N] which is locked on TRecordId.GetHashCode % N.

            // This Upsert was an Insert: For the FasterKV Insert fast path, changeTracker is null.
            if (changeTracker is null || changeTracker.UpdateOp == UpdateOperation.Insert)
            {
                foreach (var group in this.psfGroups.Values)
                {
                    // Fast Insert path: No IPUCache lookup is done for Inserts, so this is called directly here.
                    var status = group.ExecuteAndStore(data, recordId, PSFExecutePhase.Insert, changeTracker);
                    if (status != Status.OK)
                    {
                        // TODOerr: handle errors
                    }
                }
                return Status.OK;
            }

            // This Upsert was an IPU or RCU
            return this.Update(changeTracker);
        }

        /// <summary>
        /// Updates a PSF key/RecordId entry, possibly by RCU (Read-Copy-Update)
        /// </summary>
        /// <param name="changeTracker">Tracks changes for an existing Key/RecordId entry</param>
        /// <returns>A status code indicating the result of the operation</returns>
        public Status Update(PSFChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            foreach (var group in this.psfGroups.Values)
            {
                var status = group.Update(changeTracker);
                if (status != Status.OK)
                {
                    // TODOerr: handle errors
                }
            }
            return Status.OK;
        }

        /// <summary>
        /// Deletes a PSF key/RecordId entry from the chain, possibly by insertion of a "marked deleted" record
        /// </summary>
        /// <param name="changeTracker">Tracks changes for an existing Key/RecordId entry</param>
        /// <returns>A status code indicating the result of the operation</returns>
        public Status Delete(PSFChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            foreach (var group in this.psfGroups.Values)
            {
                var status = group.Delete(changeTracker);
                if (status != Status.OK)
                {
                    // TODOerr: handle errors
                }
            }
            return Status.OK;
        }

        /// <summary>
        /// Obtains a list of registered PSF names organized by the groups defined in previous RegisterPSF calls.
        /// </summary>
        /// <returns>A list of registered PSF names organized by the groups defined in previous RegisterPSF calls.</returns>
        public string[][] GetRegisteredPSFNames() => throw new NotImplementedException("TODO");

        /// <summary>
        /// Creates an instance of a <see cref="PSFChangeTracker{TProviderData, TRecordId}"/> to track changes for an existing Key/RecordId entry.
        /// </summary>
        /// <returns>An instance of a <see cref="PSFChangeTracker{TProviderData, TRecordId}"/> to track changes for an existing Key/RecordId entry.</returns>
        public PSFChangeTracker<TProviderData, TRecordId> CreateChangeTracker() 
            => new PSFChangeTracker<TProviderData, TRecordId>(this.psfGroups.Values.Select(group => group.Id));

        /// <summary>
        /// Sets the data for the state of a provider's data record prior to an update.
        /// </summary>
        /// <param name="changeTracker">Tracks changes for the Key/RecordId entry that will be updated.</param>
        /// <param name="data">The provider's data prior to the update; will be passed to the PSF execution</param>
        /// <param name="recordId">The record Id to be stored for any matching PSFs</param>
        /// <param name="executePSFsNow">Whether PSFs should be executed now or deferred. Should be 'true' if the provider's value type is an Object,
        ///     because the update will likely change the object's internal values, and thus a deferred 'before' execution will pick up the updated values instead.</param>
        /// <returns>A status code indicating the result of the operation</returns>
        public Status SetBeforeData(PSFChangeTracker<TProviderData, TRecordId> changeTracker, TProviderData data, TRecordId recordId, bool executePSFsNow)
        {
            changeTracker.SetBeforeData(data, recordId);
            if (executePSFsNow)
            {
                foreach (var group in this.psfGroups.Values)
                {
                    var status = group.GetBeforeKeys(changeTracker);
                    if (status != Status.OK)
                    {
                        // TODOerr: handle errors
                    }
                }
                changeTracker.HasBeforeKeys = true;
            }
            return Status.OK;
        }

        /// <summary>
        /// Sets the data for the state of a provider's data record after to an update.
        /// </summary>
        /// <param name="changeTracker">Tracks changes for the Key/RecordId entry that will be updated.</param>
        /// <param name="data">The provider's data after to the update; will be passed to the PSF execution</param>
        /// <param name="recordId">The record Id to be stored for any matching PSFs</param>
        /// <returns>A status code indicating the result of the operation</returns>
        public Status SetAfterData(PSFChangeTracker<TProviderData, TRecordId> changeTracker, TProviderData data, TRecordId recordId)
        {
            changeTracker.SetAfterData(data, recordId);
            return Status.OK;
        }

        private static long NextGroupId = 0;

        private void AddGroup<TPSFKey>(PSFGroup<TProviderData, TPSFKey, TRecordId> group) where TPSFKey : struct
        {
            var gId = Interlocked.Increment(ref NextGroupId);
            this.psfGroups.TryAdd(gId - 1, group);
        }

        private void VerifyIsBlittable<TPSFKey>()
        {
            if (!Utility.IsBlittable<TPSFKey>())
                throw new PSFArgumentException("The PSF Key type must be blittable.");
        }

        private PSF<TPSFKey, TRecordId> GetImplementingPSF<TPSFKey>(IPSF ipsf)
        {
            if (ipsf is null)
                throw new PSFArgumentException($"The PSF cannot be null.");
            var psf = ipsf as PSF<TPSFKey, TRecordId>;
            Guid id = default;
            if (psf is null || !this.psfNames.TryGetValue(psf.Name, out id) || id != psf.Id)
                throw new PSFArgumentException($"The PSF {psf.Name} with Id {(psf is null ? "(unavailable)" : id.ToString())} is not registered with this FasterKV.");
            return psf;
        }

        private void VerifyIsOurPSF(params IPSF[] psfs)
        {
            foreach (var psf in psfs)
            {
                if (psf is null)
                    throw new PSFArgumentException($"The PSF cannot be null.");
                if (!this.psfNames.ContainsKey(psf.Name))
                    throw new PSFArgumentException($"The PSF {psf.Name} is not registered with this FasterKV.");
            }
        }

        private void VerifyIsOurPSF<TPSFKey>(IEnumerable<(IPSF, IEnumerable<TPSFKey>)> psfsAndKeys)
        {
            if (psfsAndKeys is null)
                throw new PSFArgumentException($"The PSF enumerable cannot be null.");
            foreach (var psfAndKeys in psfsAndKeys)
                this.VerifyIsOurPSF(psfAndKeys.Item1);
        }

        private void VerifyIsOurPSF<TPSFKey1, TPSFKey2>(IEnumerable<(IPSF, IEnumerable<TPSFKey1>)> psfsAndKeys1,
                                                        IEnumerable<(IPSF, IEnumerable<TPSFKey2>)> psfsAndKeys2)
        {
            VerifyIsOurPSF(psfsAndKeys1);
            VerifyIsOurPSF(psfsAndKeys2);
        }

        private void VerifyIsOurPSF<TPSFKey1, TPSFKey2, TPSFKey3>(IEnumerable<(IPSF, IEnumerable<TPSFKey1>)> psfsAndKeys1,
                                                        IEnumerable<(IPSF, IEnumerable<TPSFKey2>)> psfsAndKeys2,
                                                        IEnumerable<(IPSF, IEnumerable<TPSFKey3>)> psfsAndKeys3)
        {
            VerifyIsOurPSF(psfsAndKeys1);
            VerifyIsOurPSF(psfsAndKeys2);
            VerifyIsOurPSF(psfsAndKeys3);
        }

        private static void VerifyRegistrationSettings<TPSFKey>(PSFRegistrationSettings<TPSFKey> registrationSettings) where TPSFKey : struct
        {
            if (registrationSettings is null)
                throw new PSFArgumentException("PSFRegistrationSettings is required");
            if (registrationSettings.LogSettings is null)
                throw new PSFArgumentException("PSFRegistrationSettings.LogSettings is required");
            if (registrationSettings.CheckpointSettings is null)
                throw new PSFArgumentException("PSFRegistrationSettings.CheckpointSettings is required");

            // TODOdcr: Support ReadCache and CopyReadsToTail for PSFs
            if (!(registrationSettings.LogSettings.ReadCacheSettings is null) || registrationSettings.LogSettings.CopyReadsToTail)
                throw new PSFArgumentException("PSFs do not support ReadCache or CopyReadsToTail");
        }

        /// <summary>
        /// Register a <see cref="PSF{TPSFKey, TRecordId}"/> with a simple definition.
        /// </summary>
        /// <typeparam name="TPSFKey">The type of the key returned from the <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="registrationSettings">Registration settings for the secondary FasterKV instances, etc.</param>
        /// <param name="def">The PSF definition</param>
        /// <returns>A PSF implementation(</returns>
        public IPSF RegisterPSF<TPSFKey>(PSFRegistrationSettings<TPSFKey> registrationSettings, IPSFDefinition<TProviderData, TPSFKey> def)
            where TPSFKey : struct
        {
            this.VerifyIsBlittable<TPSFKey>();
            VerifyRegistrationSettings(registrationSettings);
            if (def is null)
                throw new PSFArgumentException("PSF definition cannot be null");

            // This is a very rare operation and unlikely to have any contention, and locking the dictionary
            // makes it much easier to recover from duplicates if needed.
            lock (this.psfNames)
            {
                if (psfNames.ContainsKey(def.Name))
                    throw new PSFArgumentException($"A PSF named {def.Name} is already registered in another group");
                var group = new PSFGroup<TProviderData, TPSFKey, TRecordId>(registrationSettings, new[] { def }, this.psfGroups.Count);
                AddGroup(group);
                var psf = group[def.Name];
                this.psfNames.TryAdd(psf.Name, psf.Id);
                return psf;
            }
        }

        /// <summary>
        /// Register multiple <see cref="PSF{TPSFKey, TRecordId}"/>s with a vector of definitions.
        /// </summary>
        /// <typeparam name="TPSFKey">The type of the key returned from the <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="registrationSettings">Registration settings for the secondary FasterKV instances, etc.</param>
        /// <param name="defs">The PSF definitions</param>
        /// <returns>A PSF implementation(</returns>
        public IPSF[] RegisterPSF<TPSFKey>(PSFRegistrationSettings<TPSFKey> registrationSettings, IPSFDefinition<TProviderData, TPSFKey>[] defs)
            where TPSFKey : struct
        {
            this.VerifyIsBlittable<TPSFKey>();
            VerifyRegistrationSettings(registrationSettings);
            if (defs is null || defs.Length == 0 || defs.Any(def => def is null) || defs.Length == 0)
                throw new PSFArgumentException("PSF definitions cannot be null or empty");

            // We use stackalloc for speed and can recurse in pending operations, so make sure we don't blow the stack.
            if (defs.Length > Constants.kInvalidPsfOrdinal)
                throw new PSFArgumentException($"There can be no more than {Constants.kInvalidPsfOrdinal} PSFs in a single Group");
            const int maxKeySize = 256;
            if (Utility.GetSize(default(KeyPointer<TPSFKey>)) > maxKeySize)
                throw new PSFArgumentException($"The size of the PSF key can be no more than {maxKeySize} bytes");

            // This is a very rare operation and unlikely to have any contention, and locking the dictionary
            // makes it much easier to recover from duplicates if needed.
            lock (this.psfNames)
            {
                for (var ii = 0; ii < defs.Length; ++ii)
                {
                    var def = defs[ii];
                    if (psfNames.ContainsKey(def.Name))
                        throw new PSFArgumentException($"A PSF named {def.Name} is already registered in another group");
                    for (var jj = ii + 1; jj < defs.Length; ++jj)
                    {
                        if (defs[jj].Name == def.Name)
                            throw new PSFArgumentException($"The PSF name {def.Name} cannot be specfied twice");
                    }
                }

                var group = new PSFGroup<TProviderData, TPSFKey, TRecordId>(registrationSettings, defs, this.psfGroups.Count);
                AddGroup(group);
                foreach (var psf in group.PSFs)
                    this.psfNames.TryAdd(psf.Name, psf.Id);
                return group.PSFs;
            }
        }

        /// <summary>
        /// Does a synchronous scan of a single PSF for records matching a single key
        /// </summary>
        /// <typeparam name="TPSFKey">The type of the key returned from the <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf">The PSF to be queried</param>
        /// <param name="key">The <typeparamref name="TPSFKey"/> identifying the records to be retrieved</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An enumeration of the <typeparamref name="TRecordId"/>s matching <paramref name="key"/></returns>
        public IEnumerable<TRecordId> QueryPSF<TPSFKey>(IPSF psf, TPSFKey key, PSFQuerySettings querySettings)
            where TPSFKey : struct
        {
            var psfImpl = this.GetImplementingPSF<TPSFKey>(psf);
            querySettings ??= PSFQuerySettings.Default;
            foreach (var recordId in psfImpl.Query(key, querySettings))
            {
                if (querySettings.IsCanceled)
                    yield break;
                yield return recordId;
            }
        }

#if DOTNETCORE
        /// <summary>
        /// Does an asynchronous scan of a single PSF for records matching a single key
        /// </summary>
        /// <typeparam name="TPSFKey">The type of the key returned from the <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf">The PSF to be queried</param>
        /// <param name="key">The <typeparamref name="TPSFKey"/> identifying the records to be retrieved</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An async enumeration of the <typeparamref name="TRecordId"/>s matching <paramref name="key"/></returns>
        public async IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey>(IPSF psf, TPSFKey key, PSFQuerySettings querySettings)
            where TPSFKey : struct
        {
            var psfImpl = this.GetImplementingPSF<TPSFKey>(psf);
            querySettings ??= PSFQuerySettings.Default;
            await foreach (var recordId in psfImpl.QueryAsync(key, querySettings))
            {
                if (querySettings.IsCanceled)
                    yield break;
                yield return recordId;
            }
        }

#endif // DOTNETCORE

        /// <summary>
        /// Does a synchronous scan of a single PSF for records matching any of multiple keys, unioning the results.
        /// </summary>
        /// <typeparam name="TPSFKey">The type of the key returned from the <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf">The PSF to be queried</param>
        /// <param name="keys">The <typeparamref name="TPSFKey"/>s identifying the records to be retrieved</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An enumeration of the <typeparamref name="TRecordId"/>s matching <paramref name="keys"/></returns>
        public IEnumerable<TRecordId> QueryPSF<TPSFKey>(IPSF psf, IEnumerable<TPSFKey> keys, PSFQuerySettings querySettings)
            where TPSFKey : struct
        {
            this.VerifyIsOurPSF(psf);
            querySettings ??= PSFQuerySettings.Default;

            // The recordIds cannot overlap between keys (unless something's gone wrong), so return them all.
            // TODOperf: Consider a PQ ordered on secondary FKV LA so we can walk through in parallel (and in memory sequence) in one PsfRead(Key|Address) loop.
            foreach (var key in keys)
            {
                foreach (var recordId in QueryPSF(psf, key, querySettings))
                {
                    if (querySettings.IsCanceled)
                        yield break;
                    yield return recordId;
                }
            }
        }

#if DOTNETCORE
        /// <summary>
        /// Does an asynchronous scan of a single PSF for records matching any of multiple keys, unioning the results.
        /// </summary>
        /// <typeparam name="TPSFKey">The type of the key returned from the <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf">The PSF to be queried</param>
        /// <param name="keys">The <typeparamref name="TPSFKey"/>s identifying the records to be retrieved</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An async enumeration of the <typeparamref name="TRecordId"/>s matching <paramref name="keys"/></returns>
        public async IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey>(IPSF psf, IEnumerable<TPSFKey> keys, PSFQuerySettings querySettings)
            where TPSFKey : struct
        {
            this.VerifyIsOurPSF(psf);
            querySettings ??= PSFQuerySettings.Default;

            // The recordIds cannot overlap between keys (unless something's gone wrong), so return them all.
            // TODOperf: Consider a PQ ordered on secondary FKV LA so we can walk through in parallel (and in memory sequence) in one PsfRead(Key|Address) loop.
            foreach (var key in keys)
            {
                await foreach (var recordId in QueryPSFAsync(psf, key, querySettings))
                {
                    if (querySettings.IsCanceled)
                        yield break;
                    yield return recordId;
                }
            }
        }

#endif // DOTNETCORE

        /// <summary>
        /// Does a synchronous scan of one key on each of two PSFs, returning records matching these keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey1">The type of the key returned from the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key returned from the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first PSF to be queried</param>
        /// <param name="psf2">The second PSF to be queried</param>
        /// <param name="key1">The <typeparamref name="TPSFKey1"/> identifying the records to be retrieved from <paramref name="psf1"/></param>
        /// <param name="key2">The <typeparamref name="TPSFKey2"/> identifying the records to be retrieved from <paramref name="psf2"/></param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IEnumerable<TRecordId> QueryPSF<TPSFKey1, TPSFKey2>(
                     IPSF psf1, TPSFKey1 key1,
                     IPSF psf2, TPSFKey2 key2,
                    Func<bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            this.VerifyIsOurPSF(psf1, psf2);
            querySettings ??= PSFQuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(psf1, this.QueryPSF(psf1, key1, querySettings), psf2, this.QueryPSF(psf2, key2, querySettings),
                                                      matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0]), querySettings).Run();
        }

#if DOTNETCORE
        /// <summary>
        /// Does an synchronous scan of one key on each of two PSFs, returning records matching these keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey1">The type of the key returned from the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key returned from the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first PSF to be queried</param>
        /// <param name="psf2">The second PSF to be queried</param>
        /// <param name="key1">The <typeparamref name="TPSFKey1"/> identifying the records to be retrieved from <paramref name="psf1"/></param>
        /// <param name="key2">The <typeparamref name="TPSFKey2"/> identifying the records to be retrieved from <paramref name="psf2"/></param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An async enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey1, TPSFKey2>(
                     IPSF psf1, TPSFKey1 key1,
                     IPSF psf2, TPSFKey2 key2,
                    Func<bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            this.VerifyIsOurPSF(psf1, psf2);
            querySettings ??= PSFQuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(psf1, this.QueryPSFAsync(psf1, key1, querySettings), psf2, this.QueryPSFAsync(psf2, key2, querySettings),
                                                           matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0]), querySettings).Run();
        }

#endif // DOTNETCORE

        /// <summary>
        /// Does a synchronous scan of multiple keys on each of two PSFs, returning records matching any of those keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey1">The type of the key returned from the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key returned from the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first PSF to be queried</param>
        /// <param name="psf2">The second PSF to be queried</param>
        /// <param name="keys1">The <typeparamref name="TPSFKey1"/>s identifying the records to be retrieved from <paramref name="psf1"/></param>
        /// <param name="keys2">The <typeparamref name="TPSFKey2"/>s identifying the records to be retrieved from <paramref name="psf2"/></param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IEnumerable<TRecordId> QueryPSF<TPSFKey1, TPSFKey2>(
                     IPSF psf1, IEnumerable<TPSFKey1> keys1,
                     IPSF psf2, IEnumerable<TPSFKey2> keys2,
                    Func<bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            this.VerifyIsOurPSF(psf1, psf2);
            querySettings ??= PSFQuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(psf1, this.QueryPSF(psf1, keys1, querySettings), psf2, this.QueryPSF(psf2, keys2, querySettings),
                                                      matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0]), querySettings).Run();
        }

#if DOTNETCORE
        /// <summary>
        /// Does an asynchronous scan of multiple keys on each of two PSFs, returning records matching any of those keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey1">The type of the key returned from the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key returned from the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first PSF to be queried</param>
        /// <param name="psf2">The second PSF to be queried</param>
        /// <param name="keys1">The <typeparamref name="TPSFKey1"/>s identifying the records to be retrieved from <paramref name="psf1"/></param>
        /// <param name="keys2">The <typeparamref name="TPSFKey2"/>s identifying the records to be retrieved from <paramref name="psf2"/></param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An async enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey1, TPSFKey2>(
                     IPSF psf1, IEnumerable<TPSFKey1> keys1,
                     IPSF psf2, IEnumerable<TPSFKey2> keys2,
                    Func<bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            this.VerifyIsOurPSF(psf1, psf2);
            querySettings ??= PSFQuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(psf1, this.QueryPSFAsync(psf1, keys1, querySettings), psf2, this.QueryPSFAsync(psf2, keys2, querySettings),
                                                      matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0]), querySettings).Run();
        }
#endif // DOTNETCORE

        /// <summary>
        /// Does a synchronous scan of one key on each of three PSFs, returning records matching these keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey1">The type of the key returned from the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key returned from the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey3">The type of the key returned from the third <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first PSF to be queried</param>
        /// <param name="psf2">The second PSF to be queried</param>
        /// <param name="psf3">The third PSF to be queried</param>
        /// <param name="key1">The <typeparamref name="TPSFKey1"/> identifying the records to be retrieved from <paramref name="psf1"/></param>
        /// <param name="key2">The <typeparamref name="TPSFKey2"/> identifying the records to be retrieved from <paramref name="psf2"/></param>
        /// <param name="key3">The <typeparamref name="TPSFKey3"/> identifying the records to be retrieved from <paramref name="psf3"/></param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IEnumerable<TRecordId> QueryPSF<TPSFKey1, TPSFKey2, TPSFKey3>(
                     IPSF psf1, TPSFKey1 key1,
                     IPSF psf2, TPSFKey2 key2,
                     IPSF psf3, TPSFKey3 key3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
        {
            this.VerifyIsOurPSF(psf1, psf2, psf3);
            querySettings ??= PSFQuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(psf1, this.QueryPSF(psf1, key1, querySettings), psf2, this.QueryPSF(psf2, key2, querySettings),
                                                      psf3, this.QueryPSF(psf3, key3, querySettings),
                                                      matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0], matchIndicators[2][0]), querySettings).Run();
        }

#if DOTNETCORE
        /// <summary>
        /// Does an asynchronous scan of one key on each of three PSFs, returning records matching these keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey1">The type of the key returned from the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key returned from the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey3">The type of the key returned from the third <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first PSF to be queried</param>
        /// <param name="psf2">The second PSF to be queried</param>
        /// <param name="psf3">The third PSF to be queried</param>
        /// <param name="key1">The <typeparamref name="TPSFKey1"/> identifying the records to be retrieved from <paramref name="psf1"/></param>
        /// <param name="key2">The <typeparamref name="TPSFKey2"/> identifying the records to be retrieved from <paramref name="psf2"/></param>
        /// <param name="key3">The <typeparamref name="TPSFKey3"/> identifying the records to be retrieved from <paramref name="psf3"/></param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An async enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey1, TPSFKey2, TPSFKey3>(
                     IPSF psf1, TPSFKey1 key1,
                     IPSF psf2, TPSFKey2 key2,
                     IPSF psf3, TPSFKey3 key3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
        {
            this.VerifyIsOurPSF(psf1, psf2, psf3);
            querySettings ??= PSFQuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(psf1, this.QueryPSFAsync(psf1, key1, querySettings), psf2, this.QueryPSFAsync(psf2, key2, querySettings),
                                                      psf3, this.QueryPSFAsync(psf3, key3, querySettings),
                                                      matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0], matchIndicators[2][0]), querySettings).Run();
        }
#endif // DOTNETCORE

        /// <summary>
        /// Does a synchronous scan of multiple keys on each of three PSFs, returning records matching any of those keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey1">The type of the key returned from the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key returned from the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey3">The type of the key returned from the third <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first PSF to be queried</param>
        /// <param name="psf2">The second PSF to be queried</param>
        /// <param name="psf3">The third PSF to be queried</param>
        /// <param name="keys1">The <typeparamref name="TPSFKey1"/>s identifying the records to be retrieved from <paramref name="psf1"/></param>
        /// <param name="keys2">The <typeparamref name="TPSFKey2"/>s identifying the records to be retrieved from <paramref name="psf2"/></param>
        /// <param name="keys3">The <typeparamref name="TPSFKey3"/>s identifying the records to be retrieved from <paramref name="psf3"/></param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IEnumerable<TRecordId> QueryPSF<TPSFKey1, TPSFKey2, TPSFKey3>(
                     IPSF psf1, IEnumerable<TPSFKey1> keys1,
                     IPSF psf2, IEnumerable<TPSFKey2> keys2,
                     IPSF psf3, IEnumerable<TPSFKey3> keys3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
        {
            this.VerifyIsOurPSF(psf1, psf2, psf3);
            querySettings ??= PSFQuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(psf1, this.QueryPSF(psf1, keys1, querySettings), psf2, this.QueryPSF(psf2, keys2, querySettings),
                                                      psf3, this.QueryPSF(psf3, keys3, querySettings),
                                                      matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0], matchIndicators[2][0]), querySettings).Run();
        }

#if DOTNETCORE
        /// <summary>
        /// Does an asynchronous scan of multiple keys on each of three PSFs, returning records matching any of those keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey1">The type of the key returned from the first <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey2">The type of the key returned from the second <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <typeparam name="TPSFKey3">The type of the key returned from the third <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psf1">The first PSF to be queried</param>
        /// <param name="psf2">The second PSF to be queried</param>
        /// <param name="psf3">The third PSF to be queried</param>
        /// <param name="keys1">The <typeparamref name="TPSFKey1"/>s identifying the records to be retrieved from <paramref name="psf1"/></param>
        /// <param name="keys2">The <typeparamref name="TPSFKey2"/>s identifying the records to be retrieved from <paramref name="psf2"/></param>
        /// <param name="keys3">The <typeparamref name="TPSFKey3"/>s identifying the records to be retrieved from <paramref name="psf3"/></param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An async enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey1, TPSFKey2, TPSFKey3>(
                     IPSF psf1, IEnumerable<TPSFKey1> keys1,
                     IPSF psf2, IEnumerable<TPSFKey2> keys2,
                     IPSF psf3, IEnumerable<TPSFKey3> keys3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
        {
            this.VerifyIsOurPSF(psf1, psf2, psf3);
            querySettings ??= PSFQuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(psf1, this.QueryPSFAsync(psf1, keys1, querySettings), psf2, this.QueryPSFAsync(psf2, keys2, querySettings),
                                                      psf3, this.QueryPSFAsync(psf3, keys3, querySettings),
                                                      matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0], matchIndicators[2][0]), querySettings).Run();
        }
#endif // DOTNETCORE

        // Power user versions. Anything more complicated than these can be post-processed with LINQ.

        /// <summary>
        /// Does a synchronous scan of multiple keys on each of multiple PSFs with the same <typeparamref name="TPSFKey"/> type, returning records matching any of those keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey">The type of the key returned from the <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <param name="psfsAndKeys">An enumeration of tuples containing a <see cref="PSF{TPSFKey, TRecordId}"/> and the <typeparamref name="TPSFKey"/>s to be queried on it</param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IEnumerable<TRecordId> QueryPSF<TPSFKey>(
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey> keys)> psfsAndKeys,
                    Func<bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey : struct
        {
            this.VerifyIsOurPSF(psfsAndKeys);
            querySettings ??= PSFQuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(new[] { psfsAndKeys.Select(tup => ((IPSF)tup.psf, this.QueryPSF(tup.psf, tup.keys, querySettings))) },
                                                      matchIndicators => matchPredicate(matchIndicators[0]), querySettings).Run();
        }

#if DOTNETCORE
        /// <summary>
        /// Does an asynchronous scan of multiple keys on each of multiple PSFs with the same <typeparamref name="TPSFKey"/> type, returning records matching any of those keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey">The type of the key returned from the <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <param name="psfsAndKeys">An enumeration of tuples containing a <see cref="PSF{TPSFKey, TRecordId}"/> and the <typeparamref name="TPSFKey"/>s to be queried on it</param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An async enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey>(
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey> keys)> psfsAndKeys,
                    Func<bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey : struct
        {
            this.VerifyIsOurPSF(psfsAndKeys);
            querySettings ??= PSFQuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(new[] { psfsAndKeys.Select(tup => ((IPSF)tup.psf, this.QueryPSFAsync(tup.psf, tup.keys, querySettings))) },
                                                      matchIndicators => matchPredicate(matchIndicators[0]), querySettings).Run();
        }
#endif // DOTNETCORE

        /// <summary>
        /// Does a synchronous scan of multiple keys on each of multiple PSFs on each of two TPSFKey types, returning records matching any of those keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey1">The type of the key returned from the first set of <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPSFKey2">The type of the key returned from the second set of <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <param name="psfsAndKeys1">The first enumeration of tuples containing a <see cref="PSF{TPSFKey, TRecordId}"/> and the TPSFKey to be queried on it</param>
        /// <param name="psfsAndKeys2">The second enumeration of tuples containing a <see cref="PSF{TPSFKey, TRecordId}"/> and the TPSFKey to be queried on it</param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IEnumerable<TRecordId> QueryPSF<TPSFKey1, TPSFKey2>(
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey1> keys)> psfsAndKeys1,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey2> keys)> psfsAndKeys2,
                    Func<bool[], bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            this.VerifyIsOurPSF(psfsAndKeys1, psfsAndKeys2);
            querySettings ??= PSFQuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(new[] {psfsAndKeys1.Select(tup => ((IPSF)tup.psf, this.QueryPSF(tup.psf, tup.keys, querySettings))),
                                                             psfsAndKeys2.Select(tup => ((IPSF)tup.psf, this.QueryPSF(tup.psf, tup.keys, querySettings)))},
                                                      matchIndicators => matchPredicate(matchIndicators[0], matchIndicators[1]), querySettings).Run();
        }

#if DOTNETCORE
        /// <summary>
        /// Does an asynchronous scan of multiple keys on each of multiple PSFs on each of two TPSFKey types, returning records matching any of those keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey1">The type of the key returned from the first set of <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPSFKey2">The type of the key returned from the second set of <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <param name="psfsAndKeys1">The first enumeration of tuples containing a <see cref="PSF{TPSFKey, TRecordId}"/> and the TPSFKey to be queried on it</param>
        /// <param name="psfsAndKeys2">The second enumeration of tuples containing a <see cref="PSF{TPSFKey, TRecordId}"/> and the TPSFKey to be queried on it</param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An async enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey1, TPSFKey2>(
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey1> keys)> psfsAndKeys1,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey2> keys)> psfsAndKeys2,
                    Func<bool[], bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            this.VerifyIsOurPSF(psfsAndKeys1, psfsAndKeys2);
            querySettings ??= PSFQuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(new[] {psfsAndKeys1.Select(tup => ((IPSF)tup.psf, this.QueryPSFAsync(tup.psf, tup.keys, querySettings))),
                                                             psfsAndKeys2.Select(tup => ((IPSF)tup.psf, this.QueryPSFAsync(tup.psf, tup.keys, querySettings)))},
                                                      matchIndicators => matchPredicate(matchIndicators[0], matchIndicators[1]), querySettings).Run();
        }
#endif // DOTNETCORE

        /// <summary>
        /// Does a synchronous scan of multiple keys on each of multiple PSFs on each of three TPSFKey types, returning records matching any of those keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey1">The type of the key returned from the first set of <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPSFKey2">The type of the key returned from the second set of <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPSFKey3">The type of the key returned from the third set of <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <param name="psfsAndKeys1">The first enumeration of tuples containing a <see cref="PSF{TPSFKey, TRecordId}"/> and the TPSFKey to be queried on it</param>
        /// <param name="psfsAndKeys2">The second enumeration of tuples containing a <see cref="PSF{TPSFKey, TRecordId}"/> and the TPSFKey to be queried on it</param>
        /// <param name="psfsAndKeys3">The third enumeration of tuples containing a <see cref="PSF{TPSFKey, TRecordId}"/> and the TPSFKey to be queried on it</param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IEnumerable<TRecordId> QueryPSF<TPSFKey1, TPSFKey2, TPSFKey3>(
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey1> keys)> psfsAndKeys1,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey2> keys)> psfsAndKeys2,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey3> keys)> psfsAndKeys3,
                    Func<bool[], bool[], bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
        {
            this.VerifyIsOurPSF(psfsAndKeys1, psfsAndKeys2, psfsAndKeys3);
            querySettings ??= PSFQuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(new[] {psfsAndKeys1.Select(tup => ((IPSF)tup.psf, this.QueryPSF(tup.psf, tup.keys, querySettings))),
                                                             psfsAndKeys2.Select(tup => ((IPSF)tup.psf, this.QueryPSF(tup.psf, tup.keys, querySettings))),
                                                             psfsAndKeys3.Select(tup => ((IPSF)tup.psf, this.QueryPSF(tup.psf, tup.keys, querySettings)))},
                                                      matchIndicators => matchPredicate(matchIndicators[0], matchIndicators[1], matchIndicators[2]), querySettings).Run();
        }

#if DOTNETCORE
        /// <summary>
        /// Does an asynchronous scan of multiple keys on each of multiple PSFs on each of three TPSFKey types, returning records matching any of those keys, with a union or intersection defined by <paramref name="matchPredicate"/>
        /// </summary>
        /// <typeparam name="TPSFKey1">The type of the key returned from the first set of <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPSFKey2">The type of the key returned from the second set of <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <typeparam name="TPSFKey3">The type of the key returned from the third set of <see cref="PSF{TPSFKey, TRecordId}"/>s</typeparam>
        /// <param name="psfsAndKeys1">The first enumeration of tuples containing a <see cref="PSF{TPSFKey, TRecordId}"/> and the TPSFKey to be queried on it</param>
        /// <param name="psfsAndKeys2">The second enumeration of tuples containing a <see cref="PSF{TPSFKey, TRecordId}"/> and the TPSFKey to be queried on it</param>
        /// <param name="psfsAndKeys3">The third enumeration of tuples containing a <see cref="PSF{TPSFKey, TRecordId}"/> and the TPSFKey to be queried on it</param>
        /// <param name="matchPredicate">Takes boolean parameters indicating which PSFs are matched by the current record, and returns a boolean indicating whether
        ///     that record should be included in the result set</param>
        /// <param name="querySettings">Options for the PSF query operation</param>
        /// <returns>An async enumeration of the <typeparamref name="TRecordId"/>s matching the PSF keys and <paramref name="matchPredicate"/></returns>
        public IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey1, TPSFKey2, TPSFKey3>(
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey1> keys)> psfsAndKeys1,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey2> keys)> psfsAndKeys2,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey3> keys)> psfsAndKeys3,
                    Func<bool[], bool[], bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
        {
            this.VerifyIsOurPSF(psfsAndKeys1, psfsAndKeys2, psfsAndKeys3);
            querySettings ??= PSFQuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(new[] {psfsAndKeys1.Select(tup => ((IPSF)tup.psf, this.QueryPSFAsync(tup.psf, tup.keys, querySettings))),
                                                             psfsAndKeys2.Select(tup => ((IPSF)tup.psf, this.QueryPSFAsync(tup.psf, tup.keys, querySettings))),
                                                             psfsAndKeys3.Select(tup => ((IPSF)tup.psf, this.QueryPSFAsync(tup.psf, tup.keys, querySettings)))},
                                                      matchIndicators => matchPredicate(matchIndicators[0], matchIndicators[1], matchIndicators[2]), querySettings).Run();
        }
#endif // DOTNETCORE

        #region Checkpoint Operations
        // TODO Separate Tasks for each group's commit/restore operations?

        /// <summary>
        /// For each <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/>, take a full checkpoint of the FasterKV implementing the group's PSFs.
        /// </summary>
        public bool TakeFullCheckpoint()
            => this.psfGroups.Values.Aggregate(true, (result, group) => group.TakeFullCheckpoint() && result);

        /// <summary>
        /// For each <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/>, complete ongoing checkpoint (spin-wait)
        /// </summary>
        public Task CompleteCheckpointAsync(CancellationToken token = default)
        {
            var tasks = this.psfGroups.Values.Select(group => group.CompleteCheckpointAsync(token).AsTask()).ToArray();
            return Task.WhenAll(tasks);
        }

        /// <summary>
        /// For each <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/>, take a checkpoint of the Index (hashtable) only
        /// </summary>
        public bool TakeIndexCheckpoint()
            => this.psfGroups.Values.Aggregate(true, (result, group) => group.TakeIndexCheckpoint() && result);

        /// <summary>
        /// For each <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/>, take a checkpoint of the hybrid log only
        /// </summary>
        public bool TakeHybridLogCheckpoint() 
            => this.psfGroups.Values.Aggregate(true, (result, group) => group.TakeHybridLogCheckpoint() && result);

        /// <summary>
        /// For each <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/>, recover from last successful checkpoints
        /// </summary>
        public void Recover()
        {
            foreach (var group in this.psfGroups.Values)
                group.Recover();
        }
        #endregion Checkpoint Operations

        #region Log Operations

        /// <summary>
        /// Flush logs for all <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/>s until their current tail (records are still retained in memory)
        /// </summary>
        /// <param name="wait">Synchronous wait for operation to complete</param>
        public void FlushLogs(bool wait)
        {
            foreach (var group in this.psfGroups.Values)
                group.FlushLog(wait);
        }

        /// <summary>
        /// Flush logs for all <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/>s and evict all records from memory
        /// </summary>
        /// <param name="wait">Synchronous wait for operation to complete</param>
        /// <returns>When wait is false, this tells whether the full eviction was successfully registered with FASTER</returns>
        public bool FlushAndEvictLogs(bool wait)
        {
            foreach (var group in this.psfGroups.Values)
            {
                if (!group.FlushAndEvictLog(wait))
                {
                    // TODO handle error on FlushAndEvictLogs
                }
            }
            return true;
        }

        /// <summary>
        /// Delete logs for all <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/>s entirely from memory. Cannot allocate on the log
        /// after this point. This is a synchronous operation.
        /// </summary>
        public void DisposeLogsFromMemory()
        {
            foreach (var group in this.psfGroups.Values)
                group.DisposeLogFromMemory();
        }
        #endregion Log Operations
    }
}
