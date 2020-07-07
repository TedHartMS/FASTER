﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// Interface to FASTER key-value store
    /// </summary>
    public interface IFasterKV<Key, Value, Input, Output, Context, Functions> : IDisposable
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        #region Session Operations (Deprecated)

        /// <summary>
        /// Start a session with FASTER. FASTER sessions correspond to threads issuing
        /// operations to FASTER.
        /// </summary>
        /// <returns>Session identifier</returns>
        [Obsolete("Use NewSession() instead.")]
        Guid StartSession();

        /// <summary>
        /// Continue a session after recovery. Provide FASTER with the identifier of the
        /// session that is being continued.
        /// </summary>
        /// <param name="guid"></param>
        /// <returns>Sequence number for resuming operations</returns>
        [Obsolete("Use ResumeSession() instead.")] 
        CommitPoint ContinueSession(Guid guid);

        /// <summary>
        /// Stop a session and de-register the thread from FASTER.
        /// </summary>
        [Obsolete("Use and dispose NewSession() instead.")] 
        void StopSession();

        /// <summary>
        /// Refresh the session epoch. The caller is required to invoke Refresh periodically
        /// in order to guarantee system liveness.
        /// </summary>
        [Obsolete("Use NewSession(), where Refresh() is not required by default.")] 
        void Refresh();

        #endregion

        #region Core Index Operations (Deprecated)

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">Key of read</param>
        /// <param name="input">Input argument used by Reader to select what part of value to read</param>
        /// <param name="output">Reader stores the read result in output</param>
        /// <param name="context">User context to identify operation in asynchronous callback</param>
        /// <param name="serialNo">Increasing sequence number of operation (used for recovery)</param>
        /// <returns>Status of operation</returns>
        [Obsolete("Use NewSession() and invoke Read() on the session.")] 
        Status Read(ref Key key, ref Input input, ref Output output, Context context, long serialNo);

        /// <summary>
        /// (Blind) upsert operation
        /// </summary>
        /// <param name="key">Key of read</param>
        /// <param name="value">Value being upserted</param>
        /// <param name="context">User context to identify operation in asynchronous callback</param>
        /// <param name="serialNo">Increasing sequence number of operation (used for recovery)</param>
        /// <returns>Status of operation</returns>
        [Obsolete("Use NewSession() and invoke Upsert() on the session.")] 
        Status Upsert(ref Key key, ref Value value, Context context, long serialNo);

        /// <summary>
        /// Atomic read-modify-write operation
        /// </summary>
        /// <param name="key">Key of read</param>
        /// <param name="input">Input argument used by RMW callback to perform operation</param>
        /// <param name="context">User context to identify operation in asynchronous callback</param>
        /// <param name="serialNo">Increasing sequence number of operation (used for recovery)</param>
        /// <returns>Status of operation</returns>
        [Obsolete("Use NewSession() and invoke RMW() on the session.")] 
        Status RMW(ref Key key, ref Input input, Context context, long serialNo);

        /// <summary>
        /// Delete entry (use tombstone if necessary)
        /// Hash entry is removed as a best effort (if key is in memory and at 
        /// the head of hash chain.
        /// Value is set to null (using ConcurrentWrite) if it is in mutable region
        /// </summary>
        /// <param name="key">Key of delete</param>
        /// <param name="context">User context to identify operation in asynchronous callback</param>
        /// <param name="serialNo">Increasing sequence number of operation (used for recovery)</param>
        /// <returns>Status of operation</returns>
        [Obsolete("Use NewSession() and invoke Delete() on the session.")] 
        Status Delete(ref Key key, Context context, long serialNo);

        /// <summary>
        /// Complete all pending operations issued by this session
        /// </summary>
        /// <param name="wait">Whether we spin-wait for pending operations to complete</param>
        /// <returns>Whether all pending operations have completed</returns>
        [Obsolete("Use NewSession() and invoke CompletePending() on the session.")] 
        bool CompletePending(bool wait);

        #endregion

        #region New Session Operations

        /// <summary>
        /// Start a new client session with FASTER.
        /// </summary>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code. Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <returns>Session instance</returns>
        ClientSession<Key, Value, Input, Output, Context, Functions> NewSession(string sessionId = null, bool threadAffinitized = false);

        /// <summary>
        /// Resume (continue) prior client session with FASTER, used during
        /// recovery from failure.
        /// </summary>
        /// <param name="sessionId">ID/name of previous session to resume</param>
        /// <param name="commitPoint">Prior commit point of durability for session</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code. Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <returns>Session instance</returns>
        ClientSession<Key, Value, Input, Output, Context, Functions> ResumeSession(string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false);

        #endregion

        #region PSF Registration
        /// <summary>
        /// Register a <see cref="PSF{TPSFKey, TRecordId}"/> with a simple definition.
        /// </summary>
        /// <example>
        /// static TPSFKey? sizePsfFunc(ref TKVKey key, ref TKVValue value) => new TPSFKey(value.size);
        /// var sizePsfDef = new FasterKVPSFDefinition{TKVKey, TKVValue, TPSFKey}("sizePSF", sizePsfFunc);
        /// var sizePsf = fht.RegisterPSF(sizePsfDef);
        /// </example>
        /// <typeparam name="TPSFKey">The type of the key value returned from the <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="def">A FasterKV-specific form of a PSF definition</param>
        /// <param name="registrationSettings">Optional registration settings for the secondary FasterKV instances, etc.</param>
        /// <returns>A FasterKV-specific PSF implementation whose TRecordId is long(</returns>
        IPSF RegisterPSF<TPSFKey>(
                FasterKVPSFDefinition<Key, Value, TPSFKey> def,
                PSFRegistrationSettings<TPSFKey> registrationSettings = null)
            where TPSFKey : struct;

        /// <summary>
        /// Register a <see cref="PSF{TPSFKey, TRecordId}"/> with a simple definition.
        /// </summary>
        /// <example>
        /// static TPSFKey? sizePsfFunc(ref TKVKey key, ref TKVValue value) => new TPSFKey(value.size);
        /// var sizePsfDef = new FasterKVPSFDefinition{TKVKey, TKVValue, TPSFKey}("sizePSF", sizePsfFunc);
        /// var sizePsf = fht.RegisterPSF(new [] { sizePsfDef });
        /// </example>
        /// <typeparam name="TPSFKey">The type of the key value returned from the <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="defs">An array of FasterKV-specific forms of PSF definitions</param>
        /// <param name="registrationSettings">Optional registration settings for the secondary FasterKV instances, etc.</param>
        /// <returns>A FasterKV-specific PSF implementation whose TRecordId is long(</returns>
        IPSF[] RegisterPSF<TPSFKey>
                (FasterKVPSFDefinition<Key, Value, TPSFKey>[] defs,
                PSFRegistrationSettings<TPSFKey> registrationSettings = null)
            where TPSFKey : struct;

        /// <summary>
        /// Register a <see cref="PSF{TPSFKey, TRecordId}"/> with a simple definition.
        /// </summary>
        /// <example>
        /// var sizePsf = fht.RegisterPSF("sizePsf", (k, v) => new TPSFKey(v.size));
        /// </example>
        /// <typeparam name="TPSFKey">The type of the key value returned from the <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psfName">The name of the PSF; must be unique across all PSFGroups in this FasterKV instance</param>
        /// <param name="psfFunc">A Func implementing the PSF, it will be wrapped in a delegate</param>
        /// <param name="registrationSettings">Optional registration settings for the secondary FasterKV instances, etc.</param>
        /// <returns>A FasterKV-specific <see cref="PSF{TPSFKey, TRecordId}"/> implementation whose TRecordId is long</returns>
        IPSF RegisterPSF<TPSFKey>(
                string psfName, Func<Key, Value, TPSFKey?> psfFunc,
                PSFRegistrationSettings<TPSFKey> registrationSettings = null)
            where TPSFKey : struct;

        /// <summary>
        /// Register multiple <see cref="PSF{TPSFKey, TRecordId}"/> with no registration settings.
        /// </summary>
        /// <example>
        /// var sizePsf = fht.RegisterPSF(("sizePsf", (k, v) => new TPSFKey(v.size)),
        ///                               ("colorPsf", (k, v) => new TPSFKey(v.color)));
        /// </example>
        /// <typeparam name="TPSFKey">The type of the key value returned from the <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psfFuncs">One or more tuples containing a PSF name and implementing Func; the name must be 
        /// unique across all PSFGroups in this FasterKV instance, and the Func will be wrapped in a delegate</param>
        /// <remarks>"params" won't allow the optional fromAddress and keyComparer, so an overload is provided
        /// to specify those</remarks>
        IPSF[] RegisterPSF<TPSFKey>(
                params (string, Func<Key, Value, TPSFKey?>)[] psfFuncs)
            where TPSFKey : struct;

        /// <summary>
        /// Register multiple <see cref="PSF{TPSFKey, TRecordId}"/>s with registration settings.
        /// </summary>
        /// <example>
        /// // Unfortunately the array type cannot be implicitly deduced in current versions of the compiler
        /// var sizePsf = fht.RegisterPSF(new (string, Func{TKVKey, TKVValue, TPSFKey})[] {
        ///                                        ("sizePsf", (k, v) => new TPSFKey(v.size)),
        ///                                        ("colorPsf", (k, v) => new TPSFKey(v.color))},
        ///                               registrationSettings);
        /// </example>
        /// <typeparam name="TPSFKey">The type of the key value returned from the <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
        /// <param name="psfDefs">One or more tuples containing a PSF name and implementing Func; the name must be 
        /// unique across all PSFGroups in this FasterKV instance, and the Func will be wrapped in a delegate</param>
        /// <param name="registrationSettings">Optional registration settings for the secondary FasterKV instances, etc.</param>
        /// <remarks>If the registrationSettings parameters are null, then it is simpler to call the "params" overload
        /// than to create the vector explicitly</remarks>
        IPSF[] RegisterPSF<TPSFKey>(
                (string, Func<Key, Value, TPSFKey?>)[] psfDefs,
                PSFRegistrationSettings<TPSFKey> registrationSettings = null)
            where TPSFKey : struct;

        /// <summary>
        /// Returns the names of registered <see cref="PSF{TPSFKey, TRecordId}"/>s for use in recovery.
        /// TODO: Supplement or replace this with an app version string.
        /// </summary>
        /// <returns>An array of string arrays; each outer array corresponds to a 
        ///     <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/></returns>
        string[][] GetRegisteredPSFNames();

        #endregion PSF Registration

        #region Growth and Recovery

        /// <summary>
        /// Grow the hash index
        /// </summary>
        /// <returns></returns>
        bool GrowIndex();

        /// <summary>
        /// Take full checkpoint of FASTER
        /// </summary>
        /// <param name="token">Token describing checkpoint</param>
        /// <param name="targetVersion">upper limit (inclusive) of the version included</param>
        /// <returns>Whether checkpoint was initiated</returns>
        bool TakeFullCheckpoint(out Guid token, long targetVersion = -1);

        /// <summary>
        /// Take checkpoint of FASTER index only (not log)
        /// </summary>
        /// <param name="token">Token describing checkpoin</param>
        /// <returns>Whether checkpoint was initiated</returns>
        bool TakeIndexCheckpoint(out Guid token);

        /// <summary>
        /// Take checkpoint of FASTER log only (not index)
        /// </summary>
        /// <param name="token">Token describing checkpoin</param>
        /// <param name="targetVersion">upper limit (inclusive) of the version included</param>
        /// <returns>Whether checkpoint was initiated</returns>
        bool TakeHybridLogCheckpoint(out Guid token, long targetVersion = -1);

        /// <summary>
        /// Recover from last successfuly checkpoints
        /// </summary>
        void Recover();

        /// <summary>
        /// Recover using full checkpoint token
        /// </summary>
        /// <param name="fullcheckpointToken"></param>
        void Recover(Guid fullcheckpointToken);

        /// <summary>
        /// Recover using a separate index and log checkpoint token
        /// </summary>
        /// <param name="indexToken"></param>
        /// <param name="hybridLogToken"></param>
        void Recover(Guid indexToken, Guid hybridLogToken);

        /// <summary>
        /// Complete ongoing checkpoint (spin-wait)
        /// </summary>
        /// <returns>Whether checkpoint has completed</returns>
        ValueTask CompleteCheckpointAsync(CancellationToken token = default);

        #endregion

        #region Other Operations

        /// <summary>
        /// Get number of (non-zero) hash entries in FASTER
        /// </summary>
        long EntryCount { get; }

        /// <summary>
        /// Get size of index in #cache lines (64 bytes each)
        /// </summary>
        long IndexSize { get; }

        /// <summary>
        /// Get comparer used by this instance of FASTER
        /// </summary>
        IFasterEqualityComparer<Key> Comparer { get; }

        /// <summary>
        /// Dump distribution of #entries in hash table
        /// </summary>
        string DumpDistribution();

        /// <summary>
        /// Get accessor for FASTER hybrid log
        /// </summary>
        LogAccessor<Key, Value, Input, Output, Context, Functions> Log { get; }

        /// <summary>
        /// Get accessor for FASTER read cache
        /// </summary>
        LogAccessor<Key, Value, Input, Output, Context, Functions> ReadCache { get; }

        #endregion
    }
}