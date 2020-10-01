﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

extern alias FasterCore;

using FC = FasterCore::FASTER.core;
using PSF.Index;
using System;
using System.Linq;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Threading;
using System.Runtime.CompilerServices;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FASTER.PSF
{
    // PSF-enabled wrapper FasterKV
    public class PSFFasterKV<TKVKey, TKVValue> : FC.IFasterKV<TKVKey, TKVValue>
    {
        private static readonly ConcurrentDictionary<FC.IFasterKV<TKVKey, TKVValue>, PSFFasterKV<TKVKey, TKVValue>> fkvDictionary 
            = new ConcurrentDictionary<FC.IFasterKV<TKVKey, TKVValue>, PSFFasterKV<TKVKey, TKVValue>>();

        private readonly FC.FasterKV<TKVKey, TKVValue> fkv;
        private readonly PSFManager<FasterKVProviderData<TKVKey, TKVValue>, long> psfManager;

        private PSFFasterKV(FC.FasterKV<TKVKey, TKVValue> fkv)
        {
            this.fkv = fkv;
            this.psfManager = new PSFManager<FasterKVProviderData<TKVKey, TKVValue>, long>();
        }

        /// <summary>
        /// Provides a PSF wrapper for a <see cref="FC.FasterKV{Key, Value}"/> instance.
        /// </summary>
        public static PSFFasterKV<TKVKey, TKVValue> GetOrCreateWrapper(FC.FasterKV<TKVKey, TKVValue> fkv) 
            => fkvDictionary.TryGetValue(fkv, out var psfKV)
                ? psfKV
                : fkvDictionary.GetOrAdd(fkv, new PSFFasterKV<TKVKey, TKVValue>(fkv));

        #region PSF Registration API
        /// <inheritdoc/>
        public IPSF RegisterPSF<TPSFKey>(PSFRegistrationSettings<TPSFKey> registrationSettings,
                                         FasterKVPSFDefinition<TKVKey, TKVValue, TPSFKey> def)
            where TPSFKey : struct
            => this.psfManager.RegisterPSF(registrationSettings, def);

        /// <inheritdoc/>
        public IPSF[] RegisterPSF<TPSFKey>(PSFRegistrationSettings<TPSFKey> registrationSettings,
                                           params FasterKVPSFDefinition<TKVKey, TKVValue, TPSFKey>[] defs)
            where TPSFKey : struct
            => this.psfManager.RegisterPSF(registrationSettings, defs);

        /// <inheritdoc/>
        public IPSF RegisterPSF<TPSFKey>(PSFRegistrationSettings<TPSFKey> registrationSettings,
                                         string psfName, Func<TKVKey, TKVValue, TPSFKey?> psfFunc)
            where TPSFKey : struct
            => this.psfManager.RegisterPSF(registrationSettings, new FasterKVPSFDefinition<TKVKey, TKVValue, TPSFKey>(psfName, psfFunc));

        /// <inheritdoc/>
        public IPSF[] RegisterPSF<TPSFKey>(PSFRegistrationSettings<TPSFKey> registrationSettings,
                                           params (string, Func<TKVKey, TKVValue, TPSFKey?>)[] psfFuncs)
            where TPSFKey : struct
            => this.psfManager.RegisterPSF(registrationSettings, psfFuncs.Select(e => new FasterKVPSFDefinition<TKVKey, TKVValue, TPSFKey>(e.Item1, e.Item2)).ToArray());
        #endregion PSF Registration API

        #region IFasterKV implementations

        #region New Session Operations

        public FC.ClientSession<TKVKey, TKVValue, TInput, TOutput, Context, Functions> NewSession<TInput, TOutput, Context, Functions>(Functions functions, string sessionId = null, 
                                bool threadAffinitized = false, FC.IVariableLengthStruct<TKVValue, TInput> variableLengthStruct = null)
            where Functions : FC.IFunctions<TKVKey, TKVValue, TInput, TOutput, Context>
            => throw new PSFInvalidOperationException("Must use NewPSFSession");

        public FC.ClientSession<TKVKey, TKVValue, TInput, TOutput, Context, Functions> ResumeSession<TInput, TOutput, Context, Functions>(Functions functions, string sessionId, 
                                out FC.CommitPoint commitPoint, bool threadAffinitized = false, FC.IVariableLengthStruct<TKVValue, TInput> variableLengthStruct = null)
            where Functions : FC.IFunctions<TKVKey, TKVValue, TInput, TOutput, Context>
            => throw new PSFInvalidOperationException("Must use ResumePSFSession");

        public PSFClientSession<TKVKey, TKVValue, TInput, TOutput, Context, Functions> NewPSFSession<TInput, TOutput, Context, Functions>(Functions functions, string sessionId = null, 
                                bool threadAffinitized = false, FC.IVariableLengthStruct<TKVValue, TInput> variableLengthStruct = null)
            where Functions : FC.IFunctions<TKVKey, TKVValue, TInput, TOutput, Context>
        {
            var wrapperFunctions = new WrapperFunctions<TKVKey, TKVValue, TInput, TOutput, Context>(functions, this.fkv.Log, this.fkv.RecordAccessor, this.psfManager);
            var session = this.fkv.NewSession(wrapperFunctions, sessionId, threadAffinitized, variableLengthStruct);
            var livenessFunctions = new LivenessFunctions<TKVKey, TKVValue>();
            var livenessSession = this.fkv.NewSession(livenessFunctions);
            return new PSFClientSession<TKVKey, TKVValue, TInput, TOutput, Context, Functions>(this.Log, wrapperFunctions, session, livenessFunctions, livenessSession, this.psfManager);
        }

        public PSFClientSession<TKVKey, TKVValue, TInput, TOutput, Context, Functions> ResumePSFSession<TInput, TOutput, Context, Functions>(Functions functions, string sessionId, 
                                out FC.CommitPoint commitPoint, bool threadAffinitized = false, FC.IVariableLengthStruct<TKVValue, TInput> variableLengthStruct = null)
            where Functions : FC.IFunctions<TKVKey, TKVValue, TInput, TOutput, Context>
        {
            var wrapperFunctions = new WrapperFunctions<TKVKey, TKVValue, TInput, TOutput, Context>(functions, this.fkv.Log, this.fkv.RecordAccessor, this.psfManager);
            var session = this.fkv.ResumeSession(wrapperFunctions, sessionId, out commitPoint, threadAffinitized, variableLengthStruct);
            var livenessFunctions = new LivenessFunctions<TKVKey, TKVValue>();
            var livenessSession = this.fkv.NewSession(livenessFunctions);
            return new PSFClientSession<TKVKey, TKVValue, TInput, TOutput, Context, Functions>(this.Log, wrapperFunctions, session, livenessFunctions, livenessSession, this.psfManager);
        }

        #endregion New Session Operations

        #region Growth and Recovery

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool GrowIndex()
            => this.fkv.GrowIndex() && this.psfManager.GrowIndex();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TakeFullCheckpoint(out Guid token)
            // Do not return the PSF token here. TODO: Handle failure of PSFManager.TakeFullCheckpoint
            => this.fkv.TakeFullCheckpoint(out token) && this.psfManager.TakeFullCheckpoint();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TakeFullCheckpoint(out Guid token, FC.CheckpointType checkpointType)
            // Do not return the PSF token here. TODO: Handle failure of PSFManager.TakeFullCheckpoint
            => this.fkv.TakeFullCheckpoint(out token, checkpointType) && this.psfManager.TakeFullCheckpoint(checkpointType);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask<(bool success, Guid token)> TakeFullCheckpointAsync(FC.CheckpointType checkpointType, CancellationToken cancellationToken = default)
        {
            var (success, token) = await this.fkv.TakeFullCheckpointAsync(checkpointType, cancellationToken);
            // Do not return the PSF token here. TODO: Handle failure of PSFManager.TakeFullCheckpoint
            return (success && await this.psfManager.TakeFullCheckpointAsync(checkpointType, cancellationToken), token);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TakeIndexCheckpoint(out Guid token)
            // Do not return the PSF token here. TODO: Handle failure of PSFManager.TakeIndexCheckpoint
            => this.fkv.TakeIndexCheckpoint(out token) && this.psfManager.TakeIndexCheckpoint();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask<(bool success, Guid token)> TakeIndexCheckpointAsync(CancellationToken cancellationToken = default)
        {
            var (success, token) = await this.fkv.TakeIndexCheckpointAsync(cancellationToken);
            // Do not return the PSF token here. TODO: Handle failure of PSFManager.TakeIndexCheckpoint
            return (success && await this.psfManager.TakeIndexCheckpointAsync(cancellationToken), token);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TakeHybridLogCheckpoint(out Guid token)
            // Do not return the PSF token here. TODO: Handle failure of PSFManager.TakeHybridLogCheckpoint
            => this.fkv.TakeHybridLogCheckpoint(out token) && this.psfManager.TakeHybridLogCheckpoint();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TakeHybridLogCheckpoint(out Guid token, FC.CheckpointType checkpointType)
            // Do not return the PSF token here. TODO: Handle failure of PSFManager.TakeHybridLogCheckpoint
            => this.fkv.TakeHybridLogCheckpoint(out token, checkpointType) && this.psfManager.TakeHybridLogCheckpoint(checkpointType);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public async ValueTask<(bool success, Guid token)> TakeHybridLogCheckpointAsync(FC.CheckpointType checkpointType, CancellationToken cancellationToken = default)
        {
            var (success, token) = await this.fkv.TakeHybridLogCheckpointAsync(checkpointType, cancellationToken);
            // Do not return the PSF token here. TODO: Handle failure of PSFManager.TakeHybridLogCheckpoint
            return (success && await this.psfManager.TakeHybridLogCheckpointAsync(checkpointType, cancellationToken), token);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Recover()
        {
            // TODO: RecoverAsync with separate Tasks for primary fkv and each psfGroup
            this.fkv.Recover();
            this.psfManager.Recover();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Recover(Guid fullcheckpointToken)
        {
            this.fkv.Recover(fullcheckpointToken);
            this.psfManager.Recover(fullcheckpointToken);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Recover(Guid indexToken, Guid hybridLogToken)
        {
            this.fkv.Recover(indexToken, hybridLogToken);
            this.psfManager.Recover(indexToken, hybridLogToken);
        }

        public async ValueTask CompleteCheckpointAsync(CancellationToken token = default)
        {
            // Simple sequence to avoid allocating Tasks as there is no Task.WhenAll for ValueTask
            var vt1 = this.fkv.CompleteCheckpointAsync(token);
            var vt2 = this.psfManager.CompleteCheckpointAsync(token);
            await vt1;
            await vt2;
        }

        #endregion Growth and Recovery

        #region Other Operations

        public long EntryCount => this.fkv.EntryCount;

        public long IndexSize => this.fkv.IndexSize;

        public FC.IFasterEqualityComparer<TKVKey> Comparer => this.fkv.Comparer;

        public string DumpDistribution() => this.fkv.DumpDistribution();

        public FC.LogAccessor<TKVKey, TKVValue> Log => this.fkv.Log;

        public FC.LogAccessor<TKVKey, TKVValue> ReadCache => this.fkv.ReadCache;

        #endregion Other Operations

        public void Dispose() => this.fkv.Dispose();

        #endregion IFasterKV implementations
    }
}
