﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

using System;

namespace FASTER.core
{
    /// <summary>
    /// The Functions for the TRecordId (which is the Value param to the secondary FasterKV); mostly pass-through
    /// </summary>
    /// <typeparam name="TPSFKey">The type of the <see cref="PSF{TPSFKey, TRecordId}"/> result key</typeparam>
    /// <typeparam name="TRecordId">The type of the <see cref="PSF{TPSFKey, TRecordId}"/> value</typeparam>
    public class PSFFunctions<TPSFKey, TRecordId> : IFunctions<CompositeKey<TPSFKey>, TRecordId, PSFInputSecondary<TPSFKey>,
                                                               PSFOutputSecondary<TPSFKey, TRecordId>, PSFContext>
        where TPSFKey: struct
        where TRecordId: struct
    {
        // TODO: remove stuff that has been moved to PSFOutput.Visit, etc.

        #region Upserts
        public bool ConcurrentWriter(ref CompositeKey<TPSFKey> _, ref TRecordId src, ref TRecordId dst)
        {
            dst = src;
            return true;
        }

        public void SingleWriter(ref CompositeKey<TPSFKey> _, ref TRecordId src, ref TRecordId dst)
            => dst = src;

        public void UpsertCompletionCallback(ref CompositeKey<TPSFKey> _, ref TRecordId value, PSFContext ctx)
        { /* TODO: UpsertCompletionCallback */ }
        #endregion Upserts

        #region Reads
        public void ConcurrentReader(ref CompositeKey<TPSFKey> key, ref PSFInputSecondary<TPSFKey> input, ref TRecordId value, ref PSFOutputSecondary<TPSFKey, TRecordId> dst)
            => throw new PSFInternalErrorException("PSFOutput.Visit instead of ConcurrentReader should be called on PSF-implementing FasterKVs");

        public unsafe void SingleReader(ref CompositeKey<TPSFKey> _, ref PSFInputSecondary<TPSFKey> input, ref TRecordId value, ref PSFOutputSecondary<TPSFKey, TRecordId> dst)
            => throw new PSFInternalErrorException("PSFOutput.Visit instead of SingleReader should be called on PSF-implementing FasterKVs");

        public void ReadCompletionCallback(ref CompositeKey<TPSFKey> _, ref PSFInputSecondary<TPSFKey> input, ref PSFOutputSecondary<TPSFKey, TRecordId> output, PSFContext ctx, Status status)
        { /* TODO: ReadCompletionCallback */ }
        #endregion Reads

        #region RMWs
        public bool NeedCopyUpdate(ref CompositeKey<TPSFKey> _, ref PSFInputSecondary<TPSFKey> input, ref TRecordId value) => true;

        public void CopyUpdater(ref CompositeKey<TPSFKey> _, ref PSFInputSecondary<TPSFKey> input, ref TRecordId oldValue, ref TRecordId newValue)
            => throw new PSFInternalErrorException("RMW should not be done on PSF-implementing FasterKVs");

        public void InitialUpdater(ref CompositeKey<TPSFKey> _, ref PSFInputSecondary<TPSFKey> input, ref TRecordId value)
            => throw new PSFInternalErrorException("RMW should not be done on PSF-implementing FasterKVs");

        public bool InPlaceUpdater(ref CompositeKey<TPSFKey> _, ref PSFInputSecondary<TPSFKey> input, ref TRecordId value)
            => throw new PSFInternalErrorException("RMW should not be done on PSF-implementing FasterKVs");

        public void RMWCompletionCallback(ref CompositeKey<TPSFKey> _, ref PSFInputSecondary<TPSFKey> input, PSFContext ctx, Status status)
            => throw new PSFInternalErrorException("RMW should not be done on PSF-implementing FasterKVs");
        #endregion RMWs

        public void DeleteCompletionCallback(ref CompositeKey<TPSFKey> _, PSFContext ctx)
        { /* TODO: DeleteCompletionCallback */ }

        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
        { /* TODO: CheckpointCompletionCallback */ }
    }
}
