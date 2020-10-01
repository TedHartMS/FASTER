﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

extern alias FasterCore;

using FC = FasterCore::FASTER.core;
using PSF.Index;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System;

namespace FASTER.PSF
{
    internal class WrapperFunctions<TKVKey, TKVValue, Input, Output, Context> : FC.IFunctions<TKVKey, TKVValue, Input, Output, Context>
    {
        // Permanent
        private readonly FC.IFunctions<TKVKey, TKVValue, Input, Output, Context> userFunctions;
        private readonly FC.LogAccessor<TKVKey, TKVValue> logAccessor;
        private readonly FC.RecordAccessor<TKVKey, TKVValue> recordAccessor;
        private readonly PSFManager<FasterKVProviderData<TKVKey, TKVValue>, long> psfManager;

        // Ephemeral
        internal PSFChangeTracker<FasterKVProviderData<TKVKey, TKVValue>, long> ChangeTracker;
        internal long LogicalAddress;

        internal WrapperFunctions(FC.IFunctions<TKVKey, TKVValue, Input, Output, Context> userFunctions,
                                  FC.LogAccessor<TKVKey, TKVValue> logAccessor,
                                  FC.RecordAccessor<TKVKey, TKVValue> recAcc,
                                  PSFManager<FasterKVProviderData<TKVKey, TKVValue>, long> psfMgr)
        {
            this.userFunctions = userFunctions;
            this.logAccessor = logAccessor;
            this.recordAccessor = recAcc;
            this.psfManager = psfMgr;
        }

        internal void Clear()
        {
            this.ChangeTracker = null;
            this.LogicalAddress = FC.Constants.kInvalidAddress;
        }

        internal bool IsSet => !(this.ChangeTracker is null) || this.LogicalAddress != FC.Constants.kInvalidAddress;

        #region IFunctions implementations
        public void ConcurrentReader(ref TKVKey key, ref Input input, ref TKVValue value, ref Output output, long logicalAddress)
            => this.userFunctions.ConcurrentReader(ref key, ref input, ref value, ref output, logicalAddress);
        public void SingleReader(ref TKVKey key, ref Input input, ref TKVValue value, ref Output output, long logicalAddresss)
            => this.userFunctions.SingleReader(ref key, ref input, ref value, ref output, logicalAddresss);

        public void SingleWriter(ref TKVKey key, ref TKVValue oldValue, ref TKVValue newValue, long logicalAddress)
        {
            var isDelete = this.recordAccessor.IsTombstone(logicalAddress);
            this.userFunctions.SingleWriter(ref key, ref oldValue, ref newValue, logicalAddress);

            // This is called in the following cases:
            // When we do not have a previous record:
            //  - Upsert did not find the key so this is a pure insert, which goes through a fast path that does not allocate a ChangeTracker.
            //  - Upsert or Delete found a key that is on-disk; neither will fetch a record from disk to verify a key match. Instead,
            //    FasterKV writes a new record at the tail, and for Upsert, the liveness check at query time avoids duplicates at prior addresses.
            //  - This is a copy to the readcache, which should not be considered in indexing, and does not affect the validity of the RecordId.
            // Otherwise, this is an RCU, so is one of:
            //  - Upsert or Delete found the key in the immutable region. We do not have the old record address here, so this requires a liveness
            //    check at query time to avoid reporting duplicates at the old record address.
            //  - A read from disk is copying the record to the tail of the log. This is similar to RCU; we don't have the old record address, and the
            //    query-time liveness check will see the Key resolved at a higher address and short-circuit before reporting duplicates at the
            //    old record address.

            if (isDelete)
            {
                this.ChangeTracker = this.psfManager.CreateChangeTracker();
                SetBeforeData(ref key, logicalAddress, isIpu: false);
                this.ChangeTracker.UpdateOp = UpdateOperation.Delete;
                return;
            }

            // Because we do not have the old logicalAddress, we cannot RCU; instead we must simply insert a new record, using the fast path.
            //if (oldAddress != FC.Constants.kInvalidAddress) SetRCU(ref key, oldAddress, newAddress);
            this.LogicalAddress = logicalAddress;
        }

        public bool ConcurrentWriter(ref TKVKey key, ref TKVValue src, ref TKVValue dst, long logicalAddress = -1)
        {
            // Save the PreUpdate values.
            this.ChangeTracker = this.psfManager.CreateChangeTracker();
            var isDelete = this.recordAccessor.IsTombstone(logicalAddress);
            SetBeforeData(ref key, logicalAddress, isIpu: !isDelete);
            if (this.userFunctions.ConcurrentWriter(ref key, ref src, ref dst, logicalAddress))
            {
                this.ChangeTracker.UpdateOp = isDelete ? UpdateOperation.Delete : UpdateOperation.IPU;
                if (!isDelete)
                    SetAfterData(ref key, logicalAddress);
                return true;
            }
            return false;
        }

        public void InitialUpdater(ref TKVKey key, ref Input input, ref TKVValue value, long logicalAddress)
        {
            this.userFunctions.InitialUpdater(ref key, ref input, ref value, logicalAddress);

            // Key not found or the record was tombstoned, so this is an Insert only. This cannot go through the fast path because it does
            // not have a value passed in (instead, it has the input).
            this.ChangeTracker = this.psfManager.CreateChangeTracker();
            SetBeforeData(ref key, logicalAddress, isIpu: false);
            this.ChangeTracker.UpdateOp = UpdateOperation.Insert;
        }
        public void CopyUpdater(ref TKVKey key, ref Input input, ref TKVValue oldValue, ref TKVValue newValue, long oldLogicalAddress, long newLogicalAddress)
        {
            // The old record was valid but not in mutable range, so this is an RCU
            this.userFunctions.CopyUpdater(ref key, ref input, ref oldValue, ref newValue, oldLogicalAddress, newLogicalAddress);
            SetRCU(ref key, oldLogicalAddress, newLogicalAddress);
        }
        public bool InPlaceUpdater(ref TKVKey key, ref Input input, ref TKVValue value, long logicalAddress)
        {
            // Get the PreUpdate values (or the secondary FKV position in the IPUCache).
            this.ChangeTracker = this.psfManager.CreateChangeTracker();
            SetBeforeData(ref key, logicalAddress, isIpu: true);
            if (this.userFunctions.InPlaceUpdater(ref key, ref input, ref value, logicalAddress))
            {
                SetAfterData(ref key, logicalAddress);
                this.ChangeTracker.UpdateOp = UpdateOperation.IPU;
                return true;
            }
            return false;
        }

        public void ReadCompletionCallback(ref TKVKey key, ref Input input, ref Output output, Context ctx, FC.Status status, long previousAddress)
            => this.userFunctions.ReadCompletionCallback(ref key, ref input, ref output, ctx, status, previousAddress);
        public void RMWCompletionCallback(ref TKVKey key, ref Input input, Context ctx, FC.Status status)
            => this.userFunctions.RMWCompletionCallback(ref key, ref input, ctx, status);
        public void UpsertCompletionCallback(ref TKVKey key, ref TKVValue value, Context ctx)
            => this.userFunctions.UpsertCompletionCallback(ref key, ref value, ctx);
        public void DeleteCompletionCallback(ref TKVKey key, Context ctx)
            => this.userFunctions.DeleteCompletionCallback(ref key, ctx);
        public void CheckpointCompletionCallback(string sessionId, FC.CommitPoint commitPoint)
            => CheckpointCompletionCallback(sessionId, commitPoint);

        #endregion IFunctions implementations

        #region Utilities
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private FasterKVProviderData<TKVKey, TKVValue> CreateProviderData(ref TKVKey key, long logicalAddress) 
            => new FasterKVProviderData<TKVKey, TKVValue>(this.logAccessor.GetKeyContainer(ref key),
                                                          this.logAccessor.GetValueContainer(ref this.recordAccessor.GetValue(logicalAddress)));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe static void GetAfterRecordId(PSFChangeTracker<FasterKVProviderData<TKVKey, TKVValue>, long> changeTracker, ref TKVValue value)   // TODO unused but will be needed; may need to be moved
        {
            // This indirection is needed because this is the primary FasterKV.
            Debug.Assert(typeof(TKVValue) == typeof(long));
            var recordId = changeTracker.AfterRecordId;
            Buffer.MemoryCopy(Unsafe.AsPointer(ref recordId), Unsafe.AsPointer(ref value), sizeof(long), sizeof(long));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SetBeforeData(ref TKVKey key, long logicalAddress, bool isIpu)
            // If the value has objects, then an in-place RMW to the data in that object will also affect BeforeData, so we must get the PSFs now. // TODOperf this is in session lock
            // TODOdoc: If you Read an object value and modify that fetched "ref value" directly, you will break PSFs (the before data is overwritten before we have
            // a chance to see it and create the keys). An Upsert must use a separate value.
            => this.psfManager.SetBeforeData(this.ChangeTracker, CreateProviderData(ref key, logicalAddress), logicalAddress, isIpu && this.recordAccessor.ValueHasObjects());

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SetAfterData(ref TKVKey key, long logicalAddress)
            => this.psfManager.SetAfterData(this.ChangeTracker, CreateProviderData(ref key, logicalAddress), logicalAddress);

        private void SetRCU(ref TKVKey key, long oldLogicalAddress, long newLogicalAddress)
        {
            this.ChangeTracker = this.psfManager.CreateChangeTracker();
            SetBeforeData(ref key, oldLogicalAddress, isIpu: false);
            SetAfterData(ref key, newLogicalAddress);
            this.ChangeTracker.UpdateOp = UpdateOperation.RCU;
        }
        #endregion Utilities
    }
}
