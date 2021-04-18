// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace FASTER.indexes.HashValueIndex
{
    internal unsafe partial class SecondaryFasterKV<TPKey> : FasterKV<TPKey, RecordId>
    {
        internal class Functions : IAdvancedFunctions<TPKey, RecordId, Input, Output, Context>, IInputAccessor<Input>
        {
            readonly SecondaryFasterKV<TPKey> fkv;
            readonly KeyAccessor<TPKey> keyAccessor;

            internal Functions(SecondaryFasterKV<TPKey> fkv, KeyAccessor<TPKey> keyAcc)
            {
                this.fkv = fkv;
                this.keyAccessor = keyAcc;
            }

            #region IInputAccessor

            public bool IsDelete(ref Input input) => input.IsDelete;    // TODO needed?
            public bool SetDelete(ref Input input, bool value) => input.IsDelete = value;

            #endregion IInputAccessor

            #region IFunctions implementation

            #region Reads
            public void ConcurrentReader(ref TPKey queryKeyPointerRefAsKeyRef, ref Input input, ref RecordId value, ref Output output, ref RecordInfo recordInfo, long logicalAddress)
            {
                // Note: ConcurrentReader is not called for ReadCache, even if we eventually support ReadCache in SubsetIndex secondary KVs.
                Debug.Assert(logicalAddress > FASTER.core.Constants.kTempInvalidAddress);
                CopyInMemoryDataToOutput(ref queryKeyPointerRefAsKeyRef, ref input, ref value, ref output, logicalAddress);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void CopyInMemoryDataToOutput(ref TPKey queryKeyPointerRefAsKeyRef, ref Input input, ref RecordId value, ref Output output, long logicalAddress)
            {
                ref KeyPointer<TPKey> storedKeyPointer = ref this.keyAccessor.GetKeyPointerRefFromKeyPointerLogicalAddress(logicalAddress);
                Debug.Assert(input.PredicateOrdinal == storedKeyPointer.PredicateOrdinal, "Mismatched input and stored Predicate ordinal");

                output.RecordId = value;
                output.PreviousAddress = storedKeyPointer.PreviousAddress;
                
                output.IsDeleted = storedKeyPointer.IsDeleted;

#if DEBUG
                ref KeyPointer<TPKey> queryKeyPointer = ref KeyPointer<TPKey>.CastFromKeyRef(ref queryKeyPointerRefAsKeyRef);
                Debug.Assert(input.PredicateOrdinal == queryKeyPointer.PredicateOrdinal, "Mismatched input and query Predicate ordinal");
#endif
            }

            public unsafe void SingleReader(ref TPKey queryKeyPointerRefAsKeyRef, ref Input input, ref RecordId value, ref Output output, long logicalAddress)
            {
                if (logicalAddress <= FASTER.core.Constants.kTempInvalidAddress)
                {
                    // This is a ReadCache record. Note if we do support ReadCache in SubsetIndex secondary KVs: ReadCompletionCallback won't be called, so no need to flag it in Output.
                    Debug.Fail("Invalid logicalAddress");
                    return;
                }

                if (logicalAddress >= this.fkv.hlog.HeadAddress)
                {
                    CopyInMemoryDataToOutput(ref queryKeyPointerRefAsKeyRef, ref input, ref value, ref output, logicalAddress);
                    return;
                }

                // This record is not in memory, which means we're being called from InternalCompletePendingRead. We can't dereference logicalAddress,
                // but KeyAccessor can help us navigate to the query key.
                long recordPhysicalAddress = this.keyAccessor.GetRecordAddressFromValueRef(ref value);

                ref KeyPointer<TPKey> queryKeyPointer = ref KeyPointer<TPKey>.CastFromKeyRef(ref queryKeyPointerRefAsKeyRef);
                ref KeyPointer<TPKey> storedKeyPointer = ref this.keyAccessor.GetKeyPointerRefFromRecordPhysicalAddress(recordPhysicalAddress, queryKeyPointer.PredicateOrdinal);

                output.RecordId = value;
                output.PreviousAddress = storedKeyPointer.PreviousAddress;
                output.IsDeleted = storedKeyPointer.IsDeleted;

                Debug.Assert(input.PredicateOrdinal == queryKeyPointer.PredicateOrdinal, "Mismatched input and query Predicate ordinal");
                Debug.Assert(input.PredicateOrdinal == storedKeyPointer.PredicateOrdinal, "Mismatched input and stored Predicate ordinal");
            }

            public void ReadCompletionCallback(ref TPKey _, ref Input input, ref Output output, Context ctx, Status status, RecordInfo recordInfo) { }
            #endregion Reads

#if DEBUG
            private const string NotUsedForHashValueIndex = "HashValueIndex SecondaryFasterKVs should not use this IFunctions method";

            #region Upserts
            public bool ConcurrentWriter(ref TPKey _, ref RecordId src, ref RecordId dst, ref RecordInfo recordInfo, long logicalAddress) => throw new HashValueIndexInternalErrorException(NotUsedForHashValueIndex);

            public void SingleWriter(ref TPKey _, ref RecordId src, ref RecordId dst) => throw new HashValueIndexInternalErrorException(NotUsedForHashValueIndex);

            public void UpsertCompletionCallback(ref TPKey _, ref RecordId value, Context ctx) => throw new HashValueIndexInternalErrorException(NotUsedForHashValueIndex);
            #endregion Upserts

            #region RMWs
            public bool NeedCopyUpdate(ref TPKey _, ref Input input, ref RecordId value)
                => throw new HashValueIndexInternalErrorException(NotUsedForHashValueIndex);

            public void CopyUpdater(ref TPKey _, ref Input input, ref RecordId oldValue, ref RecordId newValue)
                => throw new HashValueIndexInternalErrorException(NotUsedForHashValueIndex);

            public void InitialUpdater(ref TPKey _, ref Input input, ref RecordId value)
                => throw new HashValueIndexInternalErrorException(NotUsedForHashValueIndex);

            public bool InPlaceUpdater(ref TPKey _, ref Input input, ref RecordId value, ref RecordInfo recordInfo, long logicalAddress)
                => throw new HashValueIndexInternalErrorException(NotUsedForHashValueIndex);

            public void RMWCompletionCallback(ref TPKey _, ref Input input, Context ctx, Status status)
                => throw new HashValueIndexInternalErrorException(NotUsedForHashValueIndex);
            #endregion RMWs

            public void DeleteCompletionCallback(ref TPKey _, Context ctx)
                => throw new HashValueIndexInternalErrorException(NotUsedForHashValueIndex);

            public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) { }

            public void ConcurrentDeleter(ref TPKey key, ref RecordId value, ref RecordInfo recordInfo, long address) { }

            public bool SupportsLocking => false;

            public void Lock(ref RecordInfo recordInfo, ref TPKey key, ref RecordId value, LockType lockType, ref long lockContext) { }

            public bool Unlock(ref RecordInfo recordInfo, ref TPKey key, ref RecordId value, LockType lockType, long lockContext) => true;
#endif
            #endregion IFunctions implementation
        }
    }
}
