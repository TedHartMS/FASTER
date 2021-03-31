// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable IDE0060 // Remove unused parameter

using FASTER.core;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace FASTER.indexes.HashValueIndex
{
    internal unsafe partial class FasterKVHVI<TPKey> : FasterKV<TPKey, long>
    {
        internal class Functions : IAdvancedFunctions<TPKey, long, Input, Output, Context>, IInputAccessor<Input>
        {
            FasterKVHVI<TPKey> fkv;
            readonly KeyAccessor<TPKey> keyAccessor;

            internal Functions(FasterKVHVI<TPKey> fkv, KeyAccessor<TPKey> keyAcc)
            {
                this.fkv = fkv;
                this.keyAccessor = keyAcc;
            }

            #region IInputAccessor

            public bool IsDelete(ref Input input) => input.IsDelete;
            public bool SetDelete(ref Input input, bool value) => input.IsDelete = value;

            #endregion IInputAccessor

            #region IFunctions implementation

            private const string NotUsedForHVI = "HashValueIndex-implementing FasterKVs should not use this IFunctions method";

            #region Upserts
            public bool ConcurrentWriter(ref TPKey _, ref long src, ref long dst, ref RecordInfo recordInfo, long logicalAddress) => throw new InternalErrorExceptionHVI(NotUsedForHVI);

            public void SingleWriter(ref TPKey _, ref long src, ref long dst) => throw new InternalErrorExceptionHVI(NotUsedForHVI);

            public void UpsertCompletionCallback(ref TPKey _, ref long value, Context ctx) => throw new InternalErrorExceptionHVI(NotUsedForHVI);
            #endregion Upserts

            #region Reads
            public void ConcurrentReader(ref TPKey queryKeyPointerRefAsKeyRef, ref Input input, ref long value, ref Output output, ref RecordInfo recordInfo, long logicalAddress)
            {
                // Note: ConcurrentReader is not called for ReadCache, even if we eventually support ReadCache in SubsetIndex secondary KVs.
                Debug.Assert(logicalAddress > FASTER.core.Constants.kTempInvalidAddress);
                CopyInMemoryDataToOutput(ref queryKeyPointerRefAsKeyRef, ref input, ref value, ref output, logicalAddress);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void CopyInMemoryDataToOutput(ref TPKey queryKeyPointerRefAsKeyRef, ref Input input, ref long value, ref Output output, long logicalAddress)
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

            public unsafe void SingleReader(ref TPKey queryKeyPointerRefAsKeyRef, ref Input input, ref long value, ref Output output, long logicalAddress)
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

            public void ReadCompletionCallback(ref TPKey _, ref Input input, ref Output output, Context ctx, Status status, RecordInfo recordInfo)
            {
                output.PendingResultStatus = status;
            }
            #endregion Reads

            #region RMWs
            public bool NeedCopyUpdate(ref TPKey _, ref Input input, ref long value)
                => throw new InternalErrorExceptionHVI(NotUsedForHVI);

            public void CopyUpdater(ref TPKey _, ref Input input, ref long oldValue, ref long newValue)
                => throw new InternalErrorExceptionHVI(NotUsedForHVI);

            public void InitialUpdater(ref TPKey _, ref Input input, ref long value)
                => throw new InternalErrorExceptionHVI(NotUsedForHVI);

            public bool InPlaceUpdater(ref TPKey _, ref Input input, ref long value, ref RecordInfo recordInfo, long logicalAddress)
                => throw new InternalErrorExceptionHVI(NotUsedForHVI);

            public void RMWCompletionCallback(ref TPKey _, ref Input input, Context ctx, Status status)
                => throw new InternalErrorExceptionHVI(NotUsedForHVI);
#endregion RMWs

            public void DeleteCompletionCallback(ref TPKey _, Context ctx)
                => throw new InternalErrorExceptionHVI(NotUsedForHVI);

            public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) { }

            public void ConcurrentDeleter(ref TPKey key, ref long value, ref RecordInfo recordInfo, long address) { }

            public bool SupportsLocking => throw new System.NotImplementedException();

            public void Lock(ref RecordInfo recordInfo, ref TPKey key, ref long value, LockType lockType, ref long lockContext) { }

            public bool Unlock(ref RecordInfo recordInfo, ref TPKey key, ref long value, LockType lockType, long lockContext) => true;
            #endregion IFunctions implementation
        }
    }
}
