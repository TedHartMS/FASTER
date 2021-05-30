// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Diagnostics;

namespace FASTER.indexes.HashValueIndex
{
    internal partial class SecondaryFasterKV<TPKey> : FasterKV<TPKey, RecordId>
    {
        private protected override OperationStatus RetryOperationStatus<TInput, TOutput, TContext, FasterSession>(FasterExecutionContext<TInput, TOutput, TContext> currentCtx,
                                                                        ref PendingContext<TInput, TOutput, TContext> pendingContext, FasterSession fasterSession)
        {
            // TODO: TEst RetryOperationStatus
            OperationStatus internalStatus;
            switch (pendingContext.type)
            {
                case OperationType.READ:
                    internalStatus = this.IndexInternalRead(ref pendingContext.key.Get(),
                                         ref pendingContext.input.Get(),
                                         ref pendingContext.output,
                                         pendingContext.recordInfo.PreviousAddress,
                                         pendingContext.userContext,
                                         ref pendingContext, fasterSession, currentCtx, pendingContext.serialNum);
                    break;
                case OperationType.UPSERT:
                    internalStatus = this.IndexInternalInsert(ref pendingContext.key.Get(),
                                         pendingContext.value.Get(),
                                         ref pendingContext.input.Get(),
                                         pendingContext.userContext,
                                         ref pendingContext, fasterSession, currentCtx, pendingContext.serialNum);
                    // If this assert fires, we'll have to virtualize the retry and callback switches in InternalCompleteRetryRequest.
                    Debug.Assert(internalStatus != OperationStatus.RETRY_LATER, "Insertion should not go pending");
                    break;
                default:
                    throw new HashValueIndexInternalErrorException($"Should not be retrying operation {pendingContext.type}");
            };

            return internalStatus;
        }

        private protected override unsafe bool RecoverFromPage(long startRecoveryAddress,
                                     long fromLogicalAddressInPage,
                                     long untilLogicalAddressInPage,
                                     long pageLogicalAddress,
                                     long pagePhysicalAddress,
                                     int nextVersion, bool undoNextVersion)
        {
            bool touched = false;

            var pointer = fromLogicalAddressInPage;
            while (pointer < untilLogicalAddressInPage)
            {
                var recordStart = pagePhysicalAddress + pointer;
                ref RecordInfo info = ref hlog.GetInfo(recordStart);

                if (info.IsNull())
                {
                    pointer += RecordInfo.GetLength();
                    continue;
                }

                if (!info.Invalid)
                {
                    ref CompositeKey<TPKey> storedCompositeKey = ref CompositeKey<TPKey>.CastFromFirstKeyPointerRefAsKeyRef(ref hlog.GetKey(recordStart));
                    long storedKeyLogicalAddress = pageLogicalAddress + pointer + RecordInfo.GetLength();
                    var predCount = this.KeyAccessor.KeyCount;
                    var isGoodVersion = info.Version != RecordInfo.GetShortVersion(nextVersion) || !undoNextVersion;
                    if (!isGoodVersion)
                    {
                        touched = true;
                        info.Invalid = true;
                    }

                    for (var predOrdinal = 0; predOrdinal < predCount; ++predOrdinal, storedKeyLogicalAddress += this.KeyAccessor.KeyPointerSize)
                    {
                        var hash = comparer.GetHashCode64(ref hlog.GetKey(recordStart));
                        var tag = (ushort)((ulong)hash >> core.Constants.kHashTagShift);

                        var bucket = default(HashBucket*);
                        var slot = default(int);
                        var entry = default(HashBucketEntry);
                        FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry, hlog.BeginAddress);

                        entry.Tag = tag;
                        entry.Pending = false;
                        entry.Tentative = false;
                        if (isGoodVersion)
                        {
                            // This record's version is good, so link the predicate into its chains.
                            entry.Address = storedKeyLogicalAddress;
                            bucket->bucket_entries[slot] = entry.word;
                        }
                        else
                        {
                            // This record has a future version and we're undoing those, so bypass this record in the predicate's chain.
                            ref KeyPointer<TPKey> storedKeyPointer = ref KeyAccessor.GetKeyPointerRef(ref storedCompositeKey, predOrdinal);
                            if (storedKeyPointer.PreviousAddress < startRecoveryAddress)
                            {
                                entry.Address = storedKeyPointer.PreviousAddress;
                                bucket->bucket_entries[slot] = entry.word;
                            }
                        }
                        storedKeyLogicalAddress += this.KeyAccessor.KeyPointerSize;
                    }
                }
                pointer += hlog.GetRecordSize(recordStart).Item2;
            }

            return touched;
        }
    }
}
