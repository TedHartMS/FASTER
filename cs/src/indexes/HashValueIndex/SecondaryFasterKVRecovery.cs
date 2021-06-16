﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Diagnostics;

namespace FASTER.indexes.HashValueIndex
{
    internal partial class SecondaryFasterKV<TPKey> : FasterKV<TPKey, RecordId>
    {
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
                            this.highWaterRecordId = hlog.GetValue(recordStart);
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