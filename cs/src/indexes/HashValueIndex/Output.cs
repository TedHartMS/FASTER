﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace FASTER.indexes.HashValueIndex
{
    internal unsafe partial class FasterKVHVI<TPKey> : FasterKV<TPKey, long>
    {
        /// <summary>
        /// Output from Reads on the secondary FasterKV instance (stores Predicate key chains).
        /// </summary>
        internal struct Output
        {
            internal long RecordId;

            internal long PreviousAddress;

            internal bool IsDeleted;

            // Used only for ReadCompletionCallback.
            internal Status PendingResultStatus;

            public override string ToString() => $"rId {this.RecordId}, prevAddr {this.PreviousAddress}";
        }
    }
}
