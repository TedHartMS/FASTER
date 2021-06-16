﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace FASTER.indexes.HashValueIndex
{
    internal unsafe partial class SecondaryFasterKV<TPKey> : FasterKV<TPKey, RecordId>
    {
        /// <summary>
        /// Output from Reads on the secondary FasterKV instance (stores Predicate key chains).
        /// </summary>
        internal struct Output
        {
            internal RecordId RecordId;

            internal long PreviousAddress;

            internal bool IsDeleted;

            public override string ToString() => $"rId ({this.RecordId}), prevAddr {this.PreviousAddress}, isDel {this.IsDeleted}";
        }
    }
}