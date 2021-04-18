// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace FASTER.indexes.HashValueIndex
{
    internal partial class SecondaryFasterKV<TPKey> : FasterKV<TPKey, RecordId>
    {
        /// <summary>
        /// Context for operations on the secondary FasterKV instance.
        /// </summary>
        internal class Context
        {
            internal Functions Functions;
        }
    }
}
