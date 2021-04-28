// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.indexes.HashValueIndex
{
    internal static class Constants
    {
        public const int kInvalidPredicateOrdinal = 255; // 0-based ordinals; this is also the max count

        // For query, get all records (not segmented)
        public const int kGetAllRecords = -1;
    }
}
