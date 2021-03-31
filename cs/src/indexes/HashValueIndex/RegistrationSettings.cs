// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace FASTER.indexes.HashValueIndex
{
    /// <summary>
    /// Options for Predicate registration.
    /// </summary>
    public class RegistrationSettings<TPKey>
    {
        /// <summary>
        /// The return value from a predicate that indicates the predicate did not match the value.
        /// </summary>
        public TPKey NullIndicator = default;

        /// <summary>
        /// When registring new Indexes over an existing store, this is the logicalAddress in the primary
        /// FasterKV at which indexing will be started. TODO: LogicalAddress is FasterKV-specific; revisit when indexing existing records.
        /// </summary>
        public long IndexFromAddress = core.Constants.kInvalidAddress;

        /// <summary>
        /// The hash table size to be used in the Index-implementing secondary FasterKV instances.
        /// </summary>
        public long HashTableSize = 0;

        /// <summary>
        /// The log settings to be used in the Index-implementing secondary FasterKV instances.
        /// </summary>
        public LogSettings LogSettings;

        /// <summary>
        /// The log settings to be used in the Index-implementing secondary FasterKV instances.
        /// </summary>
        public CheckpointSettings CheckpointSettings;

        /// <summary>
        /// Optional key comparer; if null, <typeparamref name="TPKey"/> should implement
        ///     <see cref="IFasterEqualityComparer{TPKey}"/>; otherwise a slower EqualityComparer will be used.
        /// </summary>
        public IFasterEqualityComparer<TPKey> KeyComparer;
    }
}
