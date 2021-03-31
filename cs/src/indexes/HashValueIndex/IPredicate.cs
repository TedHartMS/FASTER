// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.indexes.HashValueIndex
{
    /// <summary>
    /// A base interface for <see cref="Predicate{TKVValue, TPKey}"/> to decouple the generic type parameters.
    /// </summary>
    public interface IPredicate
    {
        /// <summary>
        /// The name of the <see cref="Predicate{TKVValue, TPKey}"/>; must be unique in the <see cref="HashValueIndex"/>.
        /// </summary>
        string Name { get; }
    }
}
