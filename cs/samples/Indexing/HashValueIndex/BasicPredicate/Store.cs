// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.indexes.HashValueIndex;
using HashValueIndexSampleCommon;

namespace BasicPredicateSample
{
    internal class Store : StoreBase
    {
        internal IPredicate PetPred;

        internal HashValueIndex<Key, Value, int> index;

        internal Store() : base(1, nameof(BasicPredicateSample))
        {
            this.index = new HashValueIndex<Key, Value, int>(base.AppName, base.FasterKV, 
                                                           CreateRegistrationSettings(0, new IntKeyComparer()), nameof(this.PetPred), v => v.SpeciesInt);
            this.AddIndex(this.index);
            this.PetPred = this.index.GetPredicate(nameof(this.PetPred));
        }
    }
}
