// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using FASTER.indexes.HashValueIndex;
using HashValueIndexSampleCommon;
using System;
using System.Threading.Tasks;

namespace MultiPredicateSample
{
    internal class Store : StoreBase
    {
        internal IPredicate CombinedPetPred, CombinedAgePred;

        internal HashValueIndex<Key, Value, AgeOrPetKey> index;

        internal Store() : base(1, nameof(MultiPredicateSample))
        {
            this.index = new HashValueIndex<Key, Value, AgeOrPetKey>(base.AppName, base.FasterKV,
                                        CreateRegistrationSettings(0, new AgeOrPetKey.Comparer()),
                                         (nameof(this.CombinedPetPred), v => new AgeOrPetKey(v.Species)),
                                         (nameof(this.CombinedAgePred), v => new AgeOrPetKey(v.Age)));
            this.AddIndex(this.index);
            this.CombinedPetPred = this.index.GetPredicate(nameof(this.CombinedPetPred));
            this.CombinedAgePred = this.index.GetPredicate(nameof(this.CombinedAgePred));
        }

        internal override async ValueTask<(bool success, Guid token)> CheckpointAsync()
        {
            var primaryResult = await base.CheckpointAsync();
            var (success, _) = await this.index.TakeFullCheckpointAsync(CheckpointType.FoldOver);
            if (!success)
                throw new ApplicationException("Cannot checkpoint Primary FKV");
            return primaryResult;
        }
    }
}
