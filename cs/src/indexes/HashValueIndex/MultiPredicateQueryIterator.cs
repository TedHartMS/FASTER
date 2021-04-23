// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Generic;
using System.Linq;

namespace FASTER.indexes.HashValueIndex
{
    internal struct PredicateIterationState<TPKey>
    {
        // Input for the query. If there is a continuation token, its PreviousAddresses is updated from recordInfo and included in the serialization.
        internal SecondaryFasterKV<TPKey>.Input Input;

        // Ephemeral target for PreviousAddress on Read. Note that the RecordId for "PreviousAddress" may already be in retrievedRecordId.
        internal RecordInfo RecordInfo;

        // RecordId that has been retrieved, input.PreviousAddress points to the address to search for the next recordId in the chain.
        internal RecordId RecordId;

        internal PredicateIterationState(SecondaryFasterKV<TPKey>.Input input)
        {
            this.Input = input;
            this.RecordInfo = new RecordInfo { PreviousAddress = input.PreviousAddress };
            this.RecordId = default;
        }

        public void Dispose() => this.Input.Dispose();
    }

    internal class MultiPredicateQueryIterator<TPKey> : IDisposable
    {
        internal PredicateIterationState<TPKey>[] states;

        // The boolean match states for the lambda.
        internal bool[] matches;

        // Active indexes are the indexes in recordInfos of the element(s) with the highest RecordIds; these will be "yield return"ed, then their PreviousAddresses retrieved.
        internal int[] activeIndexes;
        internal int activeLength;

        internal MultiPredicateQueryIterator(IEnumerable<SecondaryFasterKV<TPKey>.Input> inputs)
        {
            this.states = inputs.Select(input => new PredicateIterationState<TPKey>(input)).ToArray();
            matches = new bool[this.Length];
            activeIndexes = new int[this.Length];
            activeLength = default;
        }

        internal int Length => this.states.Length;

        internal ref PredicateIterationState<TPKey> this[int index] => ref this.states[index];

        // TODOoerf: This could be a priority queue, but we likely aren't going to have enough predicates in a single query to matter.
        internal bool Next()
        {
            // 1. Get highest address. We can order on just Address because Version should not change after the record goes ReadOnly.
            RecordId maxRecordId = default;
            int maxFirstIndex = -1;
            for (int ii = 0; ii < this.Length; ++ii)
            {
                matches[ii] = false;
                var rId = this[ii].RecordId;
                if (rId.Address > maxRecordId.Address)
                {
                    maxRecordId = rId;
                    maxFirstIndex = ii;
                }
            }
            if (maxRecordId.IsDefault)
                return false;

            // 2. Set the active-indexes vector
            this.activeLength = 0;
            for (var ii = maxFirstIndex; ii < this.Length; ++ii)
            {
                if (this[ii].RecordId.Address == maxRecordId.Address)
                {
                    activeIndexes[this.activeLength++] = ii;
                    this.matches[ii] = true;
                }
            }
            return true;
        }

        public void Dispose()
        {
            for (var ii = 0; ii < this.Length; ++ii)
                this[ii].Dispose();
        }
    }
}
