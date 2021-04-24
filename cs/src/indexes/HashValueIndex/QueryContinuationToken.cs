// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using Newtonsoft.Json;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FASTER.indexes.HashValueIndex
{
    public class QueryContinuationToken
    {
        public SerializedPredicate[] Predicates;

        public QueryContinuationToken(int numberOfPredicates) => this.Predicates = new SerializedPredicate[numberOfPredicates];

        internal ref SerializedPredicate this[int index] => ref this.Predicates[index];

        internal bool IsEmpty => this.Predicates[0].KeyState is null;

        internal static QueryContinuationToken FromString(string json)
            => JsonConvert.DeserializeObject<QueryContinuationToken>(json);

        public override string ToString() 
            => JsonConvert.SerializeObject(this, new JsonSerializerSettings { Formatting = Formatting.Indented });
    }

    public struct SerializedPredicate
    {
        public byte[] KeyState;
        public RecordId RecordId;
    }
}
