// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Newtonsoft.Json;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FASTER.indexes.HashValueIndex
{
    public class QueryContinuationToken
    {
        public SerializedPredicate[] Predicates;

        public QueryContinuationToken(int numberOfPredicates) => this.Predicates = new SerializedPredicate[numberOfPredicates];

        public static QueryContinuationToken FromString(string json)
            => JsonConvert.DeserializeObject<QueryContinuationToken>(json);

        public override string ToString() 
            => JsonConvert.SerializeObject(this, new JsonSerializerSettings { Formatting = Formatting.Indented });
    }

    public struct SerializedPredicate
    {
        public byte[] State;
    }
}
