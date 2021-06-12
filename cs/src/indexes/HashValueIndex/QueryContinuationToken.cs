// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using Newtonsoft.Json;
using System.Linq;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FASTER.indexes.HashValueIndex
{
    public class QueryContinuationToken
    {
        public long PrimaryStartAddress;
        public long PrimaryEndAddress;
        public long CrossoverAddress;
        public SerializedPredicate[] Predicates;
        public bool IsSecondaryStarted;
        public bool IsCanceled;

        public QueryContinuationToken() { /* For JsonConvert.DeserializeObject */ }

        public QueryContinuationToken(int numberOfPredicates) => this.Predicates = new SerializedPredicate[numberOfPredicates];

        internal ref SerializedPredicate this[int index] => ref this.Predicates[index];

        internal bool IsPrimaryStarted => this.PrimaryStartAddress != FASTER.core.Constants.kInvalidAddress;

        internal void SetPrimaryAddresses(long start, long end)
        {
            this.PrimaryStartAddress = start;
            this.PrimaryEndAddress = end;

            // There may be a lag between the time Primary updates its ReadOnlyAddress and the time we have put all records below that into the index.
            // So we start by going from highWaterRecordId + 1 -> end of Primary, then when we cross over to the index, we skip anything above the
            // highWaterRecordId we started with, which we remember as the crossoverAddress.
            this.CrossoverAddress = start;
        }
        internal bool IsPrimaryComplete => this.IsPrimaryStarted && this.PrimaryStartAddress >= this.PrimaryEndAddress;

        internal bool IsSecondaryComplete => this.IsSecondaryStarted && this.Predicates.All(pred => pred.IsComplete);

        internal bool IsComplete => this.IsPrimaryComplete && this.IsSecondaryComplete;

        internal static QueryContinuationToken FromString(string json)
        {
            var continuationToken = JsonConvert.DeserializeObject<QueryContinuationToken>(json);
            return continuationToken.IsCanceled
                ? throw new HashValueIndexInvalidOperationException("Query has been canceled")
                : continuationToken;
        }

        public override string ToString() 
            => JsonConvert.SerializeObject(this, new JsonSerializerSettings { Formatting = Formatting.Indented });
    }

    public struct SerializedPredicate
    {
        public long PreviousAddress;
        public RecordId RecordId;

        internal bool IsComplete => this.PreviousAddress == FASTER.core.Constants.kInvalidAddress && this.RecordId.IsDefault();
    }
}
