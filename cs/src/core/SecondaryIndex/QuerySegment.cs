// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace FASTER.core
{
    /// <summary>
    /// A segment of a segmented query (one that is retrieved via a resumable enumeration using the Continuation token).
    /// </summary>
    /// <typeparam name="TKVKey"></typeparam>
    /// <typeparam name="TKVValue"></typeparam>
    public class QuerySegment<TKVKey, TKVValue> : IEnumerable<QueryRecord<TKVKey, TKVValue>>
    {
        /// <summary>
        /// The records for the current segment
        /// </summary>        
        public List<QueryRecord<TKVKey, TKVValue>> Results { get; }
        
        /// <summary>
        /// The token to be used to continue the query
        /// </summary>
        public string ContinuationToken { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        public QuerySegment(List<QueryRecord<TKVKey, TKVValue>> results, string continuationToken)
        {
            this.Results = results;
            this.ContinuationToken = continuationToken;
        }

        /// <inheritdoc/>
        public IEnumerator<QueryRecord<TKVKey, TKVValue>> GetEnumerator() => this.Results.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => this.GetEnumerator();
    }
}
