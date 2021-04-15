﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

namespace FASTER.core
{
    /// <summary>
    /// A list of <see cref="CompletedOutputIterator{TKey, TValue, TInput, TOutput, TContext}"/> for completed outputs from a pending operation.
    /// </summary>
    /// <typeparam name="TKey">The Key type of the <see cref="FasterKV{Key, Value}"/></typeparam>
    /// <typeparam name="TValue">The Value type of the <see cref="FasterKV{Key, Value}"/></typeparam>
    /// <typeparam name="TInput">The session input type</typeparam>
    /// <typeparam name="TOutput">The session output type</typeparam>
    /// <typeparam name="TContext">The session context type</typeparam>
    /// <remarks>The session holds this list and returns an enumeration to the caller of an appropriate CompletePending overload. The session will handle
    /// disposing and clearing this list, but it is best if the caller calls Dispose() after processing the results, so the key, input, and heap containers
    /// are released as soon as possible.</remarks>
    public class CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> : IDisposable
    {
        internal const int kInitialAlloc = 32;
        internal const int kReallocMultuple = 2;
        internal CompletedOutput<TKey, TValue, TInput, TOutput, TContext>[] vector = new CompletedOutput<TKey, TValue, TInput, TOutput, TContext>[kInitialAlloc];
        internal int maxIndex = -1;
        internal int currentIndex = -1;

        internal void Add(ref FasterKV<TKey, TValue>.PendingContext<TInput, TOutput, TContext> pendingContext, Status status)
        {
            // Note: vector is never null
            if (this.maxIndex >= vector.Length - 1)
                Array.Resize(ref this.vector, this.vector.Length * kReallocMultuple);
            ++maxIndex;
            this.vector[maxIndex].Set(ref pendingContext, status);
        }

        /// <summary>
        /// Advance the iterator to the next element.
        /// </summary>
        /// <returns>False if this advances past the last element of the array, else true</returns>
        public bool Next()
        {
            if (this.currentIndex < this.maxIndex)
            {
                ++this.currentIndex;
                return true;
            }
            this.currentIndex = vector.Length;
            return false;
        }

        /// <summary>
        /// Returns a reference to the current element of the enumeration.
        /// </summary>
        /// <returns>A reference to the current element of the enumeration</returns>
        /// <exception cref="IndexOutOfRangeException"> if there is no current element, either because Next() has not been called or it has advanced
        ///     past the last element of the array
        /// </exception>
        public ref CompletedOutput<TKey, TValue, TInput, TOutput, TContext> Current => ref this.vector[this.currentIndex];

        /// <inheritdoc/>
        public void Dispose()
        {
            for (; this.maxIndex >= 0; --this.maxIndex)
                this.vector[maxIndex].Dispose();
            this.currentIndex = -1;
        }
    }

    /// <summary>
    /// Structure to hold a key and its output for a pending operation.
    /// </summary>
    /// <typeparam name="TKey">The Key type of the <see cref="FasterKV{Key, Value}"/></typeparam>
    /// <typeparam name="TValue">The Value type of the <see cref="FasterKV{Key, Value}"/></typeparam>
    /// <typeparam name="TInput">The session input type</typeparam>
    /// <typeparam name="TOutput">The session output type</typeparam>
    /// <typeparam name="TContext">The session context type</typeparam>
    /// <remarks>The session holds a list of these that it returns to the caller of an appropriate CompletePending overload. The session will handle disposing
    /// and clearing, and will manage Dispose(), but it is best if the caller calls Dispose() after processing the results, so the key, input, and heap containers
    /// are released as soon as possible.</remarks>
    public struct CompletedOutput<TKey, TValue, TInput, TOutput, TContext>
    {
        private IHeapContainer<TKey> keyContainer;
        private IHeapContainer<TInput> inputContainer;

        /// <summary>
        /// The key for this pending operation.
        /// </summary>
        public ref TKey Key => ref keyContainer.Get();

        /// <summary>
        /// The input for this pending operation.
        /// </summary>
        public ref TInput Input => ref inputContainer.Get();

        /// <summary>
        /// The output for this pending operation.
        /// </summary>
        public TOutput Output;

        /// <summary>
        /// The context for this pending operation.
        /// </summary>
        public TContext Context;

        /// <summary>
        /// The header of the record for this operation
        /// </summary>
        public RecordInfo RecordInfo;

        /// <summary>
        /// The logical address of the record for this operation
        /// </summary>
        public long Address;

        /// <summary>
        /// The status of the operation: OK or NOTFOUND
        /// </summary>
        public Status Status;

        internal void Set(ref FasterKV<TKey, TValue>.PendingContext<TInput, TOutput, TContext> pendingContext, Status status)
        {
            this.keyContainer = pendingContext.key;
            this.inputContainer = pendingContext.input;
            this.Output = pendingContext.output;
            this.Context = pendingContext.userContext;
            this.RecordInfo = pendingContext.recordInfo;
            this.Address = pendingContext.logicalAddress;
            this.Status = status;
        }

        internal void Dispose()
        {
            var tempKeyContainer = keyContainer;
            keyContainer = default;
            if (tempKeyContainer is { })
                tempKeyContainer.Dispose();

            var tempInputContainer = inputContainer;
            inputContainer = default;
            if (tempInputContainer is { })
                tempInputContainer.Dispose();

            Output = default;
            Context = default;
        }
    }
}
