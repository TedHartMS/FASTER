// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;

namespace FASTER.indexes.HashValueIndex
{
    internal interface IWorkQueueOrderedItem
    {
        void Signal();
    }

    /// <summary>
    /// Shared work queue that ensures ordering of work, with each thread submitting work being the one to complete that work.
    /// and the queue acting as a coordinator to tell threads when they must wait and signal them when they can process.
    /// </summary>
    /// <typeparam name="TKey">The key type used for ordering</typeparam>
    /// <typeparam name="TWorkItem">The value type; must be able to supply a key to be used to retrieve the value</typeparam>
    internal class WorkQueueOrdered<TKey, TWorkItem>
        where TKey: IEquatable<TKey>
        where TWorkItem: IWorkQueueOrderedItem
    {
        internal readonly ConcurrentDictionary<TKey, TWorkItem> workItems = new ConcurrentDictionary<TKey, TWorkItem>();
        internal TKey nextKey;

        public WorkQueueOrdered(TKey initialKey) => this.nextKey = initialKey;

        // The sequence of operations by the caller is:
        //    if (!queue.TryStartWork(key, creatorFunc, out var workItem) {
        //        workItem.Wait();
        //        /* Dispose if required */
        //    }
        //    queue.CompleteWork(key, nextKey);

        /// <summary>
        /// Returns true if <paramref name="key"/> is nextKey, else enqueues the work item and returns false 
        /// </summary>
        /// <param name="key">The key for this work item</param>
        /// <param name="creatorFunc">An action to crate a value for enqueueing</param>
        /// <param name="workItem">The created work item, if it was necessary to enqueue; it will be signaled when it is ready for processing</param>
        /// <remarks>The output <paramref name="workItem"/> is only created if it is necessary for the calling thread to wait, and therefore if
        /// <typeparamref name="TWorkItem"/> implements <see cref="IDisposable"/> it is the responsibility of the waiting thread to call <see cref="IDisposable.Dispose()"/></remarks>
        public bool TryStartWork(TKey key, Func<TWorkItem> creatorFunc, out TWorkItem workItem)
        {
            // We assume that ranges are distinct and adjacent (not overlapping), and each key will be sent by a single thread only.
            // Therefore, only one thread should be calling CompleteWork() at a time; multiple threads may be calling TryStartWork.
            Interlocked.MemoryBarrier();
            if (key.Equals(this.nextKey))
            {
                // This is the most common case, so we can avoid the creation of a wait event by not enqueueing.
                workItem = default;
                return true;
            }

            workItem = creatorFunc();
            this.workItems[key] = workItem;

            // Try one more time, as there may have been a race condition where CompleteWork() set this between the time we checked and the time we inserted.
            // Note that this may also have been signaled by CompleteWorker(), so TWorkItem must be able to handle being signaled more than once.
            Interlocked.MemoryBarrier();
            if (key.Equals(this.nextKey))
                workItem.Signal();
            return false;
        }

        public void CompleteWork(TKey completedAddr, TKey nextAddr)
        {
            Debug.Assert(completedAddr.Equals(this.nextKey));
            this.workItems.TryRemove(completedAddr, out _);

            this.nextKey = nextAddr;
            Interlocked.MemoryBarrier();
            if (this.workItems.TryGetValue(nextAddr, out var workItem))
                workItem.Signal();
            return;
        }
    }
}
