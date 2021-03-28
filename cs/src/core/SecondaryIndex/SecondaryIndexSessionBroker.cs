// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.core
{
    /// <summary>
    /// Allows an index to register its own concept of sessions to be attached to a primary FasterKV session.
    /// </summary>
    public class SecondaryIndexSessionBroker
    {
        readonly object sessionLock = new object();

        // This never decreases in size (if we support removal, we'll just null the slot).
        object[] indexSessions = new object[0];

        // The next free session slot; these slots are only valid for the current instantiation of the FasterKV.
        // If we support removing indexes, add a stack of free slots, and null the slot in SecondaryIndexSessionBroker.indexSessions.
        internal static long NextSessionSlot = 0;

        /// <summary>
        /// Gets a previously registered session object attached by an index to this primary FasterKV session.
        /// </summary>
        /// <param name="slot">The value passed to <see cref="ISecondaryIndex.SetSessionSlot(long)"/> by <see cref="SecondaryIndexBroker{TKVKey, TKVValue}.AddIndex(ISecondaryIndex)"/></param>
        /// <returns>The session object that was attached by an index to this primary FasterKV session, or null if none was attached.</returns>
        public object GetSessionObject(long slot) => slot < this.indexSessions.Length ? this.indexSessions[slot] : null;

        /// <summary>
        /// REgisters a session object to be attached by an index to this primary FasterKV session.
        /// </summary>
        /// <param name="slot">The value passed to <see cref="ISecondaryIndex.SetSessionSlot(long)"/> by <see cref="SecondaryIndexBroker{TKVKey, TKVValue}.AddIndex(ISecondaryIndex)"/></param>
        /// <param name="sessionObject"></param>
        /// <returns>The session object to be attached by an index to this primary FasterKV session.</returns>
        public object SetSessionObject(long slot, object sessionObject)
        {
            if (slot >= this.indexSessions.Length)
            {
                if (slot > NextSessionSlot)
                    throw new FasterException("Secondary index session slot is out of range");

                lock (sessionLock)
                {
                    var vec = new object[slot + 1];
                    Array.Copy(this.indexSessions, vec, this.indexSessions.Length);
                    this.indexSessions = vec;
                }
            }

            this.indexSessions[slot] = sessionObject;
            return sessionObject;
        }
    }
}
