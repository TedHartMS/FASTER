// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Threading;

namespace FASTER.indexes.HashValueIndex
{
    internal struct OrderedRange : IWorkQueueOrderedItem
    {
        internal long startAddress, endAddress;
        internal ManualResetEventSlim eventSlim;

        internal OrderedRange(long start, long end, bool createEvent = false)
        {
            this.startAddress = start;
            this.endAddress = end;
            this.eventSlim = createEvent ? new ManualResetEventSlim() : default;
        }

        internal void Wait() => this.eventSlim.Wait();

        public void Signal() => this.eventSlim.Set();

        public void Dispose()
        {
            this.eventSlim?.Dispose();
            this.eventSlim = default;
        }
    }
}
