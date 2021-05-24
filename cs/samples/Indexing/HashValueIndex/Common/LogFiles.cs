// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.IO;

namespace HashValueIndexSampleCommon
{
    class LogFiles
    {
        private IDevice log;
        private IDevice objLog;
        private IDevice[] IndexDevices;

        internal LogSettings LogSettings { get; }

        internal LogSettings[] IndexLogSettings { get; }

        internal string LogDir;
        internal string IndexLogDir(int indexOrdinal) => Path.Combine(this.LogDir, $"Index_{indexOrdinal:D3}");

        // Hash and log sizes
        internal const int HashSizeBits = 20;
        private const int MemorySizeBits = 29;
        private const int SegmentSizeBits = 25;
        private const int PageSizeBits = 20;

        internal LogFiles(int numIndexes, string appName)
        {
            this.LogDir = $"d:/temp/{appName}";

            // Create files for storing data. We only use one write thread to avoid disk contention.
            // We do NOT set deleteOnClose to true, so logs will survive store.Close(), which we do as part of the Checkpoint/Recovery sequence.
            this.log = Devices.CreateLogDevice(Path.Combine(this.LogDir, "hlog.log"));

            this.LogSettings = new LogSettings
            {
                LogDevice = log,
                ObjectLogDevice = objLog,
                MemorySizeBits = MemorySizeBits,
                SegmentSizeBits = SegmentSizeBits,
                PageSizeBits = PageSizeBits,
                CopyReadsToTail = CopyReadsToTail.None,
                ReadCacheSettings = null
            };

            this.IndexDevices = new IDevice[numIndexes];
            this.IndexLogSettings = new LogSettings[numIndexes];
            for (var ii = 0; ii < numIndexes; ++ii)
            {
                this.IndexDevices[ii] = Devices.CreateLogDevice(Path.Combine(this.IndexLogDir(ii), "hlog.log"));
                this.IndexLogSettings[ii] = new LogSettings { LogDevice = this.IndexDevices[ii], MemorySizeBits = MemorySizeBits, SegmentSizeBits = SegmentSizeBits, PageSizeBits = PageSizeBits };
                // Note: ReadCache and CopyReadsToTail are not supported in SubsetIndex FKVs
            }
        }

        internal void Close()
        {
            if (!(this.log is null))
            {
                this.log.Dispose();
                this.log = null;
            }
            if (!(this.objLog is null))
            {
                this.objLog.Dispose();
                this.objLog = null;
            }

            if (this.IndexDevices is { })
            {
                foreach (var device in this.IndexDevices)
                    device.Dispose();
                this.IndexDevices = null;
            }
        }
    }
}
