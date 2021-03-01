// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.benchmark
{
    enum BenchmarkType : int
    {
        Ycsb, SpanByte, ConcurrentDictionaryYcsb
    };

    [Flags]
    enum BackupMode : int
    {
        None, Restore, Backup, Both
    };

    enum LockImpl : int
    {
        None, RecordInfo
    };

    [Flags]
    enum SecondaryIndexType : int
    {
        None, Key, Value, Both
    };

    enum AddressLine : int
    {
        Before = 1,
        After = 2
    }

    public enum Op : ulong
    {
        Upsert = 0,
        Read = 1,
        ReadModifyWrite = 2
    }

    public static class YcsbGlobals
    {
        internal const string UniformDist = "uniform";    // Uniformly random distribution of keys
        internal const string ZipfDist = "zipf";          // Smooth zipf curve (most localized keys)

        internal const string InsPerSec = "ins/sec";
        internal const string OpsPerSec = "ops/sec";

        internal static string TotalOpsString(long totalOps, double seconds) => $"Total {totalOps:N0} ops done in {seconds:N3} seconds";

        internal static string OptionsString;

        internal static string LoadingTimeLine(double insertsPerSec, long elapsedMs) 
            => $"##0; {InsPerSec}: {insertsPerSec:N2}; ms: {elapsedMs:N0}";

        internal static string AddressesLine(AddressLine lineNum, long begin, long head, long rdonly, long tail)
            => $"##{(int)lineNum}; begin: {begin:N0}; head: {head:N0}; readonly: {rdonly:N0}; tail: {tail}";

        private static string BoolStr(bool value) => value ? "y" : "n";

        internal static string StatsLine(StatsLine lineNum, string opsPerSecTag, double opsPerSec)
            => $"##{(int)lineNum}; {opsPerSecTag}: {opsPerSec:N2}; {OptionsString}; sd: {BoolStr(kUseSmallData)}; sm: {BoolStr(kSmallMemoryLog)}";

        internal static string StatsLine(StatsLine lineNum, string meanTag, double mean, double stdev, double stdevpct)
            => $"##{(int)lineNum}; {meanTag}: {mean:N2}; stdev: {stdev:N1}; stdev%: {stdevpct:N1}; {OptionsString}; sd: {BoolStr(kUseSmallData)}; sm: {BoolStr(kSmallMemoryLog)}";

#if DEBUG
        internal const bool kUseSmallData = true;
        internal const bool kUseSyntheticData = true;
        internal const bool kSmallMemoryLog = false;
        internal const int kRunSeconds = 30;
#else
        internal const bool kUseSmallData = true;//false;
        internal const bool kUseSyntheticData = false;
        internal const bool kSmallMemoryLog = false;
        internal const int kRunSeconds = 30;
#endif
        internal const long kInitCount = kUseSmallData ? 2500480 : 250000000;
        internal const long kTxnCount = kUseSmallData ? 10000000 : 1000000000;
        internal const int kMaxKey = kUseSmallData ? 1 << 22 : 1 << 28;
    }
}
