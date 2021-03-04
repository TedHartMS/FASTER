﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.benchmark
{
    enum BenchmarkType : int
    {
        Ycsb,
        SpanByte,
        ConcurrentDictionaryYcsb
    };

    [Flags]
    enum BackupMode : int
    {
        None = 0,
        Restore = 1,
        Backup = 2,
        Both = 3
    };

    enum LockImpl : int
    {
        None,
        RecordInfo
    };

    [Flags]
    enum SecondaryIndexType : int
    {
        None = 0,
        Key = 1,
        Value = 2,
        Both = 3
    };

    enum AddressLineNum : int
    {
        Before = 1,
        After = 2
    }

    enum AggregateType
    {
        Running = 0,
        FinalFull = 1,
        FinalTrimmed = 2
    }

    enum StatsLineNum : int
    {
        Iteration = 3,
        RunningIns = 4,
        RunningOps = 5,
        FinalFullIns = 10,
        FinalFullOps = 11,
        FinalTrimmedIns = 20,
        FinalTrimmedOps = 21
    }

    public enum Op : ulong
    {
        Upsert = 0,
        Read = 1,
        ReadModifyWrite = 2
    }

    public static class YcsbConstants
    {
        internal const string UniformDist = "uniform";    // Uniformly random distribution of keys
        internal const string ZipfDist = "zipf";          // Smooth zipf curve (most localized keys)

        internal const string InsPerSec = "ins/sec";
        internal const string OpsPerSec = "ops/sec";

#if DEBUG
        internal const bool kUseSmallData = true;
        internal const bool kSmallMemoryLog = false;
        internal const int kRunSeconds = 30;
#else
        internal const bool kUseSmallData = false;
        internal const bool kSmallMemoryLog = false;
        internal const int kRunSeconds = 30;
#endif
        internal const long kInitCount = kUseSmallData ? 2500480 : 250000000;
        internal const long kTxnCount = kUseSmallData ? 10000000 : 1000000000;
        internal const int kMaxKey = kUseSmallData ? 1 << 22 : 1 << 28;

        internal const int kFileChunkSize = 4096;
        internal const long kChunkSize = 640;
    }
}
