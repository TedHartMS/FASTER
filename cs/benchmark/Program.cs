﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using CommandLine;
using System;

namespace FASTER.benchmark
{
    class Options
    {
        [Option('b', "benchmark", Required = false, Default = 0,
        HelpText = "Benchmark to run (0 = YCSB)")]
        public int Benchmark { get; set; }

        [Option('t', "threads", Required = false, Default = 8,
         HelpText = "Number of threads to run the workload on")]
        public int ThreadCount { get; set; }

        [Option('n', "numa", Required = false, Default = 0,
             HelpText = "0 = No sharding across NUMA sockets, 1 = Sharding across NUMA sockets")]
        public int NumaStyle { get; set; }

        [Option('k', "backup", Required = false, Default = 0,
             HelpText = "Enable Backup and Restore of FasterKV for fast test startup:" +
                        "\n    0 = None; Populate FasterKV from data" +
                        "\n    1 = Recover FasterKV from Checkpoint; if this fails, populate FasterKV from data" +
                        "\n    2 = Checkpoint FasterKV (unless it was Recovered by option 1; if option 1 is not specified, this will overwrite an existing Checkpoint)" +
                        "\n    3 = Both (Recover FasterKV if the Checkpoint is available, else populate FasterKV from data and Checkpoint it so it can be Restored in a subsequent run)")]
        public int Backup { get; set; }

        [Option('i', "index", Required = false, Default = 0,
             HelpText = "Secondary index type(s); these implement a no-op index to test the overhead on FasterKV operations:" +
                        "\n    0 = None (default)" +
                        "\n    1 = Key-based index" +
                        "\n    2 = Value-based index" +
                        "\n    3 = Both index types")]
        public int SecondaryIndexType { get; set; }

        [Option('r', "read_percent", Required = false, Default = 50,
         HelpText = "Percentage of reads (-1 for 100% read-modify-write")]
        public int ReadPercent { get; set; }

        [Option('d', "distribution", Required = false, Default = "uniform",
            HelpText = "Distribution of keys in workload")]
        public string Distribution { get; set; }
    }

    enum BenchmarkType : int
    {
        Ycsb, SpanByte, ConcurrentDictionaryYcsb
    };

    [Flags] enum BackupMode : int
    {
        None, Restore, Backup, Both
    };

    [Flags]
    enum SecondaryIndexType : int
    {
        None, Key, Value, Both
    };

    public class Program
    {
        public static void Main(string[] args)
        {
            ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
            if (result.Tag == ParserResultType.NotParsed)
            {
                return;
            }

            var options = result.MapResult(o => o, xs => new Options());
            BenchmarkType b = (BenchmarkType)options.Benchmark;

            if (b == BenchmarkType.Ycsb)
            {
                var test = new FASTER_YcsbBenchmark(options.ThreadCount, options.NumaStyle, options.Distribution, options.ReadPercent, options.Backup, options.SecondaryIndexType);
                test.Run();
            }
            else if (b == BenchmarkType.SpanByte)
            {
                var test = new FasterSpanByteYcsbBenchmark(options.ThreadCount, options.NumaStyle, options.Distribution, options.ReadPercent, options.Backup);
                test.Run();
            }
            else if (b == BenchmarkType.ConcurrentDictionaryYcsb)
            {
                var test = new ConcurrentDictionary_YcsbBenchmark(options.ThreadCount, options.NumaStyle, options.Distribution, options.ReadPercent);
                test.Run();
            }
        }
    }
}
