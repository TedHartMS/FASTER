// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using CommandLine;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http.Headers;
using System.Threading;

namespace FASTER.benchmark
{
    class Options
    {
        [Option('b', "benchmark", Required = false, Default = 0,
        HelpText = "Benchmark to run:" +
                        "\n    0 = YCSB" +
                        "\n    1 = YCSB with SpanByte" +
                        "\n    2 = ConcurrentDictionary")]
        public int Benchmark { get; set; }

        [Option('t', "threads", Required = false, Default = 8,
         HelpText = "Number of threads to run the workload on")]
        public int ThreadCount { get; set; }

        [Option('n', "numa", Required = false, Default = 0,
             HelpText = "NUMA options:" +
                        "\n    0 = No sharding across NUMA sockets" +
                        "\n    1 = Sharding across NUMA sockets")]
        public int NumaStyle { get; set; }

        [Option('k', "backup", Required = false, Default = 0,
             HelpText = "Enable Backup and Restore of FasterKV for fast test startup:" +
                        "\n    0 = None; Populate FasterKV from data" +
                        "\n    1 = Recover FasterKV from Checkpoint; if this fails, populate FasterKV from data" +
                        "\n    2 = Checkpoint FasterKV (unless it was Recovered by option 1; if option 1 is not specified, this will overwrite an existing Checkpoint)" +
                        "\n    3 = Both (Recover FasterKV if a Checkpoint is available, else populate FasterKV from data and Checkpoint it so it can be Restored in a subsequent run)")]
        public int Backup { get; set; }

        [Option('l', "locking", Required = false, Default = 0,
             HelpText = "Locking Implementation:" +
                        "\n    0 = None (default)" +
                        "\n    1 = RecordInfo.SpinLock()")]
        public int LockImpl { get; set; }

        [Option('x', "index", Required = false, Default = 0,
             HelpText = "Secondary index type(s); these implement a no-op index to test the overhead on FasterKV operations:" +
                        "\n    0 = None (default)" +
                        "\n    1 = Key-based index" +
                        "\n    2 = Value-based index" +
                        "\n    3 = Both index types")]
        public int SecondaryIndexType { get; set; }

        [Option('i', "iterations", Required = false, Default = 1,
         HelpText = "Number of iterations of the test to run")]
        public int IterationCount { get; set; }

        [Option('r', "read_percent", Required = false, Default = 50,
         HelpText = "Percentage of reads (-1 for 100% read-modify-write")]
        public int ReadPercent { get; set; }

        [Option('d', "distribution", Required = false, Default = "uniform",
            HelpText = "Distribution of keys in workload")]
        public string Distribution { get; set; }

        [Option('s', "seed", Required = false, Default = 211,
            HelpText = "Seed for synthetic data distribution")]
        public int RandomSeed { get; set; }
    }

    enum BenchmarkType : int
    {
        Ycsb, SpanByte, ConcurrentDictionaryYcsb
    };

    [Flags] enum BackupMode : int
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

    public class Program
    {
        const int kTrimResultCount = 3;// int.MaxValue; // Use some high value like int.MaxValue to bypass

        public static void Main(string[] args)
        {
            ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
            if (result.Tag == ParserResultType.NotParsed)
            {
                return;
            }

            bool verifyOption(bool isValid, string name)
            {
                if (!isValid)
                    Console.WriteLine($"Invalid {name} argument");
                return isValid;
            }
            var options = result.MapResult(o => o, xs => new Options());

            var b = (BenchmarkType)options.Benchmark;
            if (!verifyOption(Enum.IsDefined(typeof(BenchmarkType), b), "Benchmark")) return;
            if (!verifyOption(options.NumaStyle >= 0 && options.NumaStyle <= 1, "NumaStyle")) return;
            var backupMode = (BackupMode)options.Backup;
            if (!verifyOption(Enum.IsDefined(typeof(BackupMode), backupMode), "BackupMode")) return;
            var lockImpl = (LockImpl)options.LockImpl;
            if (!verifyOption(Enum.IsDefined(typeof(LockImpl), lockImpl), "Lock Implementation")) return;
            var secondaryIndexType = (SecondaryIndexType)options.SecondaryIndexType;
            if (!verifyOption(Enum.IsDefined(typeof(SecondaryIndexType), secondaryIndexType), "Secondary Index Type")) return;
            if (!verifyOption(options.IterationCount > 0, "Iteration Count")) return;
            if (!verifyOption(options.ReadPercent >= -1 && options.ReadPercent <= 100, "Read Percent")) return;
            var distribution = options.Distribution.ToLower();
            if (!verifyOption(distribution == "uniform" || distribution == "zipf", "Distribution")) return;

            Console.WriteLine($"Scenario: {b}, Locking: {(LockImpl)options.LockImpl}, Indexing: {(SecondaryIndexType)options.SecondaryIndexType}");

            var initsPerRun = new List<double>();
            var opsPerRun = new List<double>();

            void addResult((double ips, double ops) result)
            {
                initsPerRun.Add(result.ips);
                opsPerRun.Add(result.ops);
            }

            Key[] init_keys_ = default;
            Key[] txn_keys_ = default;
            KeySpanByte[] init_span_keys_ = default;
            KeySpanByte[] txn_span_keys_ = default;

            switch (b)
            {
                case BenchmarkType.Ycsb:
                    FASTER_YcsbBenchmark.LoadData(distribution, (uint)options.RandomSeed, out init_keys_, out txn_keys_);
                    break;
                case BenchmarkType.SpanByte:
                    FasterSpanByteYcsbBenchmark.LoadData(distribution, (uint)options.RandomSeed, out init_span_keys_, out txn_span_keys_);
                    break;
                case BenchmarkType.ConcurrentDictionaryYcsb:
                    ConcurrentDictionary_YcsbBenchmark.LoadData(distribution, (uint)options.RandomSeed, out init_keys_, out txn_keys_);
                    break;
                default:
                    throw new ApplicationException("Unknown benchmark type");
            }

            static void showStats(string tag, List<double> vec, string discardMessage = "")
            {
                var mean = vec.Sum() / vec.Count;
                var stddev = Math.Sqrt(vec.Sum(n => Math.Pow(n - mean, 2)) / vec.Count);
                var stddevpct = (stddev / mean) * 100;
                Console.WriteLine($"###; {tag}; {mean:N3}; sd; {stddev:N1}; {stddevpct:N1}%");
            }

            void showAllStats(string discardMessage = "")
            {
                Console.WriteLine($"Averages per second over {initsPerRun.Count} iteration(s){discardMessage}:");
                showStats("ins/sec", initsPerRun);
                showStats("ops/sec", opsPerRun);
            }

            for (var iter = 0; iter < options.IterationCount; ++iter)
            {
                if (options.IterationCount > 1)
                {
                    Console.WriteLine();
                    Console.WriteLine($"Iteration {iter + 1} of {options.IterationCount}");
                }

                switch (b)
                {
                    case BenchmarkType.Ycsb:
                        var yTest = new FASTER_YcsbBenchmark(init_keys_, txn_keys_, options.ThreadCount, options.NumaStyle, distribution, options.ReadPercent, backupMode, lockImpl, secondaryIndexType);
                        addResult(yTest.Run());
                        yTest.Dispose();
                        break;
                    case BenchmarkType.SpanByte:
                        var sTest = new FasterSpanByteYcsbBenchmark(init_span_keys_, txn_span_keys_, options.ThreadCount, options.NumaStyle, distribution, options.ReadPercent, backupMode, lockImpl, secondaryIndexType);
                        addResult(sTest.Run());
                        sTest.Dispose();
                        break;
                    case BenchmarkType.ConcurrentDictionaryYcsb:
                        var cTest = new ConcurrentDictionary_YcsbBenchmark(init_keys_, txn_keys_, options.ThreadCount, options.NumaStyle, distribution, options.ReadPercent);
                        addResult(cTest.Run());
                        cTest.Dispose();
                        break;
                    default:
                        throw new ApplicationException("Unknown benchmark type");
                }

                if (options.IterationCount > 1)
                    showAllStats();

                if (iter < options.IterationCount - 1)
                {
                    GC.Collect();
                    GC.WaitForFullGCComplete();
                    Thread.Sleep(1000);
                }
            }

            if (options.IterationCount >= kTrimResultCount)
            {
                static void discardHiLo(List<double> vec)
                {
                    vec.Sort();
#pragma warning disable IDE0056 // Use index operator (^ is not supported on .NET Framework or NETCORE pre-3.0)
                    vec[0] = vec[vec.Count - 2];        // overwrite lowest with second-highest
#pragma warning restore IDE0056 // Use index operator
                    vec.RemoveRange(vec.Count - 2, 2);  // remove highest and (now-duplicated) second-highest
                }
                discardHiLo(initsPerRun);
                discardHiLo(opsPerRun);

                Console.WriteLine();
                showAllStats($" ({options.IterationCount} iterations specified, with high and low discarded)");
            }
        }
    }
}
