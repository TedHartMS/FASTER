// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using CommandLine;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace FASTER.benchmark
{
    enum AggregateType
    {
        Running,
        FinalFull,
        FinalTrimmed
    }

    enum StatsLine : int
    {
        Iteration = 3,
        RunningIns = 4,
        RunningOps = 5,
        FinalFullIns = 10,
        FinalFullOps = 11,
        FinalTrimmedIns = 20,
        FinalTrimmedOps = 21
    }

    public class Program
    {
        const int kTrimResultCount = 3; // Use some high value like int.MaxValue to disable

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
            var distribution = options.DistributionName.ToLower();
            if (!verifyOption(distribution == YcsbGlobals.UniformDist || distribution == YcsbGlobals.ZipfDist, "Distribution")) return;

            Console.WriteLine($"Scenario: {b}, Locking: {(LockImpl)options.LockImpl}, Indexing: {(SecondaryIndexType)options.SecondaryIndexType}");

            YcsbGlobals.OptionsString = options.GetOptionsString();

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

            static void showStats(StatsLine lineNum, string tag, List<double> vec, string discardMessage = "")
            {
                var mean = vec.Sum() / vec.Count;
                var stddev = Math.Sqrt(vec.Sum(n => Math.Pow(n - mean, 2)) / vec.Count);
                var stddevpct = (stddev / mean) * 100;
                Console.WriteLine(YcsbGlobals.StatsLine(lineNum, tag, mean, stddev, stddevpct));
            }

            void showAllStats(AggregateType aggregateType, string discardMessage = "")
            {
                var aggTypeString = aggregateType == AggregateType.Running ? "Running" : "Final";
                Console.WriteLine($"{aggTypeString} averages per second over {initsPerRun.Count} iteration(s){discardMessage}:");
                var statsLineNum = aggregateType switch
                {
                    AggregateType.Running => StatsLine.RunningIns,
                    AggregateType.FinalFull => StatsLine.FinalFullIns,
                    AggregateType.FinalTrimmed => StatsLine.FinalTrimmedIns,
                    _ => throw new InvalidOperationException("Unknown AggregateType")
                };
                showStats(statsLineNum, "ins/sec", initsPerRun);
                statsLineNum = aggregateType switch
                {
                    AggregateType.Running => StatsLine.RunningOps,
                    AggregateType.FinalFull => StatsLine.FinalFullOps,
                    AggregateType.FinalTrimmed => StatsLine.FinalTrimmedOps,
                    _ => throw new InvalidOperationException("Unknown AggregateType")
                };
                showStats(statsLineNum, "ops/sec", opsPerRun);
            }

            for (var iter = 0; iter < options.IterationCount; ++iter)
            {
                Console.WriteLine();
                if (options.IterationCount > 1)
                    Console.WriteLine($"Iteration {iter + 1} of {options.IterationCount}");

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
                {
                    showAllStats(AggregateType.Running);
                    if (iter < options.IterationCount - 1)
                    {
                        GC.Collect();
                        GC.WaitForFullGCComplete();
                        Thread.Sleep(1000);
                    }
                }
            }

            Console.WriteLine();
            showAllStats(AggregateType.FinalFull);

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
                showAllStats(AggregateType.FinalTrimmed, $" ({options.IterationCount} iterations specified, with high and low discarded)");
            }
        }
    }
}
