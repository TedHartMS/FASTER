// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using CommandLine;

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

        [Option('z', "locking", Required = false, Default = 0,
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

        [Option('d', "distribution", Required = false, Default = YcsbGlobals.UniformDist,
            HelpText = "Distribution of keys in workload")]
        public string DistributionName { get; set; }

        [Option('s', "seed", Required = false, Default = 211,
            HelpText = "Seed for synthetic data distribution")]
        public int RandomSeed { get; set; }

        public string GetOptionsString()
            => $"d: {DistributionName.ToLower()}; n: {NumaStyle}; r: {ReadPercent}; t: {ThreadCount}; x: {SecondaryIndexType}; z: {LockImpl}";
    }
}
