// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable CS0162 // Unreachable code detected -- when switching on YcsbConstants 

// Define below to enable continuous performance report for dashboard
// #define DASHBOARD

using FASTER.core;
using System;
using System.Diagnostics;
using System.Threading;

namespace FASTER.benchmark
{
    public class FasterSpanByteYcsbBenchmark
    {
#if DEBUG
        const bool kDumpDistribution = false;
        const bool kAffinitizedSession = true;
        const int kPeriodicCheckpointMilliseconds = 0;
#else
        const bool kDumpDistribution = false;
        const bool kAffinitizedSession = true;
        const int kPeriodicCheckpointMilliseconds = 0;
#endif

        // *** Use these to backup and recover database for fast benchmark repeat runs
        // Use BackupMode.Backup to create the backup, unless it was recovered during BackupMode.Recover
        // Use BackupMode.Restore for fast subsequent runs
        // Does NOT work when periodic checkpointing is turned on
        readonly BackupMode backupMode;
        // ***

        // Ensure sizes are aligned to chunk sizes
        const long kInitCount = YcsbConstants.kChunkSize * (YcsbConstants.kInitCount / YcsbConstants.kChunkSize);
        const long kTxnCount = YcsbConstants.kChunkSize * (YcsbConstants.kTxnCount / YcsbConstants.kChunkSize);

        public const int kKeySize = 16;
        public const int kValueSize = 100;

        readonly ManualResetEventSlim waiter = new ManualResetEventSlim();
        readonly int threadCount;
        readonly int numaStyle;
        readonly string distribution;
        readonly int readPercent;
        readonly FunctionsSB functions;
        readonly SecondaryIndexType secondaryIndexType = SecondaryIndexType.None;
        readonly Input[] input_;

        readonly KeySpanByte[] init_keys_;
        readonly KeySpanByte[] txn_keys_;

        readonly IDevice device;
        readonly FasterKV<SpanByte, SpanByte> store;

        long idx_ = 0;
        long total_ops_done = 0;
        volatile bool done = false;

        internal FasterSpanByteYcsbBenchmark(KeySpanByte[] i_keys_, KeySpanByte[] t_keys_, TestLoader testLoader)
        {
            // Pin loading thread if it is not used for checkpointing
            if (kPeriodicCheckpointMilliseconds <= 0)
                Native32.AffinitizeThreadShardedNuma(0, 2);

            init_keys_ = i_keys_;
            txn_keys_ = t_keys_;
            threadCount = testLoader.Options.ThreadCount;
            numaStyle = testLoader.Options.NumaStyle;
            distribution = testLoader.Distribution;
            readPercent = testLoader.Options.ReadPercent;
            backupMode = testLoader.BackupMode;
            var lockImpl = testLoader.LockImpl;
            functions = new FunctionsSB(lockImpl != LockImpl.None);
            secondaryIndexType = testLoader.SecondaryIndexType;

#if DASHBOARD
            statsWritten = new AutoResetEvent[threadCount];
            for (int i = 0; i < threadCount; i++)
            {
                statsWritten[i] = new AutoResetEvent(false);
            }
            threadThroughput = new double[threadCount];
            threadAverageLatency = new double[threadCount];
            threadMaximumLatency = new double[threadCount];
            threadProgress = new long[threadCount];
            writeStats = new bool[threadCount];
            freq = Stopwatch.Frequency;
#endif
            input_ = new Input[8];
            for (int i = 0; i < 8; i++)
                input_[i].value = i;

            var path = "D:\\data\\FasterYcsbBenchmark\\";
            device = Devices.CreateLogDevice(path + "hlog", preallocateFile: true);

            if (YcsbConstants.kSmallMemoryLog)
                store = new FasterKV<SpanByte, SpanByte>
                    (YcsbConstants.kMaxKey / 2, new LogSettings { LogDevice = device, PreallocateLog = true, PageSizeBits = 22, SegmentSizeBits = 26, MemorySizeBits = 26 },
                    new CheckpointSettings { CheckPointType = CheckpointType.FoldOver, CheckpointDir = path });
            else
                store = new FasterKV<SpanByte, SpanByte>
                    (YcsbConstants.kMaxKey / 2, new LogSettings { LogDevice = device, PreallocateLog = true, MemorySizeBits = 35 },
                    new CheckpointSettings { CheckPointType = CheckpointType.FoldOver, CheckpointDir = path });

            if (secondaryIndexType.HasFlag(SecondaryIndexType.Key))
                store.SecondaryIndexBroker.AddIndex(new NullKeyIndex<Key>());
            if (secondaryIndexType.HasFlag(SecondaryIndexType.Value))
                store.SecondaryIndexBroker.AddIndex(new NullValueIndex<Value>());
        }

        public void Dispose()
        {
            store.Dispose();
            device.Dispose();
        }

        private void RunYcsb(int thread_idx)
        {
            RandomGenerator rng = new RandomGenerator((uint)(1 + thread_idx));

            if (numaStyle == 0)
                Native32.AffinitizeThreadRoundRobin((uint)thread_idx);
            else
                Native32.AffinitizeThreadShardedNuma((uint)thread_idx, 2); // assuming two NUMA sockets

            waiter.Wait();

            Stopwatch sw = new Stopwatch();
            sw.Start();

            Span<byte> value = stackalloc byte[kValueSize];
            Span<byte> input = stackalloc byte[kValueSize];
            Span<byte> output = stackalloc byte[kValueSize];

            ref SpanByte _value = ref SpanByte.Reinterpret(value);
            ref SpanByte _input = ref SpanByte.Reinterpret(input);
            SpanByteAndMemory _output = SpanByteAndMemory.FromFixedSpan(output);

            long reads_done = 0;
            long writes_done = 0;

#if DASHBOARD
            var tstart = Stopwatch.GetTimestamp();
            var tstop1 = tstart;
            var lastWrittenValue = 0;
            int count = 0;
#endif

            var session = store.For(functions).NewSession<FunctionsSB>(null, kAffinitizedSession);

            while (!done)
            {
                long chunk_idx = Interlocked.Add(ref idx_, YcsbConstants.kChunkSize) - YcsbConstants.kChunkSize;
                while (chunk_idx >= kTxnCount)
                {
                    if (chunk_idx == kTxnCount)
                        idx_ = 0;
                    chunk_idx = Interlocked.Add(ref idx_, YcsbConstants.kChunkSize) - YcsbConstants.kChunkSize;
                }

                for (long idx = chunk_idx; idx < chunk_idx + YcsbConstants.kChunkSize && !done; ++idx)
                {
                    Op op;
                    int r = (int)rng.Generate(100);
                    if (r < readPercent)
                        op = Op.Read;
                    else if (readPercent >= 0)
                        op = Op.Upsert;
                    else
                        op = Op.ReadModifyWrite;

                    if (idx % 512 == 0)
                    {
                        if (kAffinitizedSession)
                            session.Refresh();
                        session.CompletePending(false);
                    }

                    switch (op)
                    {
                        case Op.Upsert:
                            {
                                session.Upsert(ref SpanByte.Reinterpret(ref txn_keys_[idx]), ref _value, Empty.Default, 1);
                                ++writes_done;
                                break;
                            }
                        case Op.Read:
                            {
                                session.Read(ref SpanByte.Reinterpret(ref txn_keys_[idx]), ref _input, ref _output, Empty.Default, 1);
                                ++reads_done;
                                break;
                            }
                        case Op.ReadModifyWrite:
                            {
                                session.RMW(ref SpanByte.Reinterpret(ref txn_keys_[idx]), ref _input, Empty.Default, 1);
                                ++writes_done;
                                break;
                            }
                        default:
                            throw new InvalidOperationException("Unexpected op: " + op);
                    }
                }

#if DASHBOARD
                count += (int)kChunkSize;

                //Check if stats collector is requesting for statistics
                if (writeStats[thread_idx])
                {
                    var tstart1 = tstop1;
                    tstop1 = Stopwatch.GetTimestamp();
                    threadProgress[thread_idx] = count;
                    threadThroughput[thread_idx] = (count - lastWrittenValue) / ((tstop1 - tstart1) / freq);
                    lastWrittenValue = count;
                    writeStats[thread_idx] = false;
                    statsWritten[thread_idx].Set();
                }
#endif
            }

            session.CompletePending(true);
            session.Dispose();

            sw.Stop();

#if DASHBOARD
            statsWritten[thread_idx].Set();
#endif

            Console.WriteLine("Thread " + thread_idx + " done; " + reads_done + " reads, " +
                writes_done + " writes, in " + sw.ElapsedMilliseconds + " ms.");
            Interlocked.Add(ref total_ops_done, reads_done + writes_done);
        }

        public unsafe (double, double) Run()
        {
            //Native32.AffinitizeThreadShardedNuma(0, 2);

#if DASHBOARD
            var dash = new Thread(() => DoContinuousMeasurements());
            dash.Start();
#endif

            Thread[] workers = new Thread[threadCount];

            Console.WriteLine("Executing setup.");

            Stopwatch sw = new Stopwatch();
            var storeWasRecovered = false;
            if (this.backupMode.HasFlag(BackupMode.Restore) && kPeriodicCheckpointMilliseconds <= 0)
            {
                if (!YcsbConstants.kUseSmallData)
                {
                    Console.WriteLine("Skipping Recover() for kSmallData");
                }
                else
                {
                    sw.Start();
                    try
                    {
                        Console.WriteLine("Recovering FasterKV for fast restart");
                        store.Recover();
                        storeWasRecovered = true;
                    }
                    catch (Exception)
                    {
                        Console.WriteLine("Unable to recover prior store");
                    }
                    sw.Stop();
                }
            }
            if (!storeWasRecovered)
            {
                // Setup the store for the YCSB benchmark.
                Console.WriteLine("Loading FasterKV from data");
                for (int idx = 0; idx < threadCount; ++idx)
                {
                    int x = idx;
                    workers[idx] = new Thread(() => SetupYcsb(x));
                }

                // Start threads.
                foreach (Thread worker in workers)
                {
                    worker.Start();
                }

                waiter.Set();
                sw.Start();
                foreach (Thread worker in workers)
                {
                    worker.Join();
                }
                sw.Stop();
                waiter.Reset();
            }
            double insertsPerSecond = storeWasRecovered ? 0 : ((double)kInitCount / sw.ElapsedMilliseconds) * 1000;
            Console.WriteLine(TestStats.GetLoadingTimeLine(insertsPerSecond, sw.ElapsedMilliseconds));
            Console.WriteLine(TestStats.GetAddressesLine(AddressLineNum.Before, store.Log.BeginAddress, store.Log.HeadAddress, store.Log.ReadOnlyAddress, store.Log.TailAddress));

            long startTailAddress = store.Log.TailAddress;
            Console.WriteLine("Start tail address = " + startTailAddress);

            if (!storeWasRecovered && this.backupMode.HasFlag(BackupMode.Backup) && kPeriodicCheckpointMilliseconds <= 0)
            {
                Console.WriteLine("Checkpointing FasterKV for fast restart");
                store.TakeFullCheckpoint(out _);
                store.CompleteCheckpointAsync().GetAwaiter().GetResult();
                Console.WriteLine("Completed checkpoint");
            }

            // Uncomment below to dispose log from memory, use for 100% read workloads only
            // store.Log.DisposeFromMemory();

            idx_ = 0;

            if (kDumpDistribution)
                Console.WriteLine(store.DumpDistribution());

            // Ensure first checkpoint is fast
            if (kPeriodicCheckpointMilliseconds > 0)
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, true);

            Console.WriteLine("Executing experiment.");

            // Run the experiment.
            for (int idx = 0; idx < threadCount; ++idx)
            {
                int x = idx;
                workers[idx] = new Thread(() => RunYcsb(x));
            }
            // Start threads.
            foreach (Thread worker in workers)
            {
                worker.Start();
            }

            waiter.Set();
            Stopwatch swatch = new Stopwatch();
            swatch.Start();

            if (kPeriodicCheckpointMilliseconds <= 0)
            {
                Thread.Sleep(TimeSpan.FromSeconds(YcsbConstants.kRunSeconds));
            }
            else
            {
                var checkpointTaken = 0;
                while (swatch.ElapsedMilliseconds < 1000 * YcsbConstants.kRunSeconds)
                {
                    if (checkpointTaken < swatch.ElapsedMilliseconds / kPeriodicCheckpointMilliseconds)
                    {
                        if (store.TakeHybridLogCheckpoint(out _))
                        {
                            checkpointTaken++;
                        }
                    }
                }
                Console.WriteLine($"Checkpoint taken {checkpointTaken}");
            }

            swatch.Stop();

            done = true;

            foreach (Thread worker in workers)
            {
                worker.Join();
            }
            waiter.Reset();

#if DASHBOARD
            dash.Join();
#endif

            double seconds = swatch.ElapsedMilliseconds / 1000.0;
            Console.WriteLine(TestStats.GetAddressesLine(AddressLineNum.After, store.Log.BeginAddress, store.Log.HeadAddress, store.Log.ReadOnlyAddress, store.Log.TailAddress));

            double opsPerSecond = total_ops_done / seconds;
            Console.WriteLine(TestStats.GetTotalOpsString(total_ops_done, seconds));
            Console.WriteLine(TestStats.GetStatsLine(StatsLineNum.Iteration, YcsbConstants.OpsPerSec, opsPerSecond));
            return (insertsPerSecond, opsPerSecond);
        }

        private void SetupYcsb(int thread_idx)
        {
            if (numaStyle == 0)
                Native32.AffinitizeThreadRoundRobin((uint)thread_idx);
            else
                Native32.AffinitizeThreadShardedNuma((uint)thread_idx, 2); // assuming two NUMA sockets

            waiter.Wait();

            var session = store.For(functions).NewSession<FunctionsSB>(null, kAffinitizedSession);

#if DASHBOARD
            var tstart = Stopwatch.GetTimestamp();
            var tstop1 = tstart;
            var lastWrittenValue = 0;
            int count = 0;
#endif

            Span<byte> value = stackalloc byte[kValueSize];
            ref SpanByte _value = ref SpanByte.Reinterpret(value);

            for (long chunk_idx = Interlocked.Add(ref idx_, YcsbConstants.kChunkSize) - YcsbConstants.kChunkSize;
                chunk_idx < kInitCount;
                chunk_idx = Interlocked.Add(ref idx_, YcsbConstants.kChunkSize) - YcsbConstants.kChunkSize)
            {
                for (long idx = chunk_idx; idx < chunk_idx + YcsbConstants.kChunkSize; ++idx)
                {
                    if (idx % 256 == 0)
                    {
                        session.Refresh();

                        if (idx % 65536 == 0)
                        {
                            session.CompletePending(false);
                        }
                    }

                    session.Upsert(ref SpanByte.Reinterpret(ref init_keys_[idx]), ref _value, Empty.Default, 1);
                }
#if DASHBOARD
                count += (int)kChunkSize;

                //Check if stats collector is requesting for statistics
                if (writeStats[thread_idx])
                {
                    var tstart1 = tstop1;
                    tstop1 = Stopwatch.GetTimestamp();
                    threadThroughput[thread_idx] = (count - lastWrittenValue) / ((tstop1 - tstart1) / freq);
                    lastWrittenValue = count;
                    writeStats[thread_idx] = false;
                    statsWritten[thread_idx].Set();
                }
#endif
            }

            session.CompletePending(true);
            session.Dispose();
        }

#if DASHBOARD
        int measurementInterval = 2000;
        bool measureLatency;
        bool[] writeStats;
        private EventWaitHandle[] statsWritten;
        double[] threadThroughput;
        double[] threadAverageLatency;
        double[] threadMaximumLatency;
        long[] threadProgress;
        double freq;

        void DoContinuousMeasurements()
        {
            double totalThroughput, totalLatency, maximumLatency;
            double totalProgress;
            int ver = 0;

            using (var client = new WebClient())
            {
                while (!done)
                {
                    ver++;

                    Thread.Sleep(measurementInterval);

                    totalProgress = 0;
                    totalThroughput = 0;
                    totalLatency = 0;
                    maximumLatency = 0;

                    for (int i = 0; i < threadCount; i++)
                    {
                        writeStats[i] = true;
                    }


                    for (int i = 0; i < threadCount; i++)
                    {
                        statsWritten[i].WaitOne();
                        totalThroughput += threadThroughput[i];
                        totalProgress += threadProgress[i];
                        if (measureLatency)
                        {
                            totalLatency += threadAverageLatency[i];
                            if (threadMaximumLatency[i] > maximumLatency)
                            {
                                maximumLatency = threadMaximumLatency[i];
                            }
                        }
                    }

                    if (measureLatency)
                    {
                        Console.WriteLine("{0} \t {1:0.000} \t {2} \t {3} \t {4} \t {5}", ver, totalThroughput / (double)1000000, totalLatency / threadCount, maximumLatency, store.Log.TailAddress, totalProgress);
                    }
                    else
                    {
                        Console.WriteLine("{0} \t {1:0.000} \t {2} \t {3}", ver, totalThroughput / (double)1000000, store.Log.TailAddress, totalProgress);
                    }
                }
            }
        }
#endif

        #region Load Data

        internal static void CreateKeyVectors(out KeySpanByte[] i_keys, out KeySpanByte[] t_keys)
        {
            i_keys = new KeySpanByte[kInitCount];
            t_keys = new KeySpanByte[kTxnCount];
        }

        internal class KeySetter : IKeySetter<KeySpanByte>
        {
            public unsafe void Set(KeySpanByte[] vector, long idx, long value)
            {
                vector[idx].length = kKeySize - 4;
                vector[idx].value = value;
            }
        }

        #endregion
    }
}