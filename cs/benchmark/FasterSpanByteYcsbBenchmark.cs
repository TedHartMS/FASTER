﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

// Define below to enable continuous performance report for dashboard
// #define DASHBOARD

using FASTER.core;
using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTER.benchmark
{
    public class FasterSpanByteYcsbBenchmark
    {
        public enum Op : ulong
        {
            Upsert = 0,
            Read = 1,
            ReadModifyWrite = 2
        }

#if DEBUG
        const bool kDumpDistribution = false;
        const bool kUseSmallData = true;
        const bool kUseSyntheticData = true;
        const bool kSmallMemoryLog = false;
        const bool kAffinitizedSession = true;
        const int kRunSeconds = 30;
        const int kPeriodicCheckpointMilliseconds = 0;
#else
        const bool kDumpDistribution = false;
        const bool kUseSmallData = false;
        const bool kUseSyntheticData = false;
        const bool kSmallMemoryLog = false;
        const bool kAffinitizedSession = true;
        const int kRunSeconds = 30;
        const int kPeriodicCheckpointMilliseconds = 0;
#endif

        // *** Use these to backup and recover database for fast benchmark repeat runs
        // Use BackupMode.Backup to create the backup, unless it was recovered during BackupMode.Recover
        // Use BackupMode.Restore for fast subsequent runs
        // Does NOT work when periodic checkpointing is turned on
        readonly BackupMode backupMode;
        // ***

        const long kInitCount_ = kUseSmallData ? 2500480 : 250000000;
        const long kTxnCount_ = kUseSmallData ? 10000000 : 1000000000;

        // Ensure sizes are aligned to chunk sizes
        const long kInitCount = kChunkSize * (kInitCount_ / kChunkSize);
        const long kTxnCount = kChunkSize * (kTxnCount_ / kChunkSize);

        const int kMaxKey = kUseSmallData ? 1 << 22 : 1 << 28;

        public const int kKeySize = 16;
        public const int kValueSize = 100;

        const int kFileChunkSize = 4096;
        const long kChunkSize = 640;

        KeySpanByte[] init_keys_;

        KeySpanByte[] txn_keys_;

        long idx_ = 0;

        Input[] input_;
        readonly IDevice device;

        readonly FasterKV<SpanByte, SpanByte> store;

        long total_ops_done = 0;

        readonly int threadCount;
        readonly int numaStyle;
        readonly string distribution;
        readonly int readPercent;
        readonly FunctionsSB functions = new FunctionsSB();

        volatile bool done = false;

        public FasterSpanByteYcsbBenchmark(int threadCount_, int numaStyle_, string distribution_, int readPercent_, int backupOptions_)
        {
            threadCount = threadCount_;
            numaStyle = numaStyle_;
            distribution = distribution_;
            readPercent = readPercent_;
            this.backupMode = (BackupMode)backupOptions_;

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

            var path = "D:\\data\\FasterYcsbBenchmark\\";
            device = Devices.CreateLogDevice(path + "hlog", preallocateFile: true);

            if (kSmallMemoryLog)
                store = new FasterKV<SpanByte, SpanByte>
                    (kMaxKey / 2, new LogSettings { LogDevice = device, PreallocateLog = true, PageSizeBits = 22, SegmentSizeBits = 26, MemorySizeBits = 26 }, new CheckpointSettings { CheckPointType = CheckpointType.FoldOver, CheckpointDir = path });
            else
                store = new FasterKV<SpanByte, SpanByte>
                    (kMaxKey / 2, new LogSettings { LogDevice = device, PreallocateLog = true, MemorySizeBits = 35 }, new CheckpointSettings { CheckPointType = CheckpointType.FoldOver, CheckpointDir = path });
        }

        private void RunYcsb(int thread_idx)
        {
            RandomGenerator rng = new RandomGenerator((uint)(1 + thread_idx));

            if (numaStyle == 0)
                Native32.AffinitizeThreadRoundRobin((uint)thread_idx);
            else
                Native32.AffinitizeThreadShardedNuma((uint)thread_idx, 2); // assuming two NUMA sockets

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
                long chunk_idx = Interlocked.Add(ref idx_, kChunkSize) - kChunkSize;
                while (chunk_idx >= kTxnCount)
                {
                    if (chunk_idx == kTxnCount)
                        idx_ = 0;
                    chunk_idx = Interlocked.Add(ref idx_, kChunkSize) - kChunkSize;
                }

                for (long idx = chunk_idx; idx < chunk_idx + kChunkSize && !done; ++idx)
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

        public unsafe void Run()
        {
            //Native32.AffinitizeThreadShardedNuma(0, 2);

            RandomGenerator rng = new RandomGenerator();

            LoadData();

            input_ = new Input[8];
            for (int i = 0; i < 8; i++)
                input_[i].value = i;

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
                sw.Start();
                try
                {
                    Console.WriteLine("Recovering FasterKV for fast restart");
                    store.Recover();
                    storeWasRecovered = true;
                } catch (Exception)
                {
                    Console.WriteLine("Unable to recover prior store");
                }
                sw.Stop();
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

                sw.Start();
                // Start threads.
                foreach (Thread worker in workers)
                {
                    worker.Start();
                }
                foreach (Thread worker in workers)
                {
                    worker.Join();
                }
                sw.Stop();
            }
            Console.WriteLine("Loading time: {0}ms", sw.ElapsedMilliseconds);

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

            Stopwatch swatch = new Stopwatch();
            swatch.Start();

            if (kPeriodicCheckpointMilliseconds <= 0)
            {
                Thread.Sleep(TimeSpan.FromSeconds(kRunSeconds));
            }
            else
            {
                var checkpointTaken = 0;
                while (swatch.ElapsedMilliseconds < 1000 * kRunSeconds)
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

#if DASHBOARD
            dash.Join();
#endif

            double seconds = swatch.ElapsedMilliseconds / 1000.0;
            long endTailAddress = store.Log.TailAddress;
            Console.WriteLine("End tail address = " + endTailAddress);

            Console.WriteLine("Total " + total_ops_done + " ops done " + " in " + seconds + " secs.");
            Console.WriteLine("##, " + distribution + ", " + numaStyle + ", " + readPercent + ", "
                + threadCount + ", " + total_ops_done / seconds + ", "
                + (endTailAddress - startTailAddress));
            device.Dispose();
        }

        private void SetupYcsb(int thread_idx)
        {
            if (numaStyle == 0)
                Native32.AffinitizeThreadRoundRobin((uint)thread_idx);
            else
                Native32.AffinitizeThreadShardedNuma((uint)thread_idx, 2); // assuming two NUMA sockets

            var session = store.For(functions).NewSession<FunctionsSB>(null, kAffinitizedSession);

#if DASHBOARD
            var tstart = Stopwatch.GetTimestamp();
            var tstop1 = tstart;
            var lastWrittenValue = 0;
            int count = 0;
#endif

            Span<byte> value = stackalloc byte[kValueSize];
            ref SpanByte _value = ref SpanByte.Reinterpret(value);

            for (long chunk_idx = Interlocked.Add(ref idx_, kChunkSize) - kChunkSize;
                chunk_idx < kInitCount;
                chunk_idx = Interlocked.Add(ref idx_, kChunkSize) - kChunkSize)
            {
                for (long idx = chunk_idx; idx < chunk_idx + kChunkSize; ++idx)
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

        private unsafe void LoadDataFromFile(string filePath)
        {
            string init_filename = filePath + "/load_" + distribution + "_250M_raw.dat";
            string txn_filename = filePath + "/run_" + distribution + "_250M_1000M_raw.dat";

            long count = 0;
            using (FileStream stream = File.Open(init_filename, FileMode.Open, FileAccess.Read,
                FileShare.Read))
            {
                Console.WriteLine("loading keys from " + init_filename + " into memory...");
                init_keys_ = new KeySpanByte[kInitCount];

                byte[] chunk = new byte[kFileChunkSize];
                GCHandle chunk_handle = GCHandle.Alloc(chunk, GCHandleType.Pinned);
                byte* chunk_ptr = (byte*)chunk_handle.AddrOfPinnedObject();

                long offset = 0;

                while (true)
                {
                    stream.Position = offset;
                    int size = stream.Read(chunk, 0, kFileChunkSize);
                    for (int idx = 0; idx < size; idx += 8)
                    {
                        init_keys_[count].length = kKeySize - 4;
                        init_keys_[count].value = *(long*)(chunk_ptr + idx);
                        ++count;
                        if (count == kInitCount)
                            break;
                    }
                    if (size == kFileChunkSize)
                        offset += kFileChunkSize;
                    else
                        break;

                    if (count == kInitCount)
                        break;
                }

                if (count != kInitCount)
                {
                    throw new InvalidDataException("Init file load fail!");
                }
            }

            Console.WriteLine("loaded " + kInitCount + " keys.");


            using (FileStream stream = File.Open(txn_filename, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                byte[] chunk = new byte[kFileChunkSize];
                GCHandle chunk_handle = GCHandle.Alloc(chunk, GCHandleType.Pinned);
                byte* chunk_ptr = (byte*)chunk_handle.AddrOfPinnedObject();

                Console.WriteLine("loading txns from " + txn_filename + " into memory...");

                txn_keys_ = new KeySpanByte[kTxnCount];

                count = 0;
                long offset = 0;

                while (true)
                {
                    stream.Position = offset;
                    int size = stream.Read(chunk, 0, kFileChunkSize);
                    for (int idx = 0; idx < size; idx += 8)
                    {
                        txn_keys_[count].length = kKeySize - 4;
                        txn_keys_[count].value = *(long*)(chunk_ptr + idx);
                        ++count;
                        if (count == kTxnCount)
                            break;
                    }
                    if (size == kFileChunkSize)
                        offset += kFileChunkSize;
                    else
                        break;

                    if (count == kTxnCount)
                        break;
                }

                if (count != kTxnCount)
                {
                    throw new InvalidDataException("Txn file load fail!" + count + ":" + kTxnCount);
                }
            }

            Console.WriteLine("loaded " + kTxnCount + " txns.");
        }

        private void LoadData()
        {
            if (kUseSyntheticData)
            {
                LoadSyntheticData();
                return;
            }

            string filePath = "C:\\ycsb_files";

            if (!Directory.Exists(filePath))
            {
                filePath = "D:\\ycsb_files";
            }
            if (!Directory.Exists(filePath))
            {
                filePath = "E:\\ycsb_files";
            }

            if (Directory.Exists(filePath))
            {
                LoadDataFromFile(filePath);
            }
            else
            {
                Console.WriteLine("WARNING: Could not find YCSB directory, loading synthetic data instead");
                LoadSyntheticData();
            }
        }

        private void LoadSyntheticData()
        {
            Console.WriteLine("Loading synthetic data (uniform distribution)");

            init_keys_ = new KeySpanByte[kInitCount];
            long val = 0;
            for (int idx = 0; idx < kInitCount; idx++)
            {
                init_keys_[idx] = new KeySpanByte { length = kKeySize - 4, value = val++ };
            }

            Console.WriteLine("loaded " + kInitCount + " keys.");

            RandomGenerator generator = new RandomGenerator();

            txn_keys_ = new KeySpanByte[kTxnCount];

            for (int idx = 0; idx < kTxnCount; idx++)
            {
                txn_keys_[idx] = new KeySpanByte { length = kKeySize - 4, value = (long)generator.Generate64(kInitCount) };
            }

            Console.WriteLine("loaded " + kTxnCount + " txns.");

        }
        #endregion


    }
}