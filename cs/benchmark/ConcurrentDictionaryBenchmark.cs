// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

//#define DASHBOARD

using FASTER.core;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTER.benchmark
{
    public class KeyComparer : IEqualityComparer<Key>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(Key x, Key y)
        {
            return x.value == y.value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(Key obj)
        {
            return (int)Utility.GetHashCode(obj.value);
        }
    }

    public unsafe class ConcurrentDictionary_YcsbBenchmark
    {
        const int kFileChunkSize = 4096;
        const long kChunkSize = 640;

        const int kRunSeconds = 30;
        const int kCheckpointSeconds = -1;

        readonly int threadCount;
        readonly int numaStyle;
        readonly string distribution;
        readonly int readPercent;
        readonly Input[] input_;

        readonly Key[] init_keys_;
        readonly Key[] txn_keys_;

        readonly ConcurrentDictionary<Key, Value> store;

        long idx_ = 0;
        long total_ops_done = 0;
        volatile bool done = false;
        Input* input_ptr;

        public ConcurrentDictionary_YcsbBenchmark(Key[] i_keys_, Key[] t_keys_, int threadCount_, int numaStyle_, string distribution_, int readPercent_)
        {
            init_keys_ = i_keys_;
            txn_keys_ = t_keys_;
            threadCount = threadCount_;
            numaStyle = numaStyle_;
            distribution = distribution_;
            readPercent = readPercent_;

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

            store = new ConcurrentDictionary<Key, Value>(threadCount, YcsbGlobals.kMaxKey, new KeyComparer());
        }

        public void Dispose()
        {
            store.Clear();
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

            Value value = default;
            long reads_done = 0;
            long writes_done = 0;

#if DASHBOARD
            var tstart = Stopwatch.GetTimestamp();
            var tstop1 = tstart;
            var lastWrittenValue = 0;
            int count = 0;
#endif

            while (!done)
            {
                long chunk_idx = Interlocked.Add(ref idx_, kChunkSize) - kChunkSize;
                while (chunk_idx >= YcsbGlobals.kTxnCount)
                {
                    if (chunk_idx == YcsbGlobals.kTxnCount)
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

                    switch (op)
                    {
                        case Op.Upsert:
                            {
                                store[txn_keys_[idx]] = value;
                                ++writes_done;
                                break;
                            }
                        case Op.Read:
                            {
                                if (store.TryGetValue(txn_keys_[idx], out value))
                                {
                                    ++reads_done;
                                }
                                break;
                            }
                        case Op.ReadModifyWrite:
                            {
                                store.AddOrUpdate(txn_keys_[idx], *(Value*)(input_ptr + (idx & 0x7)), (k, v) => new Value { value = v.value + (input_ptr + (idx & 0x7))->value });
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

            sw.Stop();

            Console.WriteLine("Thread " + thread_idx + " done; " + reads_done + " reads, " +
                writes_done + " writes, in " + sw.ElapsedMilliseconds + " ms.");
            Interlocked.Add(ref total_ops_done, reads_done + writes_done);
        }

        public unsafe (double, double) Run()
        {
            RandomGenerator rng = new RandomGenerator();

            GCHandle handle = GCHandle.Alloc(input_, GCHandleType.Pinned);
            input_ptr = (Input*)handle.AddrOfPinnedObject();

#if DASHBOARD
            var dash = new Thread(() => DoContinuousMeasurements());
            dash.Start();
#endif

            Thread[] workers = new Thread[threadCount];

            Console.WriteLine("Executing setup.");

            // Setup the store for the YCSB benchmark.
            for (int idx = 0; idx < threadCount; ++idx)
            {
                int x = idx;
                workers[idx] = new Thread(() => SetupYcsb(x));
            }
            Stopwatch sw = new Stopwatch();
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

            double insertsPerSecond = ((double)YcsbGlobals.kInitCount / sw.ElapsedMilliseconds) * 1000;
            Console.WriteLine(YcsbGlobals.LoadingTimeLine(insertsPerSecond, sw.ElapsedMilliseconds));

            idx_ = 0;

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

            if (kCheckpointSeconds <= 0)
            {
                Thread.Sleep(TimeSpan.FromSeconds(kRunSeconds));
            }
            else
            {
                int runSeconds = 0;
                while (runSeconds < kRunSeconds)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(kCheckpointSeconds));
                    runSeconds += kCheckpointSeconds;
                }
            }

            swatch.Stop();

            done = true;

            foreach (Thread worker in workers)
            {
                worker.Join();
            }

#if DASHBOARD
            dash.Abort();
#endif

            handle.Free();
            input_ptr = null;

            double seconds = swatch.ElapsedMilliseconds / 1000.0;

            double opsPerSecond = total_ops_done / seconds;
            Console.WriteLine(YcsbGlobals.TotalOpsString(total_ops_done, seconds));
            Console.WriteLine(YcsbGlobals.StatsLine(StatsLine.Iteration, YcsbGlobals.OpsPerSec, opsPerSecond));
            return (insertsPerSecond, opsPerSecond);
        }

        private void SetupYcsb(int thread_idx)
        {
            if (numaStyle == 0)
                Native32.AffinitizeThreadRoundRobin((uint)thread_idx);
            else
                Native32.AffinitizeThreadShardedNuma((uint)thread_idx, 2); // assuming two NUMA sockets

#if DASHBOARD
            var tstart = Stopwatch.GetTimestamp();
            var tstop1 = tstart;
            var lastWrittenValue = 0;
            int count = 0;
#endif

            Value value = default;

            for (long chunk_idx = Interlocked.Add(ref idx_, kChunkSize) - kChunkSize;
                chunk_idx < YcsbGlobals.kInitCount;
                chunk_idx = Interlocked.Add(ref idx_, kChunkSize) - kChunkSize)
            {
                for (long idx = chunk_idx; idx < chunk_idx + kChunkSize; ++idx)
                {

                    Key key = init_keys_[idx];
                    store[key] = value;
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
        }

#if DASHBOARD
        int measurementInterval = 2000;
        bool allDone;
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

            if (numaStyle == 0)
                Native32.AffinitizeThreadRoundRobin((uint)threadCount + 1);
            else
                Native32.AffinitizeThreadShardedTwoNuma((uint)threadCount + 1);

            double totalThroughput, totalLatency, maximumLatency;
            double totalProgress;
            int ver = 0;

            using (var client = new WebClient())
            {
                while (!allDone)
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
                        Console.WriteLine("{0} \t {1:0.000} \t {2} \t {3} \t {4} \t {5}", ver, totalThroughput / (double)1000000, totalLatency / threadCount, maximumLatency, store.Count, totalProgress);
                    }
                    else
                    {
                        Console.WriteLine("{0} \t {1:0.000} \t {2} \t {3}", ver, totalThroughput / (double)1000000, store.Count, totalProgress);
                    }
                }
            }
        }
#endif

        #region Load Data

        private static void LoadDataFromFile(string filePath, string distribution, out Key[] i_keys, out Key[] t_keys)
        {
            string init_filename = filePath + "/load_" + distribution + "_250M_raw.dat";
            string txn_filename = filePath + "/run_" + distribution + "_250M_1000M_raw.dat";

            long count = 0;
            using (FileStream stream = File.Open(init_filename, FileMode.Open, FileAccess.Read,
                FileShare.Read))
            {
                Console.WriteLine("loading keys from " + init_filename + " into memory...");
                i_keys = new Key[YcsbGlobals.kInitCount];

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
                        i_keys[count].value = *(long*)(chunk_ptr + idx);
                        ++count;
                    }
                    if (size == kFileChunkSize)
                        offset += kFileChunkSize;
                    else
                        break;

                    if (count == YcsbGlobals.kInitCount)
                        break;
                }

                if (count != YcsbGlobals.kInitCount)
                {
                    throw new InvalidDataException("Init file load fail!");
                }
            }

            Console.WriteLine("loaded " + YcsbGlobals.kInitCount + " keys.");


            using (FileStream stream = File.Open(txn_filename, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                byte[] chunk = new byte[kFileChunkSize];
                GCHandle chunk_handle = GCHandle.Alloc(chunk, GCHandleType.Pinned);
                byte* chunk_ptr = (byte*)chunk_handle.AddrOfPinnedObject();

                Console.WriteLine("loading txns from " + txn_filename + " into memory...");

                t_keys = new Key[YcsbGlobals.kTxnCount];

                count = 0;
                long offset = 0;

                while (true)
                {
                    stream.Position = offset;
                    int size = stream.Read(chunk, 0, kFileChunkSize);
                    for (int idx = 0; idx < size; idx += 8)
                    {
                        t_keys[count] = *(Key*)(chunk_ptr + idx);
                        ++count;
                    }
                    if (size == kFileChunkSize)
                        offset += kFileChunkSize;
                    else
                        break;

                    if (count == YcsbGlobals.kTxnCount)
                        break;
                }

                if (count != YcsbGlobals.kTxnCount)
                {
                    throw new InvalidDataException("Txn file load fail!" + count + ":" + YcsbGlobals.kTxnCount);
                }
            }

            Console.WriteLine("loaded " + YcsbGlobals.kTxnCount + " txns.");
        }

        public static void LoadData(string distribution, uint seed, out Key[] i_keys, out Key[] t_keys)
        {
            if (YcsbGlobals.kUseSyntheticData || YcsbGlobals.kUseSmallData)
            {
                if (!YcsbGlobals.kUseSyntheticData)
                    Console.WriteLine("WARNING: Forcing synthetic data due to kSmallData");
                LoadSyntheticData(distribution, seed, out i_keys, out t_keys);
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
                LoadDataFromFile(filePath, distribution, out i_keys, out t_keys);
                return;
            }
            else
            {
                Console.WriteLine("WARNING: Could not find YCSB directory, loading synthetic data instead");
                LoadSyntheticData(distribution, seed, out i_keys, out t_keys);
            }
        }

        private static void LoadSyntheticData(string distribution, uint seed, out Key[] i_keys, out Key[] t_keys)
        {
            Console.WriteLine($"Loading synthetic data ({distribution} distribution), seed = {seed}");

            i_keys = new Key[YcsbGlobals.kInitCount];
            long val = 0;
            for (int idx = 0; idx < YcsbGlobals.kInitCount; idx++)
            {
                i_keys[idx] = new Key { value = val++ };
            }

            Console.WriteLine("loaded " + YcsbGlobals.kInitCount + " keys.");

            RandomGenerator generator = new RandomGenerator(seed);
            var zipf = new ZipfGenerator(generator, (int)YcsbGlobals.kInitCount, theta: 0.99);

            t_keys = new Key[YcsbGlobals.kTxnCount];

            for (int idx = 0; idx < YcsbGlobals.kTxnCount; idx++)
            {
                var rand = distribution == YcsbGlobals.UniformDist ? (long)generator.Generate64(YcsbGlobals.kInitCount) : zipf.Next();
                t_keys[idx] = new Key { value = rand };
            }

            Console.WriteLine("loaded " + YcsbGlobals.kTxnCount + " txns.");

        }
        #endregion


    }
}
