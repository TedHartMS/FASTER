// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using FASTER.indexes.HashValueIndex;
using NUnit.Framework;
using System;
using System.IO;
using System.Linq;

namespace FASTER.test.HashValueIndex
{
    public class IntKeyComparer : IFasterEqualityComparer<int>
    {
        public long GetHashCode64(ref int key) => Utility.GetHashCode(key);

        public bool Equals(ref int k1, ref int k2) => k1 == k2;
    }

    internal class HashValueIndexTestBase
    {
        // Hash and log sizes
        private const int HashSizeBits = 20;
        private const int MemorySizeBits = 29;
        private const int SegmentSizeBits = 25;
        private const int PageSizeBits = 20;

        internal const int MinRecs = 100;
        internal const int MidRecs = 500;
        internal const int MaxRecs = 5_000;
        internal const int PredMod = 50;        // Lower # means more thrashing on the hash table during insert

        private readonly string testBaseDir;
        private readonly string testDir;
        private IDevice primaryLog;
        private IDevice secondaryLog;

        internal FasterKV<int, int> primaryFkv;
        internal IPredicate[] predicates;
        internal CheckpointManager<int> outerCheckpointManager;
        internal ICheckpointManager innerCheckpointManager;
        internal HashValueIndex<int, int, int> index;

        internal HashValueIndexTestBase(int numPreds, bool filter = false)
        {
            string indexName = filter ? "FilteredIntIndex" : "IntIndex";
            this.testBaseDir = Path.Combine(TestContext.CurrentContext.TestDirectory, TestContext.CurrentContext.Test.ClassName.Split('.').Last());
            this.testDir = Path.Combine(this.testBaseDir, TestContext.CurrentContext.Test.MethodName);
            var primaryDir = Path.Combine(this.testDir, "PrimaryFKV");
            var secondaryDir = Path.Combine(this.testDir, "SecondaryFKV");

            this.primaryLog = Devices.CreateLogDevice(Path.Combine(primaryDir, "hlog.log"));

            var primaryLogSettings = new LogSettings
            {
                LogDevice = primaryLog,
                ObjectLogDevice = default,
                MemorySizeBits = MemorySizeBits,
                SegmentSizeBits = SegmentSizeBits,
                PageSizeBits = PageSizeBits,
                CopyReadsToTail = CopyReadsToTail.None,
                ReadCacheSettings = null
            };

            this.secondaryLog = Devices.CreateLogDevice(Path.Combine(secondaryDir, "hlog.log"));

            var secondaryLogSettings = new LogSettings
            {
                LogDevice = secondaryLog,
                MemorySizeBits = MemorySizeBits,
                SegmentSizeBits = SegmentSizeBits,
                PageSizeBits = PageSizeBits,
                CopyReadsToTail = CopyReadsToTail.None,
                ReadCacheSettings = null
            };

            this.primaryFkv = new FasterKV<int, int>(
                                1L << 20, primaryLogSettings,
                                new CheckpointSettings { CheckpointDir = primaryDir, CheckPointType = CheckpointType.FoldOver });

            var secondaryCheckpointSettings = new CheckpointSettings { CheckpointDir = secondaryDir, CheckPointType = CheckpointType.FoldOver };
            innerCheckpointManager = Utility.CreateDefaultCheckpointManager(secondaryCheckpointSettings);
            this.outerCheckpointManager = new CheckpointManager<int>(indexName, innerCheckpointManager);
            secondaryCheckpointSettings.CheckpointManager = this.outerCheckpointManager;

            var secondaryRegSettings = new RegistrationSettings<int>
            {
                HashTableSize = 1L << HashSizeBits,
                LogSettings = secondaryLogSettings,
                KeyComparer = new IntKeyComparer(),
                CheckpointSettings = secondaryCheckpointSettings
            };

            static string predName(int ord) => $"Pred_{ord}";

            if (filter)
            {
                var preds = Enumerable.Range(0, numPreds).Select<int, (string, Func<int, (bool, int)>)>(ord => (predName(ord), v => PredicateKeyFuncFiltered(v, ord))).ToArray();
                this.index = new HashValueIndex<int, int, int>(indexName, this.primaryFkv, secondaryRegSettings, preds);
            }
            else
            {
                var preds = Enumerable.Range(0, numPreds).Select<int, (string, Func<int, int>)>(ord => (predName(ord), v => PredicateKeyFunc(v, ord))).ToArray();
                this.index = new HashValueIndex<int, int, int>(indexName, this.primaryFkv, secondaryRegSettings, preds);
            }
            this.primaryFkv.SecondaryIndexBroker.AddIndex(this.index);
            this.predicates = Enumerable.Range(0, numPreds).Select(ord => index.GetPredicate(predName(ord))).ToArray();
        }

        static int PredicateKeyFunc(int value, int ordinal) => PredicateShiftKey(value % PredMod, ordinal);

        static (bool, int) PredicateKeyFuncFiltered(int value, int ordinal)
        {
            var mod = value % PredMod;
            return FilterSkip(mod) ? (false, default) : (true, PredicateShiftKey(mod, ordinal));
        }

        internal static bool FilterSkip(int mod) => mod < PredMod / 2;

        // Note: HashValueIndex uses Predicate Ordinal as part of its secondary hash, but do this here as well.
        internal static int PredicateShiftKey(int modKey, int ordinal) => (MaxRecs * ordinal) + modKey;

        internal void TearDown(bool deleteDir)
        {
            this.primaryFkv?.Dispose();
            this.primaryFkv = null;
            this.index?.Dispose();
            this.index = null;
            this.primaryLog.Dispose();
            this.primaryLog = null;
            this.secondaryLog.Dispose();
            this.secondaryLog = null;

            this.outerCheckpointManager?.Dispose();
            this.outerCheckpointManager = null;
            this.innerCheckpointManager?.Dispose();
            this.innerCheckpointManager = null;

            if (deleteDir)
                TestUtils.DeleteDirectory(this.testBaseDir);
        }
    }
}
