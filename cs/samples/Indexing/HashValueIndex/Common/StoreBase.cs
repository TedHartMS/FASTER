// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using FASTER.indexes.HashValueIndex;
using System;
using System.Threading.Tasks;

namespace HashValueIndexSampleCommon
{
    class StoreBase : IDisposable
    {
        internal FasterKV<Key, Value> FasterKV { get; set; }
        internal string AppName { get; }

        private LogFiles logFiles;

        internal StoreBase(int numIndexes, string appName) 
        {
            this.logFiles = new LogFiles(numIndexes, appName);
            this.AppName = appName;
            this.FasterKV = new FasterKV<Key, Value>(
                                1L << 20, this.logFiles.LogSettings,
                                new CheckpointSettings { CheckpointDir = logFiles.LogDir, CheckPointType = CheckpointType.FoldOver },
                                null, new Key.Comparer());
        }

        protected RegistrationSettings<TKey> CreateRegistrationSettings<TKey>(int indexOrdinal, IFasterEqualityComparer<TKey> keyComparer)
        {
            var regSettings = new RegistrationSettings<TKey>
            {
                HashTableSize = 1L << LogFiles.HashSizeBits,
                LogSettings = this.logFiles.IndexLogSettings[indexOrdinal],
                CheckpointSettings = new CheckpointSettings { CheckpointDir = $"{logFiles.IndexLogDir(indexOrdinal)}/index{indexOrdinal:D3}", CheckPointType = CheckpointType.FoldOver },
                KeyComparer = keyComparer
            };

            return regSettings;
        }

        internal void AddIndex(ISecondaryIndex index) => this.FasterKV.SecondaryIndexBroker.AddIndex(index);

        internal void RunInitialInserts()
        {
            Console.WriteLine($"Writing keys from 0 to {Constants.KeyCount:N0} to FASTER");

            using var session = this.FasterKV.For(new Functions()).NewSession<Functions>();
            var context = Empty.Default;
            int statusPending = 0;
            int nextId = Constants.InitialId;

            for (int ii = 0; ii < Constants.KeyCount; ++ii)
            {
                // Leave the last value unassigned from each category (we'll use it to update later)
                var key = new Key(nextId++);
                var value = ((ii & 0x3) == 3)
                            ? new Value(key.Id, Species.Cat, ii % 10)
                            : new Value(key.Id, Species.Dog, ii % 10);

                var status = session.Upsert(ref key, ref value, context);
                if (status == Status.PENDING)
                    ++statusPending;
            }

            session.CompletePending(true);

            Console.WriteLine($"Inserted {Constants.KeyCount:N0} elements; {statusPending:N0} pending");
        }

        // This causes the ReadOnlyObserver to be called
        internal void FlushAndEvict() => this.FasterKV.Log.FlushAndEvict(wait: true);

        internal void UpdateCats(QueryRecord<Key, Value>[] catsOfAge)
        {
            Console.WriteLine();
            Console.WriteLine($"Updating {catsOfAge.Length} cats aged {Constants.CatAge} to {Constants.CatAge + Constants.CatAgeIncrement}");

            using var session = this.FasterKV.For(new Functions()).NewSession<Functions>();
            int statusPending = 0;

            foreach (var cat in catsOfAge)
            {
                var value = cat.ValueRef;
                value.Age += 10;
                var status = session.RMW(ref cat.KeyRef, ref value);
                if (status == Status.PENDING)
                    ++statusPending;
            }

            Console.WriteLine($"Update completed with {statusPending:N0} pending");
        }

        internal ValueTask<(bool success, Guid token)> CheckpointAsync() => this.FasterKV.TakeFullCheckpointAsync(CheckpointType.FoldOver);

        internal void Recover() => this.FasterKV.Recover();

        public virtual void Dispose()
        {
            if (!(this.FasterKV is null))
            {
                this.FasterKV.Dispose();
                this.FasterKV = null;
            }
            if (!(this.logFiles is null))
            {
                this.logFiles.Close();
                this.logFiles = null;
            }
        }
    }
}
