﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 1591

using System;

namespace FASTER.core
{
    /// <summary>
    /// Default empty functions base class to make it easy for users to provide their own implementation of IFunctions
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    /// <typeparam name="Output"></typeparam>
    /// <typeparam name="Context"></typeparam>
    public abstract class FunctionsBase<Key, Value, Input, Output, Context> : IFunctions<Key, Value, Input, Output, Context>
    {
        public virtual void ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst) { }
        public virtual void SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst) { }

        public virtual bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst) { dst = src; return true; }
        public virtual void SingleWriter(ref Key key, ref Value src, ref Value dst) => dst = src;

        public virtual void InitialUpdater(ref Key key, ref Input input, ref Value value) { }
        public virtual bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue) => true;
        public virtual void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue) { }
        public virtual bool InPlaceUpdater(ref Key key, ref Input input, ref Value value) { return true; }

        public virtual void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status) { }
        public virtual void RMWCompletionCallback(ref Key key, ref Input input, Context ctx, Status status) { }
        public virtual void UpsertCompletionCallback(ref Key key, ref Value value, Context ctx) { }
        public virtual void DeleteCompletionCallback(ref Key key, Context ctx) { }
        public virtual void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) { }

#if !NETSTANDARD2_1
        public bool SupportsLocks => false;
        public virtual void Lock(ref RecordInfo recordInfo, ref Key key, ref Value value) { }
        public virtual void Unlock(ref RecordInfo recordInfo, ref Key key, ref Value value) { }
#endif
    }

    /// <summary>
    /// Default empty functions base class to make it easy for users to provide their own implementation of FunctionsBase
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Context"></typeparam>
    public class SimpleFunctions<Key, Value, Context> : FunctionsBase<Key, Value, Value, Value, Context>
    {
        private readonly Func<Value, Value, Value> merger;
        public SimpleFunctions() => merger = (l, r) => l;
        public SimpleFunctions(Func<Value, Value, Value> merger) => this.merger = merger;

        public override void ConcurrentReader(ref Key key, ref Value input, ref Value value, ref Value dst) => dst = value;
        public override void SingleReader(ref Key key, ref Value input, ref Value value, ref Value dst) => dst = value;

        public override bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst) { dst = src; return true; }
        public override void SingleWriter(ref Key key, ref Value src, ref Value dst) => dst = src;

        public override void InitialUpdater(ref Key key, ref Value input, ref Value value) => value = input;
        public override void CopyUpdater(ref Key key, ref Value input, ref Value oldValue, ref Value newValue) => newValue = merger(input, oldValue);
        public override bool InPlaceUpdater(ref Key key, ref Value input, ref Value value) { value = merger(input, value); return true; }

        public override void ReadCompletionCallback(ref Key key, ref Value input, ref Value output, Context ctx, Status status) { }
        public override void RMWCompletionCallback(ref Key key, ref Value input, Context ctx, Status status) { }
        public override void UpsertCompletionCallback(ref Key key, ref Value value, Context ctx) { }
        public override void DeleteCompletionCallback(ref Key key, Context ctx) { }
        public override void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) { }
    }

    public class SimpleFunctions<Key, Value> : SimpleFunctions<Key, Value, Empty>
    {
        public SimpleFunctions() : base() { }
        public SimpleFunctions(Func<Value, Value, Value> merger) : base(merger) { }
    }

    /// <summary>
    /// Default empty functions base class to make it easy for users to provide their own implementation of IAdvancedFunctions
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    /// <typeparam name="Output"></typeparam>
    /// <typeparam name="Context"></typeparam>
    public abstract class AdvancedFunctionsBase<Key, Value, Input, Output, Context> : IAdvancedFunctions<Key, Value, Input, Output, Context>
    {
        public virtual void ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, long address) { }
        public virtual void SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, long address) { }

        public virtual bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst, long address) { dst = src; return true; }
        public virtual void SingleWriter(ref Key key, ref Value src, ref Value dst, long address) => dst = src;

        public virtual void InitialUpdater(ref Key key, ref Input input, ref Value value, long address) { }
        public virtual bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue) => true;
        public virtual void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, long address) { }
        public virtual bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, long address) { return true; }

        public virtual bool ConcurrentDeleter(ref Key key, ref Value value, long address) { return false; }

        public virtual void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordInfo recordInfo) { }
        public virtual void RMWCompletionCallback(ref Key key, ref Input input, Context ctx, Status status) { }
        public virtual void UpsertCompletionCallback(ref Key key, ref Value value, Context ctx) { }
        public virtual void DeleteCompletionCallback(ref Key key, Context ctx) { }
        public virtual void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) { }

        public bool SupportsLocks => false;
        public virtual void Lock(ref RecordInfo recordInfo, ref Key key, ref Value value) { }
        public virtual void Unlock(ref RecordInfo recordInfo, ref Key key, ref Value value) { }
    }

    /// <summary>
    /// Default empty functions base class to make it easy for users to provide their own implementation of AdvancedFunctionsBase
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Context"></typeparam>
    public class AdvancedSimpleFunctions<Key, Value, Context> : AdvancedFunctionsBase<Key, Value, Value, Value, Context>
    {
        private readonly Func<Value, Value, Value> merger;
        public AdvancedSimpleFunctions() => merger = (l, r) => l;
        public AdvancedSimpleFunctions(Func<Value, Value, Value> merger) => this.merger = merger;

        public override void ConcurrentReader(ref Key key, ref Value input, ref Value value, ref Value dst, long address) => dst = value;
        public override void SingleReader(ref Key key, ref Value input, ref Value value, ref Value dst, long address) => dst = value;

        public override bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst, long address) { dst = src; return true; }
        public override void SingleWriter(ref Key key, ref Value src, ref Value dst, long address) => dst = src;

        public override void InitialUpdater(ref Key key, ref Value input, ref Value value, long address) => value = input;
        public override void CopyUpdater(ref Key key, ref Value input, ref Value oldValue, ref Value newValue, long address) => newValue = merger(input, oldValue);
        public override bool InPlaceUpdater(ref Key key, ref Value input, ref Value value, long address) { value = merger(input, value); return true; }

        public override void ReadCompletionCallback(ref Key key, ref Value input, ref Value output, Context ctx, Status status, RecordInfo recordInfo) { }
        public override void RMWCompletionCallback(ref Key key, ref Value input, Context ctx, Status status) { }
        public override void UpsertCompletionCallback(ref Key key, ref Value value, Context ctx) { }
        public override void DeleteCompletionCallback(ref Key key, Context ctx) { }
        public override void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) { }
    }

    public class AdvancedSimpleFunctions<Key, Value> : AdvancedSimpleFunctions<Key, Value, Empty>
    {
        public AdvancedSimpleFunctions() : base() { }
        public AdvancedSimpleFunctions(Func<Value, Value, Value> merger) : base(merger) { }
    }
}