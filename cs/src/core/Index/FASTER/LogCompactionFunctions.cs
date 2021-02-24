﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    internal sealed class LogVariableCompactFunctions<Key, Value, CompactionFunctions> : IFunctions<Key, Value, Empty, Empty, Empty>
        where CompactionFunctions : ICompactionFunctions<Key, Value>
    {
        private readonly VariableLengthBlittableAllocator<Key, Value> _allocator;
        private readonly CompactionFunctions _functions;

        public LogVariableCompactFunctions(VariableLengthBlittableAllocator<Key, Value> allocator, CompactionFunctions functions)
        {
            _allocator = allocator;
            _functions = functions;
        }

        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) { }
        public void ConcurrentReader(ref Key key, ref Empty input, ref Value value, ref Empty dst) { }
        public bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst) { return _functions.CopyInPlace(ref src, ref dst, _allocator.ValueLength); }
        public bool NeedCopyUpdate(ref Key key, ref Empty input, ref Value oldValue) => true;
        public void CopyUpdater(ref Key key, ref Empty input, ref Value oldValue, ref Value newValue) { }
        public void InitialUpdater(ref Key key, ref Empty input, ref Value value) { }
        public bool InPlaceUpdater(ref Key key, ref Empty input, ref Value value) => false;
        public void ReadCompletionCallback(ref Key key, ref Empty input, ref Empty output, Empty ctx, Status status) { }
        public void RMWCompletionCallback(ref Key key, ref Empty input, Empty ctx, Status status) { }
        public void SingleReader(ref Key key, ref Empty input, ref Value value, ref Empty dst) { }
        public void SingleWriter(ref Key key, ref Value src, ref Value dst) { _functions.Copy(ref src, ref dst, _allocator.ValueLength); }
        public void UpsertCompletionCallback(ref Key key, ref Value value, Empty ctx) { }
        public void DeleteCompletionCallback(ref Key key, Empty ctx) { }
        public bool SupportsLocks => false;
        public void Lock(ref RecordInfo recordInfo, ref Key key, ref Value value, OperationType opType, ref long context) { }
        public bool Unlock(ref RecordInfo recordInfo, ref Key key, ref Value value, OperationType opType, long context) => true;
    }

    internal sealed class LogCompactFunctions<Key, Value, CompactionFunctions> : IFunctions<Key, Value, Empty, Empty, Empty>
        where CompactionFunctions : ICompactionFunctions<Key, Value>
    {
        private readonly CompactionFunctions _functions;

        public LogCompactFunctions(CompactionFunctions functions)
        {
            _functions = functions;
        }

        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) { }
        public void ConcurrentReader(ref Key key, ref Empty input, ref Value value, ref Empty dst) { }
        public bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst) { return _functions.CopyInPlace(ref src, ref dst, null); }
        public bool NeedCopyUpdate(ref Key key, ref Empty input, ref Value oldValue) => true;
        public void CopyUpdater(ref Key key, ref Empty input, ref Value oldValue, ref Value newValue) { }
        public void InitialUpdater(ref Key key, ref Empty input, ref Value value) { }
        public bool InPlaceUpdater(ref Key key, ref Empty input, ref Value value) { return true; }
        public void ReadCompletionCallback(ref Key key, ref Empty input, ref Empty output, Empty ctx, Status status) { }
        public void RMWCompletionCallback(ref Key key, ref Empty input, Empty ctx, Status status) { }
        public void SingleReader(ref Key key, ref Empty input, ref Value value, ref Empty dst) { }
        public void SingleWriter(ref Key key, ref Value src, ref Value dst) { _functions.Copy(ref src, ref dst, null); }
        public void UpsertCompletionCallback(ref Key key, ref Value value, Empty ctx) { }
        public void DeleteCompletionCallback(ref Key key, Empty ctx) { }
        public bool SupportsLocks => false;
        public void Lock(ref RecordInfo recordInfo, ref Key key, ref Value value, OperationType opType, ref long context) { }
        public bool Unlock(ref RecordInfo recordInfo, ref Key key, ref Value value, OperationType opType, long context) => true;
    }

    internal unsafe struct DefaultVariableCompactionFunctions<Key, Value> : ICompactionFunctions<Key, Value>
    {
        public void Copy(ref Value src, ref Value dst, IVariableLengthStruct<Value> valueLength)
        {
            var srcLength = valueLength.GetLength(ref src);
            Buffer.MemoryCopy(
                Unsafe.AsPointer(ref src),
                Unsafe.AsPointer(ref dst),
                srcLength,
                srcLength);
        }

        public bool CopyInPlace(ref Value src, ref Value dst, IVariableLengthStruct<Value> valueLength)
        {
            var srcLength = valueLength.GetLength(ref src);
            var dstLength = valueLength.GetLength(ref dst);
            if (srcLength != dstLength)
                return false;

            Buffer.MemoryCopy(
                Unsafe.AsPointer(ref src),
                Unsafe.AsPointer(ref dst),
                dstLength,
                srcLength);

            return true;
        }

        public bool IsDeleted(in Key key, in Value value)
        {
            return false;
        }
    }

    internal unsafe struct DefaultReadOnlyMemoryCompactionFunctions<T> : ICompactionFunctions<ReadOnlyMemory<T>, Memory<T>> where T : unmanaged
    {
        public void Copy(ref Memory<T> src, ref Memory<T> dst, IVariableLengthStruct<Memory<T>> valueLength)
        {
            src.CopyTo(dst);
        }

        public bool CopyInPlace(ref Memory<T> src, ref Memory<T> dst, IVariableLengthStruct<Memory<T>> valueLength)
        {
            if (src.Length > dst.Length) return false;
            src.CopyTo(dst);
            dst.ShrinkSerializedLength(src.Length);
            return true;
        }

        public bool IsDeleted(in ReadOnlyMemory<T> key, in Memory<T> value)
        {
            return false;
        }
    }

    internal unsafe struct DefaultMemoryCompactionFunctions<T> : ICompactionFunctions<Memory<T>, Memory<T>> where T : unmanaged
    {
        public void Copy(ref Memory<T> src, ref Memory<T> dst, IVariableLengthStruct<Memory<T>> valueLength)
        {
            src.CopyTo(dst);
        }

        public bool CopyInPlace(ref Memory<T> src, ref Memory<T> dst, IVariableLengthStruct<Memory<T>> valueLength)
        {
            if (src.Length > dst.Length) return false;
            src.CopyTo(dst);
            dst.ShrinkSerializedLength(src.Length);
            return true;
        }

        public bool IsDeleted(in Memory<T> key, in Memory<T> value)
        {
            return false;
        }
    }


    internal struct DefaultCompactionFunctions<Key, Value> : ICompactionFunctions<Key, Value>
    {
        public void Copy(ref Value src, ref Value dst, IVariableLengthStruct<Value> valueLength)
        {
            dst = src;
        }

        public bool CopyInPlace(ref Value src, ref Value dst, IVariableLengthStruct<Value> valueLength)
        {
            dst = src;
            return true;
        }

        public bool IsDeleted(in Key key, in Value value)
        {
            return false;
        }
    }
}