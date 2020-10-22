﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    /// <summary>
    /// Output that encapsulates sync stack output (via SpanByte) and async heap output (via IMemoryOwner)
    /// </summary>
    public unsafe struct SpanByteAndMemory : IHeapConvertible
    {
        /// <summary>
        /// Stack output as SpanByte
        /// </summary>
        public SpanByte SpanByte;

        /// <summary>
        /// Heap output as IMemoryOwner
        /// </summary>
        public IMemoryOwner<byte> Memory;

        /// <summary>
        /// Constructor using given SpanByte
        /// </summary>
        /// <param name="spanByte"></param>
        public SpanByteAndMemory(SpanByte spanByte)
        {
            SpanByte = spanByte;
            Memory = default;
        }

        /// <summary>
        /// Get length
        /// </summary>
        public int Length
        {
            get => SpanByte.Length;
            set => SpanByte.Length = value;
        }

        /// <summary>
        /// Constructor using given IMemoryOwner
        /// </summary>
        /// <param name="memory"></param>
        public SpanByteAndMemory(IMemoryOwner<byte> memory)
        {
            SpanByte = default;
            SpanByte.Invalid = true;
            Memory = memory;
        }

        /// <summary>
        /// View a fixed Span&lt;byte&gt; as a SpanByteAndMemory
        /// </summary>
        /// <param name="span"></param>
        /// <returns></returns>
        public static SpanByteAndMemory FromFixedSpan(Span<byte> span)
        {
            return new SpanByteAndMemory { SpanByte = SpanByte.FromFixedSpan(span) };
        }


        /// <summary>
        /// Convert to be used on heap (IMemoryOwner)
        /// </summary>
        public void ConvertToHeap() { SpanByte.Invalid = true; }

        /// <summary>
        /// Is it allocated as SpanByte (on stack)?
        /// </summary>
        public bool IsSpanByte => !SpanByte.Invalid;
    }
}
