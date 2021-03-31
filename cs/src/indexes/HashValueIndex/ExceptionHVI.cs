// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FASTER.indexes.HashValueIndex
{
    /// <summary>
    /// SubsetIndex exception base type
    /// </summary>
    public class ExceptionHVI : FasterException
    {
        public ExceptionHVI() { }

        public ExceptionHVI(string message) : base(message) { }

        public ExceptionHVI(string message, Exception innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// SubsetIndex argument exception type
    /// </summary>
    public class ArgumentExceptionHVI : ExceptionHVI
    {
        public ArgumentExceptionHVI() { }

        public ArgumentExceptionHVI(string message) : base(message) { }

        public ArgumentExceptionHVI(string message, Exception innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// SubsetIndex argument exception type
    /// </summary>
    public class InvalidOperationExceptionHVI : ExceptionHVI
    {
        public InvalidOperationExceptionHVI() { }

        public InvalidOperationExceptionHVI(string message) : base(message) { }

        public InvalidOperationExceptionHVI(string message, Exception innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// SubsetIndex argument exception type
    /// </summary>
    public class InternalErrorExceptionHVI : ExceptionHVI
    {
        public InternalErrorExceptionHVI() { }

        public InternalErrorExceptionHVI(string message) : base($"Internal Error: {message}") { }

        public InternalErrorExceptionHVI(string message, Exception innerException) : base($"Internal Error: {message}", innerException) { }
    }
}
