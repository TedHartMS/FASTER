// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FASTER.indexes.HashValueIndex
{
    public class HashValueIndexException : FasterException
    {
        public HashValueIndexException() { }

        public HashValueIndexException(string message) : base(message) { }

        public HashValueIndexException(string message, Exception innerException) : base(message, innerException) { }
    }

    public class HashValueIndexArgumentException : HashValueIndexException
    {
        public HashValueIndexArgumentException() { }

        public HashValueIndexArgumentException(string message) : base(message) { }

        public HashValueIndexArgumentException(string message, Exception innerException) : base(message, innerException) { }
    }

    public class HashValueIndexInvalidOperationException : HashValueIndexException
    {
        public HashValueIndexInvalidOperationException() { }

        public HashValueIndexInvalidOperationException(string message) : base(message) { }

        public HashValueIndexInvalidOperationException(string message, Exception innerException) : base(message, innerException) { }
    }

    public class HashValueIndexInternalErrorException : HashValueIndexException
    {
        public HashValueIndexInternalErrorException() { }

        public HashValueIndexInternalErrorException(string message) : base($"Internal Error: {message}") { }

        public HashValueIndexInternalErrorException(string message, Exception innerException) : base($"Internal Error: {message}", innerException) { }
    }
}
