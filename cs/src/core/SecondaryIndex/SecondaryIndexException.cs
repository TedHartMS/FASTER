// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.Serialization;

namespace FASTER.core
{
    /// <summary>
    /// FASTER exception base type
    /// </summary>
    public class SecondaryIndexException : FasterException
    {
        /// <summary/>
        public SecondaryIndexException()
        {
        }

        /// <summary/>
        public SecondaryIndexException(string message) : base(message)
        {
        }

        /// <summary/>
        public SecondaryIndexException(string message, Exception innerException) : base(message, innerException)
        {
        }

        /// <summary/>
        public SecondaryIndexException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}