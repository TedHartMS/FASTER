// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;

namespace FASTER.core
{
    /// <summary>
    /// Carries information about a Primary FasterKV checkpoint. 
    /// </summary>
    /// <remarks>Should be carried by Secondary Index recovery info to  be returned to the SecondaryIndexBroker
    /// so it knows where to start replaying records to the index.</remarks>
    public struct PrimaryCheckpointInfo
    {
        /// <summary>
        /// The database version.
        /// </summary>
        public int Version;

        /// <summary>
        /// The latest immutable address on the Primary FasterKV log at recovery time
        /// </summary>        
        public long FlushedUntilAddress;

        /// <summary>
        /// Constructor
        /// </summary>
        public PrimaryCheckpointInfo(int version, long address)
        {
            this.Version = version;
            this.FlushedUntilAddress = address;
        }

        /// <summary>
        /// Indicates whether this instance has not been assigned values.
        /// </summary>
        public bool IsDefault() => this.Version == 0 && this.FlushedUntilAddress == 0;

        #region Serialization

        const int MetadataVersion = 1;

        /// <summary>
        /// Serialized byte size, including data members, MetadataVersion, and long checksum
        /// </summary>
        public const int SerializedSize = 24;

        /// <summary>
        /// Constructs from a byte array.
        /// </summary>
        public PrimaryCheckpointInfo(byte[] metadata)
        {
            var slice = metadata.Slice(0, 4);
            var metaVersion = BitConverter.ToInt32(slice, 0);
            if (metaVersion != MetadataVersion)
                throw new SecondaryIndexException("Unknown metadata version");
            var offset = slice.Length;

            slice = metadata.Slice(offset, 8);
            var checksum = BitConverter.ToInt64(slice, 0);
            offset += slice.Length;

            slice = metadata.Slice(offset, 4);
            this.Version = BitConverter.ToInt32(slice, 0);
            offset += slice.Length;

            slice = metadata.Slice(offset, 8);
            this.FlushedUntilAddress = BitConverter.ToInt64(slice, 0);
            offset += slice.Length;

            Debug.Assert(offset == SerializedSize);
            if (checksum != Checksum())
                throw new SecondaryIndexException("Invalid checksum for checkpoint");
        }

        /// <summary>
        /// Converts to a byte array for serialization.
        /// </summary>
        /// <returns></returns>
        public byte[] ToByteArray()
        {
            var result = new byte[SerializedSize];
            var bytes = BitConverter.GetBytes(MetadataVersion);
            Array.Copy(bytes, 0, result, 0, bytes.Length);
            var offset = bytes.Length;

            bytes = BitConverter.GetBytes(Checksum());
            Array.Copy(bytes, 0, result, offset, bytes.Length);
            offset += bytes.Length;

            bytes = BitConverter.GetBytes(Version);
            Array.Copy(bytes, 0, result, offset, bytes.Length);
            offset += bytes.Length;

            bytes = BitConverter.GetBytes(FlushedUntilAddress);
            Array.Copy(bytes, 0, result, offset, bytes.Length);
            offset += bytes.Length;

            Debug.Assert(offset == SerializedSize);
            return result;
        }

        private readonly long Checksum()
            => this.Version ^ this.FlushedUntilAddress;

        #endregion Serialization
    }
}
