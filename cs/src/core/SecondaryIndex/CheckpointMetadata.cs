// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;

namespace FASTER.core
{
    internal class CheckpointMetadata
    {
        const int MetadataVersion = 1;

        // For use with Checkpoint
        readonly List<byte[]> byteVectors;

        // Populated by Restore
        readonly Dictionary<Guid, Guid> tokenDictionary;

        internal CheckpointMetadata() 
            => this.byteVectors = new List<byte[]>() { BitConverter.GetBytes(MetadataVersion) };

        internal CheckpointMetadata(byte[] flattened)
        {
            this.tokenDictionary = new Dictionary<Guid, Guid>();
            if (flattened.Length < 4)
                return;
            var version = BitConverter.ToInt32(flattened, 0);
            if (version != MetadataVersion)
                throw new SecondaryIndexException("Invalid checkpoint metadata version");
            var offset = 4;
            while (offset < flattened.Length - 32)
            {
                var id = new Guid(flattened.Slice(offset, 16));
                offset += 16;
                var token = new Guid(flattened.Slice(offset, 16));
                offset += 16;
                tokenDictionary[id] = token;
            }
            if (offset != flattened.Length)
                throw new SecondaryIndexException("Invalid checkpoint metadata length");
        }

        internal void Append(Guid id, Guid token)
        {
            this.byteVectors.Add(id.ToByteArray());
            this.byteVectors.Add(token.ToByteArray());
        }

        internal byte[] Flatten() => this.byteVectors.SelectMany(vec => vec).ToArray();

        internal Guid GetToken(Guid id) => this.tokenDictionary.TryGetValue(id, out Guid token) ? token : default;
    }
}
