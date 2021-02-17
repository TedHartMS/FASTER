// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace FASTER.benchmark
{
    class NullKeyIndex<Key> : ISecondaryKeyIndex<Key>
    {
        public string Name => "KeyIndex";

        public bool IsMutable => true;

        public void Delete(ref Key key) { }

        public void Insert(ref Key key) { }

        public void Upsert(ref Key key, bool isMutable) { }
    }

    class NullValueIndex<Value> : ISecondaryValueIndex<Value>
    {
        public string Name => "ValueIndex";

        public bool IsMutable => true;

        public void Delete(ref Value value, long recordId) { }

        public void Insert(ref Value value, long recordId) { }

        public void Upsert(ref Value value, long recordId, bool isMutable) { }
    }
}
