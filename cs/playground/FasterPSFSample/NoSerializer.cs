﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

namespace FasterPSFSample
{
    public class NoSerializer : BinaryObjectSerializer<BlittableOrders>
    {
        public override void Deserialize(out BlittableOrders obj) 
            => throw new NotImplementedException("NoSerializer should not be instantiated");

        public override void Serialize(in BlittableOrders obj) 
            => throw new NotImplementedException("NoSerializer should not be instantiated");
    }
}
