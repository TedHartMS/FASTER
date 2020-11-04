﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace SubsetIndexSample
{
    public interface IOrders
    {
        int Id { get; set; }

        // Colors, strings, and enums are not blittable so we use int
        int SizeInt { get; set; }

        int ColorArgb { get; set; }

        int Count { get; set; }

        (int, int, int, int) MemberTuple => (this.Id, this.SizeInt, this.ColorArgb, this.Count);
    }
}
