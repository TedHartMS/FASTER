﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Text;

namespace FASTER.core
{
    public partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        /// <inheritdoc/>
        public void FlushPSFLogs(bool wait) => this.PSFManager.FlushLogs(wait);

        /// <inheritdoc/>
        public void FlushAndEvictPSFLogs(bool wait) => this.PSFManager.FlushAndEvictLogs(wait);

        /// <inheritdoc/>
        public void DisposePSFLogsFromMemory() => this.PSFManager.DisposeLogsFromMemory();
    }
}
