﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.common
{
    /// <summary>
    /// Different types of messages
    /// </summary>
    public enum MessageType : byte
    {
        /// <summary>
        /// A request to read some value in a remote Faster instance
        /// </summary>
        Read,
        /// <summary>
        /// A request to upsert some value in a remote Faster instance
        /// </summary>
        Upsert,
        /// <summary>
        /// A request to rmw on some value in a remote Faster instance
        /// </summary>
        RMW,
        /// <summary>
        /// A request to delete some value in a remote Faster instance
        /// </summary>
        Delete,

        /// <summary>
        /// An async request to read some value in a remote Faster instance
        /// </summary>
        ReadAsync,
        /// <summary>
        /// An async request to upsert some value in a remote Faster instance
        /// </summary>
        UpsertAsync,
        /// <summary>
        /// An async request to rmw on some value in a remote Faster instance
        /// </summary>
        RMWAsync,
        /// <summary>
        /// An async request to delete some value in a remote Faster instance
        /// </summary>
        DeleteAsync,
        /// <summary>
        /// Pending result
        /// </summary>
        PendingResult
    }
}