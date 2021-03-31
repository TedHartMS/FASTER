﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Diagnostics;

namespace FASTER.indexes.HashValueIndex
{
    internal partial class FasterKVHVI<TPKey> : FasterKV<TPKey, long>
    {
        internal override OperationStatus RetryOperationStatus<Input, Output, Context, FasterSession>(FasterExecutionContext<Input, Output, Context> currentCtx,
                                                                        ref PendingContext<Input, Output, Context> pendingContext, FasterSession fasterSession)
        {
            OperationStatus internalStatus;
            switch (pendingContext.type)
            {
                case OperationType.READ:
                    internalStatus = this.IndexInternalRead(ref pendingContext.key.Get(),
                                         ref pendingContext.input.Get(),
                                         ref pendingContext.output,
                                         pendingContext.recordInfo.PreviousAddress,
                                         ref pendingContext.userContext,
                                         ref pendingContext, fasterSession, currentCtx, pendingContext.serialNum);
                    break;
                case OperationType.UPSERT:
                    internalStatus = this.IndexInternalInsert(ref pendingContext.key.Get(),
                                         ref pendingContext.value.Get(),
                                         ref pendingContext.input.Get(),
                                         ref pendingContext.userContext,
                                         ref pendingContext, fasterSession, currentCtx, pendingContext.serialNum);
                    // If this assert fires, we'll have to virtualize the retry and callback switches in InternalCompleteRetryRequest.
                    Debug.Assert(internalStatus != OperationStatus.RETRY_LATER, "Insertion should not go pending");
                    break;
                default:
                    throw new InternalErrorExceptionHVI($"Should not be retrying operation {pendingContext.type}");
            };

            return internalStatus;
        }
    }
}
