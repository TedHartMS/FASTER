﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Diagnostics;

namespace FASTER.indexes.HashValueIndex
{
    internal partial class SecondaryFasterKV<TPKey> : FasterKV<TPKey, RecordId>
    {
        internal override OperationStatus RetryOperationStatus<TInput, TOutput, TContext, FasterSession>(FasterExecutionContext<TInput, TOutput, TContext> currentCtx,
                                                                        ref PendingContext<TInput, TOutput, TContext> pendingContext, FasterSession fasterSession)
        {
            // TODO: TEst RetryOperationStatus
            OperationStatus internalStatus;
            switch (pendingContext.type)
            {
                case OperationType.READ:
                    internalStatus = this.IndexInternalRead(ref pendingContext.key.Get(),
                                         ref pendingContext.input.Get(),
                                         ref pendingContext.output,
                                         pendingContext.recordInfo.PreviousAddress,
                                         pendingContext.userContext,
                                         ref pendingContext, fasterSession, currentCtx, pendingContext.serialNum);
                    break;
                case OperationType.UPSERT:
                    internalStatus = this.IndexInternalInsert(ref pendingContext.key.Get(),
                                         pendingContext.value.Get(),
                                         ref pendingContext.input.Get(),
                                         pendingContext.userContext,
                                         ref pendingContext, fasterSession, currentCtx, pendingContext.serialNum);
                    // If this assert fires, we'll have to virtualize the retry and callback switches in InternalCompleteRetryRequest.
                    Debug.Assert(internalStatus != OperationStatus.RETRY_LATER, "Insertion should not go pending");
                    break;
                default:
                    throw new HashValueIndexInternalErrorException($"Should not be retrying operation {pendingContext.type}");
            };

            return internalStatus;
        }
    }
}
