// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace FASTER.indexes.HashValueIndex
{
    internal partial class SecondaryFasterKV<TPKey> : FasterKV<TPKey, RecordId>
    {
        internal AdvancedClientSession<TPKey, RecordId, Input, Output, Context, Functions> NewSession(KeyAccessor<TPKey> keyAccessor) 
            => this.For(new Functions(this, keyAccessor)).NewSession<Functions>(threadAffinitized: false);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextIndexRead<TInput, TOutput, TContext, FasterSession>(ref TPKey key, ref TInput input, ref TOutput output, ref RecordInfo recordInfo, TContext context,
                                        FasterSession fasterSession, FasterExecutionContext<TInput, TOutput, TContext> sessionCtx)
            where FasterSession : IFasterSession<TPKey, RecordId, TInput, TOutput, TContext>
        {
            var pcontext = default(PendingContext<TInput, TOutput, TContext>);
            var internalStatus = this.IndexInternalRead(ref key, ref input, ref output, recordInfo.PreviousAddress, context, ref pcontext, fasterSession, sessionCtx, sessionCtx.serialNum);
            Debug.Assert(internalStatus != OperationStatus.RETRY_NOW);

            if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
            {
                recordInfo = pcontext.recordInfo;
                return (Status)internalStatus;
            }

            recordInfo = default;
            return HandleOperationStatus(sessionCtx, sessionCtx, ref pcontext, fasterSession, internalStatus, asyncOp: false, out _);
        }

        internal ValueTask<ReadAsyncResult<TInput, TOutput, TContext>> ContextIndexReadAsync<TInput, TOutput, TContext, FasterSession>(
                                        FasterSession fasterSession, FasterExecutionContext<TInput, TOutput, TContext> sessionCtx,
                                        ref TPKey key, ref TInput input, long startAddress, ref TContext context, long serialNo, QuerySettings querySettings)
            where FasterSession : IFasterSession<TPKey, RecordId, TInput, TOutput, TContext>
        {
            var pcontext = default(PendingContext<TInput, TOutput, TContext>);
            var diskRequest = default(AsyncIOContext<TPKey, RecordId>);
            TOutput output = default;

            fasterSession.UnsafeResumeThread();
            try
            {
                var internalStatus = this.IndexInternalRead(ref key, ref input, ref output, startAddress, context, ref pcontext, fasterSession, sessionCtx, serialNo);
                if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
                    return new ValueTask<ReadAsyncResult<TInput, TOutput, TContext>>(new ReadAsyncResult<TInput, TOutput, TContext>((Status)internalStatus, output, pcontext.recordInfo));

                var status = HandleOperationStatus(sessionCtx, sessionCtx, ref pcontext, fasterSession, internalStatus, true, out diskRequest);
                if (status != Status.PENDING)
                    return new ValueTask<ReadAsyncResult<TInput, TOutput, TContext>>(new ReadAsyncResult<TInput, TOutput, TContext>(status, output, pcontext.recordInfo));
            }
            finally
            {
                Debug.Assert(serialNo >= sessionCtx.serialNum, "Operation serial numbers must be non-decreasing");
                sessionCtx.serialNum = serialNo;
                fasterSession.UnsafeSuspendThread();
            }

            return SlowReadAsync(this, fasterSession, sessionCtx, pcontext, diskRequest, querySettings.CancellationToken);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextIndexInsert<Input, Output, Context, FasterSession>(ref TPKey key, RecordId recordId, ref Input input, Context context, FasterSession fasterSession,
                                         FasterExecutionContext<Input, Output, Context> sessionCtx)
            where FasterSession : IFasterSession<TPKey, RecordId, Input, Output, Context>
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            var internalStatus = this.IndexInternalInsert(ref key, recordId, ref input, context, ref pcontext, fasterSession, sessionCtx, sessionCtx.serialNum);
            return internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND
                ? (Status)internalStatus
                : HandleOperationStatus(sessionCtx, sessionCtx, ref pcontext, fasterSession, internalStatus, asyncOp: false, out _);
        }
    }
}
