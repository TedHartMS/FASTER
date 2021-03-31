// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace FASTER.indexes.HashValueIndex
{
    internal partial class FasterKVHVI<TPKey> : FasterKV<TPKey, long>
    {
        internal FasterKVHVI(long size, LogSettings logSettings,
            CheckpointSettings checkpointSettings = null, SerializerSettings<TPKey, long> serializerSettings = null,
            IFasterEqualityComparer<TPKey> comparer = null,
            VariableLengthStructSettings<TPKey, long> variableLengthStructSettings = null)
            : base (size, logSettings, checkpointSettings, serializerSettings, comparer, variableLengthStructSettings)
        {
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextIndexRead<TInput, TOutput, TContext, FasterSession>(ref TPKey key, ref TInput input, ref TOutput output, ref RecordInfo recordInfo, ref TContext context,
                                        FasterSession fasterSession, long serialNo, FasterExecutionContext<TInput, TOutput, TContext> sessionCtx)
            where FasterSession : IFasterSession<TPKey, long, TInput, TOutput, TContext>
        {
            var pcontext = default(PendingContext<TInput, TOutput, TContext>);
            var internalStatus = this.IndexInternalRead(ref key, ref input, ref output, recordInfo.PreviousAddress, ref context, ref pcontext, fasterSession, sessionCtx, serialNo);
            Debug.Assert(internalStatus != OperationStatus.RETRY_NOW);

            Status status;
            if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
            {
                recordInfo = pcontext.recordInfo;
                status = (Status)internalStatus;
            }
            else
            {
                recordInfo = default;
                status = HandleOperationStatus(sessionCtx, sessionCtx, ref pcontext, fasterSession, internalStatus, asyncOp: false, out _);
            }

            sessionCtx.serialNum = serialNo;
            return status;
        }

        internal ValueTask<ReadAsyncResult<TInput, TOutput, TContext>> ContextIndexReadAsync<TInput, TOutput, TContext, FasterSession>(
                                        FasterSession fasterSession, FasterExecutionContext<TInput, TOutput, TContext> sessionCtx,
                                        ref TPKey key, ref TInput input, long startAddress, ref TContext context, long serialNo, QuerySettings querySettings)
            where FasterSession : IFasterSession<TPKey, long, TInput, TOutput, TContext>
        {
            var pcontext = default(PendingContext<TInput, TOutput, TContext>);
            var diskRequest = default(AsyncIOContext<TPKey, long>);
            var output = default(TOutput);

            fasterSession.UnsafeResumeThread();
            try
            {
                var internalStatus = this.IndexInternalRead(ref key, ref input, ref output, startAddress, ref context, ref pcontext, fasterSession, sessionCtx, serialNo);
                if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
                {
                    return new ValueTask<ReadAsyncResult<TInput, TOutput, TContext>>(new ReadAsyncResult<TInput, TOutput, TContext>((Status)internalStatus, output, pcontext.recordInfo));
                }

                else
                {
                    var status = HandleOperationStatus(sessionCtx, sessionCtx, ref pcontext, fasterSession, internalStatus, true, out diskRequest);

                    if (status != Status.PENDING)
                        return new ValueTask<ReadAsyncResult<TInput, TOutput, TContext>>(new ReadAsyncResult<TInput, TOutput, TContext>(status, output, pcontext.recordInfo));
                }
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
        internal Status ContextIndexInsert<Input, Output, Context, FasterSession>(ref TPKey key, ref long value, 
                                         ref Input input, ref Context context,
                                         FasterSession fasterSession, long serialNo,
                                         FasterExecutionContext<Input, Output, Context> sessionCtx)
            where FasterSession : IFasterSession<TPKey, long, Input, Output, Context>
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            var internalStatus = this.IndexInternalInsert(ref key, ref value, ref input, ref context,
                                                      ref pcontext, fasterSession, sessionCtx, serialNo);
            var status = internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND
                ? (Status)internalStatus
                : HandleOperationStatus(sessionCtx, sessionCtx, ref pcontext, fasterSession, internalStatus, asyncOp: false, out _);

            sessionCtx.serialNum = serialNo;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextIndexDelete<Input, Output, Context, FasterSession>(ref TPKey key, ref long value, ref Input input, 
                                                                   ref Context context, FasterSession fasterSession,
                                                                   FasterExecutionContext<Input, Output, Context> sessionCtx, long serialNo)
            where FasterSession : IFasterSession<TPKey, long, Input, Output, Context>
        {
            var pcontext = default(PendingContext<Input, Output, Context>);

            ((context as FasterKVHVI<TPKey>.Context).Functions as IInputAccessor<Input>).SetDelete(ref input, false);

            var internalStatus = this.IndexInternalInsert(ref key, ref value, ref input, ref context, ref pcontext, fasterSession, sessionCtx, serialNo);
            Status status = internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND
                ? (Status)internalStatus
                : HandleOperationStatus(sessionCtx, sessionCtx, ref pcontext, fasterSession, internalStatus, asyncOp: false, out _);

            sessionCtx.serialNum = serialNo;
            return status;
        }
    }
}
