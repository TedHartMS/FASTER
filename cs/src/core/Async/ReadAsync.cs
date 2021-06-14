﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        internal sealed class ReadAsyncInternal<Input, Output, Context>
        {
            const int Completed = 1;
            const int Pending = 0;
            ExceptionDispatchInfo _exception;
            readonly FasterKV<Key, Value> _fasterKV;
            readonly IFasterSession<Key, Value, Input, Output, Context> _fasterSession;
            readonly FasterExecutionContext<Input, Output, Context> _currentCtx;
            PendingContext<Input, Output, Context> _pendingContext;
            readonly AsyncIOContext<Key, Value> _diskRequest;
            int CompletionComputeStatus;
            internal RecordInfo _recordInfo;

            internal ReadAsyncInternal(FasterKV<Key, Value> fasterKV, IFasterSession<Key, Value, Input, Output, Context> fasterSession, FasterExecutionContext<Input, Output, Context> currentCtx,
                                       PendingContext<Input, Output, Context> pendingContext, AsyncIOContext<Key, Value> diskRequest, ExceptionDispatchInfo exceptionDispatchInfo)
            {
                _exception = exceptionDispatchInfo;
                _fasterKV = fasterKV;
                _fasterSession = fasterSession;
                _currentCtx = currentCtx;
                _pendingContext = pendingContext;
                _diskRequest = diskRequest;
                CompletionComputeStatus = Pending;
                _recordInfo = default;
            }

            internal (Status, Output) Complete()
            {
                (Status, Output) _result = default;
                if (_diskRequest.asyncOperation != null
                    && CompletionComputeStatus != Completed
                    && Interlocked.CompareExchange(ref CompletionComputeStatus, Completed, Pending) == Pending)
                {
                    try
                    {
                        if (_exception == default)
                        {
                            _fasterSession.UnsafeResumeThread();
                            try
                            {
                                Debug.Assert(_fasterKV.RelaxedCPR);

                                var status = _fasterKV.InternalCompletePendingRequestFromContext(_currentCtx, _currentCtx, _fasterSession, _diskRequest, ref _pendingContext, true, out _);
                                Debug.Assert(status != Status.PENDING);
                                _result = (status, _pendingContext.output);
                                _recordInfo = _pendingContext.recordInfo;
                                _pendingContext.Dispose();
                            }
                            finally
                            {
                                _fasterSession.UnsafeSuspendThread();
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        _exception = ExceptionDispatchInfo.Capture(e);
                    }
                    finally
                    {
                        _currentCtx.ioPendingRequests.Remove(_pendingContext.id);
                        _currentCtx.asyncPendingCount--;
                        _currentCtx.pendingReads.Remove();
                    }
                }

                if (_exception != default)
                    _exception.Throw();
                return _result;
            }

            internal (Status, Output) Complete(out RecordInfo recordInfo)
            {
                var result = this.Complete();
                recordInfo = _recordInfo;
                return result;
            }
        }

        /// <summary>
        /// State storage for the completion of an async Read, or the result if the read was completed synchronously
        /// </summary>
        public struct ReadAsyncResult<Input, Output, Context>
        {
            internal readonly Status status;
            internal readonly Output output;
            readonly RecordInfo recordInfo;

            internal readonly ReadAsyncInternal<Input, Output, Context> readAsyncInternal;

            internal ReadAsyncResult(Status status, Output output, RecordInfo recordInfo)
            {
                this.status = status;
                this.output = output;
                this.recordInfo = recordInfo;
                this.readAsyncInternal = default;
            }

            internal ReadAsyncResult(
                FasterKV<Key, Value> fasterKV,
                IFasterSession<Key, Value, Input, Output, Context> fasterSession,
                FasterExecutionContext<Input, Output, Context> currentCtx,
                PendingContext<Input, Output, Context> pendingContext, AsyncIOContext<Key, Value> diskRequest, ExceptionDispatchInfo exceptionDispatchInfo)
            {
                status = Status.PENDING;
                output = default;
                this.recordInfo = default;
                readAsyncInternal = new ReadAsyncInternal<Input, Output, Context>(fasterKV, fasterSession, currentCtx, pendingContext, diskRequest, exceptionDispatchInfo);
            }

            /// <summary>
            /// Complete the read operation, after any I/O is completed.
            /// </summary>
            /// <returns>The read result, or throws an exception if error encountered.</returns>
            public (Status status, Output output) Complete()
            {
                if (status != Status.PENDING)
                    return (status, output);

                return readAsyncInternal.Complete();
            }

            /// <summary>
            /// Complete the read operation, after any I/O is completed.
            /// </summary>
            /// <returns>The read result and the previous address in the Read key's hash chain, or throws an exception if error encountered.</returns>
            public (Status status, Output output) Complete(out RecordInfo recordInfo)
            {
                if (status != Status.PENDING)
                {
                    recordInfo = this.recordInfo;
                    return (status, output);
                }

                return readAsyncInternal.Complete(out recordInfo);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<ReadAsyncResult<Input, Output, Context>> ReadAsync<Input, Output, Context>(IFasterSession<Key, Value, Input, Output, Context> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx,
            ref Key key, ref Input input, long startAddress, Context context, long serialNo, CancellationToken token, byte operationFlags = 0)
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            pcontext.operationFlags = operationFlags;
            var diskRequest = default(AsyncIOContext<Key, Value>);
            Output output = default;

            fasterSession.UnsafeResumeThread();
            try
            {
                OperationStatus internalStatus = InternalRead(ref key, ref input, ref output, startAddress, ref context, ref pcontext, fasterSession, currentCtx, serialNo);
                Debug.Assert(internalStatus != OperationStatus.RETRY_NOW);
                Debug.Assert(internalStatus != OperationStatus.RETRY_LATER);

                if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
                {
                    return new ValueTask<ReadAsyncResult<Input, Output, Context>>(new ReadAsyncResult<Input, Output, Context>((Status)internalStatus, output, pcontext.recordInfo));
                }
                else
                {
                    var status = HandleOperationStatus(currentCtx, currentCtx, ref pcontext, fasterSession, internalStatus, true, out diskRequest);

                    if (status != Status.PENDING)
                        return new ValueTask<ReadAsyncResult<Input, Output, Context>>(new ReadAsyncResult<Input, Output, Context>(status, output, pcontext.recordInfo));
                }
            }
            finally
            {
                Debug.Assert(serialNo >= currentCtx.serialNum, "Operation serial numbers must be non-decreasing");
                currentCtx.serialNum = serialNo;
                fasterSession.UnsafeSuspendThread();
            }

            return SlowReadAsync(this, fasterSession, currentCtx, pcontext, diskRequest, token);
        }

        internal static async ValueTask<ReadAsyncResult<Input, Output, Context>> SlowReadAsync<Input, Output, Context>(
            FasterKV<Key, Value> @this,
            IFasterSession<Key, Value, Input, Output, Context> fasterSession,
            FasterExecutionContext<Input, Output, Context> currentCtx,
            PendingContext<Input, Output, Context> pendingContext, AsyncIOContext<Key, Value> diskRequest, CancellationToken token = default)
        {
            currentCtx.asyncPendingCount++;
            currentCtx.pendingReads.Add();

            ExceptionDispatchInfo exceptionDispatchInfo = default;

            try
            {
                token.ThrowIfCancellationRequested();

                if (@this.epoch.ThisInstanceProtected())
                    throw new NotSupportedException("Async operations not supported over protected epoch");

                using (token.Register(() => diskRequest.asyncOperation.TrySetCanceled()))
                    diskRequest = await diskRequest.asyncOperation.Task.WithCancellationAsync(token).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                exceptionDispatchInfo = ExceptionDispatchInfo.Capture(e);
            }

            return new ReadAsyncResult<Input, Output, Context>(@this, fasterSession, currentCtx, pendingContext, diskRequest, exceptionDispatchInfo);
        }
    }
}
