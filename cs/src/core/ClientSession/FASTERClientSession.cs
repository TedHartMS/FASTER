﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Collections.Generic;
using System.Threading;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        internal Dictionary<string, IClientSession> _activeSessions = new();

        /// <summary>
        /// Client session type helper
        /// </summary>
        /// <typeparam name="Input"></typeparam>
        /// <typeparam name="Output"></typeparam>
        /// <typeparam name="Context"></typeparam>
        public struct ClientSessionBuilder<Input, Output, Context>
        {
            private readonly FasterKV<Key, Value> _fasterKV;
            private readonly IFunctions<Key, Value, Input, Output, Context> _functions;

            internal ClientSessionBuilder(FasterKV<Key, Value> fasterKV, IFunctions<Key, Value, Input, Output, Context> functions)
            {
                _fasterKV = fasterKV;
                _functions = functions;
            }

            /// <summary>
            /// Start a new client session with FASTER.
            /// </summary>
            /// <param name="functions">Callback functions</param>
            /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
            /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
            ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
            /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
            /// <returns>Session instance</returns>
            public ClientSession<Key, Value, Input, Output, Context, Functions> NewSession<Functions>(Functions functions, string sessionId = null, bool threadAffinitized = false,
                    SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null)
                where Functions : IFunctions<Key, Value, Input, Output, Context>
            {
                return _fasterKV.NewSession<Input, Output, Context, Functions>(functions, sessionId, threadAffinitized, sessionVariableLengthStructSettings);
            }

            /// <summary>
            /// Resume (continue) prior client session with FASTER; used during recovery from failure.
            /// </summary>
            /// <param name="functions">Callback functions</param>
            /// <param name="sessionId">ID/name of previous session to resume</param>
            /// <param name="commitPoint">Prior commit point of durability for session</param>
            /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
            ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
            /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
            /// <returns>Session instance</returns>
            public ClientSession<Key, Value, Input, Output, Context, Functions> ResumeSession<Functions>(Functions functions, string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false,
                    SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null)
                where Functions : IFunctions<Key, Value, Input, Output, Context>
            {
                return _fasterKV.ResumeSession<Input, Output, Context, Functions>(functions, sessionId, out commitPoint, threadAffinitized, sessionVariableLengthStructSettings);
            }

            /// <summary>
            /// Start a new client session with FASTER.
            /// </summary>
            /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
            /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
            ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
            /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
            /// <returns>Session instance</returns>
            public ClientSession<Key, Value, Input, Output, Context, Functions> NewSession<Functions>(string sessionId = null, bool threadAffinitized = false, 
                    SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null)
                where Functions : IFunctions<Key, Value, Input, Output, Context>
            {
                if (_functions == null)
                    throw new FasterException("Functions not provided for session");

                return _fasterKV.NewSession<Input, Output, Context, Functions>((Functions)_functions, sessionId, threadAffinitized, sessionVariableLengthStructSettings);
            }

            /// <summary>
            /// Resume (continue) prior client session with FASTER, used during
            /// recovery from failure.
            /// </summary>
            /// <param name="sessionId">ID/name of previous session to resume</param>
            /// <param name="commitPoint">Prior commit point of durability for session</param>
            /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
            ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
            /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
            /// <returns>Session instance</returns>
            public ClientSession<Key, Value, Input, Output, Context, Functions> ResumeSession<Functions>(string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false,
                    SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null)
                where Functions : IFunctions<Key, Value, Input, Output, Context>
            {
                if (_functions == null)
                    throw new FasterException("Functions not provided for session");

                return _fasterKV.ResumeSession<Input, Output, Context, Functions>((Functions)_functions, sessionId, out commitPoint, threadAffinitized, sessionVariableLengthStructSettings);
            }
        }

        /// <summary>
        /// Advanced Client session type helper
        /// </summary>
        /// <typeparam name="Input"></typeparam>
        /// <typeparam name="Output"></typeparam>
        /// <typeparam name="Context"></typeparam>
        public struct AdvancedClientSessionBuilder<Input, Output, Context>
        {
            private readonly FasterKV<Key, Value> _fasterKV;
            private readonly IAdvancedFunctions<Key, Value, Input, Output, Context> _functions;

            internal AdvancedClientSessionBuilder(FasterKV<Key, Value> fasterKV, IAdvancedFunctions<Key, Value, Input, Output, Context> functions)
            {
                _fasterKV = fasterKV;
                _functions = functions;
            }

            /// <summary>
            /// Start a new advanced client session with FASTER.
            /// </summary>
            /// <param name="functions">Callback functions</param>
            /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
            /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
            ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
            /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
            /// <returns>Session instance</returns>
            public AdvancedClientSession<Key, Value, Input, Output, Context, Functions> NewSession<Functions>(Functions functions, string sessionId = null, bool threadAffinitized = false,
                    SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null)
                where Functions : IAdvancedFunctions<Key, Value, Input, Output, Context>
            {
                return _fasterKV.NewAdvancedSession<Input, Output, Context, Functions>(functions, sessionId, threadAffinitized, sessionVariableLengthStructSettings);
            }

            /// <summary>
            /// Resume (continue) prior client session with FASTER with advanced functions; used during recovery from failure.
            /// </summary>
            /// <param name="functions">Callback functions</param>
            /// <param name="sessionId">ID/name of previous session to resume</param>
            /// <param name="commitPoint">Prior commit point of durability for session</param>
            /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
            ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
            /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
            /// <returns>Session instance</returns>
            public AdvancedClientSession<Key, Value, Input, Output, Context, Functions> ResumeSession<Functions>(Functions functions, string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false,
                    SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null)
                where Functions : IAdvancedFunctions<Key, Value, Input, Output, Context>
            {
                return _fasterKV.ResumeAdvancedSession<Input, Output, Context, Functions>(functions, sessionId, out commitPoint, threadAffinitized, sessionVariableLengthStructSettings);
            }

            /// <summary>
            /// Start a new advanced client session with FASTER.
            /// </summary>
            /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
            /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
            ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
            /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
            /// <returns>Session instance</returns>
            public AdvancedClientSession<Key, Value, Input, Output, Context, Functions> NewSession<Functions>(string sessionId = null, bool threadAffinitized = false,
                    SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null)
                where Functions : IAdvancedFunctions<Key, Value, Input, Output, Context>
            {
                if (_functions == null)
                    throw new FasterException("Functions not provided for session");

                return _fasterKV.NewAdvancedSession<Input, Output, Context, Functions>((Functions)_functions, sessionId, threadAffinitized, sessionVariableLengthStructSettings);
            }

            /// <summary>
            /// Resume (continue) prior client session with FASTER with advanced functions; used during recovery from failure.
            /// </summary>
            /// <param name="sessionId">ID/name of previous session to resume</param>
            /// <param name="commitPoint">Prior commit point of durability for session</param>
            /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
            ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
            /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
            /// <returns>Session instance</returns>
            public AdvancedClientSession<Key, Value, Input, Output, Context, Functions> ResumeSession<Functions>(string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false,
                    SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null)
                where Functions : IAdvancedFunctions<Key, Value, Input, Output, Context>
            {
                if (_functions == null)
                    throw new FasterException("Functions not provided for session");

                return _fasterKV.ResumeAdvancedSession<Input, Output, Context, Functions>((Functions)_functions, sessionId, out commitPoint, threadAffinitized, sessionVariableLengthStructSettings);
            }
        }

        /// <summary>
        /// Helper method to specify callback function instance along with Input, Output and Context types
        /// </summary>
        /// <typeparam name="Input"></typeparam>
        /// <typeparam name="Output"></typeparam>
        /// <typeparam name="Context"></typeparam>
        /// <returns></returns>
        public ClientSessionBuilder<Input, Output, Context> For<Input, Output, Context>(IFunctions<Key, Value, Input, Output, Context> functions)
        {
            return new ClientSessionBuilder<Input, Output, Context>(this, functions);
        }

        /// <summary>
        /// Helper method to specify callback function instance along with Input, Output and Context types for advanced client sessions
        /// </summary>
        /// <typeparam name="Input"></typeparam>
        /// <typeparam name="Output"></typeparam>
        /// <typeparam name="Context"></typeparam>
        /// <returns></returns>
        public AdvancedClientSessionBuilder<Input, Output, Context> For<Input, Output, Context>(IAdvancedFunctions<Key, Value, Input, Output, Context> functions)
        {
            return new AdvancedClientSessionBuilder<Input, Output, Context>(this, functions);
        }

        /// <summary>
        /// Start a new client session with FASTER.
        /// For performance reasons, please use FasterKV&lt;Key, Value&gt;.For(functions).NewSession&lt;Functions&gt;(...) instead of this overload.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
        ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        public ClientSession<Key, Value, Input, Output, Context, IFunctions<Key, Value, Input, Output, Context>> NewSession<Input, Output, Context>(IFunctions<Key, Value, Input, Output, Context> functions, string sessionId = null,
                bool threadAffinitized = false, SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null)
        {
            return NewSession<Input, Output, Context, IFunctions<Key, Value, Input, Output, Context>>(functions, sessionId, threadAffinitized, sessionVariableLengthStructSettings);
        }

        /// <summary>
        /// Start a new client session with FASTER.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls.
        ///     Do not use with async code. Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        internal ClientSession<Key, Value, Input, Output, Context, Functions> NewSession<Input, Output, Context, Functions>(Functions functions, string sessionId = null, bool threadAffinitized = false,
                SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
            => InternalNewSession<Input, Output, Context, Functions, ClientSession<Key, Value, Input, Output, Context, Functions>>(functions, sessionId, threadAffinitized,
                            ctx => new ClientSession<Key, Value, Input, Output, Context, Functions>(this, ctx, functions, !threadAffinitized, sessionVariableLengthStructSettings));

        /// <summary>
        /// Start a new advanced client session with FASTER.
        /// For performance reasons, please use FasterKV&lt;Key, Value&gt;.For(functions).NewSession&lt;Functions&gt;(...) instead of this overload.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
        ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        public AdvancedClientSession<Key, Value, Input, Output, Context, IAdvancedFunctions<Key, Value, Input, Output, Context>> NewSession<Input, Output, Context>(IAdvancedFunctions<Key, Value, Input, Output, Context> functions,
                string sessionId = null, bool threadAffinitized = false, SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null)
        {
            return NewAdvancedSession<Input, Output, Context, IAdvancedFunctions<Key, Value, Input, Output, Context>>(functions, sessionId, threadAffinitized, sessionVariableLengthStructSettings);
        }

        /// <summary>
        /// Start a new advanced client session with FASTER.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of session (auto-generated if not provided)</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
        ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        internal AdvancedClientSession<Key, Value, Input, Output, Context, Functions> NewAdvancedSession<Input, Output, Context, Functions>(Functions functions, string sessionId = null, bool threadAffinitized = false,
                SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null)
            where Functions : IAdvancedFunctions<Key, Value, Input, Output, Context>
            => InternalNewSession<Input, Output, Context, Functions, AdvancedClientSession<Key, Value, Input, Output, Context, Functions>>(functions, sessionId, threadAffinitized,
                            ctx => new AdvancedClientSession<Key, Value, Input, Output, Context, Functions>(this, ctx, functions, !threadAffinitized, sessionVariableLengthStructSettings));


        private TSession InternalNewSession<Input, Output, Context, Functions, TSession>(Functions functions, string sessionId, bool threadAffinitized, Func<FasterExecutionContext<Input, Output, Context>, TSession> sessionCreator)
            where TSession : IClientSession
        {
            if (functions == null)
                throw new ArgumentNullException(nameof(functions));

            if (!threadAffinitized)
                UseRelaxedCPR();

            if (sessionId == null)
                sessionId = Guid.NewGuid().ToString();
            var ctx = new FasterExecutionContext<Input, Output, Context>();
            InitContext(ctx, sessionId);
            var prevCtx = new FasterExecutionContext<Input, Output, Context>();
            InitContext(prevCtx, sessionId);
            prevCtx.version--;

            ctx.prevCtx = prevCtx;

            if (_activeSessions == null)
                Interlocked.CompareExchange(ref _activeSessions, new Dictionary<string, IClientSession>(), null);

            var session = sessionCreator(ctx);
            lock (_activeSessions)
                _activeSessions.Add(sessionId, session);
            return session;
        }

        /// <summary>
        /// Resume (continue) prior client session with FASTER; used during recovery from failure.
        /// For performance reasons this overload is not recommended if functions is value type (struct).
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of previous session to resume</param>
        /// <param name="commitPoint">Prior commit point of durability for session</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
        ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        public ClientSession<Key, Value, Input, Output, Context, IFunctions<Key, Value, Input, Output, Context>> ResumeSession<Input, Output, Context>(IFunctions<Key, Value, Input, Output, Context> functions, string sessionId,
                out CommitPoint commitPoint, bool threadAffinitized = false, SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null)
        {
            return ResumeSession<Input, Output, Context, IFunctions<Key, Value, Input, Output, Context>>(functions, sessionId, out commitPoint, threadAffinitized, sessionVariableLengthStructSettings);
        }

        /// <summary>
        /// Resume (continue) prior client session with FASTER, used during
        /// recovery from failure.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of previous session to resume</param>
        /// <param name="commitPoint">Prior commit point of durability for session</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
        ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>

        internal ClientSession<Key, Value, Input, Output, Context, Functions> ResumeSession<Input, Output, Context, Functions>(Functions functions, string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false,
                SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            return InternalResumeSession<Input, Output, Context, Functions, ClientSession<Key, Value, Input, Output, Context, Functions>>(functions, sessionId, out commitPoint, threadAffinitized,
                        ctx => new ClientSession<Key, Value, Input, Output, Context, Functions>(this, ctx, functions, !threadAffinitized, sessionVariableLengthStructSettings));
        }

        /// <summary>
        /// Resume (continue) prior client session with FASTER using advanced functions; used during recovery from failure.
        /// For performance reasons this overload is not recommended if functions is value type (struct).
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of previous session to resume</param>
        /// <param name="commitPoint">Prior commit point of durability for session</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
        ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>
        public AdvancedClientSession<Key, Value, Input, Output, Context, IAdvancedFunctions<Key, Value, Input, Output, Context>> ResumeSession<Input, Output, Context>(IAdvancedFunctions<Key, Value, Input, Output, Context> functions,
                string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false, SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null)
        {
            return ResumeAdvancedSession<Input, Output, Context, IAdvancedFunctions<Key, Value, Input, Output, Context>>(functions, sessionId, out commitPoint, threadAffinitized, sessionVariableLengthStructSettings);
        }

        /// <summary>
        /// Resume (continue) prior client session with FASTER using advanced functions; used during recovery from failure.
        /// </summary>
        /// <param name="functions">Callback functions</param>
        /// <param name="sessionId">ID/name of previous session to resume</param>
        /// <param name="commitPoint">Prior commit point of durability for session</param>
        /// <param name="threadAffinitized">For advanced users. Specifies whether session holds the thread epoch across calls. Do not use with async code.
        ///     Ensure thread calls session Refresh periodically to move the system epoch forward.</param>
        /// <param name="sessionVariableLengthStructSettings">Session-specific variable-length struct settings</param>
        /// <returns>Session instance</returns>

        internal AdvancedClientSession<Key, Value, Input, Output, Context, Functions> ResumeAdvancedSession<Input, Output, Context, Functions>(Functions functions, string sessionId, out CommitPoint commitPoint,
                bool threadAffinitized = false, SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null)
            where Functions : IAdvancedFunctions<Key, Value, Input, Output, Context>
        {
            return InternalResumeSession<Input, Output, Context, Functions, AdvancedClientSession<Key, Value, Input, Output, Context, Functions>>(functions, sessionId, out commitPoint, threadAffinitized,
                        ctx => new AdvancedClientSession<Key, Value, Input, Output, Context, Functions>(this, ctx, functions, !threadAffinitized, sessionVariableLengthStructSettings));
        }

        private TSession InternalResumeSession<Input, Output, Context, Functions, TSession>(Functions functions, string sessionId, out CommitPoint commitPoint, bool threadAffinitized,
                                                                                            Func<FasterExecutionContext<Input, Output, Context>, TSession> sessionCreator)
             where TSession : IClientSession
        {
            if (functions == null)
                throw new ArgumentNullException(nameof(functions));

            if (!threadAffinitized)
                UseRelaxedCPR();

            commitPoint = InternalContinue<Input, Output, Context>(sessionId, out var ctx);
            if (commitPoint.UntilSerialNo == -1)
                throw new Exception($"Unable to find session {sessionId} to recover");

            var session = sessionCreator(ctx);

            if (_activeSessions == null)
                Interlocked.CompareExchange(ref _activeSessions, new Dictionary<string, IClientSession>(), null);
            lock (_activeSessions)
                _activeSessions.Add(sessionId, session);
            return session;
        }

        /// <summary>
        /// Dispose session with FASTER
        /// </summary>
        /// <param name="guid"></param>
        /// <returns></returns>
        internal void DisposeClientSession(string guid)
        {
            lock (_activeSessions)
                _activeSessions.Remove(guid);
        }
    }
}