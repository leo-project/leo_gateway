%%======================================================================
%%
%% Leo Gateway
%%
%% Copyright (c) 2012-2015 Rakuten, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%%======================================================================
-module(leo_nlm_proto4_server).

-include("leo_nlm_proto4.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-export([nlmproc4_null_4/2,
         nlmproc4_test_4/3,
         nlmproc4_lock_4/3,
         nlmproc4_cancel_4/3,
         nlmproc4_unlock_4/3
        ]).

%% nlmproc4_granted_4/3,
%% nlmproc4_test_msg_4/3,
%% nlmproc4_lock_msg_4/3,
%% nlmproc4_cancel_msg_4/3,
%% nlmproc4_unlock_msg_4/3,
%% nlmproc4_granted_msg_4/3,
%% nlmproc4_test_res_4/3,
%% nlmproc4_lock_res_4/3,
%% nlmproc4_cancel_res_4/3,
%% nlmproc4_unlock_res_4/3,
%% nlmproc4_granted_res_4/3,
%% nlmproc4_share_4/3,
%% nlmproc4_unshare_4/3,
%% nlmproc4_nm_lock_4/3,
%% nlmproc4_free_all_4/3

-record(lock_record,{
          start :: non_neg_integer(),
          till  :: integer(),
          owner :: binary(),
          excl  :: boolean()
         }).
-define(NLM_LOCK_ETS, nlm_lock_ets).

-define(NLM4_GRANTED,               'NLM4_GRANTED').
-define(NLM4_DENIED,                'NLM4_DENIED').
-define(NLM4_DENIED_NOLOCKS,        'NLM4_DENIED_NOLOCKS').
-define(NLM4_BLOCKED,               'NLM4_BLOCKED').
-define(NLM4_DENIED_GRACE_PERIOD,   'NLM4_DENIED_GRACE_PERIOD').
-define(NLM4_DEADLCK,               'NLM4_DEADLCK').
-define(NLM4_ROFS,                  'NLM4_ROFS').
-define(NLM4_STALE_FH,              'NLM4_STALE_FH').
-define(NLM4_FBIG,                  'NLM4_FBIG').
-define(NLM4_FAILED,                'NLM4_FAILED').

%% ---------------------------------------------------------------------
%% API
%% ---------------------------------------------------------------------
%% @doc Called only once from a parent rpc server process to initialize this module
%%      during starting a leo_storage server.
-spec(init(any()) -> {ok, any()}).
init(_Args) ->
    ets:new(?NLM_LOCK_ETS, [set, named_table, public]),
    {ok, void}.

handle_call(Req, _From, S) ->
    ?debug("handle_call", "req:~p from:~p", [Req, _From]),
    {reply, [], S}.

handle_cast(Req, S) ->
    ?debug("handle_cast", "req:~p", [Req]),
    {reply, [], S}.

handle_info(Req, S) ->
    ?debug("handle_info", "req:~p", [Req]),
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

%% ---------------------------------------------------------------------
%% API
%% ---------------------------------------------------------------------
%% @doc
nlmproc4_null_4(_Clnt, State) ->
    ?debug("NLM_NULL", "from:~p", [_Clnt]),
    {reply, [], State}.

%% @doc
nlmproc4_test_4({Cookie, IsExclusive, Lock} = TestArgs, Clnt, State) ->
    ?debug("NLM_TEST", "Args:~p, from:~p", [TestArgs, Clnt]),
    {_CallerName, FileHandler, _Owner, _Svid, _Offset, _Length} = Lock,
    NewLock = convert_lock(Lock, IsExclusive), 
    LockRet = case test_lock(FileHandler, NewLock) of
                  {ok, _} ->
                      ?NLM4_GRANTED;
                  _ ->
                      ?NLM4_DENIED
              end,
    {reply, {Cookie, {LockRet}}, State}.

nlmproc4_lock_4({Cookie, _IsBlock, IsExclusive, Lock, _IsReclaim, _NSMState} = LockArgs, Clnt, State) ->
    ?debug("NLM_LOCK", "Args:~p, from:~p", [LockArgs, Clnt]),
    {_CallerName, FileHandler, _Owner, _Svid, _Offset, _Length} = Lock,
    NewLock = convert_lock(Lock, IsExclusive), 
    LockRet = case test_lock(FileHandler, NewLock) of
                  {ok, ExistingLocks} ->
                      ets:insert(?NLM_LOCK_ETS, {FileHandler, [NewLock | ExistingLocks]}),
                      ?NLM4_GRANTED;
                  _ ->
                      ?NLM4_DENIED
              end,
    {reply, {Cookie, {LockRet}}, State}.

nlmproc4_cancel_4({Cookie, _IsBlock, _IsExclusive, _Lock} = CancArgs, Clnt, State) ->
    ?debug("NLM_CANCEL", "Args:~p, from:~p", [CancArgs, Clnt]),
    {reply, {Cookie, {?NLM4_GRANTED}}, State}.

nlmproc4_unlock_4({Cookie, Lock} = UnlockArgs, Clnt, State) ->
    ?debug("NLM_UNLOCK", "Args:~p, from:~p", [UnlockArgs, Clnt]),
    {_CallerName, FileHandler, Owner, _Svid, Offset, Length} = Lock,
    End = Offset + Length - 1,
    case ets:lookup(?NLM_LOCK_ETS, FileHandler) of
        [{FileHandler, ExistingLocks}] ->
            NewLocks = lists:foldl(fun(CurLock, Acc) ->
                                           case CurLock#lock_record.owner of
                                               Owner ->
                                                   case modify_lock(CurLock, Offset, End) of
                                                       [] ->
                                                           Acc;
                                                       Modified ->
                                                           ?debug("nlmproc4_unlock_4/3", "Modified: ~p", [Modified]),
                                                           lists:append(Acc, Modified)
                                                   end;
                                               _ ->
                                                   [CurLock | Acc]
                                           end
                                   end, [], ExistingLocks),
            ets:insert(?NLM_LOCK_ETS, {FileHandler, NewLocks});
        _ ->
            ok
    end,
    {reply, {Cookie, {?NLM4_GRANTED}}, State}.

%% nlmproc4_granted_4(TestArgs, Clnt, State) ->
%%     ?debug("NLM_GRANTED", "Args:~p, from:~p", [TestArgs, Clnt]),
%%     {reply, [], State}.
%% 
%% nlmproc4_test_msg_4(TestArgs, Clnt, State) ->
%%     ?debug("NLM_TEST_MSG", "Args:~p, from:~p", [TestArgs, Clnt]),
%%     {reply, [], State}.
%% 
%% nlmproc4_lock_msg_4(LockArgs, Clnt, State) ->
%%     ?debug("NLM_LOCK_MSG", "Args:~p, from:~p", [LockArgs, Clnt]),
%%     {reply, [], State}.
%% 
%% nlmproc4_cancel_msg_4(CancArgs, Clnt, State) ->
%%     ?debug("NLM_CANCEL_MSG", "Args:~p, from:~p", [CancArgs, Clnt]),
%%     {reply, [], State}.
%% 
%% nlmproc4_unlock_msg_4(UnlockArgs, Clnt, State) ->
%%     ?debug("NLM_UNLOCK_MSG", "Args:~p, from:~p", [UnlockArgs, Clnt]),
%%     {reply, [], State}.
%% 
%% nlmproc4_granted_msg_4(TestArgs, Clnt, State) ->
%%     ?debug("NLM_GRANTED_MSG", "Args:~p, from:~p", [TestArgs, Clnt]),
%%     {reply, [], State}.
%% 
%% nlmproc4_test_res_4(TestArgs, Clnt, State) ->
%%     ?debug("NLM_TEST_RES", "Args:~p, from:~p", [TestArgs, Clnt]),
%%     {reply, [], State}.
%% 
%% nlmproc4_lock_res_4(LockArgs, Clnt, State) ->
%%     ?debug("NLM_LOCK_RES", "Args:~p, from:~p", [LockArgs, Clnt]),
%%     {reply, [], State}.
%% 
%% nlmproc4_cancel_res_4(CancArgs, Clnt, State) ->
%%     ?debug("NLM_CANCEL_RES", "Args:~p, from:~p", [CancArgs, Clnt]),
%%     {reply, [], State}.
%% 
%% nlmproc4_unlock_res_4(UnlockArgs, Clnt, State) ->
%%     ?debug("NLM_UNLOCK_RES", "Args:~p, from:~p", [UnlockArgs, Clnt]),
%%     {reply, [], State}.
%% 
%% nlmproc4_granted_res_4(TestArgs, Clnt, State) ->
%%     ?debug("NLM_GRANTED_RES", "Args:~p, from:~p", [TestArgs, Clnt]),
%%     {reply, [], State}.
%% 
%% nlmproc4_share_4(ShareArgs, Clnt, State) ->
%%     ?debug("NLM_SHARE", "Args:~p, from:~p", [ShareArgs, Clnt]),
%%     {reply, [], State}.
%% 
%% nlmproc4_unshare_4(ShareArgs, Clnt, State) ->
%%     ?debug("NLM_UNSHARE", "Args:~p, from:~p", [ShareArgs, Clnt]),
%%     {reply, [], State}.
%% 
%% nlmproc4_nm_lock_4(LockArgs, Clnt, State) ->
%%     ?debug("NLM_NM_LOCK", "Args:~p, from:~p", [LockArgs, Clnt]),
%%     {reply, [], State}.
%% 
%% nlmproc4_free_all_4(NotifyArgs, Clnt, State) ->
%%     ?debug("NLM_FREE_ALL", "Args:~p, from:~p", [NotifyArgs, Clnt]),
%%     {reply, [], State}.

%% ---------------------------------------------------------------------
%% INNER FUNCTIONS
%% ---------------------------------------------------------------------
convert_lock({_, _, Owner, _, Offset, Length}, Excl) ->
    End = case Length of
              0 ->
                  -1;
              _ ->
                  Offset + Length - 1
          end,
    #lock_record{
       start = Offset,
       till = End,
       owner = Owner,
       excl = Excl}.

check_lock_ranges(_, []) ->
    ok;
check_lock_ranges(NewLock, [CurLock | Rest]) ->
    Start = NewLock#lock_record.start,
    Excl = NewLock#lock_record.excl,
    Owner = NewLock#lock_record.owner,
    CurStart = CurLock#lock_record.start,
    CurEnd = CurLock#lock_record.till,
    CurExcl = CurLock#lock_record.excl,
    CurOwner = CurLock#lock_record.owner,
    IsOverlap = case Start >= CurStart of
                    true ->
                        case CurEnd of
                            -1 ->
                                true;
                            _ ->
                                Start =< CurEnd
                        end;
                    false ->
                        false
                end,
    case IsOverlap of
        true ->
            if 
                Excl == false andalso CurExcl == false ->
                    check_lock_ranges(NewLock, Rest);
                Owner == CurOwner ->
                    check_lock_ranges(NewLock, Rest);
                true ->
                    failed
            end;
        false ->
            check_lock_ranges(NewLock, Rest)
    end.

modify_lock(#lock_record{start = CurStart}, Start, -1) when Start =< CurStart ->
    [];
modify_lock(#lock_record{start = CurStart} = Lock, Start, -1) when Start > CurStart ->
    [Lock#lock_record{till = Start - 1}];
modify_lock(#lock_record{start = CurStart, till = -1} = Lock, Start, End) ->
    case End >= CurStart of
        false ->
            [];
        true ->
            Lock2 = Lock#lock_record{start = End + 1},
            case Start =< CurStart of
                true ->
                    [Lock2];
                false ->
                    [Lock#lock_record{start = CurStart, till = Start - 1}, Lock2]
            end
    end;
modify_lock(#lock_record{start = CurStart, till = CurEnd}, Start, End)
  when Start =< CurStart, End >= CurEnd ->
    [];
modify_lock(#lock_record{start = CurStart, till = CurEnd} = Lock, Start, End)
  when Start =< CurStart, End < CurEnd ->
    [Lock#lock_record{start = End + 1}];
modify_lock(#lock_record{start = CurStart, till = CurEnd} = Lock, Start, End)
  when Start > CurStart, End >= CurEnd ->
    [Lock#lock_record{till = Start - 1}];
modify_lock(#lock_record{start = CurStart, till = CurEnd} = Lock, Start, End)
  when Start > CurStart, End < CurEnd ->
    ?debug("modify_lock", "Split ~p - ~p, ~p - ~p", [CurStart, Start-1, End + 1, CurEnd]),
    Lock1 = Lock#lock_record{till = Start - 1},
    Lock2 = Lock#lock_record{start = End + 1},
    [Lock1, Lock2];
modify_lock(Lock, _, _) ->
    [Lock].

test_lock(FileHandler, NewLock) ->
    case ets:lookup(?NLM_LOCK_ETS, FileHandler) of
        [] ->
            {ok, []};
        [{FileHandler, ExistingLocks}] ->
            ?debug("test_lock/2", "Lock:~p, Exist:~p", [NewLock, ExistingLocks]),
            {check_lock_ranges(NewLock, ExistingLocks), ExistingLocks}
    end.

