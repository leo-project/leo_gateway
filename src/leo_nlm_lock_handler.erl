%%======================================================================
%%
%% Network Lock Manager written in Erlang
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
-module(leo_nlm_lock_handler).
-author('Wilson Li').

-behaviour(leo_nlm_lock_behaviour).
-behaviour(gen_server).

-include("leo_nlm_lock.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([test/2,
         lock/2,
         unlock/4]).

%% ---------------------------------------------------------------------
%% API
%% ---------------------------------------------------------------------
-spec(test(FileHandler::binary(), Lock::#lock_record{}) ->
                ok | {error, #lock_record{}}).
test(FileHandler, Lock) ->
    gen_server:call(?MODULE, {test, FileHandler, Lock}).

-spec(lock(FileHandler::binary(), Lock::#lock_record{}) ->
                ok | {error, #lock_record{}}).
lock(FileHandler, Lock) ->
    gen_server:call(?MODULE, {lock, FileHandler, Lock}).

-spec(unlock(FileHandler::binary(), Owner::binary(), Start::non_neg_integer(), End::integer()) ->
                ok).
unlock(FileHandler, Owner, Start, End) ->
    gen_server:call(?MODULE, {unlock, FileHandler, Owner, Start, End}).

%% ---------------------------------------------------------------------
%% GEN_SERVER CALLBACKS
%% ---------------------------------------------------------------------
init(_Args) ->
    ets:new(?NLM_LOCK_ETS, [set, named_table, private]),
    {ok, void}.

start_link(_Args)->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

handle_call({test, FileHandler, Lock}, _From, State) ->
    case test_lock(FileHandler, Lock) of
        {ok, _} ->
            {reply, ok, State};
        {{error, Lock}, _} ->
            {reply, {error, Lock}, State}
    end;
handle_call({lock, FileHandler, Lock}, _From, State) ->
    case test_lock(FileHandler, Lock) of
        {ok, ExistingLocks} ->
            ets:insert(?NLM_LOCK_ETS, {FileHandler, [Lock | ExistingLocks]}),
            {reply, ok, State};
        {{error, Lock}, _} ->
            {reply, {error, Lock}, State}
    end;
handle_call({unlock, FileHandler, Owner, Start, End}, _From, State) ->
    Ret = case ets:lookup(?NLM_LOCK_ETS, FileHandler) of
              [{FileHandler, ExistingLocks}] ->
                  NewLocks = lists:foldl(
                               fun(CurLock, Acc) ->
                                       case CurLock#lock_record.owner of
                                           Owner ->
                                               case modify_lock(CurLock, Start, End) of
                                                   [] ->
                                                       Acc;
                                                   Modified ->
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
    {reply, Ret, State}.

handle_cast(Req, S) ->
    ?debug("handle_cast", "req:~p", [Req]),
    {noreply, S}.

handle_info(Req, S) ->
    ?debug("handle_info", "req:~p", [Req]),
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ---------------------------------------------------------------------
%% INNER FUNCTIONS
%% ---------------------------------------------------------------------
-spec(test_lock(binary(), #lock_record{}) ->
        {ok, list(#lock_record{})} | {{error, #lock_record{}}, list(#lock_record{})}).
test_lock(FileHandler, Lock) ->
    case ets:lookup(?NLM_LOCK_ETS, FileHandler) of
        [] ->
            {ok, []};
        [{FileHandler, ExistingLocks}] ->
            {check_lock_ranges(Lock, ExistingLocks), ExistingLocks}
    end.

check_lock_ranges(_, []) ->
    ok;
check_lock_ranges(#lock_record{start = Start,
                               excl = Excl,
                               owner = Owner} = NewLock, 
                  [#lock_record{start = CurStart,
                                till = CurEnd,
                                excl = CurExcl,
                                owner = CurOwner} = CurLock | Rest]) ->
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
                    {error, CurLock}
            end;
        false ->
            check_lock_ranges(NewLock, Rest)
    end.

-spec(modify_lock(#lock_record{}, non_neg_integer(), integer()) ->
            list(#lock_record{})).
modify_lock(#lock_record{start = CurStart}, Start, -1) when Start =< CurStart ->
    [];
modify_lock(#lock_record{start = CurStart} = Lock, Start, -1) when Start > CurStart ->
    [Lock#lock_record{till = Start - 1,
                      len  = Start - CurStart}];
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
                    [Lock#lock_record{start = CurStart, 
                                      till  = Start - 1,
                                      len   = Start - CurStart}, Lock2]
            end
    end;
modify_lock(#lock_record{start = CurStart, till = CurEnd}, Start, End)
  when Start =< CurStart, End >= CurEnd ->
    [];
modify_lock(#lock_record{start = CurStart, till = CurEnd} = Lock, Start, End)
  when Start =< CurStart, End < CurEnd, End >= CurStart ->
    [Lock#lock_record{start = End + 1,
                      len   = CurEnd - End}];
modify_lock(#lock_record{start = CurStart, till = CurEnd} = Lock, Start, End)
  when Start > CurStart, End >= CurEnd, Start =< CurEnd ->
    [Lock#lock_record{till = Start - 1,
                      len  = Start - CurStart}];
modify_lock(#lock_record{start = CurStart, till = CurEnd} = Lock, Start, End)
  when Start > CurStart, End < CurEnd ->
    Lock1 = Lock#lock_record{till = Start - 1,
                             len  = Start - CurStart},
    Lock2 = Lock#lock_record{start = End + 1,
                             len  = CurEnd - End},
    [Lock1, Lock2];
modify_lock(Lock, _, _) ->
    [Lock].

%% ---------------------------------------------------------------------
%% UNIT TESTS
%% ---------------------------------------------------------------------
-ifdef(EUNIT).
check_lock_ranges_test() ->
    Lock1 = #lock_record{
                start   = 1,
                till    = 10,
                len     = 10,
                owner   = <<"6350@freebsd102">>,
                uppid   = 6350,
                excl    = false
              },
    Lock2 = #lock_record{
                start   = 20,
                till    = 30,
                len     = 11,
                owner   = <<"6350@freebsd102">>,
                uppid   = 6350,
                excl    = true
              },
    ExistingLocks = [Lock1 , Lock2],

    CheckLock1 = #lock_record{
                    start   = 2,
                    till    = 5,
                    len     = 4,
                    owner   = <<"6351@freebsd102">>,
                    uppid   = 6351,
                    excl    = true
                   },
    ?debugMsg("===== Testing check_lock_range for exclusive lock ====="),
    {error, _} = check_lock_ranges(CheckLock1, ExistingLocks),

    CheckLock2 = #lock_record{
                    start   = 2,
                    till    = 5,
                    len     = 4,
                    owner   = <<"6351@freebsd102">>,
                    uppid   = 6351,
                    excl    = false
                   },
    ?debugMsg("===== Testing check_lock_range for share lock ====="),
    ok = check_lock_ranges(CheckLock2, ExistingLocks),

    CheckLock3 = #lock_record{
                    start   = 200,
                    till    = 500,
                    len     = 301,
                    owner   = <<"6350@freebsd102">>,
                    uppid   = 6350,
                    excl    = true
                   },
    ?debugMsg("===== Testing check_lock_range for new lock ====="),
    ok = check_lock_ranges(CheckLock3, ExistingLocks),

    CheckLock4 = #lock_record{
                    start   = 5,
                    till    = 25,
                    len     = 21,
                    owner   = <<"6350@freebsd102">>,
                    uppid   = 6350,
                    excl    = true
                   },
    ?debugMsg("===== Testing check_lock_range for own lock ====="),
    ok = check_lock_ranges(CheckLock4, ExistingLocks).

modify_locks_test() ->
    Lock1 = #lock_record{
                start   = 10,
                till    = 20,
                len     = 11,
                owner   = <<"6350@freebsd102">>,
                uppid   = 6350,
                excl    = false
              },

    ?debugMsg("===== Testing modify_lock for unaffected locks ====="),
    [Lock1] = modify_lock(Lock1, 30, 40),

    ?debugMsg("===== Testing modify_lock for reducing lock range (end) ====="),
    [#lock_record{
        start   = 10,
        till    = 14,
        len     = 5,
        owner   = <<"6350@freebsd102">>,
        uppid   = 6350,
        excl    = false
       }] = modify_lock(Lock1, 15, 30),

    ?debugMsg("===== Testing modify_lock for reducing lock range (start) ====="),
    [#lock_record{
        start   = 16,
        till    = 20,
        len     = 5,
        owner   = <<"6350@freebsd102">>,
        uppid   = 6350,
        excl    = false
       }] = modify_lock(Lock1, 5, 15),

    ?debugMsg("===== Testing modify_lock for spliting lock ====="),
    [#lock_record{
        start   = 10,
        till    = 14,
        len     = 5,
        owner   = <<"6350@freebsd102">>,
        uppid   = 6350,
        excl    = false
       },
    #lock_record{
        start   = 18,
        till    = 20,
        len     = 3,
        owner   = <<"6350@freebsd102">>,
        uppid   = 6350,
        excl    = false
       }] = modify_lock(Lock1, 15, 17),

    ?debugMsg("===== Testing modify_lock for removing lock ====="),
    [] = modify_lock(Lock1, 0, -1),

    Lock2 = #lock_record{
                start   = 10,
                till    = -1,
                len     = 0,
                owner   = <<"6350@freebsd102">>,
                uppid   = 6350,
                excl    = false
              },

    ?debugMsg("===== Testing modify_lock for unlocking tail ====="),
    [#lock_record{
        start   = 10,
        till    = 14,
        len     = 5,
        owner   = <<"6350@freebsd102">>,
        uppid   = 6350,
        excl    = false
       }] = modify_lock(Lock2, 15, -1),

    ?debugMsg("===== Testing modify_lock for unlocking head ====="),
    [#lock_record{
        start   = 16,
        till    = -1,
        len     = 0,
        owner   = <<"6350@freebsd102">>,
        uppid   = 6350,
        excl    = false
       }] = modify_lock(Lock2, 0, 15).

-endif.
