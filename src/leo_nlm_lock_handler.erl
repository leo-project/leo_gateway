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
-module(leo_nlm_lock_handler).

-behaviour(leo_nlm_lock_behaviour).
-behaviour(gen_server).

-include("leo_nlm_lock.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").

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
                                                       ?debug("unlock", "Modified: ~p", [Modified]),
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
            ?debug("test_lock/2", "Lock:~p, Exist:~p", [Lock, ExistingLocks]),
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
                    ?debug("check_lock_ranges/2", "Hit ~p", [CurLock]),
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
  when Start =< CurStart, End < CurEnd, End >= CurStart ->
    [Lock#lock_record{start = End + 1}];
modify_lock(#lock_record{start = CurStart, till = CurEnd} = Lock, Start, End)
  when Start > CurStart, End >= CurEnd, Start =< CurEnd ->
    [Lock#lock_record{till = Start - 1}];
modify_lock(#lock_record{start = CurStart, till = CurEnd} = Lock, Start, End)
  when Start > CurStart, End < CurEnd ->
    ?debug("modify_lock", "Split ~p - ~p, ~p - ~p", [CurStart, Start-1, End + 1, CurEnd]),
    Lock1 = Lock#lock_record{till = Start - 1},
    Lock2 = Lock#lock_record{start = End + 1},
    [Lock1, Lock2];
modify_lock(Lock, _, _) ->
    [Lock].

