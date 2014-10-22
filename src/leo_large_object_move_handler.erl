%%======================================================================
%%
%% Leo Gateway Large Object MOVE Handler
%%
%% Copyright (c) 2012-2014 Rakuten, Inc.
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
-module(leo_large_object_move_handler).

-behaviour(gen_server).

-include("leo_gateway.hrl").
-include("leo_http.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_dcerl/include/leo_dcerl.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Application callbacks
-export([start_link/3, stop/1]).
-export([get_chunked/1]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-undef(DEF_SEPARATOR).
-undef(DEF_SEPARATOR).
-define(DEF_SEPARATOR, <<"\n">>).

-undef(DEF_TIMEOUT).
-define(DEF_TIMEOUT, 30000).

-record(state, {key = <<>>            :: binary(),
                max_obj_len = 0       :: non_neg_integer(),
                iterator              :: leo_large_object_commons:iterator()
               }).

-spec(start_link(binary(), non_neg_integer(), non_neg_integer()) ->
             ok | {error, any()}).
start_link(Key, Length, TotalChunk) ->
    gen_server:start_link(?MODULE, [Key, Length, TotalChunk], []).

%% @doc Stop this server
%%
-spec(stop(pid()) -> ok).
stop(Pid) ->
    gen_server:call(Pid, stop, ?DEF_TIMEOUT).

%% @doc Retrieve a part of chunked object from the storage cluster
%%
-spec(get_chunked(pid()) ->
             {ok, binary()} | {error, any()} | done ).
get_chunked(Pid) ->
    gen_server:call(Pid, get_chunked, ?DEF_TIMEOUT).

init([Key, Length, TotalChunk]) ->
    State = #state{key = Key,
                   max_obj_len = Length,
                   iterator = leo_large_object_commons:iterator_init(Key, TotalChunk)},
    {ok, State}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(get_chunked, _From, #state{iterator = I} = State) ->
    {Key, I2} = leo_large_object_commons:iterator_next(I),
    case Key of
        <<"">> ->
            {reply, done, State#state{iterator = I2}};
        _ ->
            {Reply, NewIterator} =
                case leo_gateway_rpc_handler:get(Key) of
                    %% only children
                    {ok, #?METADATA{cnumber = 0}, Bin} ->
                        {{ok, Bin}, I2};
                    %% both children and grand-children
                    {ok, #?METADATA{cnumber = TotalChunkedObjs}, _Bin} ->
                        %% grand-children
                        I3 = leo_large_object_commons:iterator_set_chunked(I2, Key, TotalChunkedObjs),
                        {Key2, I4} = leo_large_object_commons:iterator_next(I3),
                        case Key2 of
                            <<"">> ->
                                {done, I4};
                            _ ->
                                case leo_gateway_rpc_handler:get(Key2) of
                                    {ok, _, Bin2} ->
                                        {{ok, Bin2}, I4};
                                    {error, Cause} = Error ->
                                        ?error("handle_call/3", "key:~s, cause:~p",
                                               [binary_to_list(Key2), Cause]),
                                        {Error, I4}
                                end
                        end;
                    {error, Cause} = Error ->
                        ?error("handle_call/3", "key:~s, cause:~p",
                               [binary_to_list(Key), Cause]),
                        {Error, I2}
                end,
            {reply, Reply, State#state{iterator = NewIterator}}
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
