%%======================================================================
%%
%% Leo Gateway
%%
%% Copyright (c) 2012 Rakuten, Inc.
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
%% ---------------------------------------------------------------------
%% Leo Gateway -  RPC-Handler
%% @doc
%% @end
%%======================================================================
-module(leo_gateway_rpc_handler).

-author('Yosuke Hara').
-author('Yoshiyuki Kanno').

-export([head/1,
         get/1,
         get/2,
         get/3,
         delete/1,
         put/3,
         invoke/5
        ]).

-include("leo_gateway.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("leo_statistics/include/leo_statistics.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(ERR_TYPE_INTERNAL_ERROR, internal_server_error).

-type(method() :: get | put | delete | head).

-record(req_params, {
          req_id       = 0  :: integer(),
          timestamp    = 0  :: integer(),
          addr_id      = 0  :: integer(),
          redundancies = [] :: list()
         }).


%% @doc head object
%%
-spec(head(binary()) ->
             {ok, #metadata{}}|{error, any()}).
head(Key) ->
    %% @TODO reduce converting costs by binary_to_list
    KeyList = binary_to_list(Key),
    ReqParams = get_request_parameters(head, KeyList),
    invoke(ReqParams#req_params.redundancies,
           leo_storage_handler_object,
           head,
           [ReqParams#req_params.addr_id, KeyList],
           []).

%% @doc get object
%%
-spec(get(binary()) ->
             {ok, #metadata{}, binary()}|{error, any()}).
get(Key) ->
    %% @TODO reduce converting cost by binary_to_list
    KeyList = binary_to_list(Key),
    _ = leo_statistics_req_counter:increment(?STAT_REQ_GET),
    ReqParams = get_request_parameters(get, KeyList),
    invoke(ReqParams#req_params.redundancies,
           leo_storage_handler_object,
           get,
           [ReqParams#req_params.addr_id, KeyList, ReqParams#req_params.req_id],
           []).
-spec(get(binary(), integer()) ->
             {ok, match}|{ok, #metadata{}, binary()}|{error, any()}).
get(Key, ETag) ->
    %% @TODO reduce converting cost by binary_to_list
    KeyList = binary_to_list(Key),
    _ = leo_statistics_req_counter:increment(?STAT_REQ_GET),
    ReqParams = get_request_parameters(get, KeyList),
    invoke(ReqParams#req_params.redundancies,
           leo_storage_handler_object,
           get,
           [ReqParams#req_params.addr_id, KeyList, ETag, ReqParams#req_params.req_id],
           []).

-spec(get(binary(), integer(), integer()) ->
             {ok, #metadata{}, binary()}|{error, any()}).
get(Key, StartPos, EndPos) ->
    %% @TODO reduce converting cost by binary_to_list
    KeyList = binary_to_list(Key),
    _ = leo_statistics_req_counter:increment(?STAT_REQ_GET),
    ReqParams = get_request_parameters(get, KeyList),
    invoke(ReqParams#req_params.redundancies,
           leo_storage_handler_object,
           get,
           [ReqParams#req_params.addr_id,
            KeyList, StartPos, EndPos,
            ReqParams#req_params.req_id],
           []).


%% @doc delete object
%%
-spec(delete(binary()) ->
             ok|{error, any()}).
delete(Key) ->
    %% @TODO reduce converting cost by binary_to_list
    KeyList = binary_to_list(Key),
    _ = leo_statistics_req_counter:increment(?STAT_REQ_DEL),
    ReqParams = get_request_parameters(delete, KeyList),
    invoke(ReqParams#req_params.redundancies,
           leo_storage_handler_object,
           delete,
           [#object{addr_id   = ReqParams#req_params.addr_id,
                    key       = KeyList,
                    timestamp = ReqParams#req_params.timestamp},
            ReqParams#req_params.req_id],
           []).


%% @doc put object
%%
-spec(put(binary(), binary(), integer()) ->
             ok|{error, any()}).
put(Key, Body, Size) ->
    %% @TODO reduce converting cost by binary_to_list
    KeyList = binary_to_list(Key),
    _ = leo_statistics_req_counter:increment(?STAT_REQ_PUT),
    ReqParams = get_request_parameters(put, KeyList),
    invoke(ReqParams#req_params.redundancies,
           leo_storage_handler_object,
           put,
           [#object{addr_id   = ReqParams#req_params.addr_id,
                    key       = KeyList,
                    data      = Body,
                    dsize     = Size,
                    timestamp = ReqParams#req_params.timestamp},
            ReqParams#req_params.req_id],
           []).


%% @doc do invoke rpc calls with handling retries
%%
-spec(invoke(list(), atom(), atom(), list(), list()) ->
             ok|{ok, any()}|{error, any()}).
invoke([], _Mod, _Method, _Args, Errors) ->
    {error, error_filter(Errors)};
invoke([{_, false}|T], Mod, Method, Args, Errors) ->
    invoke(T, Mod, Method, Args, [?ERR_TYPE_INTERNAL_ERROR|Errors]);
invoke([{Node, true}|T], Mod, Method, Args, Errors) ->
    RPCKey = rpc:async_call(Node, Mod, Method, Args),
    case rpc:nb_yield(RPCKey, ?DEF_REQ_TIMEOUT) of
        %% put | delete
        {value, ok = Ret} ->
            Ret;
        %% get-1
        {value, {ok, _Meta, _Bin} = Ret} ->
            Ret;
        %% get-2
        {value, {ok, match} = Ret} ->
            Ret;
        %% head
        {value, {ok, _Meta} = Ret} ->
            Ret;
        %% error
        Error ->
            E = handle_error(Node, Mod, Method, Args, Error),
            invoke(T, Mod, Method, Args, [E|Errors])
    end.


%% @doc get request parameters.
%%
-spec(get_request_parameters(method(), string()) ->
             #req_params{}).
get_request_parameters(Method, Key) ->
    {ok, #redundancies{id = Id, nodes = Redundancies}} =
        leo_redundant_manager_api:get_redundancies_by_key(Method, Key),

    UnivDateTime = erlang:universaltime(),
    {_,_,NowPart} = erlang:now(),
    {{Y,MO,D},{H,MI,S}} = UnivDateTime,

    ReqId = erlang:phash2([Y,MO,D,H,MI,S, erlang:node(), Key, NowPart]),
    Timestamp = calendar:datetime_to_gregorian_seconds(UnivDateTime),

    #req_params{addr_id      = Id,
                redundancies = Redundancies,
                req_id       = ReqId,
                timestamp    = Timestamp}.


%% @doc error messeage filtering.
%%
error_filter([not_found = Error|_T])              -> Error;
error_filter([H|T])                               -> error_filter(T, H).
error_filter([],                            Prev) -> Prev;
error_filter([not_found = Error|_T],       _Prev) -> Error;
error_filter([_H|T],                        Prev) -> error_filter(T, Prev).


%% @doc handle an error response.
%%
handle_error(_Node, _Mod, _Method, _Args, {value, {error, not_found = Error}}) ->
    Error;
handle_error(Node, Mod, Method, _Args, {value, {error, Cause}}) ->
    ?warn("handle_error/5", "node:~w, mod:~w, method:~w, cause:~p",
          [Node, Mod, Method, Cause]),
    ?ERR_TYPE_INTERNAL_ERROR;
handle_error(Node, Mod, Method, _Args, {value, {badrpc, Cause}}) ->
    ?warn("handle_error/5", "node:~w, mod:~w, method:~w, cause:~p",
          [Node, Mod, Method, Cause]),
    ?ERR_TYPE_INTERNAL_ERROR;
handle_error(Node, Mod, Method, _Args, timeout = Error) ->
    ?warn("handle_error/5", "node:~w, mod:~w, method:~w, cause:~p",
          [Node, Mod, Method, Error]),
    Error.

