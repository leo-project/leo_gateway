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
%% ---------------------------------------------------------------------
%% Leo Gateway - RPC-Handler
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
         put/2, put/3, put/4, put/6, put/7,
         invoke/5,
         get_request_parameters/2
        ]).

-include("leo_gateway.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-undef(MAX_RETRY_TIMES).
-include_lib("leo_statistics/include/leo_statistics.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(ERR_TYPE_INTERNAL_ERROR, internal_server_error).

-record(req_params, {
          req_id       = 0  :: integer(),
          timestamp    = 0  :: integer(),
          addr_id      = 0  :: integer(),
          redundancies = [] :: list(#redundant_node{})
         }).


%% @doc Retrieve a metadata from the storage-cluster
%%
-spec(head(binary()) ->
             {ok, #?METADATA{}}|{error, any()}).
head(Key) ->
    ReqParams = get_request_parameters(head, Key),
    invoke(ReqParams#req_params.redundancies,
           leo_storage_handler_object,
           head,
           [ReqParams#req_params.addr_id, Key],
           []).

%% @doc Retrieve an object from the storage-cluster
%%
-spec(get(binary()) ->
             {ok, #?METADATA{}, binary()}|{error, any()}).
get(Key) ->
    ok = leo_metrics_req:notify(?STAT_COUNT_GET),
    ReqParams = get_request_parameters(get, Key),
    invoke(ReqParams#req_params.redundancies,
           leo_storage_handler_object,
           get,
           [ReqParams#req_params.addr_id, Key, ReqParams#req_params.req_id],
           []).
-spec(get(binary(), integer()) ->
             {ok, match}|{ok, #?METADATA{}, binary()}|{error, any()}).
get(Key, ETag) ->
    ok = leo_metrics_req:notify(?STAT_COUNT_GET),
    ReqParams = get_request_parameters(get, Key),
    invoke(ReqParams#req_params.redundancies,
           leo_storage_handler_object,
           get,
           [ReqParams#req_params.addr_id, Key, ETag, ReqParams#req_params.req_id],
           []).

-spec(get(binary(), integer(), integer()) ->
             {ok, #?METADATA{}, binary()}|{error, any()}).
get(Key, StartPos, EndPos) ->
    ok = leo_metrics_req:notify(?STAT_COUNT_GET),
    ReqParams = get_request_parameters(get, Key),
    invoke(ReqParams#req_params.redundancies,
           leo_storage_handler_object,
           get,
           [ReqParams#req_params.addr_id,
            Key, StartPos, EndPos,
            ReqParams#req_params.req_id],
           []).


%% @doc Remove an object from storage-cluster
%%
-spec(delete(binary()) ->
             ok|{error, any()}).
delete(Key) ->
    ok = leo_metrics_req:notify(?STAT_COUNT_DEL),
    ReqParams = get_request_parameters(delete, Key),
    invoke(ReqParams#req_params.redundancies,
           leo_storage_handler_object,
           delete,
           [#?OBJECT{addr_id   = ReqParams#req_params.addr_id,
                     key       = Key,
                     timestamp = ReqParams#req_params.timestamp},
            ReqParams#req_params.req_id],
           []).


%% @doc Insert an object into the storage-cluster (regular-case)
%%
-spec(put(binary(), binary()) ->
             ok|{ok, pos_integer()}|{error, any()}).
put(Key, Body) ->
    Size = byte_size(Body),
    put(Key, Body, Size, 0, 0, 0, 0).

-spec(put(binary(), binary(), integer()) ->
             ok|{ok, pos_integer()}|{error, any()}).
put(Key, Body, Size) ->
    put(Key, Body, Size, 0, 0, 0, 0).

%% @doc Insert an object into the storage-cluster (child of chunked-object)
%%
-spec(put(binary(), binary(), integer(), integer()) ->
             ok|{ok, pos_integer()}|{error, any()}).
put(Key, Body, Size, Index) ->
    put(Key, Body, Size, 0, 0, Index, 0).

%% @doc Insert an object into the storage-cluster (parent of chunked-object)
%%
-spec(put(binary(), binary(), integer(), integer(), integer(), integer()) ->
             ok|{ok, pos_integer()}|{error, any()}).
put(Key, Body, Size, ChunkedSize, TotalOfChunks, Digest) ->
    put(Key, Body, Size, ChunkedSize, TotalOfChunks, 0, Digest).

%% @doc Insert an object into the storage-cluster
%%
-spec(put(binary(), binary(), integer(), integer(), integer(), integer(), integer()) ->
             ok|{ok, pos_integer()}|{error, any()}).
put(Key, Body, Size, ChunkedSize, TotalOfChunks, ChunkIndex, Digest) ->
    ok = leo_metrics_req:notify(?STAT_COUNT_PUT),
    ReqParams = get_request_parameters(put, Key),

    invoke(ReqParams#req_params.redundancies,
           leo_storage_handler_object,
           put,
           [#?OBJECT{addr_id   = ReqParams#req_params.addr_id,
                     key       = Key,
                     data      = Body,
                     dsize     = Size,
                     timestamp = ReqParams#req_params.timestamp,
                     csize     = ChunkedSize,
                     cnumber   = TotalOfChunks,
                     cindex    = ChunkIndex,
                     checksum  = Digest
                    },
            ReqParams#req_params.req_id],
           []).


%% @doc Do invoke rpc calls with handling retries
%%
-spec(invoke(list(), atom(), atom(), list(), list()) ->
             ok|{ok, any()}|{ok, #?METADATA{}, binary()}|{error, any()}).
invoke([], _Mod, _Method, _Args, Errors) ->
    {error, error_filter(Errors)};
invoke([#redundant_node{available = false}|T], Mod, Method, Args, Errors) ->
    invoke(T, Mod, Method, Args, [?ERR_TYPE_INTERNAL_ERROR|Errors]);
invoke([#redundant_node{node      = Node,
                        available = true}|T], Mod, Method, Args, Errors) ->
    RPCKey  = rpc:async_call(Node, Mod, Method, Args),
    Timeout = timeout(Method, Args),

    case rpc:nb_yield(RPCKey, Timeout) of
        %% delete
        {value, ok = Ret} ->
            Ret;
        %% put
        {value, {ok, {etag, ETag}}} ->
            {ok, ETag};
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
        {value, {error,_Cause}} = Error ->
            {error, handle_error(Node, Mod, Method, Args, Error)};
        Error ->
            E = handle_error(Node, Mod, Method, Args, Error),
            invoke(T, Mod, Method, Args, [E|Errors])
    end.


%% @doc Get request parameters
%%
-spec(get_request_parameters(atom(), binary()) ->
             #req_params{}).
get_request_parameters(Method, Key) ->
    {ok, #redundancies{id = Id, nodes = Redundancies}} =
        leo_redundant_manager_api:get_redundancies_by_key(Method, Key),

    UnivDateTime = erlang:universaltime(),
    {_,_,NowPart} = os:timestamp(),
    {{Y,MO,D},{H,MI,S}} = UnivDateTime,

    ReqId = erlang:phash2([Y,MO,D,H,MI,S, erlang:node(), Key, NowPart]),
    Timestamp = calendar:datetime_to_gregorian_seconds(UnivDateTime),

    #req_params{addr_id      = Id,
                redundancies = Redundancies,
                req_id       = ReqId,
                timestamp    = Timestamp}.


%% @doc Error messeage filtering
%%
error_filter([not_found|_])          -> not_found;
error_filter([{error, not_found}|_]) -> not_found;
error_filter([H|T])                  -> error_filter(T, H).
error_filter([],             Prev)   -> Prev;
error_filter([not_found|_T],_Prev)   -> not_found;
error_filter([{error,not_found}|_],_Prev) -> not_found;
error_filter([_H|T],                Prev) -> error_filter(T, Prev).


%% @doc Handle an error response
%%
handle_error(_Node,_Mod,_Method,_Args, {value, {error, not_found = Error}}) ->
    Error;
handle_error(_Node,_Mod,_Method,_Args, {value, {error, unavailable = Error}}) ->
    Error;
handle_error(Node, Mod, Method, _Args, {value, {error, Cause}}) ->
    ?warn("handle_error/5",
          [{node, Node}, {mod, Mod},
           {method, Method}, {cause, Cause}]),
    ?ERR_TYPE_INTERNAL_ERROR;
handle_error(Node, Mod, Method, _Args, {value, {badrpc, Cause}}) ->
    ?warn("handle_error/5",
          [{node, Node}, {mod, Mod},
           {method, Method}, {cause, Cause}]),
    ?ERR_TYPE_INTERNAL_ERROR;
handle_error(Node, Mod, Method, _Args, timeout = Cause) ->
    ?warn("handle_error/5",
          [{node, Node}, {mod, Mod},
           {method, Method}, {cause, Cause}]),
    Cause.


%% @doc Timeout depends on length of an object
%%
timeout(Len) when ?TIMEOUT_L1_LEN > Len -> ?env_timeout_level_1();
timeout(Len) when ?TIMEOUT_L2_LEN > Len -> ?env_timeout_level_2();
timeout(Len) when ?TIMEOUT_L3_LEN > Len -> ?env_timeout_level_3();
timeout(Len) when ?TIMEOUT_L4_LEN > Len -> ?env_timeout_level_4();
timeout(_)                              -> ?env_timeout_level_5().

timeout(put, [#?OBJECT{dsize = DSize}, _]) ->
    timeout(DSize);
timeout(get, _) -> ?env_timeout_for_get();
timeout(find_by_parent_dir, _) -> ?env_timeout_for_ls();
timeout(_, _) ->
    ?DEF_REQ_TIMEOUT.
