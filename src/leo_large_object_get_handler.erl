%%======================================================================
%%
%% Leo Gateway Large Object GET Handler
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
-module(leo_large_object_get_handler).

-behaviour(gen_server).

-include("leo_gateway.hrl").
-include("leo_http.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_dcerl/include/leo_dcerl.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Application APIs 
-export([start_link/1, stop/1, get/4]).

%% get_server callbacks
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
                socket = undefined    :: any(),
                transport = undefined :: undefined | module(),
                iterator              :: leo_large_object_commons:iterator()
               }).

-spec(start_link(tuple()) ->
             ok | {error, any()}).
start_link({Key, Transport, Socket}) ->
    gen_server:start_link(?MODULE, [Key, Transport, Socket], []).

%% @doc Stop this server
%%
-spec(stop(pid()) -> ok).
stop(Pid) ->
    gen_server:call(Pid, stop, ?DEF_TIMEOUT).

%% @doc Retrieve a chunked object from the storage cluster
%%
-spec(get(pid(), integer(), any(), #?METADATA{}) ->
             ok | {error, any()}).
get(Pid, TotalOfChunkedObjs, Req, Meta) ->
    %% Since this call may take a long time in case of handling a very large file,
    %% Timeout sholud be infinity.
    gen_server:call(Pid, {get, TotalOfChunkedObjs, Req, Meta}, infinity).

%%====================================================================
%% GEN_SERVER CALLBACKS
%%====================================================================
%% Function: init(Args) -> {ok, State}          |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
init([Key, Transport, Socket]) ->
    State = #state{key = Key,
                   transport = Transport,
                   socket = Socket},
    {ok, State}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call({get, TotalOfChunkedObjs, Req, Meta}, _From,
            #state{key = Key, transport = Transport, socket = Socket} = State) ->
    Ref1 = case catch leo_cache_api:put_begin_tran(Key) of
               {ok, Ref} -> Ref;
               _ -> undefined
           end,
    Reply = handle_loop(Key, TotalOfChunkedObjs, Req, Meta, Ref1, Transport, Socket),

    case Reply of
        {ok, _Req} ->
            Mime = leo_mime:guess_mime(Key),
            CacheMeta = #cache_meta{
                           md5          = Meta#?METADATA.checksum,
                           mtime        = Meta#?METADATA.timestamp,
                           content_type = Mime},
            catch leo_cache_api:put_end_tran(Ref1, Key, CacheMeta, true);
        _ ->
            catch leo_cache_api:put_end_tran(Ref1, Key, undefined, false)
    end,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% INNTERNAL FUNCTION
%%====================================================================
%% @doc Retrieve chunked objects
%% @private
-spec(handle_loop(binary(), integer(), any(), #?METADATA{}, any(), module(), any()) ->
             {ok, any()} | {error, any()}).
handle_loop(Key, Total, Req, Meta, Ref, Transport, Socket) ->
    handle_loop(Key, Key, Total, 0, Req, Meta, Ref, Transport, Socket).

-spec(handle_loop(binary(), binary(), integer(), integer(), any(), #?METADATA{}, any(), module(), any()) ->
             {ok, any()} | {error, any()}).
handle_loop(_Key, _ChunkedKey, Total, Total, Req, _Meta, _Ref, _, _) ->
    {ok, Req};

handle_loop(OriginKey, ChunkedKey, Total, Index, Req, Meta, Ref, Transport, Socket) ->
    IndexBin = list_to_binary(integer_to_list(Index + 1)),
    Key2 = << ChunkedKey/binary, ?DEF_SEPARATOR/binary, IndexBin/binary >>,

    case leo_gateway_rpc_handler:get(Key2) of
        %% only children
        {ok, #?METADATA{cnumber = 0}, Bin} ->

            case Transport:send(Socket, Bin) of
                ok ->
                    catch leo_cache_api:put(Ref, OriginKey, Bin),
                    handle_loop(OriginKey, ChunkedKey, Total, Index + 1, Req, Meta, Ref, Transport, Socket);
                {error, Cause} ->
                    ?error("handle_loop/9", "key:~s, index:~p, cause:~p",
                           [binary_to_list(Key2), Index, Cause]),
                    {error, Cause}
            end;

        %% both children and grand-children
        {ok, #?METADATA{cnumber = TotalChunkedObjs}, _Bin} ->
            %% grand-children
            case handle_loop(OriginKey, Key2, TotalChunkedObjs, 0, Req, Meta, Ref, Transport, Socket) of
                {ok, Req} ->
                    %% children
                    handle_loop(OriginKey, ChunkedKey, Total, Index + 1, Req, Meta, Ref, Transport, Socket);
                {error, Cause} ->
                    ?error("handle_loop/9", "key:~s, index:~p, cause:~p",
                           [binary_to_list(Key2), Index, Cause]),
                    {error, Cause}
            end;
        {error, Cause} ->
            ?error("handle_loop/9", "key:~s, index:~p, cause:~p",
                   [binary_to_list(Key2), Index, Cause]),
            {error, Cause}
    end.

