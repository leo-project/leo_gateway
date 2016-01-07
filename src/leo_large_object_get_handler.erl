%%======================================================================
%%
%% Leo Gateway Large Object GET Handler
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
-define(DEF_SEPARATOR, <<"\n">>).

-undef(DEF_TIMEOUT).
-define(DEF_TIMEOUT, 30000).

-record(state, {
          key = <<>>            :: binary(),
          socket = undefined    :: any(),
          transport = undefined :: undefined | module(),
          sending_chunked_obj_len :: pos_integer(),
          iterator              :: leo_large_object_commons:iterator()
         }).

-record(req_info, {
          key = <<>> :: binary(),
          chunk_key = <<>>      :: binary(),
          request    :: any(),
          metadata   :: #?METADATA{},
          reference  :: reference(),
          transport  :: any(),
          socket     :: any(),
          sending_chunked_obj_len :: pos_integer()
         }).


%%====================================================================
%% API
%%====================================================================
-spec(start_link(Args) ->
             ok | {error, any()} when Args::tuple()).
start_link({Key, Transport, Socket, SendChunkLen}) ->
    gen_server:start_link(?MODULE, [Key, Transport, Socket, SendChunkLen], []).


%% @doc Stop this server
%%
-spec(stop(Pid) ->
             ok when Pid::pid()).
stop(Pid) ->
    gen_server:call(Pid, stop, ?DEF_TIMEOUT).


%% @doc Retrieve a chunked object from the storage cluster
%%
-spec(get(Pid, TotalOfChunkedObjs, Req, Meta) ->
             ok | {error, any()} when Pid::pid(),
                                      TotalOfChunkedObjs::non_neg_integer(),
                                      Req::any(),
                                      Meta::#?METADATA{}).
get(Pid, TotalOfChunkedObjs, Req, Meta) ->
    %% Since this call may take a long time in case of handling a very large file,
    %% Timeout sholud be infinity.
    gen_server:call(Pid, {get, TotalOfChunkedObjs, Req, Meta}, infinity).


%%====================================================================
%% GEN_SERVER CALLBACKS
%%====================================================================
init([Key, Transport, Socket, SendChunkLen]) ->
    State = #state{key = Key,
                   transport = Transport,
                   socket    = Socket,
                   sending_chunked_obj_len = SendChunkLen
                  },
    {ok, State}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call({get, TotalOfChunkedObjs, Req, Meta}, _From,
            #state{key = Key, transport = Transport, socket = Socket, sending_chunked_obj_len = SendChunkLen} = State) ->
    Ref_1 = case catch leo_cache_api:put_begin_tran(Key) of
                {ok, Ref} -> Ref;
                _ -> undefined
            end,
    Reply = handle_loop(TotalOfChunkedObjs, #req_info{key = Key,
                                                      chunk_key = Key,
                                                      request = Req,
                                                      metadata = Meta,
                                                      reference = Ref_1,
                                                      transport = Transport,
                                                      socket = Socket,
                                                      sending_chunked_obj_len = SendChunkLen}),
    case Reply of
        {ok, _Req} ->
            CacheMeta = #cache_meta{
                           md5   = Meta#?METADATA.checksum,
                           mtime = Meta#?METADATA.timestamp,
                           content_type = leo_mime:guess_mime(Key)},
            catch leo_cache_api:put_end_tran(Ref_1, Key, CacheMeta, true);
        _ ->
            catch leo_cache_api:put_end_tran(Ref_1, Key, undefined, false)
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
-spec(handle_loop(TotalChunkObjs, ReqInfo) ->
             {ok, any()} | {error, any()} when TotalChunkObjs::non_neg_integer(),
                                               ReqInfo::#req_info{}).
handle_loop(TotalChunkObjs, ReqInfo) ->
    handle_loop(0, TotalChunkObjs, ReqInfo).


%% @private
handle_loop(TotalChunkObjs, TotalChunkObjs, #req_info{request = Req}) ->
    {ok, Req};
handle_loop(Index, TotalChunkObjs, #req_info{key = AcctualKey,
                                             chunk_key = ChunkObjKey,
                                             reference = Ref,
                                             socket    = Socket,
                                             transport = Transport,
                                             sending_chunked_obj_len = SendChunkLen
                                            } = ReqInfo) ->
    IndexBin = list_to_binary(integer_to_list(Index + 1)),
    Key_1 = << ChunkObjKey/binary,
               ?DEF_SEPARATOR/binary,
               IndexBin/binary >>,

    case leo_gateway_rpc_handler:get(Key_1) of
        %%
        %% only children
        %%
        {ok, #?METADATA{cnumber = 0}, Bin} ->
            case leo_net:chunked_send(Transport, Socket, Bin, SendChunkLen) of
                ok ->
                    catch leo_cache_api:put(Ref, AcctualKey, Bin),
                    handle_loop(Index + 1, TotalChunkObjs, ReqInfo);
                {error, Cause} ->
                    ?error("handle_loop/3",
                           [{key, binary_to_list(Key_1)},
                            {index, Index}, {cause, Cause}]),
                    {error, Cause}
            end;

        %%
        %% both children and grand-children
        %%
        {ok, #?METADATA{cnumber = TotalChunkObjs_1}, _Bin} ->
            %% grand-children
            case handle_loop(0, TotalChunkObjs_1,
                             ReqInfo#req_info{chunk_key = Key_1}) of
                {ok, Req} ->
                    %% children
                    handle_loop(Index + 1, TotalChunkObjs,
                                ReqInfo#req_info{request = Req});
                {error, Cause} ->
                    ?error("handle_loop/3",
                           [{key, binary_to_list(Key_1)},
                            {index, Index}, {cause, Cause}]),
                    {error, Cause}
            end;
        {error, Cause} ->
            ?error("handle_loop/3",
                   [{key, binary_to_list(Key_1)},
                    {index, Index}, {cause, Cause}]),
            {error, Cause}
    end.
