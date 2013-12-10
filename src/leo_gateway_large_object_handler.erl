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
%%======================================================================
-module(leo_gateway_large_object_handler).

-author('Yosuke Hara').

-behaviour(gen_server).

-include("leo_gateway.hrl").
-include("leo_http.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_dcerl/include/leo_dcerl.hrl").
-include_lib("eunit/include/eunit.hrl").


%% Application callbacks
-export([start_link/1, start_link/2, stop/1]).
-export([put/2, get/4, rollback/1, result/1, state/1]).
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

-record(state, {key = <<>>         :: binary(),
                max_obj_len = 0    :: pos_integer(),
                stacked_bin = <<>> :: binary(),
                num_of_chunks = 0  :: pos_integer(),
                total_len = 0      :: pos_integer(),
                md5_context = <<>> :: binary(),
                errors = []        :: list()
               }).


%% ===================================================================
%% API
%% ===================================================================
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
-spec(start_link(binary()) ->
             ok | {error, any()}).
start_link(Key) ->
    gen_server:start_link(?MODULE, [Key, 0], []).

-spec(start_link(binary(), pos_integer()) ->
             ok | {error, any()}).
start_link(Key, Length) ->
    gen_server:start_link(?MODULE, [Key, Length], []).

%% @doc Stop this server
%%
-spec(stop(pid()) -> ok).
stop(Pid) ->
    gen_server:call(Pid, stop, ?DEF_TIMEOUT).


%% @doc Insert a chunked object into the storage cluster
%%
-spec(put(pid(), binary()) ->
             ok | {error, any()}).
put(Pid, Bin) ->
    gen_server:call(Pid, {put, Bin}, ?DEF_TIMEOUT).


%% @doc Retrieve a chunked object from the storage cluster
%%
-spec(get(pid(), integer(), any(), #metadata{}) ->
             ok | {error, any()}).
get(Pid, TotalOfChunkedObjs, Req, Meta) ->
    %% Since this call may take a long time in case of handling a very large file,
    %% Timeout sholud be infinity.
    gen_server:call(Pid, {get, TotalOfChunkedObjs, Req, Meta}, infinity).


%% @doc Make a rollback before all operations
%%
-spec(rollback(pid()) ->
             ok | {error, any()}).
rollback(Pid) ->
    gen_server:call(Pid, rollback, ?DEF_TIMEOUT).


%% @doc Retrieve a result
%%
-spec(result(pid()) ->
             ok | {error, any()}).
result(Pid) ->
    gen_server:call(Pid, result, ?DEF_TIMEOUT).


%% @doc Retrieve state
%%
-spec(state(pid()) ->
             ok | {error, any()}).
state(Pid) ->
    gen_server:call(Pid, state, ?DEF_TIMEOUT).


%%====================================================================
%% GEN_SERVER CALLBACKS
%%====================================================================
%% Function: init(Args) -> {ok, State}          |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
init([Key, Length]) ->
    {ok, #state{key = Key,
                max_obj_len = Length,
                num_of_chunks = 1,
                stacked_bin = <<>>,
                md5_context = crypto:hash_init(md5),
                errors = []}}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};


handle_call({get, TotalOfChunkedObjs, Req, Meta}, _From, #state{key = Key} = State) ->
    Mime = leo_mime:guess_mime(Key),
    Header = [?SERVER_HEADER,
              {?HTTP_HEAD_RESP_CONTENT_TYPE,  Mime},
              {?HTTP_HEAD_RESP_ETAG,          ?http_etag(Meta#metadata.checksum)},
              {?HTTP_HEAD_RESP_LAST_MODIFIED, ?http_date(Meta#metadata.timestamp)}],
    {ok, Req2} = cowboy_req:chunked_reply(?HTTP_ST_OK, Header, Req),
    Ref1 = case leo_cache_api:put_begin_tran(Key) of
               {ok, Ref} -> Ref;
               _ -> undefined
           end,
    Reply = handle_loop(Key, TotalOfChunkedObjs, Req2, Meta, Ref1),

    case Reply of
        {ok, _Req} ->
            Mime = leo_mime:guess_mime(Key),
            CacheMeta = #cache_meta{
                           md5          = Meta#metadata.checksum,
                           mtime        = Meta#metadata.timestamp,
                           content_type = Mime},
            catch leo_cache_api:put_end_tran(Ref1, Key, CacheMeta, true);
        _ ->
            catch leo_cache_api:put_end_tran(Ref1, Key, undefined, false)
    end,
    {reply, Reply, State};


handle_call({put, Bin}, _From, #state{key = Key,
                                      max_obj_len   = MaxObjLen,
                                      stacked_bin   = StackedBin,
                                      num_of_chunks = NumOfChunks,
                                      total_len     = TotalLen,
                                      md5_context   = Context} = State) ->
    Size  = erlang:byte_size(Bin),
    TotalLen_1 = TotalLen + Size,
    Bin_1 = << StackedBin/binary, Bin/binary >>,

    case (erlang:byte_size(Bin_1) >= MaxObjLen) of
        true ->
            NumOfChunksBin = list_to_binary(integer_to_list(NumOfChunks)),
            << Bin_2:MaxObjLen/binary, StackedBin_1/binary >> = Bin_1,

            case leo_gateway_rpc_handler:put(<< Key/binary, ?DEF_SEPARATOR/binary, NumOfChunksBin/binary >>,
                                             Bin_2, MaxObjLen, NumOfChunks) of
                {ok, _ETag} ->
                    Context_1 = crypto:hash_update(Context, Bin_2),
                    {reply, ok, State#state{stacked_bin   = StackedBin_1,
                                            num_of_chunks = NumOfChunks + 1,
                                            total_len     = TotalLen_1,
                                            md5_context   = Context_1}};
                {error, Cause} ->
                    ?error("handle_call/3", "key:~s, cause:~p", [binary_to_list(Key), Cause]),
                    {reply, {error, Cause}, State}
            end;
        false ->
            {reply, ok, State#state{stacked_bin = Bin_1,
                                    total_len   = TotalLen_1}}
    end;


handle_call(rollback, _From, #state{key = Key,
                                    num_of_chunks = NumOfChunks} = State) ->
    ok = delete_chunked_objects(Key, NumOfChunks),
    {reply, ok, State#state{errors = []}};


handle_call(result, _From, #state{key = Key,
                                  md5_context   = Context,
                                  stacked_bin   = StackedBin,
                                  num_of_chunks = NumOfChunks,
                                  total_len     = TotalLen,
                                  errors = []} = State) ->
    Ret = case StackedBin of
              <<>> ->
                  {ok, {NumOfChunks - 1, Context}};
              _ ->
                  NumOfChunksBin = list_to_binary(integer_to_list(NumOfChunks)),
                  Size = erlang:byte_size(StackedBin),

                  case leo_gateway_rpc_handler:put(
                         << Key/binary, ?DEF_SEPARATOR/binary, NumOfChunksBin/binary >>,
                         StackedBin, Size, NumOfChunks) of
                      {ok, _ETag} ->
                          {ok, {NumOfChunks,
                                crypto:hash_update(Context, StackedBin)}};
                      {error, Cause} ->
                          {error, Cause}
                  end
          end,

    State_1 = State#state{stacked_bin = <<>>,
                          errors = []},
    case Ret of
        {ok, {NumOfChunks_1, Context_1}} ->
            Digest = crypto:hash_final(Context_1),
            Reply  = {ok, #large_obj_info{key = Key,
                                          num_of_chunks = NumOfChunks_1,
                                          length = TotalLen,
                                          md5_context = Digest}},
            {reply, Reply, State_1#state{md5_context = Digest}};
        {error, Reason} ->
            Reply = {error, {#large_obj_info{key = Key,
                                             num_of_chunks = NumOfChunks}, Reason}},
            {reply, Reply, State_1}
    end;
handle_call(result, _From, #state{key = Key,
                                  num_of_chunks = NumOfChunks,
                                  errors = Errors} = State) ->
    Cause = lists:reverse(Errors),
    Reply = {error, {#large_obj_info{key = Key,
                                     num_of_chunks = NumOfChunks}, Cause}},
    {reply, Reply, State#state{stacked_bin = <<>>,
                               errors = []}};


handle_call(state, _From, #state{key = Key,
                                 md5_context   = Context,
                                 num_of_chunks = NumOfChunks,
                                 total_len     = TotalLen} = State) ->
    {reply, {ok, #large_obj_info{key = Key,
                                 num_of_chunks = NumOfChunks,
                                 length = TotalLen,
                                 md5_context = Context}}, State}.


%% Function: handle_cast(Msg, State) -> {noreply, State}          |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast message
handle_cast(_Msg, State) ->
    {noreply, State}.


%% Function: handle_info(Info, State) -> {noreply, State}          |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
handle_info(_Info, State) ->
    {noreply, State}.

%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
terminate(_Reason, _State) ->
    ok.

%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% INNTERNAL FUNCTION
%%====================================================================
%% @doc Retrieve chunked objects
%% @private
-spec(handle_loop(binary(), integer(), any(), #metadata{}, any()) ->
             {ok, any()}).
handle_loop(Key, Total, Req, Meta, Ref) ->
    handle_loop(Key, Key, Total, 0, Req, Meta, Ref).

-spec(handle_loop(binary(), binary(), integer(), integer(), any(), #metadata{}, any()) ->
             {ok, any()}).
handle_loop(_Key, _ChunkedKey, Total, Total, Req, _Meta, _Ref) ->
    {ok, Req};

handle_loop(OriginKey, ChunkedKey, Total, Index, Req, Meta, Ref) ->
    IndexBin = list_to_binary(integer_to_list(Index + 1)),
    Key2 = << ChunkedKey/binary, ?DEF_SEPARATOR/binary, IndexBin/binary >>,

    case leo_gateway_rpc_handler:get(Key2) of
        %% only children
        {ok, #metadata{cnumber = 0}, Bin} ->

            case cowboy_req:chunk(Bin, Req) of
                ok ->
                    leo_cache_api:put(Ref, OriginKey, Bin),
                    handle_loop(OriginKey, ChunkedKey, Total, Index + 1, Req, Meta, Ref);
                {error, Cause} ->
                    ?error("handle_loop/7", "key:~s, index:~p, cause:~p",
                           [binary_to_list(Key2), Index, Cause]),
                    {error, Cause}
            end;

        %% both children and grand-children
        {ok, #metadata{cnumber = TotalChunkedObjs}, _Bin} ->
            %% grand-children
            case handle_loop(OriginKey, Key2, TotalChunkedObjs, 0, Req, Meta, Ref) of
                {ok, Req} ->
                    %% children
                    handle_loop(OriginKey, ChunkedKey, Total, Index + 1, Req, Meta, Ref);
                {error, Cause} ->
                    ?error("handle_loop/7", "key:~s, index:~p, cause:~p",
                           [binary_to_list(Key2), Index, Cause]),
                    {error, Cause}
            end;
        {error, Cause} ->
            ?error("handle_loop/7", "key:~s, index:~p, cause:~p",
                   [binary_to_list(Key2), Index, Cause]),
            {error, Cause}
    end.

%% @doc Remove chunked objects
%% @private
-spec(delete_chunked_objects(binary(), integer()) ->
             ok).
delete_chunked_objects(_, 0) ->
    ok;
delete_chunked_objects(Key1, Index1) ->
    Index2 = list_to_binary(integer_to_list(Index1)),
    Key2   = << Key1/binary, ?DEF_SEPARATOR/binary, Index2/binary >>,

    case leo_gateway_rpc_handler:delete(Key2) of
        ok ->
            void;
        {error, Cause} ->
            ?error("delete_chunked_objects/2", "key:~s, index:~p, cause:~p",
                   [binary_to_list(Key1), Index1, Cause])
    end,
    delete_chunked_objects(Key1, Index1 - 1).

