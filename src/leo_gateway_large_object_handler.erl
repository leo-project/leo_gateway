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

-include("leo_http.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_dcerl/include/leo_dcerl.hrl").
-include_lib("eunit/include/eunit.hrl").


%% Application callbacks
-export([start_link/1, stop/1]).
-export([put/4, get/4, rollback/2, result/1]).
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

-record(state, {key = <<>>  :: binary(),
                md5_context :: binary(),
                errors = [] :: list()
               }).


%% ===================================================================
%% API
%% ===================================================================
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
-spec(start_link(binary()) ->
             ok | {error, any()}).
start_link(Key) ->
    gen_server:start_link(?MODULE, [Key], []).

%% @doc Stop this server
%%
-spec(stop(pid()) -> ok).
stop(Pid) ->
    gen_server:call(Pid, stop, ?DEF_TIMEOUT).


%% @doc Insert a chunked object into the storage cluster
%%
-spec(put(pid(), pos_integer(), pos_integer(), binary()) ->
             ok | {error, any()}).
put(Pid, Index, Size, Bin) ->
    gen_server:call(Pid, {put, Index, Size, Bin}, ?DEF_TIMEOUT).


%% @doc Retrieve a chunked object from the storage cluster
%%
-spec(get(pid(), integer(), any(), #metadata{}) ->
             ok | {error, any()}).
get(Pid, TotalOfChunkedObjs, Req, Meta) ->
    gen_server:call(Pid, {get, TotalOfChunkedObjs, Req, Meta}, ?DEF_TIMEOUT).


%% @doc Make a rollback before all operations
%%
-spec(rollback(pid(), integer()) ->
             ok | {error, any()}).
rollback(Pid, TotalOfChunkedObjs) ->
    gen_server:call(Pid, {rollback, TotalOfChunkedObjs}, ?DEF_TIMEOUT).


%% @doc Retrieve a result
%%
-spec(result(pid()) ->
             ok | {error, any()}).
result(Pid) ->
    gen_server:call(Pid, result, ?DEF_TIMEOUT).


%%====================================================================
%% GEN_SERVER CALLBACKS
%%====================================================================
%% Function: init(Args) -> {ok, State}          |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
init([Key]) ->
    {ok, #state{key = Key,
                md5_context = crypto:hash_init(md5),
                errors = []}}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};


handle_call({get, TotalOfChunkedObjs, Req, Meta}, _From, #state{key = Key} = State) ->
    {ok, Req2} = cowboy_req:chunked_reply(?HTTP_ST_OK, [?SERVER_HEADER], Req),
    {ok, Ref}  = leo_cache_api:put_begin_tran(Key),
    Reply = handle_loop(Key, TotalOfChunkedObjs, Req2, Meta, Ref),

    case Reply of
        {ok, _Req} ->
            Mime = leo_mime:guess_mime(Key),
            CacheMeta = #cache_meta{
              md5          = Meta#metadata.checksum,
              mtime        = Meta#metadata.timestamp,
              content_type = Mime},
            catch leo_cache_api:put_end_tran(Ref, Key, CacheMeta, true);
        _ ->
            catch leo_cache_api:put_end_tran(Ref, Key, undef, false)
    end,
    {reply, Reply, State};


handle_call(result, _From, #state{md5_context = Context,
                                  errors = Errors} = State) ->
    Reply = case Errors of
                [] ->
                    Digest = crypto:hash_final(Context),
                    {ok, Digest};
                _  ->
                    {error, Errors}
            end,
    {reply, Reply, State};


handle_call({put, Index, Size, Bin}, _From, #state{key = Key,
                                                   md5_context = Context,
                                                   errors = Errors} = State) ->
    IndexBin = list_to_binary(integer_to_list(Index)),
    {Ret, NewState} =
        case leo_gateway_rpc_handler:put(<< Key/binary, ?DEF_SEPARATOR/binary,
                                            IndexBin/binary >>, Bin, Size, Index) of
            {ok, _ETag} ->
                NewContext = crypto:hash_update(Context, Bin),
                {ok, State#state{md5_context = NewContext}};
            {error, Cause} ->
                ?error("handle_call/3", "key:~s, cause:~p", [binary_to_list(Key), Cause]),
                {{error, Cause}, State#state{errors = [{Index, Cause}|Errors]}}
        end,
    {reply, Ret, NewState};


handle_call({rollback, TotalOfChunkedObjs}, _From, #state{key = Key} = State) ->
    ok = delete_chunked_objects(Key, TotalOfChunkedObjs),
    {reply, ok, State#state{errors = []}}.


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

