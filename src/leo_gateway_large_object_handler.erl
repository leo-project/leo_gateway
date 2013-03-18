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

-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("eunit/include/eunit.hrl").


%% Application callbacks
-export([start_link/1, stop/1]).
-export([put/2, put/4, get/3, rollback/2, result/1]).
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

-record(state, {key = <<>>       :: binary(),
                chunk_bin = <<>> :: binary(),
                chunk_num = 0    :: pos_integer(),
                md5_context      :: binary(),
                errors = []      :: list()
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
-spec(put(pid(), integer(), integer(), binary()) ->
             ok | {error, any()}).
put(Pid, ChunkSize, Size, Bin) ->
    gen_server:call(Pid, {put, ChunkSize, Size, Bin}, ?DEF_TIMEOUT).

put(Pid, done) ->
    gen_server:call(Pid, {put, done}, ?DEF_TIMEOUT).


%% @doc Retrieve a chunked object from the storage cluster
%%
-spec(get(pid(), integer(), pid()) ->
             ok | {error, any()}).
get(Pid, TotalOfChunkedObjs, Req) ->
    gen_server:call(Pid, {get, TotalOfChunkedObjs, Req}, ?DEF_TIMEOUT).


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
                md5_context = crypto:md5_init(),
                errors = []}}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};


handle_call({get, TotalOfChunkedObjs, Req}, _From, #state{key = Key} = State) ->
    Reply = handle_loop(Key, TotalOfChunkedObjs, Req),
    {reply, Reply, State};


handle_call(result, _From, #state{md5_context = Context,
                                  errors = Errors} = State) ->
    Reply = case Errors of
                [] ->
                    Digest = crypto:md5_final(Context),
                    {ok, Digest};
                _  ->
                    {error, Errors}
            end,
    {reply, Reply, State};


handle_call({put, ChunkSize, Size0, Bin0}, _From, State) ->
    {Ret, NewState} = chunk_and_put(ChunkSize, Size0, Bin0, State),
    {reply, Ret, NewState};


handle_call({put, done}, _From, #state{chunk_bin = <<>>,
                                       chunk_num = ChunkNum0} = State) ->
    {reply, {ok, ChunkNum0}, State};
handle_call({put, done}, _From, #state{key = Key,
                                       chunk_bin = Bin,
                                       chunk_num = ChunkNum0,
                                       md5_context = Context,
                                       errors = Errors} = State) ->
    Size = byte_size(Bin),
    ChunkNum1 = ChunkNum0 + 1,
    IndexBin = list_to_binary(integer_to_list(ChunkNum1)),

    case leo_gateway_rpc_handler:put(<< Key/binary, ?DEF_SEPARATOR/binary, IndexBin/binary >>,
                                     Bin, Size, ChunkNum1) of
        {ok, _ETag} ->
            NewContext = crypto:md5_update(Context, Bin),
            {reply, {ok, ChunkNum1}, State#state{chunk_num = ChunkNum1,
                                                 md5_context = NewContext,
                                                 errors = Errors}};
        {error, Cause} ->
            ?error("handle_call/3", "key:~s, cause:~p", [binary_to_list(Key), Cause]),
            {reply, {error, Cause}, State#state{errors = [{ChunkNum1, Cause}|Errors]}}
    end;


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
%% @doc Put chunked objects
%% @private
chunk_and_put(ChunkSize, _Size, Bin0, #state{chunk_bin = ChunkBin,
                                             chunk_num = ChunkNum0} = State) ->
    Bin1  = << ChunkBin/binary, Bin0/binary >>,
    Size1 = byte_size(Bin1),

    Ret = case (ChunkSize =< Size1) of
              true ->
                  <<Acc1:ChunkSize/binary, Acc2/binary>> = Bin1,
                  {true, {ChunkNum0 + 1, Acc1, Acc2}};
              false ->
                  {false, {ChunkNum0, <<>>, Bin1}}
          end,
    chunk_and_put(Ret, State).

chunk_and_put({true, {ChunkNum, Bin, Acc}}, #state{key = Key,
                                                   md5_context = Context,
                                                   errors = Errors} = State) ->
    Size = byte_size(Bin),
    IndexBin = list_to_binary(integer_to_list(ChunkNum)),

    case leo_gateway_rpc_handler:put(<< Key/binary, ?DEF_SEPARATOR/binary, IndexBin/binary >>,
                                     Bin, Size, ChunkNum) of
        {ok, _ETag} ->
            NewContext = crypto:md5_update(Context, Bin),
            {ok, State#state{chunk_bin = Acc,
                             chunk_num = ChunkNum,
                             md5_context = NewContext,
                             errors = Errors}};
        {error, Cause} ->
            ?error("handle_call/3", "key:~s, cause:~p", [binary_to_list(Key), Cause]),
            {{error, Cause}, State#state{errors = [{ChunkNum, Cause}|Errors]}}
    end;

chunk_and_put({false, {_, _, Acc}}, State) ->
    {ok, State#state{chunk_bin = Acc}}.


%% @doc Retrieve chunked objects
%% @private
-spec(handle_loop(binary(), integer(), any()) ->
             {ok, any()}).
handle_loop(Key, Total, Req) ->
    handle_loop(Key, Total, 0, Req).

-spec(handle_loop(binary(), integer(), integer(), any()) ->
             {ok, any()}).
handle_loop(_Key, Total, Total, Req) ->
    {ok, Req};

handle_loop(Key0, Total, Index, Req) ->
    IndexBin = list_to_binary(integer_to_list(Index + 1)),
    Key1 = << Key0/binary, ?DEF_SEPARATOR/binary, IndexBin/binary >>,

    case leo_gateway_rpc_handler:get(Key1) of
        %% only children
        {ok, #metadata{cnumber = 0}, Bin} ->
            case cowboy_req:chunk(Bin, Req) of
                ok ->
                    handle_loop(Key0, Total, Index + 1, Req);
                {error, Cause} ->
                    ?error("handle_loop/4", "key:~s, index:~p, cause:~p",
                           [binary_to_list(Key0), Index, Cause]),
                    {error, Cause}
            end;

        %% both children and grand-children
        {ok, #metadata{cnumber = TotalChunkedObjs}, _Bin} ->
            %% grand-children
            case handle_loop(Key1, TotalChunkedObjs, Req) of
                {ok, Req} ->
                    %% children
                    handle_loop(Key0, Total, Index + 1, Req);
                {error, Cause} ->
                    ?error("handle_loop/4", "key:~s, index:~p, cause:~p",
                           [binary_to_list(Key0), Index, Cause]),
                    {error, Cause}
            end;
        {error, Cause} ->
            ?error("handle_loop/4", "key:~s, index:~p, cause:~p",
                   [binary_to_list(Key0), Index, Cause]),
            {error, Cause}
    end.


%% @doc Remove chunked objects
%% @private
-spec(delete_chunked_objects(binary(), integer()) ->
             ok).
delete_chunked_objects(_, 0) ->
    ok;
delete_chunked_objects(Key0, Index) ->
    IndexBin = list_to_binary(integer_to_list(Index)),
    Key1 = << Key0/binary, ?DEF_SEPARATOR/binary, IndexBin/binary >>,

    case leo_gateway_rpc_handler:delete(Key1) of
        ok ->
            void;
        {error, Cause} ->
            ?error("delete_chunked_objects/2", "key:~s, index:~p, cause:~p",
                   [binary_to_list(Key0), Index, Cause])
    end,
    delete_chunked_objects(Key0, Index - 1).
