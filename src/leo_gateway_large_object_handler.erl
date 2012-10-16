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
-include_lib("eunit/include/eunit.hrl").


%% Application callbacks
-export([start_link/0, stop/1]).
-export([put/5, get/4, rollback/3, result/1]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-undef(DEF_SEPARATOR).
-define(DEF_SEPARATOR, <<"\n">>).

-record(state, {num_of_chunks  :: integer(),
                md5_context    :: binary(),
                errors = []    :: list()
               }).


%% ===================================================================
%% API
%% ===================================================================
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
-spec(start_link() ->
             ok | {error, any()}).
start_link() ->
    gen_server:start_link(?MODULE, [], []).

%% @doc Stop this server
%%
-spec(stop(pid()) -> ok).
stop(Pid) ->
    gen_server:call(Pid, stop).


%% @doc Insert a chunked object into the storage cluster
%%
-spec(put(pid(), binary(), integer(), integer(), binary()) ->
             ok | {error, any()}).
put(Pid, Key, Index, Size, Bin) ->
    gen_server:cast(Pid, {put, Key, Index, Size, Bin}).


%% @doc Retrieve a chunked object from the storage cluster
%%
-spec(get(pid(), binary(), integer(), pid()) ->
             ok | {error, any()}).
get(Pid, Key, TotalOfChunkedObjs, Req) ->
    gen_server:call(Pid, {get, Key, TotalOfChunkedObjs, Req}).


%% @doc Make a rollback before all operations
%%
-spec(rollback(pid(), binary(), integer()) ->
             ok | {error, any()}).
rollback(Pid, Key, TotalOfChunkedObjs) ->
    gen_server:cast(Pid, {rollback, Key, TotalOfChunkedObjs}).


%% @doc Retrieve a result
%%
-spec(result(pid()) ->
             ok | {error, any()}).
result(Pid) ->
    gen_server:call(Pid, result).


%%====================================================================
%% GEN_SERVER CALLBACKS
%%====================================================================
%% Function: init(Args) -> {ok, State}          |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
init([]) ->
    Context = erlang:md5_init(),
    {ok, #state{md5_context = Context,
                errors = []}}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};


handle_call({get, Key, TotalOfChunkedObjs, Req}, _From, State) ->
    Reply = handle_loop(Key, TotalOfChunkedObjs, Req),
    {reply, Reply, State};


handle_call(result, _From, #state{md5_context = Context,
                                  errors = Errors} = State) ->
    Reply = case Errors of
                [] ->
                    Digest = erlang:md5_final(Context),
                    {ok, Digest};
                _  ->
                    {error, Errors}
            end,
    {reply, Reply, State}.


%% Function: handle_cast(Msg, State) -> {noreply, State}          |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast message
handle_cast({put, Key, Index, Size, Bin}, #state{md5_context = Context,
                                                 errors = Acc} = State) ->
    IndexBin = list_to_binary(integer_to_list(Index)),

    case leo_gateway_rpc_handler:put(<<Key/binary, ?DEF_SEPARATOR/binary, IndexBin/binary>>, Bin, Size, Index) of
        {ok, _ETag} ->
            NewContext = erlang:md5_update(Context, Bin),
            {noreply, State#state{md5_context = NewContext,
                                  errors = Acc}};
        {error, Cause} ->
            ?error("handle_cast/2", "key:~s, cause:~p", [binary_to_list(Key), Cause]),
            {noreply, State#state{errors = [{Index, Cause}|Acc]}}
    end;

handle_cast({rollback, _Key, _TotalOfChunkedObjs}, State) ->
    %% case leo_gateway_rpc_handler:delete(Key, TotalOfChunkedObjs) of
    %%     ok ->
    %%         void;
    %%     {error, Cause} ->
    %%         ?error("handle_cast/2", "key:~s, cause:~p", [binary_to_list(Key), Cause])
    %% end,
    {noreply, State#state{errors = []}};

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
%% @doc Handle retrieve chunked objects
%% @private
-spec(handle_loop(binary(), integer(), any()) ->
             {ok, any()}).
handle_loop(Key, Total, Req) ->
    handle_loop(Key, Total, 0, Req).

-spec(handle_loop(binary(), integer(), integer(), any()) ->
             {ok, any()}).
handle_loop(_Key, Total, Total, Req) ->
    {ok, Req};
handle_loop( Key, Total, Index, Req) ->
    IndexBin = list_to_binary(integer_to_list(Index+1)),

    case leo_gateway_rpc_handler:get(<<Key/binary, ?DEF_SEPARATOR/binary, IndexBin/binary>>) of
        {ok, _Metadata, Bin} ->
            ok = cowboy_http_req:chunk(Bin, Req),
            handle_loop(Key, Total, Index + 1, Req);
        {error, Cause} ->
            {error, Cause}
    end.

