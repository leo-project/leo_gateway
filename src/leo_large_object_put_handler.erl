%%======================================================================
%%
%% Leo Gateway Large Object PUT Handler
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
-module(leo_large_object_put_handler).

-behaviour(gen_server).

-include("leo_gateway.hrl").
-include("leo_http.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_dcerl/include/leo_dcerl.hrl").
-include_lib("eunit/include/eunit.hrl").


%% Application callbacks
-export([start_link/2, stop/1]).
-export([put/2, rollback/1, result/1]).
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
                stacked_bin = <<>>    :: binary(),
                num_of_chunks = 0     :: non_neg_integer(),
                total_len = 0         :: non_neg_integer(),
                md5_context = <<>>    :: any(),
                errors = []           :: list()
               }).

-spec(start_link(binary(), non_neg_integer()) ->
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

init([Key, Length]) ->
    State = #state{key = Key,
                   max_obj_len = Length,
                   num_of_chunks = 1,
                   stacked_bin = <<>>,
                   md5_context = crypto:hash_init(md5),
                   errors = []},
    {ok, State}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

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

            case leo_gateway_rpc_handler:put(
                   << Key/binary, ?DEF_SEPARATOR/binary, NumOfChunksBin/binary >>,
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


handle_call(rollback, _From, #state{key = Key} = State) ->
    ok = leo_large_object_commons:delete_chunked_objects(Key),
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
                               errors = []}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

