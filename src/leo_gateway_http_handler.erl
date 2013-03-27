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
%% Leo Gateway - HTTP Commons Handler
%% @doc
%% @end
%%======================================================================
-module(leo_gateway_http_handler).

-author('Yosuke Hara').

-include("leo_gateway.hrl").
-include("leo_http.hrl").

-export([start/1, start/2, stop/0]).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Launch http handler
%%
-spec(start(atom(), #http_options{}) ->
             ok).
start(Sup, Options) ->
    %% for ECache
    NumOfECacheWorkers    = Options#http_options.cache_workers,
    CacheRAMCapacity      = Options#http_options.cache_ram_capacity,
    CacheDiscCapacity     = Options#http_options.cache_disc_capacity,
    CacheDiscThresholdLen = Options#http_options.cache_disc_threshold_len,
    CacheDiscDirData      = Options#http_options.cache_disc_dir_data,
    CacheDiscDirJournal   = Options#http_options.cache_disc_dir_journal,
    ChildSpec0 = {ecache_sup,
                  {ecache_sup, start_link, [NumOfECacheWorkers, CacheRAMCapacity, CacheDiscCapacity,
                                            CacheDiscThresholdLen, CacheDiscDirData, CacheDiscDirJournal]},
                  permanent, ?SHUTDOWN_WAITING_TIME, supervisor, [ecache_sup]},
    {ok, _} = supervisor:start_child(Sup, ChildSpec0),

    %% for Cowboy
    ChildSpec1 = {cowboy_sup,
                  {cowboy_sup, start_link, []},
                  permanent, ?SHUTDOWN_WAITING_TIME, supervisor, [cowboy_sup]},
    {ok, _} = supervisor:start_child(Sup, ChildSpec1),

    %% launch http-handler(s)
    start(Options).


-spec(start(#http_options{}) ->
             ok).
start(#http_options{handler                = Handler,
                    port                   = Port,
                    ssl_port               = SSLPort,
                    ssl_certfile           = SSLCertFile,
                    ssl_keyfile            = SSLKeyFile,
                    num_of_acceptors       = NumOfAcceptors,
                    cache_method           = CacheMethod,
                    cache_expire           = CacheExpire,
                    cache_max_content_len  = CacheMaxContentLen,
                    cachable_content_type  = CachableContentTypes,
                    cachable_path_pattern  = CachablePathPatterns} = Props) ->
    InternalCache = (CacheMethod == 'inner'),
    Dispatch      = cowboy_router:compile(
                      [{'_', [{'_', Handler,
                               [?env_layer_of_dirs(), InternalCache, Props]}]}]),

    Config = case InternalCache of
                 %% Using inner-cache
                 true ->
                     [{env, [{dispatch, Dispatch}]}];
                 %% Using http-cache
                 false ->
                     CacheCondition = #cache_condition{expire          = CacheExpire,
                                                       max_content_len = CacheMaxContentLen,
                                                       content_types   = CachableContentTypes,
                                                       path_patterns   = CachablePathPatterns},
                     [{env,        [{dispatch, Dispatch}]},
                      {onrequest,  Handler:onrequest(CacheCondition)},
                      {onresponse, Handler:onresponse(CacheCondition)}]
             end,

    {ok, _Pid1}= cowboy:start_http(Handler, NumOfAcceptors,
                                   [{port, Port}], Config),
    {ok, _Pid2}= cowboy:start_https(list_to_atom(lists:append([atom_to_list(Handler), "_ssl"])),
                                    NumOfAcceptors,
                                    [{port,     SSLPort},
                                     {certfile, SSLCertFile},
                                     {keyfile,  SSLKeyFile}],
                                    Config),
    ok.


%% @doc Stop proc(s)
%%
-spec(stop() ->
             ok).
stop() ->
    {ok, HttpOption} = leo_gateway_app:get_options(),
    Handler = HttpOption#http_options.handler,
    cowboy:stop_listener(Handler),
    cowboy:stop_listener(list_to_atom(lists:append([atom_to_list(Handler), "_ssl"]))),
    ok.

