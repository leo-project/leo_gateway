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
%% Leo Gateway - Supervisor
%%
%% @doc
%% @end
%%======================================================================
-module(leo_gateway_sup).

-author('Yosuke Hara').

-behaviour(supervisor).

-include("leo_gateway.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").


%% External exports
-export([start_link/0, upgrade/0]).

%% supervisor callbacks
-export([init/1]).

%% @spec start_link() -> ServerRet
%% @doc API for starting the supervisor.
start_link() ->
    setup_mnesia(),

    ListenPort      = ?env_listening_port(leo_gateway),
    NumOfAcceptors  = ?env_num_of_acceptors(leo_gateway),
    io:format("*             port: ~p~n", [ListenPort]),
    io:format("* num of acceptors: ~p~n", [NumOfAcceptors]),

    HookModules =
        case ?env_cache_plugin() of
            none      -> [];
            undefined -> [];
            ModCache ->
                CacheExpire          = ?env_cache_expire(),
                CacheMaxContentLen   = ?env_cache_max_content_len(),
                CachableContentTypes = ?env_cachable_content_type(),
                CachablePathPatterns = ?env_cachable_path_pattern(),
                io:format("*        mod cache: ~p~n", [ModCache]),
                io:format("*     cache_expire: ~p~n", [CacheExpire]),
                io:format("*  max_content_len: ~p~n", [CacheMaxContentLen]),
                io:format("*    content_types: ~p~n", [CachableContentTypes]),
                io:format("*    path_patterns: ~p~n", [CachablePathPatterns]),
                [{ModCache, [{expire,                CacheExpire},
                             {max_content_len,       CacheMaxContentLen},
                             {cachable_content_type, CachableContentTypes},
                             {cachable_path_pattern, CachablePathPatterns}
                            ]}]
        end,
    supervisor:start_link({local, ?MODULE}, ?MODULE,
                          [ListenPort, NumOfAcceptors, HookModules]).

%% @spec upgrade() -> ok
%% @doc Add processes if necessary.
upgrade() ->
    {ok, {_, Specs}} = init([]),

    Old = sets:from_list(
            [Name || {Name, _, _, _} <- supervisor:which_children(?MODULE)]),
    New = sets:from_list([Name || {Name, _, _, _, _, _} <- Specs]),
    Kill = sets:subtract(Old, New),

    sets:fold(fun (Id, ok) ->
                      supervisor:terminate_child(?MODULE, Id),
                      supervisor:delete_child(?MODULE, Id),
                      ok
              end, ok, Kill),

    [supervisor:start_child(?MODULE, Spec) || Spec <- Specs],
    ok.


%% @spec init([]) -> SupervisorTree
%% @doc supervisor callback.
init([ListenPort, AccessorPoolSize, HookModules]) ->
    Ip = case os:getenv("MOCHIWEB_IP") of
             false -> "0.0.0.0";
             Any   -> Any
         end,
    
    WebConfig0 = [{ip, Ip},
                 {port, ListenPort},
                 {acceptor_pool_size, AccessorPoolSize},
                 {docroot, "."}],
    WebConfig1 =
        case HookModules of
            [] -> WebConfig0;
            _  -> lists:reverse([{hook_modules, HookModules}|WebConfig0])
        end,

    Web = {leo_gateway_web_mochi,
           {leo_gateway_web_mochi, start, [WebConfig1]},
           permanent, ?SHUTDOWN_WAITING_TIME, worker, dynamic},
    {ok, {{one_for_one, 10, 10}, [Web]}}.


%%--------------------------------------------------------------------
%% Internal Functions.
%%--------------------------------------------------------------------
setup_mnesia() ->
    application:start(mnesia),

    MnesiaMode = disc_copies,
    mnesia:change_table_copy_type(schema, node(), MnesiaMode),
    leo_redundant_manager_mnesia:create_members(MnesiaMode),
    mnesia:wait_for_tables([members], 30000),
    ok.

