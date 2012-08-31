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
%% -------------------------------------------------------------------
%% Leo Gateway - API
%% @doc
%% @end
%%====================================================================
-module(leo_gateway_api).
-author('Yosuke Hara').

-include("leo_gateway.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([get_cluster_node_status/0,
         register_in_monitor/1,
         purge/1
        ]).

%% @doc purge API
-spec(purge(string()) -> ok).
purge(Path) ->
    HookMods = mochiweb_socket_server:get(leo_s3_http_mochi, hook_modules),
    ?info("purge/1", "path: ~p hooks: ~p ~n",[Path, HookMods]),
    _ = mochiweb:on_purge_hook_modules(Path, HookMods),
    ok.

%% @doc get cluster node status (gateway).
%%
-spec(get_cluster_node_status() -> {ok, #cluster_node_status{}}).
get_cluster_node_status() ->
    {ok, Version} = application:get_key(leo_gateway, vsn),
    {RingHashCur, RingHashPrev} =
        case leo_redundant_manager_api:checksum(ring) of
            {ok, {Chksum0, Chksum1}} -> {Chksum0, Chksum1};
            _ -> {[], []}
        end,

    Directories = [{log,    ?env_log_dir(leo_gateway)},
                   {mnesia, mnesia:system_info(directory)}
                  ],
    RingHashes  = [{ring_cur,  RingHashCur},
                   {ring_prev, RingHashPrev }
                  ],
    Statistics  = [{total_mem_usage,  erlang:memory(total)},
                   {system_mem_usage, erlang:memory(system)},
                   {proc_mem_usage,   erlang:memory(system)},
                   {ets_mem_usage,    erlang:memory(ets)},
                   {num_of_procs,     erlang:system_info(process_count)}
                  ],

    {ok, #cluster_node_status{type    = gateway,
                              version = Version,
                              dirs    = Directories,
                              ring_checksum = RingHashes,
                              statistics    = Statistics}}.


%% @doc register in a manager monitor.
%%
-spec(register_in_monitor(first|again) -> ok).
register_in_monitor(RequestedTimes) ->
    ManagerNodes = ?env_manager_nodes(leo_gateway),

    case whereis(leo_gateway_sup) of
        undefined ->
            {error, not_found};
        Pid ->
            Fun = fun(Node, false) ->
                          NodeAtom = list_to_atom(Node),
                          case leo_misc:node_existence(NodeAtom) of
                              true ->
                                  case rpc:call(NodeAtom, leo_manager_api, register,
                                                [RequestedTimes, Pid, erlang:node(), gateway], ?DEF_TIMEOUT) of
                                      ok ->
                                          true;
                                      {error, Cause} ->
                                          ?error("register_in_monitor/1", "manager:~w, cause:~p", [NodeAtom, Cause]),
                                          false;
                                      {badrpc, Cause} ->
                                          ?error("register_in_monitor/1", "manager:~w, cause:~p", [NodeAtom, Cause]),
                                          false
                                  end;
                              false ->
                                  false
                          end;
                     (_Node, true) ->
                          true
                  end,
            case lists:foldl(Fun, false,  ManagerNodes) of
                true ->
                    ok;
                false ->
                    {error, ?ERROR_COULD_NOT_CONNECT}
            end
    end.

