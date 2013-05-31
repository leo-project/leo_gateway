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
-include("leo_http.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_s3_libs/include/leo_s3_libs.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([get_node_status/0,
         update_manager_nodes/1,
         register_in_monitor/1, register_in_monitor/2,
         purge/1, set_endpoint/1
        ]).

%% @doc Purge an object into the cache
-spec(purge(string()) -> ok).
purge(Path) ->
    BinPath = list_to_binary(Path),
    _ = leo_cache_api:delete(BinPath),
    ok.

%% @doc Get node status (gateway).
%%
-spec(get_node_status() -> {ok, #cluster_node_status{}}).
get_node_status() ->
    {ok, Version} = application:get_env(leo_gateway, system_version),
    {RingHashCur, RingHashPrev} =
        case leo_redundant_manager_api:checksum(ring) of
            {ok, {Chksum0, Chksum1}} -> {Chksum0, Chksum1};
            _ -> {[], []}
        end,

    SNMPAgent = case application:get_env(leo_storage, snmp_agent) of
                   {ok, EnvSNMPAgent} -> EnvSNMPAgent;
                   _ -> []
               end,
    Directories = [{log,        ?env_log_dir(leo_gateway)},
                   {mnesia,     []},
                   {snmp_agent, SNMPAgent}
                  ],
    RingHashes  = [{ring_cur,  RingHashCur},
                   {ring_prev, RingHashPrev }
                  ],
    Statistics  = [{vm_version,       erlang:system_info(version)},
                   {total_mem_usage,  erlang:memory(total)},
                   {system_mem_usage, erlang:memory(system)},
                   {proc_mem_usage,   erlang:memory(processes)},
                   {ets_mem_usage,    erlang:memory(ets)},
                   {num_of_procs,     erlang:system_info(process_count)},
                   {process_limit,    erlang:system_info(process_limit)},
                   {kernel_poll,      erlang:system_info(kernel_poll)},
                   {thread_pool_size, erlang:system_info(thread_pool_size)}
                  ],

    HttpProps  = ?env_http_properties(),
    CacheProps = ?env_cache_properties(),
    LBProps    = ?env_large_object_properties(),

    HttpConf  = [
                 {handler,                  leo_misc:get_value('handler',                  HttpProps,  ?DEF_HTTTP_HANDLER)},
                 {port,                     leo_misc:get_value('port',                     HttpProps,  ?DEF_HTTP_PORT)},
                 {ssl_port,                 leo_misc:get_value('ssl_port',                 HttpProps,  ?DEF_HTTP_SSL_PORT)},
                 {acceptors,                leo_misc:get_value('num_of_acceptors',         HttpProps,  ?DEF_HTTP_NUM_OF_ACCEPTORS)},
                 {http_cache,               leo_misc:get_value('http_cache',               CacheProps, ?DEF_HTTP_CACHE)},
                 {cache_workers,            leo_misc:get_value('cache_workers',            CacheProps, ?DEF_CACHE_WORKERS)},
                 {cache_ram_capacity,       leo_misc:get_value('cache_ram_capacity',       CacheProps, ?DEF_CACHE_RAM_CAPACITY)},
                 {cache_disc_capacity,      leo_misc:get_value('cache_disc_capacity',      CacheProps, ?DEF_CACHE_DISC_CAPACITY)},
                 {cache_disc_threshold_len, leo_misc:get_value('cache_disc_threshold_len', CacheProps, ?DEF_CACHE_DISC_THRESHOLD_LEN)},
                 {cache_disc_dir_data,      leo_misc:get_value('cache_disc_dir_data',      CacheProps, ?DEF_CACHE_DISC_DIR_DATA)},
                 {cache_disc_dir_journal,   leo_misc:get_value('cache_disc_dir_journal',   CacheProps, ?DEF_CACHE_DISC_DIR_JOURNAL)},
                 {cache_expire,             leo_misc:get_value('cache_expire',             CacheProps, ?DEF_CACHE_EXPIRE)},
                 {cache_max_content_len,    leo_misc:get_value('cache_max_content_len',    CacheProps, ?DEF_CACHE_MAX_CONTENT_LEN)},
                 {cachable_content_type,    leo_misc:get_value('cachable_content_type',    CacheProps, [])},
                 {cachable_path_pattern,    leo_misc:get_value('cachable_path_pattern',    CacheProps, [])},
                 {max_chunked_objs,         leo_misc:get_value('max_chunked_objs',         LBProps,    ?DEF_LOBJ_MAX_CHUNKED_OBJS)},
                 {max_len_for_obj,          leo_misc:get_value('max_len_for_obj',          LBProps,    ?DEF_LOBJ_MAX_LEN_FOR_OBJ)},
                 {chunked_obj_len,          leo_misc:get_value('chunked_obj_len',          LBProps,    ?DEF_LOBJ_CHUNK_OBJ_LEN)},
                 {threshold_obj_len,        leo_misc:get_value('threshold_obj_len',        LBProps,    ?DEF_LOBJ_THRESHOLD_OBJ_LEN)}
                ],
    {ok, [{type,          gateway},
          {version,       Version},
          {dirs,          Directories},
          {ring_checksum, RingHashes},
          {statistics,    Statistics},
          {http_conf,     HttpConf}
         ]}.


%% @doc Register into the manager-monitor.
%%
-spec(register_in_monitor(first|again) -> ok).
register_in_monitor(RequestedTimes) ->
    case whereis(leo_gateway_sup) of
        undefined ->
            {error, not_found};
        Pid ->
            register_in_monitor(Pid, RequestedTimes)
    end.

register_in_monitor(Pid, RequestedTimes) ->
    ManagerNodes = ?env_manager_nodes(leo_gateway),
    register_in_monitor(ManagerNodes, Pid, RequestedTimes).

register_in_monitor([],_,_) ->
    {error, ?ERROR_COULD_NOT_CONNECT};
register_in_monitor([Node1|Rest], Pid, RequestedTimes) ->
    Node2 = list_to_atom(Node1),
    Ret = case leo_misc:node_existence(Node2) of
              true ->
                  case rpc:call(Node2, leo_manager_api, register,
                                [RequestedTimes, Pid, erlang:node(), gateway], ?DEF_TIMEOUT) of
                      ok ->
                          true;
                      {error, Cause} ->
                          ?warn("register_in_monitor/3",
                                 "manager:~w, cause:~p", [Node2, Cause]),
                          false;
                      {badrpc, Cause} ->
                          ?warn("register_in_monitor/3",
                                 "manager:~w, cause:~p", [Node2, Cause]),
                          false
                  end;
              false ->
                  false
          end,
    case Ret of
        true ->
            ok;
        false ->
            register_in_monitor(Rest, Pid, RequestedTimes)
    end.


%% @doc Set s3-endpoint from manager
%%
set_endpoint(Endpoint) ->
    leo_s3_endpoint:set_endpoint(Endpoint).

%% @doc update manager nodes
%%
-spec(update_manager_nodes(list()) ->
             ok).
update_manager_nodes(Managers) ->
    ?update_env_manager_nodes(leo_gateway, Managers),
    ok = leo_membership:update_manager_nodes(Managers),
    leo_s3_libs:update_providers(Managers).

