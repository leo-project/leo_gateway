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
%% Leo Gateway - Application
%% @doc
%% @end
%%======================================================================
-module(leo_gateway_app).

-author('Yosuke Hara').

-include("leo_gateway.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-behaviour(application).
-export([start/2, stop/1, inspect_cluster_status/2]).

-define(CHECK_INTERVAL, 3000).

-ifdef(TEST).
-define(get_several_info_from_manager(_Args),
        fun() ->
                _ = get_system_config_from_manager([]),
                _ = get_members_from_manager([]),

                [{ok, [#system_conf{n = 1,
                                    w = 1,
                                    r = 1,
                                    d = 1}]},
                 {ok, [#member{node  = 'node_0',
                               state = 'running'}]}]
        end).
-else.
-define(get_several_info_from_manager(X),  {get_system_config_from_manager(X),
                                            get_members_from_manager(X)}).
-endif.

%% @spec start(_Type, _StartArgs) -> ServerRet
%% @doc application start callback for leo_gateway.
start(_Type, _StartArgs) ->
    leo_gateway_deps:ensure(),
    App = leo_gateway,

    %% Launch Logger(s)
    DefLogDir = "./log/",
    LogDir    = case application:get_env(App, log_appender) of
                    {ok, [{file, Options}|_]} ->
                        leo_misc:get_value(path, Options,  DefLogDir);
                    _ ->
                        DefLogDir
                end,
    ok = leo_logger_client_message:new(LogDir, ?env_log_level(App), log_file_appender()),

    %% Launch Supervisor
    Res = leo_gateway_sup:start_link(),
    after_process_0(Res).


%% @spec stop(_State) -> ServerRet
%% @doc application stop callback for leo_gateway.
stop(_State) ->
    ok.


%% @doc Inspect the cluster-status
%%
-spec(inspect_cluster_status(any(), list()) ->
             pid()).
inspect_cluster_status(Res, ManagerNodes) ->
    case ?get_several_info_from_manager(ManagerNodes) of
        {{ok, SystemConf}, {ok, Members}} ->
            case get_cluster_state(Members) of
                ?STATE_STOP ->
                    timer:apply_after(?CHECK_INTERVAL, ?MODULE, inspect_cluster_status,
                                      [ok, ManagerNodes]);
                ?STATE_RUNNING ->
                    ok = after_process_1(SystemConf, Members)
            end;
        {{ok,_SystemConf}, {error,_Cause}} ->
            timer:apply_after(?CHECK_INTERVAL, ?MODULE, inspect_cluster_status,
                              [ok, ManagerNodes]);
        Error ->
            io:format("~p:~s,~w - cause:~p~n", [?MODULE, "after_process/1", ?LINE, Error]),
            Error
    end,
    Res.


%%--------------------------------------------------------------------
%% Internal Functions.
%%--------------------------------------------------------------------
-define(STAT_INTERVAL_10,   10000).
-define(STAT_INTERVAL_60,   60000).
-define(STAT_INTERVAL_300, 300000).

%% @doc After process of start_link
%% @private
-spec(after_process_0({ok, pid()} | {error, any()}) ->
             {ok, pid()} | {error, any()}).
after_process_0({ok, _Pid} = Res) ->
    ManagerNodes0  = ?env_manager_nodes(leo_gateway),
    ManagerNodes1 = lists:map(fun(X) -> list_to_atom(X) end, ManagerNodes0),
    inspect_cluster_status(Res, ManagerNodes1);

after_process_0(Error) ->
    io:format("~p:~s,~w - cause:~p~n", [?MODULE, "after_process/1", ?LINE, Error]),
    Error.


%% @doc After process of start_link
%% @private
-spec(after_process_1(#system_conf{}, list(#member{})) ->
             ok).
after_process_1(SystemConf, Members) ->
    %% Launch SNMPA
    App = leo_gateway,
    ok = leo_statistics_api:start_link(leo_manager),
    ok = leo_statistics_metrics_vm:start_link(?STAT_INTERVAL_10),
    ok = leo_statistics_metrics_vm:start_link(?STAT_INTERVAL_60),
    ok = leo_statistics_metrics_vm:start_link(?STAT_INTERVAL_300),
    ok = leo_statistics_metrics_req:start_link(?STAT_INTERVAL_60),
    ok = leo_statistics_metrics_req:start_link(?STAT_INTERVAL_300),
    ok = leo_s3_http_cache_statistics:start_link(?STAT_INTERVAL_60),
    ok = leo_s3_http_cache_statistics:start_link(?STAT_INTERVAL_300),

    %% Launch Redundant-manager
    ManagerNodes    = ?env_manager_nodes(leo_gateway),
    NewManagerNodes = lists:map(fun(X) -> list_to_atom(X) end, ManagerNodes),

    ok = leo_redundant_manager_api:start(gateway, NewManagerNodes, ?env_queue_dir(App)),
    {ok,_,_} = leo_redundant_manager_api:create(
                 Members, [{n, SystemConf#system_conf.n},
                           {r, SystemConf#system_conf.r},
                           {w, SystemConf#system_conf.w},
                           {d, SystemConf#system_conf.d},
                           {bit_of_ring, SystemConf#system_conf.bit_of_ring}]),

    %% Launch S3Libs:Auth/Bucket/EndPoint
    ok = leo_s3_libs:start(slave, [{'provider', NewManagerNodes}]),

    %% Launch a listener - [s3_http]
    case ?env_listener() of
        ?S3_HTTP ->
            ok = leo_s3_http_api:start(leo_gateway_sup, App)
    end,

    %% Register in THIS-Process
    ok = leo_gateway_api:register_in_monitor(first),
    lists:foldl(fun(N, false) ->
                        {ok, Checksums} = leo_redundant_manager_api:checksum(ring),
                        case rpc:call(N, leo_manager_api, notify,
                                      [launched, gateway, node(), Checksums], ?DEF_TIMEOUT) of
                            ok -> true;
                            _  -> false
                        end;
                   (_, true) ->
                        void
                end, false, NewManagerNodes),
    ok.


%% @doc Retrieve system-configuration from manager-node(s)
%% @private
-spec(get_system_config_from_manager(list()) ->
             {ok, #system_conf{}} | {error, any()}).
get_system_config_from_manager([]) ->
    {error, 'could_not_get_system_config'};
get_system_config_from_manager([Manager|T]) ->
    case leo_misc:node_existence(Manager) of
        true ->
            case rpc:call(Manager, leo_manager_api, get_system_config, [], ?DEF_TIMEOUT) of
                {ok, SystemConf} ->
                    {ok, SystemConf};
                {badrpc, Why} ->
                    {error, Why};
                {error, Cause} ->
                    ?error("get_system_config_from_manager/1", "cause:~p", [Cause]),
                    get_system_config_from_manager(T)
            end;
        false ->
            get_system_config_from_manager(T)
    end.


%% @doc Retrieve members-list from manager-node(s)
%% @private
-spec(get_members_from_manager(list()) ->
             {ok, list()} | {error, any()}).
get_members_from_manager([]) ->
    {error, 'could_not_get_members'};
get_members_from_manager([Manager|T]) ->
    case rpc:call(Manager, leo_manager_api, get_members, [], ?DEF_TIMEOUT) of
        {ok, Members} ->
            {ok, Members};
        {badrpc, Why} ->
            {error, Why};
        {error, Cause} ->
            ?error("get_members_from_manager/1", "cause:~p", [Cause]),
            get_members_from_manager(T)
    end.


%% @doc
%% @private
-spec(get_cluster_state(list(#member{})) ->
             node_state()).
get_cluster_state([]) ->
    ?STATE_STOP;
get_cluster_state([#member{state = ?STATE_RUNNING}|_]) ->
    ?STATE_RUNNING;
get_cluster_state([_|T]) ->
    get_cluster_state(T).


%% @doc Retrieve log-appneder(s)
%% @private
-spec(log_file_appender() ->
             list()).
log_file_appender() ->
    case application:get_env(leo_gateway, log_appender) of
        undefined   -> log_file_appender([], []);
        {ok, Value} -> log_file_appender(Value, [])
    end.

log_file_appender([], []) ->
    [{?LOG_ID_FILE_INFO,  ?LOG_APPENDER_FILE},
     {?LOG_ID_FILE_ERROR, ?LOG_APPENDER_FILE}];
log_file_appender([], Acc) ->
    lists:reverse(Acc);
log_file_appender([{Type, _}|T], Acc) when Type == file ->
    log_file_appender(T, [{?LOG_ID_FILE_ERROR, ?LOG_APPENDER_FILE}|[{?LOG_ID_FILE_INFO, ?LOG_APPENDER_FILE}|Acc]]);
%% @TODO
log_file_appender([{Type, _}|T], Acc) when Type == zmq ->
    log_file_appender(T, [{?LOG_ID_ZMQ, ?LOG_APPENDER_ZMQ}|Acc]).

