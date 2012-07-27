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
-export([start/2, stop/1]).

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
                        proplists:get_value(path, Options,  DefLogDir);
                    _ ->
                        DefLogDir
                end,
    ok = leo_logger_client_message:new(LogDir, ?env_log_level(App), log_file_appender()),
    Res = leo_gateway_sup:start_link(),
    after_process(Res).


%% @spec stop(_State) -> ServerRet
%% @doc application stop callback for leo_gateway.
stop(_State) ->
    ok.


%%--------------------------------------------------------------------
%% Internal Functions.
%%--------------------------------------------------------------------
%% @doc After process of start_link
%% @private
-spec(after_process({ok, pid()} | {error, any()}) ->
             {ok, pid()} | {error, any()}).
after_process({ok, Pid} = Res) ->
    App = leo_gateway,
    ManagerNodes    = ?env_manager_nodes(App),
    NewManagerNodes = lists:map(fun(X) -> list_to_atom(X) end, ManagerNodes),

    case ?get_several_info_from_manager(NewManagerNodes) of
        {{ok, SystemConf}, {ok, Members}} ->
            %% Launch SNMPA
            ok = leo_statistics_api:start(leo_gateway_sup, App,
                                          [{snmp, [leo_statistics_metrics_vm,
                                                   leo_statistics_metrics_req,
                                                   leo_s3_http_cache_statistics]},
                                           {stat, [leo_statistics_metrics_vm]}]),

            %% Launch Redundant-manager
            ok = leo_redundant_manager_api:start(gateway, NewManagerNodes, ?env_queue_dir(App)),
            {ok,_,_} = leo_redundant_manager_api:create(Members, [{n, SystemConf#system_conf.n},
                                                                  {r, SystemConf#system_conf.r},
                                                                  {w, SystemConf#system_conf.w},
                                                                  {d, SystemConf#system_conf.d},
                                                                  {bit_of_ring, SystemConf#system_conf.bit_of_ring}]),

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

            %% Launch s3 bucket api
            ok = leo_s3_bucket_api:start(slave, [{'provider', NewManagerNodes}]),

            %% Launch s3 bucket api
            ok = leo_s3_auth_api:start(slave, [{'provider', NewManagerNodes}]),

            %% Launch a listener - [s3_http]
            case ?env_listener() of
                ?S3_HTTP ->
                    ok = leo_s3_http_api:start(Pid, App)
            end,
            Res;
        Error ->
            io:format("~p:~s,~w - cause:~p~n", [?MODULE, "after_process/1", ?LINE, Error]),
            Error
    end;
after_process(Error) ->
    io:format("~p:~s,~w - cause:~p~n", [?MODULE, "after_process/1", ?LINE, Error]),
    Error.


%% @doc Retrieve system-configuration from manager-node(s)
%% @private
-spec(get_system_config_from_manager(list()) ->
             {ok, #system_conf{}} | {error, any()}).
get_system_config_from_manager([]) ->
    {error, 'could_not_get_system_config'};
get_system_config_from_manager([Manager|T]) ->
    case leo_utils:node_existence(Manager) of
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
log_file_appender([{Type, _}|T], Acc) when Type == zmq ->
    %% @TODO
    log_file_appender(T, [{?LOG_ID_ZMQ, ?LOG_APPENDER_ZMQ}|Acc]).

