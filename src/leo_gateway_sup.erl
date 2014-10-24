%%======================================================================
%%
%% Leo Gateway
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

%% External exports
-export([start_link/0]).

%% supervisor callbacks
-export([init/1]).

%% @spec start_link() -> ServerRet
%% @doc API for starting the supervisor.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @spec init([]) -> SupervisorTree
%% @doc supervisor callback.
init([]) ->
    WD_MaxMemCapacity = ?env_watchdog_max_mem_capacity(),
    WD_CheckInterval  = ?env_watchdog_check_interval(),

    WatchDogs = [{leo_watchdog_rex,
                  {leo_watchdog_rex, start_link,
                   [WD_MaxMemCapacity, WD_CheckInterval]},
                  permanent,
                  ?SHUTDOWN_WAITING_TIME,
                  worker,
                  [leo_watchdog_rex]}],
    {ok, {_SupFlags = {one_for_one, 5, 60}, WatchDogs}}.
