%%======================================================================
%%
%% Leo S3 HTTP
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
%% Leo S3 HTTP - Cache Statistics
%% @doc
%% @end
%%======================================================================
-module(leo_s3_http_cache_statistics).

-author('Yosuke Hara').

-behaviour(leo_statistics_behaviour).

-include_lib("leo_statistics/include/leo_statistics.hrl").
-include_lib("ecache/include/ecache.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_link/1]).
-export([init/0, handle_call/2]).

-define(SNMP_MSG_REPLICATE,  'num-of-msg-replicate').
-define(SNMP_MSG_SYNC_VNODE, 'num-of-msg-sync-vnode').
-define(SNMP_MSG_REBALANCE,  'num-of-msg-rebalance').


-define(SNMP_CACHE_HIT_COUNT,  'cache-hit-count').
-define(SNMP_CACHE_MISS_COUNT, 'cache-miss-count').
-define(SNMP_CACHE_NUM_OF_OBJ, 'cache-object-count').
-define(SNMP_CACHE_TOTAL_SIZE, 'cache-object-size').


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
start_link(Interval) ->
    ok = leo_statistics_api:start_link(?MODULE, Interval),
    ok.

%%--------------------------------------------------------------------
%% Callback
%%--------------------------------------------------------------------
%% @doc Initialize metrics.
%%
-spec(init() ->
             ok).
init() ->
    ok.


%% @doc Synchronize values.
%%
-spec(handle_call(sync, ?STAT_INTERVAL_1M | ?STAT_INTERVAL_5M) ->
             ok).
handle_call(sync, ?STAT_INTERVAL_1M) ->
    Stats = case catch ecache_server:stats() of
                {'EXIT', _Cause} -> #stats{};
                Value            -> Value
            end,

    #stats{gets        = NumOfRead,
           hits        = HitCount,
           records     = NumOfObjects,
           cached_size = TotalOfSize} = Stats,

    catch snmp_generic:variable_set(?SNMP_CACHE_HIT_COUNT,  HitCount),
    catch snmp_generic:variable_set(?SNMP_CACHE_MISS_COUNT, NumOfRead - HitCount),
    catch snmp_generic:variable_set(?SNMP_CACHE_NUM_OF_OBJ, NumOfObjects),
    catch snmp_generic:variable_set(?SNMP_CACHE_TOTAL_SIZE, TotalOfSize),
    ok;

handle_call(sync, ?STAT_INTERVAL_5M) ->
    ok.

