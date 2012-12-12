%%====================================================================
%%
%% LeoFS Gateway
%%
%% Copyright (c) 2012
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
%% LeoFS Gateway
%%
%%====================================================================
-author('Yosuke Hara').

-define(SHUTDOWN_WAITING_TIME, 2000).
-define(MAX_RESTART,              5).
-define(MAX_TIME,                60).

-define(DEF_LISTEN_PORT,    8080).
-define(DEF_NUM_OF_ACCS,    32).
-define(DEF_RPC_HANDLER,    'leo_gateway_web_model').
-define(DEF_LAYERS_OF_DIRS, {3, 12}).
-define(DEF_CACHE_EXPIRE, 60).
-define(DEF_CACHE_MAX_CONTENT_LEN, 1024000).
-define(DEF_CACHE_CONTENT_TYPES, []). %all
-define(DEF_CACHE_PATH_PATTERNS, []). %all

-define(S3_HTTP, leo_s3_http). %% listener-1
-define(T6_HTTP, leo_t6).      %% listener-2

-ifdef(TEST).
-define(DEF_TIMEOUT,     1000).
-define(DEF_REQ_TIMEOUT, 1000).
-else.
-define(DEF_TIMEOUT,      5000). %%  5 sec
-define(DEF_REQ_TIMEOUT, 30000). %% 30 sec
-endif.

%% error
-define(ERROR_COULD_NOT_CONNECT, "could not connect").
-define(MSG_INCOMPLETE_BODY,   {400, 'incomplete body'  }).
-define(MSG_INVALID_ARGUMENT,  {400, 'invalid argument' }).
-define(MSG_INVALID_URI,       {400, 'invalid uri'      }).
-define(MSG_KEY_TOO_LONG,      {400, 'key too long'     }).
-define(MSG_REQ_TIMEOUT,       {400, 'request timeout'  }).
-define(MSG_FILE_NOT_FOUND,    {404, 'file not found'   }).
-define(MSG_INTERNAL_ERROR,    {500, 'internal_error'   }).
-define(MSG_SLOW_DOWN,         {503, 'slow down'        }).

%% timeout
-define(TIMEOUT_L1_LEN,   65535).
-define(TIMEOUT_L2_LEN,  131071).
-define(TIMEOUT_L3_LEN,  524287).
-define(TIMEOUT_L4_LEN, 1048576).

-define(TIMEOUT_L1_SEC,    5000).
-define(TIMEOUT_L2_SEC,    7000).
-define(TIMEOUT_L3_SEC,   10000).
-define(TIMEOUT_L4_SEC,   20000).
-define(TIMEOUT_L5_SEC,   30000).


%% macros.
-define(def_gateway_conf(),
        fun() ->
                {ok, Hostname} = inet:gethostname(),
                {?DEF_LISTEN_PORT, ?DEF_NUM_OF_ACCS,
                 [list_to_atom("leo_remote_manager_test@" ++ Hostname)]}
        end).

-define(env_layer_of_dirs(),
        case application:get_env(leo_gateway, layer_of_dirs) of
            {ok, GW_Val_1} -> GW_Val_1;
            _ -> ?DEF_LAYERS_OF_DIRS
        end).

-define(env_listener(),
        case application:get_env(leo_gateway, listener) of
            {ok, Listener} -> Listener;
            _ -> ?S3_HTTP
        end).

-define(env_timeout_level_1(),
        case application:get_env(leo_gateway, timeout_level_1) of
            {ok, EnvTimeoutL1} -> EnvTimeoutL1;
            _ -> ?TIMEOUT_L1_SEC
        end).

-define(env_timeout_level_2(),
        case application:get_env(leo_gateway, timeout_level_2) of
            {ok, EnvTimeoutL2} -> EnvTimeoutL2;
            _ -> ?TIMEOUT_L2_SEC
        end).

-define(env_timeout_level_3(),
        case application:get_env(leo_gateway, timeout_level_3) of
            {ok, EnvTimeoutL3} -> EnvTimeoutL3;
            _ -> ?TIMEOUT_L3_SEC
        end).

-define(env_timeout_level_4(),
        case application:get_env(leo_gateway, timeout_level_4) of
            {ok, EnvTimeoutL4} -> EnvTimeoutL4;
            _ -> ?TIMEOUT_L4_SEC
        end).

-define(env_timeout_level_5(),
        case application:get_env(leo_gateway, timeout_level_5) of
            {ok, EnvTimeoutL5} -> EnvTimeoutL5;
            _ -> ?TIMEOUT_L5_SEC
        end).


%% REQ/RESP ERRORS
-record(error_code, {
          code             :: atom(),
          description      :: list(),
          http_status_code :: integer()}).


-record(statistics, {
          id        = 0        :: integer(),
          read      = 0        :: integer(),
          write     = 0        :: integer(),
          delete    = 0        :: integer(),
          head      = 0        :: integer(),
          pending   = 0        :: integer(),
          error     = 0        :: integer(),
          total_mem_usage  = 0 :: integer(),
          system_mem_usage = 0 :: integer(),
          proc_mem_usage   = 0 :: integer(),
          num_of_procs     = 0 :: integer()
         }).
