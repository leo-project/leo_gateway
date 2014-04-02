%%====================================================================
%%
%% LeoFS Gateway
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
%% -------------------------------------------------------------------
%% LeoFS Gateway
%%
%%====================================================================
-author('Yosuke Hara').

-define(SHUTDOWN_WAITING_TIME, 2000).
-define(S3_HTTP, leo_s3_http).
-define(DEF_LAYERS_OF_DIRS, {1, 64}).

-ifdef(TEST).
-define(DEF_TIMEOUT,     1000).
-define(DEF_REQ_TIMEOUT, 1000).
-else.
-define(DEF_TIMEOUT,      5000). %%  5 sec
-define(DEF_REQ_TIMEOUT, 30000). %% 30 sec
-endif.

%% error
-define(ERROR_COULD_NOT_CONNECT, "Could not connect").
-define(ERROR_NOT_MATCH_LENGTH,  "Not match object length").
-define(ERROR_FAIL_PUT_OBJ,      "Fail put an object").
-define(ERROR_FAIL_RETRIEVE_OBJ, "Fail retrieve an object").


%% @pending
%% -define(MSG_INCOMPLETE_BODY,   {400, 'incomplete body'  }).
%% -define(MSG_INVALID_ARGUMENT,  {400, 'invalid argument' }).
%% -define(MSG_INVALID_URI,       {400, 'invalid uri'      }).
%% -define(MSG_KEY_TOO_LONG,      {400, 'key too long'     }).
%% -define(MSG_REQ_TIMEOUT,       {400, 'request timeout'  }).
%% -define(MSG_FILE_NOT_FOUND,    {404, 'file not found'   }).
%% -define(MSG_INTERNAL_ERROR,    {500, 'internal_error'   }).
%% -define(MSG_SLOW_DOWN,         {503, 'slow down'        }).

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


%% large-object
-record(large_obj_info, {key = <<>>         :: binary(),
                         length = 0         :: pos_integer(),
                         num_of_chunks = 0  :: pos_integer(),
                         md5_context = <<>> :: binary()
                        }).

%% access-log
-define(LOG_GROUP_ID_ACCESS, 'log_grp_access_log').
-define(LOG_ID_ACCESS,       'log_id_access_log').
-define(LOG_FILENAME_ACCESS, "access").


-define(env_bucket_prop_sync_interval(),
        case application:get_env(leo_gateway, bucket_prop_sync_interval) of
            {ok, EnvBucketPropSyncInterval} -> EnvBucketPropSyncInterval;
            _ -> 300 %% 300sec/5min
        end).

-define(env_http_properties(),
        case application:get_env(leo_gateway, http) of
            {ok, EnvHttp} -> EnvHttp;
            _ -> []
        end).

-define(env_layer_of_dirs(),
        case application:get_env(leo_gateway, layer_of_dirs) of
            {ok, EnvLayerOfDirs} -> EnvLayerOfDirs;
            _ -> ?DEF_LAYERS_OF_DIRS
        end).

-define(env_listener(),
        case application:get_env(leo_gateway, listener) of
            {ok, EnvListener} -> EnvListener;
            _ -> ?S3_HTTP
        end).

-define(env_cache_properties(),
        case application:get_env(leo_gateway, cache) of
            {ok, EnvCache} -> EnvCache;
            _ -> []
        end).

-define(env_large_object_properties(),
        case application:get_env(leo_gateway, large_object) of
            {ok, EnvLargeObject} -> EnvLargeObject;
            _ -> []
        end).

-define(env_recover_properties(),
        case application:get_env(leo_gateway, recover) of
            {ok, EnvRecover} -> EnvRecover;
            _ -> []
        end).


%% Timeout-related:
%%
-define(env_timeout(),
        case application:get_env(leo_gateway, timeout) of
            {ok, EnvTimeout} -> EnvTimeout;
            _ -> []
        end).

-define(env_timeout_level_1(),
        case leo_misc:get_env(leo_gateway, level_1) of
            {ok, EnvTimeoutL1} -> EnvTimeoutL1;
            _ -> ?TIMEOUT_L1_SEC
        end).

-define(env_timeout_level_2(),
        case leo_misc:get_env(leo_gateway, level_2) of
            {ok, EnvTimeoutL2} -> EnvTimeoutL2;
            _ -> ?TIMEOUT_L2_SEC
        end).

-define(env_timeout_level_3(),
        case leo_misc:get_env(leo_gateway, level_3) of
            {ok, EnvTimeoutL3} -> EnvTimeoutL3;
            _ -> ?TIMEOUT_L3_SEC
        end).

-define(env_timeout_level_4(),
        case leo_misc:get_env(leo_gateway, level_4) of
            {ok, EnvTimeoutL4} -> EnvTimeoutL4;
            _ -> ?TIMEOUT_L4_SEC
        end).

-define(env_timeout_level_5(),
        case leo_misc:get_env(leo_gateway, timeout_level_5) of
            {ok, EnvTimeoutL5} -> EnvTimeoutL5;
            _ -> ?TIMEOUT_L5_SEC
        end).

%% QoS related
-define(env_qos_stat_enable(),
        case leo_misc:get_env(leo_gateway, is_enable_qos_stat) of
            {ok, _IsEnableQoSStat} -> _IsEnableQoSStat;
            _ -> false
        end).
-define(env_qos_notify_enable(),
        case leo_misc:get_env(leo_gateway, is_enable_qos_notify) of
            {ok, _IsEnableQoSNotify} -> _IsEnableQoSNotify;
            _ -> false
        end).


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

%% access-log
%%
-define(get_child_num(_Path_1),
        begin
            case string:str(_Path_1, "\n") of
                0 ->
                    {_Path_1, 0};
                _Index ->
                    case list_to_integer(string:sub_string(_Path_1, _Index + 1)) of
                        {'EXIT',_} ->
                            {_Path_1, 0};
                        _Num->
                            {string:sub_string(_Path_1, 1, _Index -1), _Num}
                    end
            end
        end).

-define(access_log_get(_Bucket, _Path, _Size, _Response),
        begin
            {_OrgPath, _ChildNum} = ?get_child_num(binary_to_list(_Path)),
            leo_logger_client_common:append(
              {?LOG_ID_ACCESS,
               #message_log{format  = "[GET]\t~s\t~s\t~w\t~w\t~s\t~w\t~w\n",
                            message = [binary_to_list(_Bucket),
                                       _OrgPath,
                                       _ChildNum,
                                       _Size,
                                       leo_date:date_format(),
                                       leo_date:clock(),
                                       _Response]}})
        end).
-define(access_log_put(_Bucket, _Path, _Size, _Response),
        begin
            {_OrgPath, _ChildNum} = ?get_child_num(binary_to_list(_Path)),
            leo_logger_client_common:append(
              {?LOG_ID_ACCESS,
               #message_log{format  = "[PUT]\t~s\t~s\t~w\t~w\t~s\t~w\t~w\n",
                            message = [binary_to_list(_Bucket),
                                       _OrgPath,
                                       _ChildNum,
                                       _Size,
                                       leo_date:date_format(),
                                       leo_date:clock(),
                                       _Response
                                      ]}
              })
        end).
-define(access_log_delete(_Bucket, _Path, _Size, _Response),
        begin
            {_OrgPath, _ChildNum} = ?get_child_num(binary_to_list(_Path)),
            leo_logger_client_common:append(
              {?LOG_ID_ACCESS,
               #message_log{format  = "[DELETE]\t~s\t~s\t~w\t~w\t~s\t~w\t~w\n",
                            message = [binary_to_list(_Bucket),
                                       _OrgPath,
                                       _ChildNum,
                                       _Size,
                                       leo_date:date_format(),
                                       leo_date:clock(),
                                       _Response
                                      ]}
              })
        end).
-define(access_log_head(_Bucket, _Path, _Response),
        begin
            {_OrgPath, _ChildNum} = ?get_child_num(binary_to_list(_Path)),
            leo_logger_client_common:append(
              {?LOG_ID_ACCESS,
               #message_log{format  = "[HEAD]\t~s\t~s\t~w\t~w\t~s\t~w\t~w\n",
                            message = [binary_to_list(_Bucket),
                                       _OrgPath,
                                       _ChildNum,
                                       0,
                                       leo_date:date_format(),
                                       leo_date:clock(),
                                       _Response
                                      ]}
              })
        end).

