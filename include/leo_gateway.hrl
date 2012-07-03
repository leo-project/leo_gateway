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
-vsn('0.9.0').

-define(SHUTDOWN_WAITING_TIME, 2000).
-define(MAX_RESTART,              5).
-define(MAX_TIME,                60).

-define(DEF_LISTEN_PORT,    8080).
-define(DEF_NUM_OF_ACCS,    32).
-define(DEF_LAYERS_OF_DIRS, {3, 12}).
-define(DEF_CACHE_EXPIRE, 60).
-define(DEF_CACHE_MAX_CONTENT_LEN, 1024000).
-define(DEF_CACHE_CONTENT_TYPES, []). %all
-define(DEF_CACHE_PATH_PATTERNS, []). %all

-ifdef(TEST).
-define(DEF_TIMEOUT,     1000).
-define(DEF_REQ_TIMEOUT, 1000).
-else.
-define(DEF_TIMEOUT,      3000). %%  3 sec
-define(DEF_REQ_TIMEOUT, 30000). %% 30 sec
-endif.

%% error.
-define(ERROR_COULD_NOT_CONNECT, "could not connect").
-define(MSG_INCOMPLETE_BODY,   {400, 'incomplete body'  }).
-define(MSG_INVALID_ARGUMENT,  {400, 'invalid argument' }).
-define(MSG_INVALID_URI,       {400, 'invalid uri'      }).
-define(MSG_KEY_TOO_LONG,      {400, 'key too long'     }).
-define(MSG_REQ_TIMEOUT,       {400, 'request timeout'  }).
-define(MSG_FILE_NOT_FOUND,    {404, 'file not found'   }).
-define(MSG_INTERNAL_ERROR,    {500, 'internal_error'   }).
-define(MSG_SLOW_DOWN,         {503, 'slow down'        }).

%% s3 response xmls
-define(XML_BUCKET_LIST,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        ++ "<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01\">"
        ++ "<Owner><ID>LeoFS</ID><DisplayName>webfile</DisplayName></Owner><Buckets>"
        ++ "~s"
        ++ "</Buckets></ListAllMyBucketsResult>").

-define(XML_OBJ_LIST,
        "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
        ++ "<Name>standalone</Name>"
        ++ "<Prefix></Prefix><Marker></Marker>"
        ++ "<MaxKeys>50</MaxKeys><Delimiter>/</Delimiter>"
        ++ "<IsTruncated>false</IsTruncated>"
        ++ "~s"
        ++ "</ListBucketResult>").

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

-define(env_cache_plugin(),
        case application:get_env(leo_gateway, cache_plugin) of
            {ok, Plugin} -> Plugin;
            _ -> none
        end).

-define(env_cache_expire(),
        case application:get_env(leo_gateway, cache_expire) of
            {ok, Expire} -> Expire;
            _ -> ?DEF_CACHE_EXPIRE
        end).

-define(env_cache_max_content_len(),
        case application:get_env(leo_gateway, cache_max_content_len) of
            {ok, MaxLen} -> MaxLen;
            _ -> ?DEF_CACHE_MAX_CONTENT_LEN
        end).

-define(env_cachable_content_type(),
        case application:get_env(leo_gateway, cachable_content_type) of
            {ok, ContentTypes} -> ContentTypes;
            _ -> ?DEF_CACHE_CONTENT_TYPES
        end).

-define(env_cachable_path_pattern(),
        case application:get_env(leo_gateway, cachable_path_pattern) of
            {ok, PathPatterns} -> PathPatterns;
            _ -> ?DEF_CACHE_PATH_PATTERNS
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

-define(error_resp(Type),
        case Type of
            imcomplete_body ->
                #error_code{code = 'ImcompleteBody',
                            description = "You did not provide the number of bytes specified by the Content-Length HTTP header",
                            http_status_code = 400};
            internal_error ->
                #error_code{code = 'InnternalError',
                            description = "We encountered an internal error. Please try again.",
                            http_status_code = 500};
            invalid_argument ->
                #error_code{code = 'InvalidArgument',
                            description = "Invalid Argument.",
                            http_status_code = 400};
            invalid_uri ->
                #error_code{code = 'InvalidURI',
                            description = "Couldn't pare the specified URI.",
                            http_status_code = 400};
            key_too_long ->
                #error_code{code = 'KeyTooLong',
                            description = "Your key is long",
                            http_status_code = 400};

            missing_content_length ->
                #error_code{code = 'MissingContentLength',
                            description = "You must provide the Content-Length HTTP header.",
                            http_status_code = 411};

            missing_request_body_error ->
                #error_code{code = 'MissingRequestBodyError',
                            description = "This happens when the user sends an empty data as a request. Request body is empty.",
                            http_status_code = 400};

            request_timeout ->
                #error_code{code = 'RequestTimeout',
                            description = "Your socket connection to the server was not read from or written to within the timeout period.",
                            http_status_code = 400};

            slow_down ->
                #error_code{code = 'SlowDown',
                            description = "Please reduce your request rate.",
                            http_status_code = 503}
        end).

