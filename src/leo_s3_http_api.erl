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
%% Leo S3 HTTP - API
%% @doc
%% @end
%%======================================================================
-module(leo_s3_http_api).

-author('Yosuke Hara').

-export([start/2, start/3]).

-include("leo_gateway.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("eunit/include/eunit.hrl").

-type(http_server() :: mochiweb | cowboy).

-define(env_s3_http(AppName),
        case application:get_env(AppName, s3_http) of
            {ok, Config} -> Config;
            _ -> []
        end).


%% @doc Provide processes which are cache and http_server
%%
-spec(start(pid(), atom()) ->
             tuple()).
start(Sup, AppName) ->
    S3_HTTP_Config = ?env_s3_http(AppName),
    HTTPServer = leo_misc:get_value('http_server', S3_HTTP_Config, 'mochiweb'),
    start(Sup, HTTPServer, S3_HTTP_Config).

-spec(start(pid(), http_server(), list()) ->
             tuple()).
start(Sup, mochiweb, S3_HTTP_Config) ->
    ListenPort     = leo_misc:get_value('port',             S3_HTTP_Config, 8080),
    SSLListenPort  = leo_misc:get_value('ssl_port',         S3_HTTP_Config, 8443),
    SSLCertFile    = leo_misc:get_value('ssl_certfile',     S3_HTTP_Config, "./server_cert.pem"),
    SSLKeyFile     = leo_misc:get_value('ssl_keyfile',      S3_HTTP_Config, "./server_key.pem"),
    NumOfAcceptors = leo_misc:get_value('num_of_acceptors', S3_HTTP_Config,   32),

    ?info("start/3", "engine: ~p",          [mochiweb]),
    ?info("start/3", "port: ~p",            [ListenPort]),
    ?info("start/3", "ssl port: ~p",        [SSLListenPort]),
    ?info("start/3", "ssl certfile: ~p",    [SSLCertFile]),
    ?info("start/3", "ssl keyfile: ~p",     [SSLKeyFile]),
    ?info("start/3", "num of acceptors: ~p",[NumOfAcceptors]),

    HookModules =
        case leo_misc:get_value('cache_plugin', S3_HTTP_Config) of
            undefined -> [];
            ModCache ->
                CacheExpire          = leo_misc:get_value('cache_expire',          S3_HTTP_Config, 300),
                CacheMaxContentLen   = leo_misc:get_value('cache_max_content_len', S3_HTTP_Config, 1000000),
                CachableContentTypes = leo_misc:get_value('cachable_content_type', S3_HTTP_Config, []),
                CachablePathPatterns = leo_misc:get_value('cachable_path_pattern', S3_HTTP_Config, []),

                ?info("start/3", "mod cache: ~p",       [ModCache]),
                ?info("start/3", "cache expire: ~p",    [CacheExpire]),
                ?info("start/3", "max_content_len: ~p", [CacheMaxContentLen]),
                ?info("start/3", "content_types: ~p",   [CachableContentTypes]),
                ?info("start/3", "path_patterns: ~p",   [CachablePathPatterns]),

                [{ModCache, [{expire,                CacheExpire},
                             {max_content_len,       CacheMaxContentLen},
                             {cachable_content_type, CachableContentTypes},
                             {cachable_path_pattern, CachablePathPatterns}
                            ]}]
        end,

    Ip = case os:getenv("MOCHIWEB_IP") of
             false -> "0.0.0.0";
             Any   -> Any
         end,

    WebConfig0 = [{ip, Ip},
                  {port, ListenPort},
                  {acceptor_pool_size, NumOfAcceptors},
                  {ssl_port, SSLListenPort},
                  {ssl_certfile, SSLCertFile},
                  {ssl_keyfile, SSLKeyFile},
                  {docroot, "."}],
    WebConfig1 =
        case HookModules of
            [] -> WebConfig0;
            _  -> lists:reverse([{hook_modules, HookModules}|WebConfig0])
        end,

    ChildSpec =
        {leo_s3_http_mochi,
         {leo_s3_http_mochi, start, [WebConfig1]},
         permanent, ?SHUTDOWN_WAITING_TIME, worker, dynamic},
    {ok, _} = supervisor:start_child(Sup, ChildSpec),
    ok;

start(_Sup, cowboy, S3_HTTP_Config) ->
    ListenPort     = leo_misc:get_value('port',             S3_HTTP_Config, 8080),
    SSLListenPort  = leo_misc:get_value('ssl_port',         S3_HTTP_Config, 8443),
    SSLCertFile    = leo_misc:get_value('ssl_certfile',     S3_HTTP_Config, "./server_cert.pem"),
    SSLKeyFile     = leo_misc:get_value('ssl_keyfile',      S3_HTTP_Config, "./server_key.pem"),
    NumOfAcceptors = leo_misc:get_value('num_of_acceptors', S3_HTTP_Config,   32),

    ?info("start/3", "engine: ~p",          [cowboy]),
    ?info("start/3", "port: ~p",            [ListenPort]),
    ?info("start/3", "ssl port: ~p",        [SSLListenPort]),
    ?info("start/3", "ssl certfile: ~p",    [SSLCertFile]),
    ?info("start/3", "ssl keyfile: ~p",     [SSLKeyFile]),
    ?info("start/3", "num of acceptors: ~p",[NumOfAcceptors]),

    WebConfig0 = [
                  {port, ListenPort},
                  {acceptor_pool_size, NumOfAcceptors},
                  {ssl_port, SSLListenPort},
                  {ssl_certfile, SSLCertFile},
                  {ssl_keyfile, SSLKeyFile},
                  {docroot, "."}],

    WebConfig1 = case proplists:get_value('cache_plugin', S3_HTTP_Config) of
                     undefined -> WebConfig0;
                     ModCache ->
                         CacheExpire          = proplists:get_value('cache_expire',          S3_HTTP_Config, 300),
                         CacheMaxContentLen   = proplists:get_value('cache_max_content_len', S3_HTTP_Config, 1000000),
                         CachableContentTypes = proplists:get_value('cachable_content_type', S3_HTTP_Config, []),
                         CachablePathPatterns = proplists:get_value('cachable_path_pattern', S3_HTTP_Config, []),

                         ?info("start/3", "mod cache: ~p",       [ModCache]),
                         ?info("start/3", "cache expire: ~p",    [CacheExpire]),
                         ?info("start/3", "max_content_len: ~p", [CacheMaxContentLen]),
                         ?info("start/3", "content_types: ~p",   [CachableContentTypes]),
                         ?info("start/3", "path_patterns: ~p",   [CachablePathPatterns]),

                         [{hook_modules,          ModCache},
                          {expire,                CacheExpire},
                          {max_content_len,       CacheMaxContentLen},
                          {cachable_content_type, CachableContentTypes},
                          {cachable_path_pattern, CachablePathPatterns}|WebConfig0]
                 end,
    leo_s3_http_cowboy:start(WebConfig1),
    ok.



