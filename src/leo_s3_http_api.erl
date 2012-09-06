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
    HTTPServer = proplists:get_value('http_server', S3_HTTP_Config, 'mochiweb'),
    start(Sup, HTTPServer, S3_HTTP_Config).

-spec(start(pid(), http_server(), list()) ->
             tuple()).
start(Sup, mochiweb, S3_HTTP_Config) ->
    ListenPort     = proplists:get_value('port',             S3_HTTP_Config, 8080),
    NumOfAcceptors = proplists:get_value('num_of_acceptors', S3_HTTP_Config,   32),
    SSLListenPort  = proplists:get_value('ssl_port',         S3_HTTP_Config, 8443),
    SSLCertFile    = proplists:get_value('ssl_certfile',     S3_HTTP_Config, "./server_cert.pem"),
    SSLKeyFile     = proplists:get_value('ssl_keyfile',      S3_HTTP_Config, "./server_key.pem"),
    io:format("*             port: ~p~n", [ListenPort]),
    io:format("* num of acceptors: ~p~n", [NumOfAcceptors]),
    io:format("*         ssl port: ~p~n", [SSLListenPort]),
    io:format("*     ssl certfile: ~p~n", [SSLCertFile]),
    io:format("*      ssl keyfile: ~p~n", [SSLKeyFile]),

    HookModules =
        case proplists:get_value('cache_plugin', S3_HTTP_Config) of
            undefined -> [];
            ModCache ->
                CacheExpire          = proplists:get_value('cache_expire',          S3_HTTP_Config, 300),
                CacheMaxContentLen   = proplists:get_value('cache_max_content_len', S3_HTTP_Config, 1000000),
                CachableContentTypes = proplists:get_value('cachable_content_type', S3_HTTP_Config, []),
                CachablePathPatterns = proplists:get_value('cachable_path_pattern', S3_HTTP_Config, []),
                io:format("*        mod cache: ~p~n", [ModCache]),
                io:format("*     cache_expire: ~p~n", [CacheExpire]),
                io:format("*  max_content_len: ~p~n", [CacheMaxContentLen]),
                io:format("*    content_types: ~p~n", [CachableContentTypes]),
                io:format("*    path_patterns: ~p~n", [CachablePathPatterns]),
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

start(Sup, cowboy, S3_HTTP_Config) ->
    ListenPort     = proplists:get_value('port',             S3_HTTP_Config, 8080),
    NumOfAcceptors = proplists:get_value('num_of_acceptors', S3_HTTP_Config,   32),
    SSLListenPort  = proplists:get_value('ssl_port',         S3_HTTP_Config, 8443),
    SSLCertFile    = proplists:get_value('ssl_certfile',     S3_HTTP_Config, "./server_cert.pem"),
    SSLKeyFile     = proplists:get_value('ssl_keyfile',      S3_HTTP_Config, "./server_key.pem"),
    io:format("*             port: ~p~n", [ListenPort]),
    io:format("* num of acceptors: ~p~n", [NumOfAcceptors]),
    io:format("*         ssl port: ~p~n", [SSLListenPort]),
    io:format("*     ssl certfile: ~p~n", [SSLCertFile]),
    io:format("*      ssl keyfile: ~p~n", [SSLKeyFile]),
    WebConfig = [
                 {port, ListenPort},
                 {acceptor_pool_size, NumOfAcceptors},
                 {ssl_port, SSLListenPort},
                 {ssl_certfile, SSLCertFile},
                 {ssl_keyfile, SSLKeyFile},
                 {docroot, "."}],
    ChildSpec =
        {leo_s3_http_cowboy,
         {leo_s3_http_cowboy, start, [WebConfig]},
         permanent, ?SHUTDOWN_WAITING_TIME, worker, dynamic},
    {ok, _} = supervisor:start_child(Sup, ChildSpec),
    ok.



