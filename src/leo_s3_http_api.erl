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
-export([get_options/2]).

-include("leo_gateway.hrl").
-include("leo_s3_http.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("eunit/include/eunit.hrl").

-type(http_server() :: cowboy).

-define(env_s3_http(AppName),
        case application:get_env(AppName, s3_http) of
            {ok, Config} -> Config;
            _ -> []
        end).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Provide processes which are cache and http_server
%%
-spec(start(pid(), atom()) ->
             tuple()).
start(Sup, AppName) ->
    S3_HTTP_Config = ?env_s3_http(AppName),
    HTTPServer = leo_misc:get_value('http_server', S3_HTTP_Config, 'cowboy'),
    start(Sup, HTTPServer, S3_HTTP_Config).

-spec(start(pid(), http_server(), list()) ->
             tuple()).
start(Sup, cowboy = HTTPServer, S3_HTTP_Config) ->
    {ok, HTTPOptions} = get_options(HTTPServer, S3_HTTP_Config),

    ChildSpec = {cowboy_sup,
                 {cowboy_sup, start_link, []},
                 permanent, ?SHUTDOWN_WAITING_TIME, supervisor, [cowboy_sup]},
    {ok, _} = supervisor:start_child(Sup, ChildSpec),

    leo_s3_http_cowboy:start(HTTPOptions),
    ok.


%% @doc Retrieve options
%% @private
-spec(get_options(cowboy, list()) ->
             {ok, #http_options{}}).
get_options(HTTPServer, Options) ->
    UseS3API             = leo_misc:get_value('s3_api',                 Options, true),
    Port                 = leo_misc:get_value('port',                   Options, 8080),
    SSLPort              = leo_misc:get_value('ssl_port',               Options, 8443),
    SSLCertFile          = leo_misc:get_value('ssl_certfile',           Options, "./server_cert.pem"),
    SSLKeyFile           = leo_misc:get_value('ssl_keyfile',            Options, "./server_key.pem"),
    NumOfAcceptors       = leo_misc:get_value('num_of_acceptors',       Options,   32),
    CacheMethod          = leo_misc:get_value('cache_method',           Options, ?CACHE_HTTP),
    CacheExpire          = leo_misc:get_value('cache_expire',           Options, 300),
    CacheMaxContentLen   = leo_misc:get_value('cache_max_content_len',  Options, 1000000),
    CachableContentTypes = leo_misc:get_value('cachable_content_type',  Options, []),
    CachablePathPatterns = leo_misc:get_value('cachable_path_pattern',  Options, []),
    MaxChunkedObjs       = leo_misc:get_value('max_chunked_objs',       Options, [1000]),      %%  1000
    MaxObjLen            = leo_misc:get_value('max_len_for_obj',        Options, [524288000]), %% 500.0MB
    ChunkedObjLen        = leo_misc:get_value('chunked_obj_len',        Options, [5242880]),   %%   5.0MB
    ThresholdObjLen      = leo_misc:get_value('threshold_obj_len',      Options, [5767168]),   %%   5.5MB

    ?info("start/3", "s3-api: ~p",                  [UseS3API]),
    ?info("start/3", "http-server: ~p",             [HTTPServer]),
    ?info("start/3", "port: ~p",                    [Port]),
    ?info("start/3", "ssl port: ~p",                [SSLPort]),
    ?info("start/3", "ssl certfile: ~p",            [SSLCertFile]),
    ?info("start/3", "ssl keyfile: ~p",             [SSLKeyFile]),
    ?info("start/3", "num of acceptors: ~p",        [NumOfAcceptors]),
    ?info("start/3", "cache_method: ~p",            [CacheMethod]),
    ?info("start/3", "cache expire: ~p",            [CacheExpire]),
    ?info("start/3", "cache_max_content_len: ~p",   [CacheMaxContentLen]),
    ?info("start/3", "cacheable_content_types: ~p", [CachableContentTypes]),
    ?info("start/3", "cacheable_path_patterns: ~p", [CachablePathPatterns]),
    ?info("start/3", "max_chunked_obj: ~p",         [MaxChunkedObjs]),
    ?info("start/3", "max_len_for_obj: ~p",         [MaxObjLen]),
    ?info("start/3", "chunked_obj_len: ~p",         [ChunkedObjLen]),
    ?info("start/3", "threshold_obj_len: ~p",       [ThresholdObjLen]),

    CachableContentTypes1 = cast_type_list_to_binary(CachableContentTypes),
    CachablePathPatterns1 = case cast_type_list_to_binary(CachablePathPatterns) of
                                [] -> [];
                                List ->
                                    lists:foldl(
                                      fun(P, Acc) ->
                                              case re:compile(P) of
                                                  {ok, MP} -> [MP|Acc];
                                                  _        -> Acc
                                              end
                                      end, [], List)
                            end,

    {ok, #http_options{s3_api                 = UseS3API,
                       port                   = Port,
                       ssl_port               = SSLPort,
                       ssl_certfile           = SSLCertFile,
                       ssl_keyfile            = SSLKeyFile,
                       num_of_acceptors       = NumOfAcceptors,
                       cache_method           = CacheMethod,
                       cache_expire           = CacheExpire,
                       cache_max_content_len  = CacheMaxContentLen,
                       cachable_content_type  = CachableContentTypes1,
                       cachable_path_pattern  = CachablePathPatterns1,
                       max_chunked_objs       = MaxChunkedObjs,
                       max_len_for_obj        = MaxObjLen,
                       chunked_obj_len        = ChunkedObjLen,
                       threshold_obj_len      = ThresholdObjLen
                      }}.

%% @doc
%% @private
cast_type_list_to_binary([]) ->
    [];
cast_type_list_to_binary(List) ->
    lists:map(fun(I) ->
                      case catch list_to_binary(I) of
                          {'EXIT', _} -> I;
                          Bin         -> Bin
                      end
              end, List).

