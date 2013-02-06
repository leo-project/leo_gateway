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

-export([start/1, get_options/0]).

-include("leo_gateway.hrl").
-include("leo_s3_http.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("eunit/include/eunit.hrl").


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Provide processes which are cache and http_server
%%
-spec(start(pid()) ->
             tuple()).
start(Sup) ->
    {ok, Options} = get_options(),

    %% for ECache
    NumOfECacheWorkers = Options#http_options.cache_workers,
    TotalCacheCapacity = Options#http_options.cache_capacity,
    ?debugVal({NumOfECacheWorkers, TotalCacheCapacity}),

    ChildSpec0 = {ecache_sup,
                  {ecache_sup, start_link, [NumOfECacheWorkers, TotalCacheCapacity]},
                  permanent, ?SHUTDOWN_WAITING_TIME, supervisor, [ecache_sup]},
    {ok, _} = supervisor:start_child(Sup, ChildSpec0),

    %% for Cowboy
    ChildSpec1 = {cowboy_sup,
                  {cowboy_sup, start_link, []},
                  permanent, ?SHUTDOWN_WAITING_TIME, supervisor, [cowboy_sup]},
    {ok, _} = supervisor:start_child(Sup, ChildSpec1),

    leo_s3_http_cowboy:start(Options),
    ok.


%% @doc Retrieve properties
%%
-spec(get_options() ->
             {ok, #http_options{}}).
get_options() ->
    %% Retrieve http-related properties:
    HttpProp = ?env_http_properties(),
    UseS3API             = leo_misc:get_value('s3_api',           HttpProp, true),
    Port                 = leo_misc:get_value('port',             HttpProp, 8080),
    SSLPort              = leo_misc:get_value('ssl_port',         HttpProp, 8443),
    SSLCertFile          = leo_misc:get_value('ssl_certfile',     HttpProp, "./server_cert.pem"),
    SSLKeyFile           = leo_misc:get_value('ssl_keyfile',      HttpProp, "./server_key.pem"),
    NumOfAcceptors       = leo_misc:get_value('num_of_acceptors', HttpProp,   32),

    %% Retrieve cache-related properties:
    CacheProp = ?env_cache_properties(),
    UserHttpCache        = leo_misc:get_value('http_cache',            CacheProp, false),
    CacheWorkers         = leo_misc:get_value('cache_workers',         CacheProp, 64),
    CacheCapacity        = leo_misc:get_value('cache_capacity',        CacheProp, 64000000), %% about 64MB
    CacheExpire          = leo_misc:get_value('cache_expire',          CacheProp, 300),      %% 300sec
    CacheMaxContentLen   = leo_misc:get_value('cache_max_content_len', CacheProp, 1000000),  %% about 1MB
    CachableContentTypes = leo_misc:get_value('cachable_content_type', CacheProp, []),
    CachablePathPatterns = leo_misc:get_value('cachable_path_pattern', CacheProp, []),

    CacheMethod = case UserHttpCache of
                      true  -> ?CACHE_HTTP;
                      false -> ?CACHE_INNER
                  end,
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

    %% Retrieve large-object-related properties:
    LargeObjectProp = ?env_large_object_properties(),
    MaxChunkedObjs       = leo_misc:get_value('max_chunked_objs',  LargeObjectProp, [1000]),      %% 1000
    MaxObjLen            = leo_misc:get_value('max_len_for_obj',   LargeObjectProp, [524288000]), %% 500.0MB
    ChunkedObjLen        = leo_misc:get_value('chunked_obj_len',   LargeObjectProp, [5242880]),   %%   5.0MB
    ThresholdObjLen      = leo_misc:get_value('threshold_obj_len', LargeObjectProp, [5767168]),   %%   5.5MB

    %% Retrieve timeout-values
    lists:foreach(fun({K, T}) ->
                          leo_misc:set_env(leo_gateway, K, T)
                  end, ?env_timeout()),

    HttpOptions = #http_options{s3_api                 = UseS3API,
                                port                   = Port,
                                ssl_port               = SSLPort,
                                ssl_certfile           = SSLCertFile,
                                ssl_keyfile            = SSLKeyFile,
                                num_of_acceptors       = NumOfAcceptors,
                                cache_method           = CacheMethod,
                                cache_workers          = CacheWorkers,
                                cache_capacity         = CacheCapacity,
                                cache_expire           = CacheExpire,
                                cache_max_content_len  = CacheMaxContentLen,
                                cachable_content_type  = CachableContentTypes1,
                                cachable_path_pattern  = CachablePathPatterns1,
                                max_chunked_objs       = MaxChunkedObjs,
                                max_len_for_obj        = MaxObjLen,
                                chunked_obj_len        = ChunkedObjLen,
                                threshold_obj_len      = ThresholdObjLen},
    ?info("start/3", "s3-api: ~p",                  [UseS3API]),
    ?info("start/3", "port: ~p",                    [Port]),
    ?info("start/3", "ssl port: ~p",                [SSLPort]),
    ?info("start/3", "ssl certfile: ~p",            [SSLCertFile]),
    ?info("start/3", "ssl keyfile: ~p",             [SSLKeyFile]),
    ?info("start/3", "num of acceptors: ~p",        [NumOfAcceptors]),
    ?info("start/3", "cache_method: ~p",            [CacheMethod]),
    ?info("start/3", "cache workers: ~p",           [CacheWorkers]),
    ?info("start/3", "cache capacity: ~p",          [CacheCapacity]),
    ?info("start/3", "cache expire: ~p",            [CacheExpire]),
    ?info("start/3", "cache_max_content_len: ~p",   [CacheMaxContentLen]),
    ?info("start/3", "cacheable_content_types: ~p", [CachableContentTypes]),
    ?info("start/3", "cacheable_path_patterns: ~p", [CachablePathPatterns]),
    ?info("start/3", "max_chunked_obj: ~p",         [MaxChunkedObjs]),
    ?info("start/3", "max_len_for_obj: ~p",         [MaxObjLen]),
    ?info("start/3", "chunked_obj_len: ~p",         [ChunkedObjLen]),
    ?info("start/3", "threshold_obj_len: ~p",       [ThresholdObjLen]),
    {ok, HttpOptions}.


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

