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
%% Leo S3 HTTP - powered by Cowboy version
%% @doc
%% @end
%%======================================================================
-module(leo_s3_http_cowboy).

-author('Yosuke Hara').
-author('Yoshiyuki Kanno').

-export([start/1, stop/0]).
-export([init/3, handle/2, terminate/2]).

-include("leo_gateway.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(HTTP_GET,        'GET').
-define(HTTP_POST,       'POST').
-define(HTTP_PUT,        'PUT').
-define(HTTP_DELETE,     'DELETE').
-define(HTTP_HEAD,       'HEAD').
-define(SERVER_HEADER,   {<<"Server">>,<<"LeoFS">>}).
-define(QUERY_PREFIX,    <<"prefix">>).
-define(QUERY_DELIMITER, "delimiter").
-define(QUERY_MAX_KEYS,  "max-keys").
-define(STR_SLASH,       "/").

-define(ERR_TYPE_INTERNAL_ERROR, internal_server_error).

-record(req_params, {
          token_length      :: integer(),
          min_layers        :: integer(),
          max_layers        :: integer(),
          is_dir = false    :: boolean(),
          qs_prefix         :: string(),
          has_inner_cache   :: boolean()
         }).

-record(cache, {
          etag         = 0    :: integer(), % actual value is checksum
          mtime        = 0    :: integer(), % gregorian_seconds
          content_type = ""   :: list(),    % from a Content-Type header
          body         = <<>> :: binary()
         }).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% start web-server.
%%
-spec(start(list()) ->
             ok).
start(Options) ->
    {_DocRoot, Options1} = get_option(docroot, Options),
    {PoolSize, Options2} = get_option(acceptor_pool_size, Options1),
    HookModules = proplists:get_value(hook_modules, Options2),

    HasInnerCache = HookModules =:= undefined,
    case HasInnerCache of
        true ->
            application:start(ecache_app);
        _ ->
            void
    end,
    LayerOfDirs = ?env_layer_of_dirs(),

    application:start(cowboy),
    Dispatch = [
                {'_', [
                       {'_', ?MODULE, [LayerOfDirs, HasInnerCache]}
                      ]}
               ],
    cowboy:start_listener(leo_gateway_http_listener, PoolSize,
                          cowboy_tcp_transport, Options2,
                          cowboy_http_protocol, [{dispatch, Dispatch}]
                         ).

-spec(stop() ->
             ok).
stop() ->
    cowboy:stop_listener(leo_gateway_http_listener).

%% cowboy http handlers
init({_Any, http}, Req, Opts) ->
    {ok, Req, Opts}.

handle(Req, [{NumOfMinLayers, NumOfMaxLayers}, HasInnerCache] = State) ->
    {SplitHost, _Req} = cowboy_http_req:host(Req),
    case SplitHost of
        [H|T] when length(T) =:= 0 ->
            ?error("loop/3", "invalid hostname due to lack of a subdomain, host:~p~n", [H]),
            {ok, Req2} = cowboy_http_req:reply(500, [?SERVER_HEADER], Req),
            {ok, Req2, State};
        [Sub|_T] ->
            {RawPath, _Req} = cowboy_http_req:raw_path(Req),
            BinPath = <<Sub/binary, RawPath/binary>>,
            Path = erlang:binary_to_list(BinPath),
            io:format("path:~w~n",[Path]),
            handle(Req, [{NumOfMinLayers, NumOfMaxLayers}, HasInnerCache], Path)
    end.

handle(Req0, [{NumOfMinLayers, NumOfMaxLayers}, HasInnerCache] = State, Path) ->
    Length = erlang:length(string:tokens(Path, ?STR_SLASH)),
    {HTTPMethod, Req1} = case cowboy_http_req:method(Req0) of
                             {?HTTP_POST, _Req1} -> {?HTTP_PUT, _Req1};
                             {Other, _Req1}      -> {Other,     _Req1}
                         end,

    {Prefix, IsDir, Req2} =
        case cowboy_http_req:qs_val(?QUERY_PREFIX, Req1) of
            {undefined, _Req2} ->
                {undefined, false, _Req2};
            {Param, _Req2} ->
                {binary_to_list(Param), true, _Req2}
        end,

    case catch exec(first, HTTPMethod, Req2, Path, #req_params{
                                               token_length     = Length,
                                               min_layers       = NumOfMinLayers,
                                               max_layers       = NumOfMaxLayers,
                                               has_inner_cache  = HasInnerCache,
                                               is_dir           = IsDir,
                                               qs_prefix        = Prefix
                                              }) of
        {'EXIT', Reason} ->
            ?error("loop/3", "path:~w, reason:~w", [Path, Reason]),
            {ok, Req3} = cowboy_http_req:reply(500, [?SERVER_HEADER], Req2),
            {ok, Req3, State};
        {ok, Req4} ->
            erlang:garbage_collect(self()),
            {ok, Req4, State}
    end.

terminate(_Req, _State) ->
    ok.

%%--------------------------------------------------------------------
%%% INTERNAL FUNCTIONS
%%--------------------------------------------------------------------

%% @doc constraint violation.
%%
exec(first, _HTTPMethod, Req,_Key, #req_params{token_length = Len,
                                               min_layers   = Min,
                                               max_layers   = Max}) when Len < (Min - 1);
                                                                         Len > Max ->
    cowboy_http_req:reply(404, [?SERVER_HEADER], Req);

%% @doc operations on buckets.
%%
exec(first, ?HTTP_GET, Req, Key,
     #req_params{token_length = _TokenLen,
                 is_dir       = true,
                 qs_prefix    = Prefix
                }) ->
    case leo_s3_http_bucket:get_bucket_list(Key, none, none, 1000, Prefix) of
        {ok, Meta, XML} when is_list(Meta) == true ->
            cowboy_http_req:reply(200, [?SERVER_HEADER], XML, Req);
        {error, not_found} ->
            cowboy_http_req:reply(404, [?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            cowboy_http_req:reply(500, [?SERVER_HEADER], Req);
        {error, timeout} ->
            cowboy_http_req:reply(504, [?SERVER_HEADER], Req)
    end;

%% @doc GET operation on Object if inner cache is enabled.
%%
exec(first, ?HTTP_GET = HTTPMethod, Req, Key, #req_params{is_dir = false, has_inner_cache = true} = Params) ->
    case ecache_server:get(Key) of
        undefined ->
            exec(first, HTTPMethod, Req, Key, Params#req_params{has_inner_cache = false});
        BinCached ->
            Cached = binary_to_term(BinCached),
            exec(next, HTTPMethod, Req, Key, Params, Cached)
    end;

%% @doc GET operation on Object.
%%
exec(first, ?HTTP_GET, Req, Key, #req_params{is_dir = false}) ->
    case leo_gateway_rpc_handler:get(Key) of
        {ok, Meta, RespObject} ->
            cowboy_http_req:reply(200,
                                  [?SERVER_HEADER,
                                   {<<"Content-Type">>,  mochiweb_util:guess_mime(Key)},
                                   {<<"ETag">>,          leo_hex:integer_to_hex(Meta#metadata.checksum)},
                                   {<<"Last-Modified">>, rfc1123_date(Meta#metadata.timestamp)}],
                                  RespObject, Req);
        {error, not_found} ->
            cowboy_http_req:reply(404, [?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            cowboy_http_req:reply(500, [?SERVER_HEADER], Req);
        {error, timeout} ->
            cowboy_http_req:reply(504, [?SERVER_HEADER], Req)
    end;

%% @doc HEAD operation on Object.
%%
exec(first, ?HTTP_HEAD, Req, Key, #req_params{is_dir = false}) ->
    case leo_gateway_rpc_handler:head(Key) of
        {ok, #metadata{del = 0} = Meta} ->
            cowboy_http_req:reply(200,
                                  [?SERVER_HEADER,
                                   {<<"Content-Type">>,   mochiweb_util:guess_mime(Key)},
                                   {<<"ETag">>,           leo_hex:integer_to_hex(Meta#metadata.checksum)},
                                   {<<"Content-Length">>, Meta#metadata.dsize},
                                   {<<"Last-Modified">>,  rfc1123_date(Meta#metadata.timestamp)}],
                                  Req);
        {ok, #metadata{del = 1}} ->
            cowboy_http_req:reply(404, [?SERVER_HEADER], Req);
        {error, not_found} ->
            cowboy_http_req:reply(404, [?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            cowboy_http_req:reply(500, [?SERVER_HEADER], Req);
        {error, timeout} ->
            cowboy_http_req:reply(504, [?SERVER_HEADER], Req)
    end;

%% @doc DELETE operation on Object.
%%
exec(first, ?HTTP_DELETE, Req, Key, #req_params{is_dir = false}) ->
    case leo_gateway_rpc_handler:delete(Key) of
        ok ->
            cowboy_http_req:reply(204, [?SERVER_HEADER], Req);
        {error, not_found} ->
            cowboy_http_req:reply(404, [?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            cowboy_http_req:reply(500, [?SERVER_HEADER], Req);
        {error, timeout} ->
            cowboy_http_req:reply(504, [?SERVER_HEADER], Req)
    end;

%% @doc POST/PUT operation on Objects.
%%
exec(first, ?HTTP_PUT, Req, Key, #req_params{is_dir = false}) ->
    {Has, _Req} = cowboy_http_req:has_body(Req),
    {ok, Bin, Req2} = case Has of
                          %% for support uploading zero byte files.
                          false -> {ok, <<>>, Req};
                          true -> cowboy_http_req:body(Req)
                      end,
    {Size, Req3} = cowboy_http_req:body_length(Req2),
    case leo_gateway_rpc_handler:put(Key, Bin, Size) of
        ok ->
            cowboy_http_req:reply(200, [?SERVER_HEADER], Req3);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            cowboy_http_req:reply(500, [?SERVER_HEADER], Req3);
        {error, timeout} ->
            cowboy_http_req:reply(504, [?SERVER_HEADER], Req3)
    end;

%% @doc invalid request.
%%
exec(first, _, Req, _, _) ->
    cowboy_http_req:reply(400, [?SERVER_HEADER], Req).

%% @doc GET operation with Etag
%%
exec(next, ?HTTP_GET, Req, Key, #req_params{is_dir = false, has_inner_cache = true}, Cached) ->
    case leo_gateway_rpc_handler:get(Key, Cached#cache.etag) of
        {ok, match} ->
            cowboy_http_req:reply(200,
                                  [?SERVER_HEADER,
                                   {<<"Content-Type">>,  Cached#cache.content_type},
                                   {<<"ETag">>,          leo_hex:integer_to_hex(Cached#cache.etag)},
                                   {<<"X-From-Cache">>,  <<"True">>},
                                   {<<"Last-Modified">>, rfc1123_date(Cached#cache.mtime)}],
                                  Cached#cache.body, Req);
        {ok, Meta, RespObject} ->
            Mime = mochiweb_util:guess_mime(Key),
            BinVal = term_to_binary(#cache{
                                       etag = Meta#metadata.checksum,
                                       mtime = Meta#metadata.timestamp,
                                       content_type = Mime,
                                       body = RespObject
                                      }),
            ecache_server:set(Key, BinVal),
            cowboy_http_req:reply(200,
                                  [?SERVER_HEADER,
                                   {<<"Content-Type">>,  Mime},
                                   {<<"ETag">>,          leo_hex:integer_to_hex(Meta#metadata.checksum)},
                                   {<<"Last-Modified">>, rfc1123_date(Meta#metadata.timestamp)}],
                                  RespObject, Req);
        {error, not_found} ->
            cowboy_http_req:reply(404, [?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            cowboy_http_req:reply(500, [?SERVER_HEADER], Req);
        {error, timeout} ->
            cowboy_http_req:reply(504, [?SERVER_HEADER], Req)
    end.

%% @doc get options from mochiweb.
%%
-spec(get_option(atom(), list()) ->
             {any(), any()}).
get_option(Option, Options) ->
    {proplists:get_value(Option, Options),
     proplists:delete(Option, Options)}.

%% @doc RFC-1123 datetime.
%%
-spec(rfc1123_date(integer()) ->
             string()).
rfc1123_date(Date) ->
    httpd_util:rfc1123_date(
      calendar:universal_time_to_local_time(
        calendar:gregorian_seconds_to_datetime(Date))).

