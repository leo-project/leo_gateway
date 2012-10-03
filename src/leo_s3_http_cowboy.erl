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
-include("leo_s3_http.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_s3_libs/include/leo_s3_auth.hrl").
-include_lib("eunit/include/eunit.hrl").

-undef(SERVER_HEADER).
-define(SERVER_HEADER, {<<"Server">>,<<"LeoFS">>}).
-define(SSL_PROC_NAME, list_to_atom(lists:append([?MODULE_STRING, "_ssl"]))).

-record(cache_condition, {
          expire                = 0  :: integer(), %% specified per sec
          max_content_len       = 0  :: integer(), %% No cache if Content-Length of a response header was &gt this
          content_types         = [] :: list(),    %% like ["image/png", "image/gif", "image/jpeg"]
          path_patterns         = [] :: list()     %% compiled regular expressions
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
    {SSLPort,  Options2} = get_option(ssl_port,     Options1),
    {SSLCert,  Options3} = get_option(ssl_certfile, Options2),
    {SSLKey,   Options4} = get_option(ssl_keyfile,  Options3),
    {PoolSize, Options5} = get_option(acceptor_pool_size, Options4),
    HookModules = leo_misc:get_value(hook_modules, Options5),
    UseAuth     = leo_misc:get_value(use_auth, Options5, true),

    LayerOfDirs = ?env_layer_of_dirs(),
    HasInnerCache = HookModules =:= undefined,
    Dispatch = [
                {'_', [
                       {'_', ?MODULE, [LayerOfDirs, HasInnerCache, UseAuth]}
                      ]}
               ],
    HttpOpts = case HasInnerCache of
                   true ->
                       %% using inner cache
                       [{dispatch, Dispatch}];
                   _ ->
                       CacheCondition = #cache_condition{
                         expire          = proplists:get_value(expire, Options5, 300),
                         max_content_len = proplists:get_value(max_content_len, Options5, 1024 * 1024),
                         content_types   = proplists:get_value(cachable_content_type, Options5, []),
                         path_patterns   = lists:foldl(
                                             fun(P, Acc) ->
                                                     case re:compile(P) of
                                                         {ok, MP} ->
                                                             [MP|Acc];
                                                         _Error ->
                                                             Acc
                                                     end
                                             end, [],
                                             proplists:get_value(cachable_path_pattern, Options5, []))
                        },
                       [{dispatch,   Dispatch},
                        {onrequest,  onrequest(CacheCondition)},
                        {onresponse, onresponse(CacheCondition)}]
               end,

    application:start(ecache),
    application:start(cowboy),

    cowboy:start_listener(?MODULE, PoolSize,
                          cowboy_tcp_transport, Options5,
                          cowboy_http_protocol, HttpOpts),
    cowboy:start_listener(?SSL_PROC_NAME, PoolSize,
                          cowboy_ssl_transport, [{port, SSLPort},
                                                 {certfile, SSLCert},
                                                 {keyfile, SSLKey}],
                          cowboy_http_protocol, HttpOpts).


%% @doc
-spec(stop() ->
             ok).
stop() ->
    cowboy:stop_listener(?MODULE),
    cowboy:stop_listener(?SSL_PROC_NAME).


%% @doc Initializer
init({_Any, http}, Req, Opts) ->
    {ok, Req, Opts}.


%% @doc
handle(Req, State) ->
    Key = gen_key(Req),
    handle(Req, State, Key).

handle(Req, [{NumOfMinLayers, NumOfMaxLayers}, HasInnerCache, UseAuth] = State, Path) ->

    HTTPMethod = case cowboy_http_req:method(Req) of
                     {?HTTP_POST, _} -> ?HTTP_PUT;
                     {Other, _}      -> Other
                 end,

    {Prefix, IsDir, Path2, Req2} = case cowboy_http_req:qs_val(<<"prefix">>, Req) of
                                       {undefined, Req1} ->
                                           HasTermSlash = case string:right(Path, 1) of
                                                              ?STR_SLASH -> true;
                                                              _Else      -> false
                                                          end,
                                           {none, HasTermSlash, Path, Req1};
                                       {BinParam, Req1} ->
                                           NewPath = case string:right(Path, 1) of
                                                         ?STR_SLASH -> Path;
                                                         _Else      -> Path ++ ?STR_SLASH
                                                     end,
                                           {binary_to_list(BinParam), true, NewPath, Req1}
                                   end,
    TokenLen = erlang:length(string:tokens(Path2, ?STR_SLASH)),

    case cowboy_http_req:qs_val(<<"acl">>, Req2) of
        {undefined, _} ->
            case catch auth(UseAuth, Req2, HTTPMethod, Path2, TokenLen) of
                {error, _Cause} ->
                    {ok, Req3} = cowboy_http_req:reply(403, [?SERVER_HEADER], Req2),
                    {ok, Req3, State};
                {ok, AccessKeyId} ->
                    {RangeHeader, _} = cowboy_http_req:header('Range', Req2),
                    case catch exec1(HTTPMethod, Req2, Path2,
                                     #req_params{token_length     = TokenLen,
                                                 access_key_id    = AccessKeyId,
                                                 min_layers       = NumOfMinLayers,
                                                 max_layers       = NumOfMaxLayers,
                                                 has_inner_cache  = HasInnerCache,
                                                 range_header     = RangeHeader,
                                                 is_dir           = IsDir,
                                                 is_cached        = true,
                                                 qs_prefix        = Prefix}) of
                        {'EXIT', Reason} ->
                            ?error("loop1/4", "path:~p, reason:~p", [Path2, Reason]),
                            {ok, Req3} = cowboy_http_req:reply(500, [?SERVER_HEADER], Req2),
                            {ok, Req3, State};
                        {ok, Req3} ->
                            Req4 = cowboy_http_req:compact(Req3),
                                                %erlang:garbage_collect(self()),
                            {ok, Req4, State}
                    end;
                {'EXIT', Reason} ->
                    ?error("loop1/4", "path:~w, reason:~p", [Path2, Reason]),
                    {ok, Req3} = cowboy_http_req:reply(500, [?SERVER_HEADER], Req2),
                    {ok, Req3, State}
            end;
        _ ->
            {ok, Req3} = cowboy_http_req:reply(404, [?SERVER_HEADER], Req2),
            {ok, Req3, State}
    end.


%% @doc Terminater
terminate(_Req, _State) ->
    ok.


%%--------------------------------------------------------------------
%% Callbacks
%%--------------------------------------------------------------------
%% @doc
onrequest(#cache_condition{expire = Expire}) ->
    fun(Req) ->
            {Method, _} = cowboy_http_req:method(Req),
            onrequest_fun1(Method, Req, Expire)
    end.


%% @doc
%% @private
onrequest_fun1(?HTTP_GET, Req, Expire) ->
    Key = gen_key(Req),
    Ret = ecache_server:get(Key),
    onrequest_fun2(Req, Expire, Key, Ret);
onrequest_fun1(_, Req, _) ->
    Req.


%% @doc
%% @private
onrequest_fun2(Req, _, _, not_found) ->
    Req;
onrequest_fun2(Req, Expire, Key, {ok, CachedObj}) ->
    #cache{mtime        = MTime,
           content_type = ContentType,
           etag         = Checksum,
           body         = Body} = binary_to_term(CachedObj),

    Now = leo_date:now(),
    Diff = Now - MTime,

    case (Diff > Expire) of
        true ->
            _ = ecache_server:delete(Key),
            Req;
        false ->
            LastModified = leo_http:rfc1123_date(MTime),
            Date  = leo_http:rfc1123_date(Now),
            Heads = [?SERVER_HEADER,
                     {<<"Last-Modified">>, LastModified},
                     {<<"Content-Type">>,  ContentType},
                     {<<"Date">>,          Date},
                     {<<"Age">>,           integer_to_list(Diff)},
                     {<<"Etag">>,          integer_to_list(Checksum, 16)},
                     {<<"Cache-Control">>, "max-age=" ++ integer_to_list(Expire)}],

            IMSSec = case cowboy_http_req:parse_header('If-Modified-Since', Req) of
                         {undefined, _} ->
                             0;
                         {IMSDateTime, _} ->
                             calendar:datetime_to_gregorian_seconds(IMSDateTime)
                     end,
            case IMSSec of
                MTime ->
                    {ok, Req2} = cowboy_http_req:reply(304, Heads, Req),
                    Req2;
                _ ->
                    {ok, Req2} = cowboy_http_req:set_resp_body(Body, Req),
                    {ok, Req3} = cowboy_http_req:reply(200, Heads, Req2),
                    Req3
            end
    end.


%% @doc
%% @private
onresponse(#cache_condition{expire = Expire} = Config) ->

    FilterFuns = [fun is_cachable_req1/5,
                  fun is_cachable_req2/5,
                  fun is_cachable_req3/5],
    fun(Status, Headers, Req) ->
            Key = gen_key(Req),
            case lists:all(fun(Fun) -> Fun(Key, Config, Status, Headers, Req) end, FilterFuns) of
                true ->
                    DateSec = leo_date:now(),
                    ContentType = case lists:keyfind(<<"Content-Type">>, 1, Headers) of
                                      false ->
                                          "application/octet-stream";
                                      {_, Val} ->
                                          Val
                                  end,
                    {ok, Body, _} = cowboy_http_req:get_resp_body(Req),
                    Bin = term_to_binary(
                            #cache{mtime        = DateSec,
                                   etag         = leo_hex:binary_to_integer(erlang:md5(Body)),
                                   content_type = ContentType,
                                   body         = Body}),

                    _ = ecache_server:put(Key, Bin),

                    Headers2 = lists:keydelete(<<"Last-Modified">>, 1, Headers),
                    Headers3 = [{<<"Cache-Control">>, "max-age=" ++ integer_to_list(Expire)},
                                {<<"Last-Modified">>, leo_http:rfc1123_date(DateSec)}
                                |Headers2],
                    {ok, Req2} = cowboy_http_req:reply(200, Headers3, Req),
                    Req2;
                false -> Req
            end
    end.


%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------
%% @doc
%% @private
is_cachable_path(Path) ->
    fun(MP) ->
            case re:run(Path, MP) of
                {match, _Cap} ->
                    true;
                _Else ->
                    false
            end
    end.


%% @doc
%% @private
is_cachable_req1(_Key, #cache_condition{max_content_len = MaxLen}, Status, Headers, Req) ->
    {Method, _} = cowboy_http_req:method(Req),
    {ok, Body, _} = cowboy_http_req:get_resp_body(Req),
    HasNOTCacheControl = case lists:keyfind(<<"Cache-Control">>, 1, Headers) of
                             false ->
                                 true;
                             _ ->
                                 false
                         end,
    Status =:= 200 andalso
        Method =:= 'GET' andalso
        is_binary(Body) andalso
        size(Body) > 0 andalso
        size(Body) < MaxLen andalso
        HasNOTCacheControl.


%% @doc
%% @private
is_cachable_req2(Key, #cache_condition{path_patterns = PPs}, _Status, _Headers, _Req)
  when is_list(PPs) andalso length(PPs) > 0 ->
    lists:any(is_cachable_path(Key), PPs);
is_cachable_req2(_, _, _Status, _Headers, _Req) ->
    true.


%% @doc
%% @private
is_cachable_req3(_Key, #cache_condition{content_types = CTs}, _Status, Headers, _Req)
  when is_list(CTs) andalso length(CTs) > 0 ->
    case lists:keyfind(<<"Content-Type">>, 1, Headers) of
        false ->
            false;
        {_, ContentType} ->
            lists:member(ContentType, CTs)
    end;
is_cachable_req3(_, _, _Status, _Headers, _Req) ->
    true.


%% @doc
%% @private
gen_key(Req) ->
    EndPoints1 = case leo_s3_endpoint:get_endpoints() of
                     {ok, EndPoints0} ->
                         lists:map(fun({endpoint,EP,_}) -> EP end, EndPoints0);
                     _ -> []
                 end,
    {BinHost, _} = cowboy_http_req:raw_host(Req),
    {BinRawPath, _} = cowboy_http_req:raw_path(Req),
    BinPath = cowboy_http:urldecode(BinRawPath),
    leo_http:key(EndPoints1, binary_to_list(BinHost), binary_to_list(BinPath)).


%% @doc constraint violation.
%% @private
exec1(_HTTPMethod, Req,_Key, #req_params{token_length = Len,
                                         max_layers   = Max}) when Len > Max ->
    cowboy_http_req:reply(404, [?SERVER_HEADER], Req);

%% @doc GET operation on buckets & Dirs.
%% @private
exec1(?HTTP_GET, Req, Key, #req_params{is_dir        = true,
                                       access_key_id = AccessKeyId,
                                       qs_prefix     = Prefix}) ->
    case leo_s3_http_bucket:get_bucket_list(AccessKeyId, Key, none, none, 1000, Prefix) of
        {ok, Meta, XML} when is_list(Meta) == true ->
            {ok, Req2} = cowboy_http_req:set_resp_body(XML, Req),
            cowboy_http_req:reply(200, [?SERVER_HEADER,
                                        {<<"Content-Type">>, "application/xml"},
                                        {<<"Date">>, leo_http:rfc1123_date(leo_date:now())}
                                       ], Req2);
        {error, not_found} ->
            cowboy_http_req:reply(404, [?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            cowboy_http_req:reply(500, [?SERVER_HEADER], Req);
        {error, timeout} ->
            cowboy_http_req:reply(504, [?SERVER_HEADER], Req)
    end;

%% @doc PUT operation on buckets.
%% @private
exec1(?HTTP_PUT, Req, Key, #req_params{token_length  = 1,
                                       access_key_id = AccessKeyId}) ->
    [Bucket|_] = string:tokens(Key, "/"),
    case leo_s3_http_bucket:put_bucket(AccessKeyId, Bucket) of
        ok ->
            cowboy_http_req:reply(200, [?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            cowboy_http_req:reply(500, [?SERVER_HEADER], Req);
        {error, timeout} ->
            cowboy_http_req:reply(504, [?SERVER_HEADER], Req)
    end;

%% @doc DELETE operation on buckets.
%% @private
exec1(?HTTP_DELETE, Req, Key, #req_params{token_length  = 1,
                                          access_key_id = AccessKeyId}) ->
    case leo_s3_http_bucket:delete_bucket(AccessKeyId, Key) of
        ok ->
            cowboy_http_req:reply(204, [?SERVER_HEADER], Req);
        not_found ->
            cowboy_http_req:reply(404, [?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            cowboy_http_req:reply(500, [?SERVER_HEADER], Req);
        {error, timeout} ->
            cowboy_http_req:reply(504, [?SERVER_HEADER], Req)
    end;

%% @doc HEAD operation on buckets.
%% @private
exec1(?HTTP_HEAD, Req, Key, #req_params{token_length  = 1,
                                        access_key_id = AccessKeyId}) ->
    case leo_s3_http_bucket:head_bucket(AccessKeyId, Key) of
        ok ->
            cowboy_http_req:reply(200, [?SERVER_HEADER], Req);
        not_found ->
            cowboy_http_req:reply(404, [?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            cowboy_http_req:reply(500, [?SERVER_HEADER], Req);
        {error, timeout} ->
            cowboy_http_req:reply(504, [?SERVER_HEADER], Req)
    end;

%% @doc GET operation on Object with Range Header.
%% @private
exec1(?HTTP_GET, Req, Key, #req_params{is_dir       = false,
                                       range_header = RangeHeader}) when RangeHeader =/= undefined ->
    [_,ByteRangeSpec|_] = string:tokens(binary_to_list(RangeHeader), "="),
    ByteRangeSet = string:tokens(ByteRangeSpec, "-"),
    {Start, End} = case length(ByteRangeSet) of
                       1 ->
                           [StartStr|_] = ByteRangeSet,
                           {list_to_integer(StartStr), 0};
                       2 ->
                           [StartStr,EndStr|_] = ByteRangeSet,
                           {list_to_integer(StartStr), list_to_integer(EndStr) + 1};
                       _ ->
                           {undefined, undefined}
                   end,
    case Start of
        undefined ->
            cowboy_http_req:reply(416, [?SERVER_HEADER], Req);
        _ ->
            case leo_gateway_rpc_handler:get(Key, Start, End) of
                {ok, _Meta, RespObject} ->
                    Mime = mochiweb_util:guess_mime(Key),
                    {ok, Req2} = cowboy_http_req:set_resp_body(RespObject, Req),
                    cowboy_http_req:reply(206,
                                          [?SERVER_HEADER,
                                           {<<"Content-Type">>,  Mime}],
                                          Req2);
                {error, not_found} ->
                    cowboy_http_req:reply(404, [?SERVER_HEADER], Req);
                {error, ?ERR_TYPE_INTERNAL_ERROR} ->
                    cowboy_http_req:reply(500, [?SERVER_HEADER], Req);
                {error, timeout} ->
                    cowboy_http_req:reply(504, [?SERVER_HEADER], Req)
            end
    end;

%% @doc GET operation on Object if inner cache is enabled.
%% @private
exec1(?HTTP_GET = HTTPMethod, Req, Key, #req_params{is_dir = false,
                                                    is_cached = true,
                                                    has_inner_cache = true} = Params) ->
    case ecache_server:get(Key) of
        not_found ->
            exec1(HTTPMethod, Req, Key, Params#req_params{is_cached = false});
        {ok, CachedObj} ->
            Cached = binary_to_term(CachedObj),
            exec2(HTTPMethod, Req, Key, Params, Cached)
    end;

%% @doc GET operation on Object.
%% @private
exec1(?HTTP_GET, Req, Key, #req_params{is_dir = false, has_inner_cache = HasInnerCache}) ->
    case leo_gateway_rpc_handler:get(Key) of
        {ok, Meta, RespObject} ->
            Mime = mochiweb_util:guess_mime(Key),

            case HasInnerCache of
                true ->
                    BinVal = term_to_binary(#cache{etag = Meta#metadata.checksum,
                                                   mtime = Meta#metadata.timestamp,
                                                   content_type = Mime,
                                                   body = RespObject}),
                    ecache_server:put(Key, BinVal);
                _ ->
                    ok
            end,
            {ok, Req2} = cowboy_http_req:set_resp_body(RespObject, Req),
            cowboy_http_req:reply(200,
                                  [?SERVER_HEADER,
                                   {<<"Content-Type">>,  Mime},
                                   {<<"Etag">>,          erlang:integer_to_list(Meta#metadata.checksum, 16)},
                                   {<<"Last-Modified">>, leo_http:rfc1123_date(Meta#metadata.timestamp)}],
                                  Req2);
        {error, not_found} ->
            cowboy_http_req:reply(404, [?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            cowboy_http_req:reply(500, [?SERVER_HEADER], Req);
        {error, timeout} ->
            cowboy_http_req:reply(504, [?SERVER_HEADER], Req)
    end;

%% @doc HEAD operation on Object.
%% @private
exec1(?HTTP_HEAD, Req, Key, _Params) ->
    case leo_gateway_rpc_handler:head(Key) of
        {ok, #metadata{del = 0} = Meta} ->
            TimeStamp = leo_http:rfc1123_date(Meta#metadata.timestamp),
            Headers   = [?SERVER_HEADER,
                         {<<"Content-Type">>,   mochiweb_util:guess_mime(Key)},
                         {<<"Etag">>,           erlang:integer_to_list(Meta#metadata.checksum, 16)},
                         {<<"Content-Length">>, erlang:integer_to_list(Meta#metadata.dsize)},
                         {<<"Last-Modified">>,  TimeStamp}],
            cowboy_http_req:reply(200, Headers, Req);
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
%% @private
exec1(?HTTP_DELETE, Req, Key, _Params) ->
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
%% @private
exec1(?HTTP_PUT, Req, Key, Params) ->
    do_put1(get_header(Req, <<"X-Amz-Metadata-Directive">>), Req, Key, Params);

%% @doc invalid request.
%% @private
exec1(_, Req, _, _) ->
    cowboy_http_req:reply(400, [?SERVER_HEADER], Req).


%% @doc GET operation with Etag
%% @private
exec2(?HTTP_GET, Req, Key, #req_params{is_dir = false, has_inner_cache = true}, Cached) ->
    case leo_gateway_rpc_handler:get(Key, Cached#cache.etag) of
        {ok, match} ->
            {ok, Req2} = cowboy_http_req:set_resp_body(Cached#cache.body, Req),
            cowboy_http_req:reply(200,
                                  [?SERVER_HEADER,
                                   {<<"Content-Type">>,  Cached#cache.content_type},
                                   {<<"Etag">>,          erlang:integer_to_list(Cached#cache.etag, 16)},
                                   {<<"Last-Modified">>, leo_http:rfc1123_date(Cached#cache.mtime)},
                                   {<<"X-From-Cache">>, <<"True">>}],
                                  Req2);
        {ok, Meta, RespObject} ->
            Mime = mochiweb_util:guess_mime(Key),
            BinVal = term_to_binary(#cache{etag = Meta#metadata.checksum,
                                           mtime = Meta#metadata.timestamp,
                                           content_type = Mime,
                                           body = RespObject}),

            _ = ecache_server:put(Key, BinVal),

            {ok, Req2} = cowboy_http_req:set_resp_body(RespObject, Req),
            cowboy_http_req:reply(200,
                                  [?SERVER_HEADER,
                                   {<<"Content-Type">>,  Mime},
                                   {<<"Etag">>,          erlang:integer_to_list(Meta#metadata.checksum, 16)},
                                   {<<"Last-Modified">>, leo_http:rfc1123_date(Meta#metadata.timestamp)}],
                                  Req2);
        {error, not_found} ->
            cowboy_http_req:reply(404, [?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            cowboy_http_req:reply(500, [?SERVER_HEADER], Req);
        {error, timeout} ->
            cowboy_http_req:reply(504, [?SERVER_HEADER], Req)
    end.


%% @doc POST/PUT operation on Objects. NORMAL
%% @private
do_put1("", Req, Key, _Params) ->
    {Has, _Req} = cowboy_http_req:has_body(Req),
    {ok, Bin, Req2} = case Has of
                          %% for support uploading zero byte files.
                          false -> {ok, <<>>, Req};
                          %% Cowboy handle a `Expect` header automatically
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

%% @doc POST/PUT operation on Objects. COPY/REPLACE
%% @private
do_put1(Directive, Req, Key, _Params) ->
    CS = get_header(Req, <<"X-Amz-Copy-Source">>),
                                                % need to trim head '/' when cooperating with s3fs(-c)
    CS2 = case string:left(CS, 1) of
              "/" ->
                  [_H|Rest] = CS,
                  Rest;
              _ ->
                  CS
          end,
    case leo_gateway_rpc_handler:get(CS2) of
        {ok, Meta, RespObject} ->
            do_put2(Directive, Req, Key, Meta, RespObject);
        {error, not_found} ->
            cowboy_http_req:reply(404, [?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            cowboy_http_req:reply(500, [?SERVER_HEADER], Req);
        {error, timeout} ->
            cowboy_http_req:reply(504, [?SERVER_HEADER], Req)
    end.

%% @doc POST/PUT operation on Objects. COPY
%% @private
do_put2(Directive, Req, Key, Meta, Bin) ->
    Size = size(Bin),
    case {Directive, leo_gateway_rpc_handler:put(Key, Bin, Size)} of
        {?HTTP_HEAD_X_AMZ_META_DIRECTIVE_COPY, ok} ->
            resp_copyobj_xml(Req, Meta);
        {?HTTP_HEAD_X_AMZ_META_DIRECTIVE_REPLACE, ok} ->
            do_put3(Req, Key, Meta);
        {_, {error, ?ERR_TYPE_INTERNAL_ERROR}} ->
            cowboy_http_req:reply(500, [?SERVER_HEADER], Req);
        {_, {error, timeout}} ->
            cowboy_http_req:reply(504, [?SERVER_HEADER], Req)
    end.


%% @doc POST/PUT operation on Objects. REPLACE
%% @private
do_put3(Req, Key, Meta) when Key == Meta#metadata.key ->
    resp_copyobj_xml(Req, Meta);
do_put3(Req, _Key, Meta) ->
    case leo_gateway_rpc_handler:delete(Meta#metadata.key) of
        ok ->
            resp_copyobj_xml(Req, Meta);
        {error, not_found} ->
            resp_copyobj_xml(Req, Meta);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            cowboy_http_req:reply(500, [?SERVER_HEADER], Req);
        {error, timeout} ->
            cowboy_http_req:reply(504, [?SERVER_HEADER], Req)
    end.


%% @doc getter helper function. return "" if specified header is undefined
%% @private
get_header(Req, Key) ->
    case cowboy_http_req:header(Key, Req) of
        {undefined, _} ->
            "";
        {Val, _} ->
            binary_to_list(Val)
    end.


%% @doc POST/PUT operation on Objects. REPLACE
%% @private
resp_copyobj_xml(Req, Meta) ->
    XML = io_lib:format(?XML_COPY_OBJ_RESULT,
                        [leo_http:web_date(Meta#metadata.timestamp),
                         erlang:integer_to_list(Meta#metadata.checksum, 16)]),
    {ok, Req2} = cowboy_http_req:set_resp_body(XML, Req),
    cowboy_http_req:reply(200, [?SERVER_HEADER,
                                {<<"Content-Type">>, "application/xml"},
                                {<<"Date">>,         leo_http:rfc1123_date(leo_date:now())}
                               ], Req2).


%% @doc Authentication
%% @private
auth(false, _Req, _HTTPMethod, _Path, _TokenLen) ->
    {ok, []};
auth(true, Req, HTTPMethod, Path, TokenLen) when (TokenLen =< 1) orelse
                                                 (TokenLen > 1 andalso
                                                                 (HTTPMethod == ?HTTP_PUT orelse
                                                                  HTTPMethod == ?HTTP_DELETE)) ->
    %% bucket operations must be needed to auth
    %% AND alter object operations as well
    case cowboy_http_req:header('Authorization', Req) of
        {undefined, _} ->
            {error, undefined};
        {BinAuthorization, _} ->
            Bucket = case TokenLen >= 1 of
                         true  -> hd(string:tokens(Path, ?STR_SLASH));
                         false -> []
                     end,

            IsCreateBucketOp = (TokenLen == 1 andalso HTTPMethod == ?HTTP_PUT),
            {BinRawUri, _} = cowboy_http_req:raw_path(Req),
            {BinQueryString, _} = cowboy_http_req:raw_qs(Req),
            {Headers, _} = cowboy_http_req:headers(Req),
            SignParams = #sign_params{http_verb    = HTTPMethod,
                                      content_md5  = get_header(Req, 'Content-MD5'),
                                      content_type = get_header(Req, 'Content-Type'),
                                      date         = get_header(Req, 'Date'),
                                      bucket       = Bucket,
                                      uri          = binary_to_list(BinRawUri),
                                      query_str    = binary_to_list(BinQueryString),
                                      amz_headers  = leo_http:get_amz_headers4cow(Headers)
                                     },
            leo_s3_auth:authenticate(binary_to_list(BinAuthorization), SignParams, IsCreateBucketOp)
    end;

auth(true, _Req, _HTTPMethod, _Path, _TokenLen) ->
    {ok, []}.


%% @doc get options from mochiweb.
%% @private
-spec(get_option(atom(), list()) ->
             {any(), any()}).
get_option(Option, Options) ->
    {leo_misc:get_value(Option, Options),
     lists:keydelete(Option, 1, Options)}.

