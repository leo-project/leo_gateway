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
%% Leo S3 HTTP - Mochiweb
%% @doc
%% @end
%%======================================================================
-module(leo_s3_http_mochi).

-author('Yosuke Hara').
-author('Yoshiyuki Kanno').

-export([start/1, stop/0, loop/3]).

-include("leo_gateway.hrl").
-include("leo_s3_http.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_s3_auth/include/leo_s3_auth.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% Start web-server
%%
-spec(start(list()) ->
             ok).
start(Options) ->
    {_DocRoot, Options1} = get_option(docroot,      Options),
    {SSLPort,  Options2} = get_option(ssl_port,     Options1),
    {SSLCert,  Options3} = get_option(ssl_certfile, Options2),
    {SSLKey,   Options4} = get_option(ssl_keyfile,  Options3),
    HookModules = proplists:get_value(hook_modules, Options4),

    HasInnerCache = HookModules =:= undefined,
    case HasInnerCache of
        true ->
            application:start(ecache_app);
        _ ->
            void
    end,
    LayerOfDirs = ?env_layer_of_dirs(),

    Loop = fun (Req) -> ?MODULE:loop(Req, LayerOfDirs, HasInnerCache) end,
    mochiweb_http:start([{name, ?MODULE}, {loop, Loop} | Options4]),
    mochiweb_http:start([{name, ssl_proc_name()},
                         {loop, Loop},
                         {ssl, true},
                         {ssl_opts, [{certfile, SSLCert},
                                     {keyfile,  SSLKey}]},
                         {port, SSLPort}]).


%% @doc Stop web-server
%%
-spec(stop() ->
             ok).
stop() ->
    mochiweb_http:stop(?MODULE),
    mochiweb_http:stop(ssl_proc_name()).

%% @doc ssl process name
ssl_proc_name() ->
    list_to_atom(?MODULE_STRING ++ "_ssl").

%% @doc Handling HTTP-Request/Response
%%
-spec(loop(any(), tuple(), boolean()) ->
             ok).
loop(Req, {NumOfMinLayers, NumOfMaxLayers}, HasInnerCache) ->
    [Host|_] = string:tokens(Req:get_header_value("host"), ":"),
    Key = leo_http:key(Host, Req:get(path)),

    loop1(Req, {NumOfMinLayers, NumOfMaxLayers}, HasInnerCache, Key).


%%--------------------------------------------------------------------
%%% INTERNAL FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Handling HTTP-Request/Response
%% @private
-spec(loop1(any(), tuple(), boolean(), string()) ->
             ok).
loop1(Req, {NumOfMinLayers, NumOfMaxLayers}, HasInnerCache, Path) ->
    HTTPMethod = case Req:get(method) of
                     ?HTTP_POST -> ?HTTP_PUT;
                     Other      -> Other
                 end,

    QueryString = Req:parse_qs(),
    {Prefix, IsDir, Path2} = case proplists:get_value(?QUERY_PREFIX, QueryString, undefined) of
                                 undefined ->
                                     HasTermSlash = case string:right(Path, 1) of
                                                        ?STR_SLASH -> true;
                                                        _Else      -> false
                                                    end,
                                     {none, HasTermSlash, Path};
                                 Param     ->
                                     NewPath = case string:right(Path, 1) of
                                                   ?STR_SLASH -> Path;
                                                   _Else      -> Path ++ ?STR_SLASH
                                               end,
                                     {Param, true, NewPath}
                             end,
    TokenLen = erlang:length(string:tokens(Path2, ?STR_SLASH)),

    case catch auth(Req, HTTPMethod, Path2, TokenLen) of
        {error, _Cause} ->
            Req:respond({403, [?SERVER_HEADER], []});
        {ok, AccessKeyId} ->
            case catch exec1(HTTPMethod, Req, Path2, #req_params{token_length     = TokenLen,
                                                                 access_key_id    = AccessKeyId,
                                                                 min_layers       = NumOfMinLayers,
                                                                 max_layers       = NumOfMaxLayers,
                                                                 has_inner_cache  = HasInnerCache,
                                                                 is_dir           = IsDir,
                                                                 is_cached        = true,
                                                                 qs_prefix        = Prefix}) of
                {'EXIT', Reason} ->
                    ?error("loop1/4", "path:~p, reason:~p", [Path2, Reason]),
                    Req:respond({500, [?SERVER_HEADER], []});
                _ ->
                    erlang:garbage_collect(self())
            end;
        {'EXIT', Reason} ->
            ?error("loop1/4", "path:~w, reason:~p", [Path2, Reason]),
            Req:respond({500, [?SERVER_HEADER], []})
    end.


%% @doc constraint violation.
%%
exec1(_HTTPMethod, Req,_Key, #req_params{token_length = Len,
                                         max_layers   = Max}) when Len > Max ->
    Req:respond({404, [?SERVER_HEADER], []}),
    ok;

%% @doc GET operation on buckets & Dirs.
%%
exec1(?HTTP_GET, Req, Key, #req_params{is_dir        = true,
                                       access_key_id = AccessKeyId,
                                       qs_prefix     = Prefix}) ->
    case leo_s3_http_bucket:get_bucket_list(AccessKeyId, Key, none, none, 1000, Prefix) of
        {ok, Meta, XML} when is_list(Meta) == true ->
            Req:respond({200, [?SERVER_HEADER], XML});
        {error, not_found} ->
            Req:respond({404, [?SERVER_HEADER], []});
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            Req:respond({500, [?SERVER_HEADER], []});
        {error, timeout} ->
            Req:respond({504, [?SERVER_HEADER], []})
    end;

%% @doc PUT operation on buckets.
%%
exec1(?HTTP_PUT, Req, Key, #req_params{token_length  = 1,
                                       access_key_id = AccessKeyId}) ->
    case leo_s3_http_bucket:put_bucket(AccessKeyId, Key) of
        ok ->
            Req:respond({200, [?SERVER_HEADER], []});
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            Req:respond({500, [?SERVER_HEADER], []});
        {error, timeout} ->
            Req:respond({504, [?SERVER_HEADER], []})
    end;

%% @doc DELETE operation on buckets.
%%
exec1(?HTTP_DELETE, Req, Key, #req_params{token_length  = 1,
                                          access_key_id = AccessKeyId}) ->
    case leo_s3_http_bucket:delete_bucket(AccessKeyId, Key) of
        ok ->
            Req:respond({204, [?SERVER_HEADER], []});
        not_found ->
            Req:respond({404, [?SERVER_HEADER], []});
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            Req:respond({500, [?SERVER_HEADER], []});
        {error, timeout} ->
            Req:respond({504, [?SERVER_HEADER], []})
    end;

%% @doc HEAD operation on buckets.
%%
exec1(?HTTP_HEAD, Req, Key, #req_params{token_length  = 1,
                                        access_key_id = AccessKeyId}) ->
    case leo_s3_http_bucket:head_bucket(AccessKeyId, Key) of
        ok ->
            Req:respond({200, [?SERVER_HEADER], []});
        not_found ->
            Req:respond({404, [?SERVER_HEADER], []});
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            Req:respond({500, [?SERVER_HEADER], []});
        {error, timeout} ->
            Req:respond({504, [?SERVER_HEADER], []})
    end;


%% @doc GET operation on Object if inner cache is enabled.
%%
exec1(?HTTP_GET = HTTPMethod, Req, Key, #req_params{is_dir = false,
                                                    is_cached = true,
                                                    has_inner_cache = true} = Params) ->
    case ecache_server:get(Key) of
        undefined ->
            exec1(HTTPMethod, Req, Key, Params#req_params{is_cached = false});
        BinCached ->
            Cached = binary_to_term(BinCached),
            exec2(HTTPMethod, Req, Key, Params, Cached)
    end;

%% @doc GET operation on Object.
%%
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
                    ecache_server:set(Key, BinVal);
                _ ->
                    ok
            end,
            Req:respond({200,
                         [?SERVER_HEADER,
                          {"Content-Type",  Mime},
                          {"ETag",          leo_hex:integer_to_hex(Meta#metadata.checksum)},
                          {"Last-Modified", leo_http:rfc1123_date(Meta#metadata.timestamp)}],
                         RespObject});
        {error, not_found} ->
            Req:respond({404, [?SERVER_HEADER], []});
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            Req:respond({500, [?SERVER_HEADER], []});
        {error, timeout} ->
            Req:respond({504, [?SERVER_HEADER], []})
    end;

%% @doc HEAD operation on Object.
%%
exec1(?HTTP_HEAD, Req, Key, _Params) ->
    case leo_gateway_rpc_handler:head(Key) of
        {ok, #metadata{del = 0} = Meta} ->
            Req:start_response_length({200,
                                       [?SERVER_HEADER,
                                        {"Content-Type",  mochiweb_util:guess_mime(Key)},
                                        {"ETag",          leo_hex:integer_to_hex(Meta#metadata.checksum)},
                                        {"Last-Modified", leo_http:rfc1123_date(Meta#metadata.timestamp)}],
                                       Meta#metadata.dsize});
        {ok, #metadata{del = 1}} ->
            Req:respond({404, [?SERVER_HEADER], []});
        {error, not_found} ->
            Req:respond({404, [?SERVER_HEADER], []});
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            Req:respond({500, [?SERVER_HEADER], []});
        {error, timeout} ->
            Req:respond({504, [?SERVER_HEADER], []})
    end;

%% @doc DELETE operation on Object.
%%
exec1(?HTTP_DELETE, Req, Key, _Params) ->
    case leo_gateway_rpc_handler:delete(Key) of
        ok ->
            Req:respond({204, [?SERVER_HEADER], []});
        {error, not_found} ->
            Req:respond({404, [?SERVER_HEADER], []});
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            Req:respond({500, [?SERVER_HEADER], []});
        {error, timeout} ->
            Req:respond({504, [?SERVER_HEADER], []})
    end;

%% @doc POST/PUT operation on Objects.
%%
exec1(?HTTP_PUT, Req, Key, _Params) ->
    Size  = list_to_integer(Req:get_header_value("content-length")),
    case Req:get_header_value(?HTTP_HEAD_EXPECT) of
        ?HTTP_HEAD_100_CONTINUE ->
            Req:respond({100, [?SERVER_HEADER], []});
        _ ->
            void
    end,

    Bin = case (Size == 0) of
              %% for support uploading zero byte files.
              true  -> <<>>;
              false -> Req:recv(Size)
          end,

    case leo_gateway_rpc_handler:put(Key, Bin, Size) of
        ok ->
            Req:respond({200, [?SERVER_HEADER], []});
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            Req:respond({500, [?SERVER_HEADER], []});
        {error, timeout} ->
            Req:respond({504, [?SERVER_HEADER], []})
    end;

%% @doc invalid request.
%%
exec1(_, Req, _, _) ->
    Req:respond({400, [?SERVER_HEADER], []}).


%% @doc GET operation with Etag
%%
exec2(?HTTP_GET, Req, Key, #req_params{is_dir = false, has_inner_cache = true}, Cached) ->
    case leo_gateway_rpc_handler:get(Key, Cached#cache.etag) of
        {ok, match} ->
            Req:respond({200,
                         [?SERVER_HEADER,
                          {"Content-Type",  Cached#cache.content_type},
                          {"ETag",          leo_hex:integer_to_hex(Cached#cache.etag)},
                          {"X-From-Cache",  "True"},
                          {"Last-Modified", leo_http:rfc1123_date(Cached#cache.mtime)}],
                         Cached#cache.body});
        {ok, Meta, RespObject} ->
            Mime = mochiweb_util:guess_mime(Key),
            BinVal = term_to_binary(#cache{etag = Meta#metadata.checksum,
                                           mtime = Meta#metadata.timestamp,
                                           content_type = Mime,
                                           body = RespObject}),
            ecache_server:set(Key, BinVal),
            Req:respond({200,
                         [?SERVER_HEADER,
                          {"Content-Type",  Mime},
                          {"ETag",          leo_hex:integer_to_hex(Meta#metadata.checksum)},
                          {"Last-Modified", leo_http:rfc1123_date(Meta#metadata.timestamp)}],
                         RespObject});
        {error, not_found} ->
            Req:respond({404, [?SERVER_HEADER], []});
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            Req:respond({500, [?SERVER_HEADER], []});
        {error, timeout} ->
            Req:respond({504, [?SERVER_HEADER], []})
    end.


%% @doc getter helper function. return "" if specified header is undefined
get_header(Req, Key) ->
    case Req:get_header_value(Key) of
        undefined ->
            "";
        Val ->
            Val
    end.


%% @doc auth
auth(Req, HTTPMethod, Path, TokenLen) when (TokenLen =< 1) orelse
                                           (TokenLen > 1 andalso
                                                           (HTTPMethod == ?HTTP_PUT orelse
                                                            HTTPMethod == ?HTTP_DELETE)) ->
    %% bucket operations must be needed to auth
    %% AND alter object operations as well
    case Req:get_header_value("Authorization") of
        undefined ->
            {error, undefined};
        Authorization ->
            Bucket = case TokenLen >= 1 of
                         true  -> hd(string:tokens(Path, ?STR_SLASH));
                         false -> []
                     end,

            IsCreateBucketOp = (TokenLen == 1 andalso HTTPMethod == ?HTTP_PUT),
            {_, QueryString, _} = mochiweb_util:urlsplit_path(Req:get(raw_path)),

            SignParams = #sign_params{http_verb    = HTTPMethod,
                                      content_md5  = get_header(Req, ?HTTP_HEAD_MD5),
                                      content_type = get_header(Req, ?HTTP_HEAD_CONTENT_TYPE),
                                      date         = get_header(Req, ?HTTP_HEAD_DATE),
                                      bucket       = Bucket,
                                      uri          = Req:get(path),
                                      query_str    = QueryString,
                                      amz_headers  = leo_http:get_amz_headers(Req:get(headers))
                                     },
            leo_s3_auth_api:authenticate(Authorization, SignParams, IsCreateBucketOp)
    end;

auth(_Req, _HTTPMethod, _Path, _TokenLen) ->
    {ok, []}.


%% @doc get options from mochiweb.
%%
-spec(get_option(atom(), list()) ->
             {any(), any()}).
get_option(Option, Options) ->
    {proplists:get_value(Option, Options),
     proplists:delete(Option, Options)}.

