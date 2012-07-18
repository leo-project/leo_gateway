%%======================================================================
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
%% ---------------------------------------------------------------------
%% LeoFS Gateway - Web
%% @doc
%% @end
%%======================================================================
-module(leo_gateway_web_mochi).
-author('Yosuke Hara').
-author('Yoshiyuki Kanno').
-vsn('0.9.1').

-export([start/1, stop/0, loop/3]).

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
-define(SERVER_HEADER,   {"Server","LeoFS"}).
-define(QUERY_PREFIX,    "prefix").
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
          has_inner_cache   :: boolean(),
          is_cached         :: boolean()
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
%% Start web-server
%%
-spec(start(list()) ->
             ok).
start(Options) ->
    ?debugVal(Options),

    {_DocRoot, Options1} = get_option(docroot, Options),
    HookModules = proplists:get_value(hook_modules, Options1),

    HasInnerCache = HookModules =:= undefined,
    case HasInnerCache of
        true ->
            application:start(ecache_app);
        _ ->
            void
    end,
    LayerOfDirs = ?env_layer_of_dirs(),

    Loop = fun (Req) -> ?MODULE:loop(Req, LayerOfDirs, HasInnerCache) end,
    mochiweb_http:start([{name, ?MODULE}, {loop, Loop} | Options1]).


%% @doc Stop web-server
%%
-spec(stop() ->
             ok).
stop() ->
    mochiweb_http:stop(?MODULE).


%% @doc handling HTTP-Request/Response
%%
-spec(loop(any(), tuple(), boolean()) ->
             ok).
loop(Req, {NumOfMinLayers, NumOfMaxLayers}, HasInnerCache) ->
    [Host|_] = string:tokens(Req:get_header_value("host"), ":"),
    Key = leo_http:key(?S3_DEFAULT_ENDPOINT, Host, Req:get(path)),
    loop1(Req, {NumOfMinLayers, NumOfMaxLayers}, HasInnerCache, Key).

%% @private
%%
-spec(loop1(any(), tuple(), boolean(), string()) ->
             ok).
loop1(Req, {NumOfMinLayers, NumOfMaxLayers}, HasInnerCache, Path) ->
    HTTPMethod = case Req:get(method) of
                     ?HTTP_POST -> ?HTTP_PUT;
                     Other      -> Other
                 end,

    QueryString = Req:parse_qs(),
    {Prefix, IsDir} = case proplists:get_value(?QUERY_PREFIX, QueryString, undefined) of
                          undefined -> {none, false};
                          Param     -> {Param, true}
                      end,

    case catch exec(first, HTTPMethod, Req, Path, #req_params{token_length     = erlang:length(string:tokens(Path, ?STR_SLASH)),
                                                              min_layers       = NumOfMinLayers,
                                                              max_layers       = NumOfMaxLayers,
                                                              has_inner_cache  = HasInnerCache,
                                                              is_dir           = IsDir,
                                                              is_cached        = true,
                                                              qs_prefix        = Prefix}) of
        {'EXIT', Reason} ->
            ?error("loop1/4", "path:~w, reason:~p", [Path, Reason]),
            Req:respond({500, [?SERVER_HEADER], []});
        _ ->
            erlang:garbage_collect(self())
    end.

%%--------------------------------------------------------------------
%%% INTERNAL FUNCTIONS
%%--------------------------------------------------------------------

%% @doc constraint violation.
%%
exec(first, _HTTPMethod, Req,_Key, #req_params{token_length = Len,
                                               min_layers   = Min,
                                               max_layers   = Max}) when Len < (Min - 1);
                                                                         Len > Max ->
    Req:respond({404, [?SERVER_HEADER], []}),
    ok;

%% @doc operations on buckets.
%%
exec(first, ?HTTP_GET, Req, Key,
     #req_params{token_length = _TokenLen,
                 is_dir       = true,
                 qs_prefix    = Prefix
                }) ->
    case leo_gateway_web_model:get_bucket_list(Key, none, none, 1000, Prefix) of
        {ok, Meta, XML} when is_list(Meta) == true ->
            Req:respond({200, [?SERVER_HEADER], XML});
        {error, not_found} ->
            Req:respond({404, [?SERVER_HEADER], []});
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            Req:respond({500, [?SERVER_HEADER], []});
        {error, timeout} ->
            Req:respond({504, [?SERVER_HEADER], []})
    end;

%% @doc GET operation on Object if inner cache is enabled.
%%
exec(first, ?HTTP_GET = HTTPMethod, Req, Key, #req_params{is_dir = false,
                                                          is_cached = true,
                                                          has_inner_cache = true} = Params) ->
    case ecache_server:get(Key) of
        undefined ->
            exec(first, HTTPMethod, Req, Key, Params#req_params{is_cached = false});
        BinCached ->
            Cached = binary_to_term(BinCached),
            exec(next, HTTPMethod, Req, Key, Params, Cached)
    end;

%% @doc GET operation on Object.
%%
exec(first, ?HTTP_GET, Req, Key, #req_params{is_dir = false, has_inner_cache = HasInnerCache}) ->
    case leo_gateway_web_model:get_object(Key) of
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
                          {"Last-Modified", rfc1123_date(Meta#metadata.timestamp)}],
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
exec(first, ?HTTP_HEAD, Req, Key, #req_params{is_dir = false}) ->
    case leo_gateway_web_model:head_object(Key) of
        {ok, #metadata{del = 0} = Meta} ->
            Req:start_response_length({200,
                                       [?SERVER_HEADER,
                                        {"Content-Type",  mochiweb_util:guess_mime(Key)},
                                        {"ETag",          leo_hex:integer_to_hex(Meta#metadata.checksum)},
                                        {"Last-Modified", rfc1123_date(Meta#metadata.timestamp)}],
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
exec(first, ?HTTP_DELETE, Req, Key, #req_params{is_dir = false}) ->
    case leo_gateway_web_model:delete_object(Key) of
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
exec(first, ?HTTP_PUT, Req, Key, #req_params{is_dir = false}) ->
    Size = list_to_integer(Req:get_header_value("content-length")),
    Bin = case (Size == 0) of
              %% for support uploading zero byte files.
              true  -> <<>>;
              false -> Req:recv(Size)
          end,
    case leo_gateway_web_model:put_object(Key, Bin, Size) of
        ok ->
            Req:respond({200, [?SERVER_HEADER], []});
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            Req:respond({500, [?SERVER_HEADER], []});
        {error, timeout} ->
            Req:respond({504, [?SERVER_HEADER], []})
    end;

%% @doc invalid request.
%%
exec(first, _, Req, _, _) ->
    Req:respond({400, [?SERVER_HEADER], []}).

%% @doc GET operation with Etag
%%
exec(next, ?HTTP_GET, Req, Key, #req_params{is_dir = false, has_inner_cache = true}, Cached) ->
    case leo_gateway_web_model:get_object(Key, Cached#cache.etag) of
        {ok, match} ->
            Req:respond({200,
                         [?SERVER_HEADER,
                          {"Content-Type",  Cached#cache.content_type},
                          {"ETag",          leo_hex:integer_to_hex(Cached#cache.etag)},
                          {"X-From-Cache",  "True"},
                          {"Last-Modified", rfc1123_date(Cached#cache.mtime)}],
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
                          {"Last-Modified", rfc1123_date(Meta#metadata.timestamp)}],
                         RespObject});
        {error, not_found} ->
            Req:respond({404, [?SERVER_HEADER], []});
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            Req:respond({500, [?SERVER_HEADER], []});
        {error, timeout} ->
            Req:respond({504, [?SERVER_HEADER], []})
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

