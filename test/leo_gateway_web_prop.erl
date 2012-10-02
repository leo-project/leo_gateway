%%====================================================================
%%
%% Leo Gateway
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
%% -------------------------------------------------------------------
%% Leo Gateway - Property TEST
%% @doc
%% @end
%%====================================================================
-module(leo_gateway_web_prop).

-author('Yoshiyuki Kanno').

-export([test/0, test/1]).
-export([prop_http_req/0]).

-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("proper/include/proper.hrl").
-include("leo_gateway.hrl").
-include("leo_s3_http.hrl").

-define(TARGET_HOST, "localhost").
-define(TARGET_PORT, "8080").

%% @doc extend basic types
%%
method() -> union(['head', 'get', 'put', 'delete']).
result() -> union(['ok', 'not_found', 'timeout', 'error']).
digit_char() -> integer(16#30, 16#39).
alpha_char() -> integer(16#61, 16#7a).
uri_char() -> union([digit_char(), alpha_char()]). 
bucket() -> list(uri_char()).
path()   -> list(uri_char()).

test() ->
    test(128).
test(N) ->
    proper:quickcheck(?MODULE:prop_http_req(), N).

prop_http_req() ->
    ?FORALL({Method, Bucket, Path, Body, Result}, 
            {method(), bucket(), path(), binary(), result()},
            begin
                Url = url_gen(Bucket, Path),
                Headers = headers_gen(),
                RawResp = raw_resp_gen(Method, Result, Body),
                meck_begin(Method, RawResp),
                try
                    collect("http random requests", http_req(Method, Url, Headers, Body, RawResp))
                catch
                    throw:Reason ->
                        io:format(user, "error:~p~n",[Reason]),
                        false
                after
                    meck_end(Method)
                end
            end). 

meck_begin(Method, RawResp) ->
    meck:new(leo_s3_auth),
    meck:expect(leo_s3_auth, authenticate, 3, {ok, "AccessKey"}),
    meck:new(leo_s3_endpoint),
    meck:expect(leo_s3_endpoint, get_endpoints, 0, ["localhost"]),
    meck:new(leo_gateway_rpc_handler),
    meck:expect(leo_gateway_rpc_handler, Method, 1, RawResp),
    meck:expect(leo_gateway_rpc_handler, Method, 2, RawResp),
    meck:expect(leo_gateway_rpc_handler, Method, 3, RawResp).

meck_end(_M) ->
    meck:unload(leo_s3_auth),
    meck:unload(leo_s3_endpoint),
    meck:unload(leo_gateway_rpc_handler).

http_req(Method, Url, Headers, _Body, RawResp) when Method =/= 'put' ->
    case httpc:request(Method, {Url, [{"connection", "close"}|Headers]}, [], [{body_format, binary}]) of
        {ok, {{_, SC, _}, RespHeaders, RespBody}} ->
            %io:format(user, "[not put]sc:~p headers:~p body:~p~n",[SC, RespHeaders, RespBody]),
            http_check_resp(SC, RespHeaders, RespBody, Method, RawResp);
        {error, Reason} ->
            io:format(user, "[not put]error:~p~n",[Reason]),
            true
    end;
http_req(Method, Url, Headers, Body, RawResp) ->
    case httpc:request(Method, {Url, Headers, "text/plain", Body}, [], []) of
        {ok, {{_, SC, _}, RespHeaders, RespBody}} ->
            http_check_resp(SC, RespHeaders, RespBody, Method, RawResp);
        {error, Reason} ->
            io:format(user, "[put]error:~p~n",[Reason]),
            true
    end.

http_check_resp(SC, _RespHeaders, _RespBody, _Method, {error, not_found}) ->
    SC =:= 404;
http_check_resp(SC, _RespHeaders, _RespBody, _Method, {error, ?ERR_TYPE_INTERNAL_ERROR}) ->
    SC =:= 500;
http_check_resp(SC, _RespHeaders, _RespBody, _Method, {error, timeout}) ->
    SC =:= 504;
http_check_resp(SC, _RespHeaders, _RespBody, 'delete', ok) ->
    SC =:= 204;
http_check_resp(SC, _RespHeaders, _RespBody, 'put', ok) ->
    SC =:= 200;
http_check_resp(SC, RespHeaders, _RespBody, 'head', {ok, #metadata{dsize = DSize}}) ->
    ContentLength = list_to_integer(proplists:get_value("content-length", RespHeaders, "0")),
    SC =:= 200 andalso ContentLength =:= DSize;
http_check_resp(SC, RespHeaders, RespBody, 'get', {ok, #metadata{dsize = DSize}, Body}) ->
    ContentLength = list_to_integer(proplists:get_value("content-length", RespHeaders, "0")),
    SC =:= 200 andalso ContentLength =:= DSize andalso RespBody =:= Body;
http_check_resp(_, _, _, _, _) ->
    false.

%% @doc inner functions
url_gen(Bucket, Path) when length(Bucket) > 0 andalso length(Path) > 0 ->
    lists:append(["http://",
                 ?TARGET_HOST,":", 
                 ?TARGET_PORT,"/",
                 Bucket,"/",
                 Path]);
url_gen(Bucket, _Path) when length(Bucket) > 0 ->
    lists:append(["http://",
                 ?TARGET_HOST,":", 
                 ?TARGET_PORT,"/",
                 Bucket,"/",
                 Bucket]);
url_gen(_Bucket, _Path) ->
    lists:append(["http://",
                 ?TARGET_HOST,":", 
                 ?TARGET_PORT,"/bucket/file"]).


headers_gen() ->
    [{"Authorization","auth"}].

raw_resp_gen('head', 'ok', Body) ->
    {ok, #metadata{
             del = 0,
             timestamp = calendar:datetime_to_gregorian_seconds(calendar:universal_time()),
             checksum = 0,
             dsize = size(Body)}};
raw_resp_gen('head', 'not_found', _Body) ->
    {error, not_found};
raw_resp_gen('head', 'timeout', _Body) ->
    {error, timeout};
raw_resp_gen('head', 'error', _Body) ->
    {error, ?ERR_TYPE_INTERNAL_ERROR};

raw_resp_gen('get', 'ok', Body) ->
    {ok, #metadata{
             del = 0,
             timestamp = calendar:datetime_to_gregorian_seconds(calendar:universal_time()),
             checksum = 0,
             dsize = size(Body)}, Body};
raw_resp_gen('get', 'not_found', _Body) ->
    {error, not_found};
raw_resp_gen('get', 'timeout', _Body) ->
    {error, timeout};
raw_resp_gen('get', 'error', _Body) ->
    {error, ?ERR_TYPE_INTERNAL_ERROR};

raw_resp_gen('put', 'ok', _Body) ->
    ok;
raw_resp_gen('put', 'not_found', _Body) ->
    ok;
raw_resp_gen('put', 'timeout', _Body) ->
    {error, timeout};
raw_resp_gen('put', 'error', _Body) ->
    {error, ?ERR_TYPE_INTERNAL_ERROR};

raw_resp_gen('delete', 'ok', _Body) ->
    ok;
raw_resp_gen('delete', 'not_found', _Body) ->
    {error, not_found};
raw_resp_gen('delete', 'timeout', _Body) ->
    {error, timeout};
raw_resp_gen('delete', 'error', _Body) ->
    {error, ?ERR_TYPE_INTERNAL_ERROR}.



