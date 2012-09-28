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
    test(64).
test(N) ->
    proper:quickcheck(?MODULE:prop_http_req(), N).

prop_http_req() ->
    ?FORALL({Method, Bucket, Path, Body, Result}, 
            {method(), bucket(), path(), binary(), result()},
            begin
                Url = url_gen(Bucket, Path),
                Headers = headers_gen(),
                RawResp = raw_resp_gen(Method, Result, Body),
                io:format(user, "method:~p url:~s, resp:~p~n",[Method, Url, RawResp]),
                meck_begin(Method, RawResp),
                try
                    http_req(Method, Url, Headers, Body, RawResp)
                catch
                    throw:Reason ->
                        throw(Reason)
                after
                    meck_end(Method)
                end,
                true
            end). 

meck_begin(_M, _R) ->
    meck:new(leo_s3_auth),
    meck:expect(leo_s3_auth, authenticate, 3, {ok, "AccessKey"}).

meck_end(_M) ->
    meck:unload(leo_s3_auth).

http_req(_Method, _Url, _Headers, _Body, _RawResp) ->
    nop.

%% @doc inner functions
url_gen(Bucket, Path) when length(Bucket) > 0 andalso length(Path) > 0 ->
    io_lib:format("http://~s:~s/~s/~s",
                  [?TARGET_HOST, 
                   ?TARGET_PORT,
                   Bucket,
                   Path]);
url_gen(Bucket, _Path) when length(Bucket) > 0 ->
    io_lib:format("http://~s:~s/~s",
                  [?TARGET_HOST, 
                   ?TARGET_PORT,
                   Bucket]);
url_gen(_Bucket, _Path) ->
    io_lib:format("http://~s:~s/default", [?TARGET_HOST, ?TARGET_PORT]).


headers_gen() ->
    [].

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



