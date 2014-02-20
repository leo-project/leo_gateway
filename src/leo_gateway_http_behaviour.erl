%%======================================================================
%%
%% Leo Gateway
%%
%% Copyright (c) 2012-2014 Rakuten, Inc.
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
%% Leo Gateway - HTTP-behabiour
%% @doc
%% @end
%%======================================================================
-module(leo_gateway_http_behaviour).
-author('Yosuke Hara').

-include("leo_http.hrl").

%% Bucket handlers
-callback(get_bucket(Req::any(), Key::binary(), HttpParams::#req_params{}) ->
                 {ok, Req::any()}).
-callback(put_bucket(Req::any(), Key::binary(), HttpParams::#req_params{}) ->
                 {ok, Req::any()}).
-callback(delete_bucket(Req::any(), Key::binary(), HttpParams::#req_params{}) ->
                 {ok, Req::any()}).
-callback(head_bucket(Req::any(), Key::binary(), HttpParams::#req_params{}) ->
                 {ok, Req::any()}).

%% Object handlers
-callback(get_object(Req::any(), Key::binary(), HttpParams::#req_params{}) ->
                 {ok, Req::any()}).
-callback(put_object(Req::any(), Key::binary(), HttpParams::#req_params{}) ->
                 {ok, Req::any()}).
-callback(delete_object(Req::any(), Key::binary(), HttpParams::#req_params{}) ->
                 {ok, Req::any()}).
-callback(head_object(Req::any(), Key::binary(), HttpParams::#req_params{}) ->
                 {ok, Req::any()}).
-callback(range_object(Req::any(), Key::binary(), HttpParams::#req_params{}) ->
                 {ok, Req::any()}).
-callback(get_object_with_cache(Req::any(), Key::binary(),
                                CacheObj::binary(), HttpParams::#req_params{}) ->
                 {ok, Req::any()}).
