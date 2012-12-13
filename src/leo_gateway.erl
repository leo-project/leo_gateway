%%======================================================================
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
%% ---------------------------------------------------------------------
%% Leo Gateway
%% @doc
%% @end
%%======================================================================
-module(leo_gateway).
-author('Yosuke Hara').
-author('Yoshiyuki Kanno').

-include("leo_gateway.hrl").

-export([start/0, stop/0]).

ensure_started(App) ->
    case application:start(App) of
        ok ->
            ok;
        {error, {already_started, App}} ->
            ok
    end.

%% @spec start() -> ok
%% @doc Start the leo_gateway server.
start() ->
    ensure_started(crypto),

    case application:start(leo_gateway) of
        {error, Cause} ->
            io:format("~n"),
            io:format("[ERROR] ~p did not started - cause: ~p~n", [erlang:node(), Cause]),
            io:format("~n"),
            stop(),
            init:stop();
        Ret ->
            Ret
    end.

%% @spec stop() -> ok
%% @doc Stop the leo_gateway server.
stop() ->
    Res = application:stop(leo_gateway),
    application:stop(crypto),
    application:stop(mnesia),
    Res.
