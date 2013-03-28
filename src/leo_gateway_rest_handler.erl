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
%% Leo Gateway Rest Handler
%% @doc
%% @end
%%======================================================================
-module(leo_gateway_rest_handler).

-author('Yosuke Hara').

-export([init/3, handle/2, terminate/3]).
-export([onrequest/1, onresponse/1]).

-include("leo_gateway.hrl").
-include("leo_http.hrl").

-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Initializer
init({_Any, http}, Req, Opts) ->
    {ok, Req, Opts}.


%% @doc Handle a request
%% @callback
handle(Req, State) ->
    Key = gen_key(Req),
    handle(Req, State, Key).

handle(Req, [{NumOfMinLayers, NumOfMaxLayers}, HasInnerCache, Props] = State, Path) ->
    TokenLen = length(binary:split(Path, [?BIN_SLASH], [global, trim])),
    HTTPMethod0 = cowboy_req:get(method, Req),

    ReqParams = #req_params{path              = Path,
                            token_length      = TokenLen,
                            min_layers        = NumOfMinLayers,
                            max_layers        = NumOfMaxLayers,
                            has_inner_cache   = HasInnerCache,
                            is_cached         = true,
                            max_chunked_objs  = Props#http_options.max_chunked_objs,
                            max_len_for_obj   = Props#http_options.max_len_for_obj,
                            chunked_obj_len   = Props#http_options.chunked_obj_len,
                            threshold_obj_len = Props#http_options.threshold_obj_len},
    handle1(Req, HTTPMethod0, Path, ReqParams, State).


%% For Regular cases
%%
handle1(Req0, HTTPMethod0, Path, Params, State) ->
    HTTPMethod1 = case HTTPMethod0 of
                      ?HTTP_POST -> ?HTTP_PUT;
                      Other      -> Other
                  end,

    case catch leo_gateway_http_handler:invoke(HTTPMethod1, Req0, Path, Params) of
        {'EXIT', Cause} ->
            ?error("handle1/5", "path:~s, cause:~p", [binary_to_list(Path), Cause]),
            {ok, Req1} = ?reply_internal_error([?SERVER_HEADER], Req0),
            {ok, Req1, State};
        {ok, Req1} ->
            Req2 = cowboy_req:compact(Req1),
            {ok, Req2, State}
    end.


%% @doc Terminater
terminate(_Reason, _Req, _State) ->
    ok.


%%--------------------------------------------------------------------
%% Callbacks
%%--------------------------------------------------------------------
%% @doc Handle request
%%
onrequest(CacheCondition) ->
    leo_gateway_http_handler:onrequest(CacheCondition, fun gen_key/1).


%% @doc Handle response
%%
onresponse(CacheCondition) ->
    leo_gateway_http_handler:onresponse(CacheCondition, fun gen_key/1).


%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------
%% @doc Create a key
%% @private
gen_key(Req) ->
    {RawPath, _} = cowboy_req:path(Req),
    cowboy_http:urldecode(RawPath).

