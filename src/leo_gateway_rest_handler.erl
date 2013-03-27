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

-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
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

    case catch exec1(HTTPMethod1, Req0, Path, Params) of
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
%% Compile Options:
%%
-compile({inline, [gen_key/1, exec1/4, exec2/5, put1/4,
                   put_small_object/3, put_large_object/4]}).

%% @doc Create a key
%% @private
gen_key(Req) ->
    {RawPath, _} = cowboy_req:path(Req),
    cowboy_http:urldecode(RawPath).


%% ---------------------------------------------------------------------
%% INVALID OPERATION
%% ---------------------------------------------------------------------
%% @doc Constraint violation.
%% @private
exec1(_HTTPMethod, Req,_Key, #req_params{token_length = Len,
                                         max_layers   = Max}) when Len > Max ->
    ?reply_not_found([?SERVER_HEADER], Req);


%% ---------------------------------------------------------------------
%% For OBJECT-OPERATION
%% ---------------------------------------------------------------------
%% @doc GET operation on Object if inner cache is enabled.
%% @private
exec1(?HTTP_GET = HTTPMethod, Req, Key, #req_params{is_dir = false,
                                                    is_cached = true,
                                                    has_inner_cache = true} = Params) ->
    case ecache_api:get(Key) of
        not_found ->
            exec1(HTTPMethod, Req, Key, Params#req_params{is_cached = false});
        {ok, CachedObj} ->
            Cached = binary_to_term(CachedObj),
            exec2(HTTPMethod, Req, Key, Params, Cached)
    end;

%% @doc GET operation on Object.
%% @private
exec1(?HTTP_GET, Req, Key, #req_params{is_dir = false,
                                       has_inner_cache = HasInnerCache}) ->
    case leo_gateway_rpc_handler:get(Key) of
        %% For regular case (NOT a chunked object)
        {ok, #metadata{cnumber = 0} = Meta, RespObject} ->
            Mime = leo_mime:guess_mime(Key),

            case HasInnerCache of
                true ->
                    BinVal = term_to_binary(#cache{etag = Meta#metadata.checksum,
                                                   mtime = Meta#metadata.timestamp,
                                                   content_type = Mime,
                                                   body = RespObject}),
                    _ = ecache_api:put(Key, BinVal);
                false ->
                    void
            end,
            Req2 = cowboy_req:set_resp_body(RespObject, Req),
            ?reply_ok([?SERVER_HEADER,
                       {?HTTP_HEAD_CONTENT_TYPE,  Mime},
                       {?HTTP_HEAD_ETAG4AWS, lists:append(["\"",
                                                           leo_hex:integer_to_hex(Meta#metadata.checksum, 32),
                                                           "\""])},
                       {?HTTP_HEAD_LAST_MODIFIED, leo_http:rfc1123_date(Meta#metadata.timestamp)}],
                      Req2);

        %% For a chunked object.
        {ok, #metadata{cnumber = TotalChunkedObjs}, _RespObject} ->
            {ok, Pid}  = leo_gateway_large_object_handler:start_link(Key),
            {ok, Req2} = cowboy_req:chunked_reply(?HTTP_ST_OK, [?SERVER_HEADER], Req),

            Ret = leo_gateway_large_object_handler:get(Pid, TotalChunkedObjs, Req2),
            catch leo_gateway_large_object_handler:stop(Pid),

            case Ret of
                {ok, Req3} ->
                    {ok, Req3};
                {error, Cause} ->
                    ?error("exec1/4", "path:~s, cause:~p", [binary_to_list(Key), Cause]),
                    ?reply_internal_error([?SERVER_HEADER], Req)
            end;
        {error, not_found} ->
            ?reply_not_found([?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?reply_internal_error([?SERVER_HEADER], Req);
        {error, timeout} ->
            ?reply_timeout([?SERVER_HEADER], Req)
    end;


%% @doc HEAD operation on Object.
%% @private
exec1(?HTTP_HEAD, Req, Key, _Params) ->
    case leo_gateway_rpc_handler:head(Key) of
        {ok, #metadata{del = 0} = Meta} ->
            Timestamp = leo_http:rfc1123_date(Meta#metadata.timestamp),
            Headers   = [?SERVER_HEADER,
                         {?HTTP_HEAD_CONTENT_TYPE, leo_mime:guess_mime(Key)},
                         {?HTTP_HEAD_ETAG4AWS, lists:append(["\"",
                                                             leo_hex:integer_to_hex(Meta#metadata.checksum, 32),
                                                             "\""])},
                         {?HTTP_HEAD_CONTENT_LENGTH, erlang:integer_to_list(Meta#metadata.dsize)},
                         {?HTTP_HEAD_LAST_MODIFIED,  Timestamp}],
            ?reply_ok(Headers, Req);
        {ok, #metadata{del = 1}} ->
            ?reply_not_found([?SERVER_HEADER], Req);
        {error, not_found} ->
            ?reply_not_found([?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?reply_internal_error([?SERVER_HEADER], Req);
        {error, timeout} ->
            ?reply_timeout([?SERVER_HEADER], Req)
    end;

%% @doc DELETE operation on Object.
%% @private
exec1(?HTTP_DELETE, Req, Key, _Params) ->
    case leo_gateway_rpc_handler:delete(Key) of
        ok ->
            ?reply_no_content([?SERVER_HEADER], Req);
        {error, not_found} ->
            ?reply_not_found([?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?reply_internal_error([?SERVER_HEADER], Req);
        {error, timeout} ->
            ?reply_timeout([?SERVER_HEADER], Req)
    end;

%% @doc POST/PUT operation on Objects.
%% @private
exec1(?HTTP_PUT, Req, Key, Params) ->
    put1(?BIN_EMPTY, Req, Key, Params);

%% @doc invalid request.
%% @private
exec1(_, Req, _, _) ->
    ?reply_bad_request([?SERVER_HEADER], Req).


%% @doc GET operation with Etag
%% @private
exec2(?HTTP_GET, Req, Key, #req_params{is_dir = false, has_inner_cache = true}, Cached) ->
    case leo_gateway_rpc_handler:get(Key, Cached#cache.etag) of
        {ok, match} ->
            Req2 = cowboy_req:set_resp_body(Cached#cache.body, Req),
            ?reply_ok([?SERVER_HEADER,
                       {?HTTP_HEAD_CONTENT_TYPE,  Cached#cache.content_type},
                       {?HTTP_HEAD_ETAG4AWS, lists:append(["\"",
                                                           leo_hex:integer_to_hex(Cached#cache.etag, 32),
                                                           "\""])},
                       {?HTTP_HEAD_LAST_MODIFIED, leo_http:rfc1123_date(Cached#cache.mtime)},
                       {?HTTP_HEAD_X_FROM_CACHE,  <<"True">>}],
                      Req2);
        {ok, Meta, RespObject} ->
            Mime = leo_mime:guess_mime(Key),
            BinVal = term_to_binary(#cache{etag = Meta#metadata.checksum,
                                           mtime = Meta#metadata.timestamp,
                                           content_type = Mime,
                                           body = RespObject}),

            _ = ecache_api:put(Key, BinVal),

            Req2 = cowboy_req:set_resp_body(RespObject, Req),
            ?reply_ok([?SERVER_HEADER,
                       {?HTTP_HEAD_CONTENT_TYPE,  Mime},
                       {?HTTP_HEAD_ETAG4AWS, lists:append(["\"",
                                                           leo_hex:integer_to_hex(Meta#metadata.checksum, 32),
                                                           "\""])},
                       {?HTTP_HEAD_LAST_MODIFIED, leo_http:rfc1123_date(Meta#metadata.timestamp)}],
                      Req2);
        {error, not_found} ->
            ?reply_not_found([?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?reply_internal_error([?SERVER_HEADER], Req);
        {error, timeout} ->
            ?reply_timeout([?SERVER_HEADER], Req)
    end.


%% @doc POST/PUT operation on Objects. NORMAL
%% @private
put1(?BIN_EMPTY, Req, Key, Params) ->
    {Size0, _} = cowboy_req:body_length(Req),

    case (Size0 >= Params#req_params.threshold_obj_len) of
        true when Size0 >= Params#req_params.max_len_for_obj ->
            ?reply_bad_request([?SERVER_HEADER], Req);
        true when Params#req_params.is_upload == false ->
            put_large_object(Req, Key, Size0, Params);
        false ->
            Ret = case cowboy_req:has_body(Req) of
                      true ->
                          case cowboy_req:body(Req) of
                              {ok, Bin0, Req0} ->
                                  {ok, {Size0, Bin0, Req0}};
                              {error, Cause} ->
                                  {error, Cause}
                          end;
                      false ->
                          {ok, {0, ?BIN_EMPTY, Req}}
                  end,
            put_small_object(Ret, Key, Params)
    end.


%% @doc Put a small object into the storage
%% @private
put_small_object({error, Cause}, _, _) ->
    {error, Cause};
put_small_object({ok, {Size, Bin, Req}}, Key, Params) ->
    CIndex = case Params#req_params.upload_part_num of
                 <<>> -> 0;
                 PartNum ->
                     case is_integer(PartNum) of
                         true ->
                             PartNum;
                         false ->
                             list_to_integer(binary_to_list(PartNum))
                     end
             end,

    case leo_gateway_rpc_handler:put(Key, Bin, Size, CIndex) of
        {ok, ETag} ->
            case Params#req_params.has_inner_cache of
                true  ->
                    Mime = leo_mime:guess_mime(Key),
                    BinVal = term_to_binary(#cache{etag = ETag,
                                                   mtime = leo_date:now(),
                                                   content_type = Mime,
                                                   body = Bin}),
                    _ = ecache_api:put(Key, BinVal);
                false -> void
            end,
            ?reply_ok([?SERVER_HEADER,
                       {?HTTP_HEAD_ETAG4AWS,
                        lists:append(["\"",
                                      leo_hex:integer_to_hex(ETag, 32),
                                      "\""])}
                      ], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?reply_internal_error([?SERVER_HEADER], Req);
        {error, timeout} ->
            ?reply_timeout([?SERVER_HEADER], Req)
    end.


%% @doc Put a large-object into the storage
%% @private
put_large_object(Req, Key, Size, #req_params{chunked_obj_len=ChunkedSize})->
    {ok, Pid}  = leo_gateway_large_object_handler:start_link(Key),

    Ret2 = case catch put_large_object(
                        cowboy_req:stream_body(Req), Key, Size, ChunkedSize, 0, 1, Pid) of
               {'EXIT', Cause} ->
                   {error, Cause};
               Ret1 ->
                   Ret1
           end,
    catch leo_gateway_large_object_handler:stop(Pid),
    Ret2.

put_large_object({ok, Data, Req}, Key, Size, ChunkedSize, TotalSize, Counter, Pid) ->
    DataSize = byte_size(Data),

    catch leo_gateway_large_object_handler:put(Pid, ChunkedSize, Data),
    put_large_object(cowboy_req:stream_body(Req), Key, Size, ChunkedSize,
                     TotalSize + DataSize, Counter + 1, Pid);

put_large_object({done, Req}, Key, Size, ChunkedSize, TotalSize, Counter, Pid) ->
    case catch leo_gateway_large_object_handler:put(Pid, done) of
        {ok, TotalChunks} ->
            case catch leo_gateway_large_object_handler:result(Pid) of
                {ok, Digest0} when Size == TotalSize ->
                    Digest1 = leo_hex:raw_binary_to_integer(Digest0),

                    case leo_gateway_rpc_handler:put(
                           Key, ?BIN_EMPTY, Size, ChunkedSize, TotalChunks, Digest1) of
                        {ok, _ETag} ->
                            ?reply_ok([?SERVER_HEADER,
                                       {?HTTP_HEAD_ETAG4AWS,
                                        lists:append(["\"",
                                                      leo_hex:integer_to_hex(Digest1, 32),
                                                      "\""])}
                                      ], Req);
                        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
                            ?reply_internal_error([?SERVER_HEADER], Req);
                        {error, timeout} ->
                            ?reply_timeout([?SERVER_HEADER], Req)
                    end;
                {_, _Cause} ->
                    ok = leo_gateway_large_object_handler:rollback(Pid, TotalChunks),
                    ?reply_internal_error([?SERVER_HEADER], Req)
            end;
        {error, _Cause} ->
            ok = leo_gateway_large_object_handler:rollback(Pid, Counter),
            ?reply_internal_error([?SERVER_HEADER], Req)
    end;

%% An error occurred while reading the body, connection is gone.
put_large_object({error, Cause}, Key, _Size, _ChunkedSize, _TotalSize, Counter, Pid) ->
    ?error("handle_cast/2", "key:~s, cause:~p", [binary_to_list(Key), Cause]),
    ok = leo_gateway_large_object_handler:rollback(Pid, Counter).

