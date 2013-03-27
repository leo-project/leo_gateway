%%======================================================================
%%
%% Leo S3 Handler
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
%% Leo S3 Handler
%% @doc
%% @end
%%======================================================================
-module(leo_gateway_s3_handler).

-author('Yosuke Hara').
-author('Yoshiyuki Kanno').

-export([init/3, handle/2, terminate/3]).
-export([onrequest/1, onresponse/1]).

-include("leo_gateway.hrl").
-include("leo_http.hrl").

-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("leo_s3_libs/include/leo_s3_auth.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("xmerl/include/xmerl.hrl").

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
    {Prefix, IsDir, Path2, Req2} =
        case cowboy_req:qs_val(?HTTP_HEAD_PREFIX, Req) of
            {undefined, Req1} ->
                HasTermSlash = (?BIN_SLASH ==
                                    binary:part(Path, {byte_size(Path)-1, 1})),
                {none, HasTermSlash, Path, Req1};
            {BinParam, Req1} ->
                NewPath = case binary:part(Path, {byte_size(Path)-1, 1}) of
                              ?BIN_SLASH -> Path;
                              _Else      -> <<Path/binary, ?BIN_SLASH/binary>>
                          end,
                {BinParam, true, NewPath, Req1}
        end,

    TokenLen = length(binary:split(Path2, [?BIN_SLASH], [global, trim])),
    HTTPMethod0 = cowboy_req:get(method, Req),

    case cowboy_req:qs_val(?HTTP_QS_BIN_ACL, Req2) of
        {undefined, _} ->
            ReqParams = request_params(
                          Req2, #req_params{path              = Path2,
                                            token_length      = TokenLen,
                                            min_layers        = NumOfMinLayers,
                                            max_layers        = NumOfMaxLayers,
                                            qs_prefix         = Prefix,
                                            has_inner_cache   = HasInnerCache,
                                            is_cached         = true,
                                            is_dir            = IsDir,
                                            max_chunked_objs  = Props#http_options.max_chunked_objs,
                                            max_len_for_obj   = Props#http_options.max_len_for_obj,
                                            chunked_obj_len   = Props#http_options.chunked_obj_len,
                                            threshold_obj_len = Props#http_options.threshold_obj_len}),
            AuthRet = auth1(Req2, HTTPMethod0, Path2, TokenLen),
            handle1(AuthRet, Req2, HTTPMethod0, Path2, ReqParams, State);
        _ ->
            {ok, Req3} = ?reply_not_found([?SERVER_HEADER], Req2),
            {ok, Req3, State}
    end.


%% @doc Handle a request (sub)
%% @private
handle1({error, _Cause}, Req0,_,_,_,State) ->
    {ok, Req1} = ?reply_forbidden([?SERVER_HEADER], Req0),
    {ok, Req1, State};

%% For Multipart Upload - Initiation
%%
handle1({ok,_AccessKeyId}, Req0, ?HTTP_POST, _, #req_params{path = Path0,
                                                            is_upload = true}, State) ->
    %% Insert a metadata into the storage-cluster
    NowBin = list_to_binary(integer_to_list(leo_date:now())),
    UploadId    = leo_hex:binary_to_hex(crypto:md5(<< Path0/binary, NowBin/binary >>)),
    UploadIdBin = list_to_binary(UploadId),

    {ok, Req2} =
        case leo_gateway_rpc_handler:put(<<Path0/binary, ?STR_NEWLINE, UploadIdBin/binary>> , <<>>, 0) of
            {ok, _ETag} ->
                %% Response xml to a client
                [Bucket|Path1] = leo_misc:binary_tokens(Path0, ?BIN_SLASH),
                XML = gen_upload_initiate_xml(Bucket, Path1, UploadId),

                Req1 = cowboy_req:set_resp_body(XML, Req0),
                ?reply_ok([?SERVER_HEADER], Req1);
            {error, timeout} ->
                ?reply_timeout([?SERVER_HEADER], Req0);
            {error, Cause} ->
                ?error("handle1/6", "path:~s, cause:~p", [binary_to_list(Path0), Cause]),
                ?reply_internal_error([?SERVER_HEADER], Req0)
        end,
    {ok, Req2, State};

%% For Multipart Upload - Upload a part of an object
%%
handle1({ok,_AccessKeyId}, Req0, ?HTTP_PUT, _,
        #req_params{upload_id = UploadId,
                    upload_part_num  = PartNum0,
                    max_chunked_objs = MaxChunkedObjs}, State) when UploadId /= <<>>,
                                                                    PartNum0 > MaxChunkedObjs ->
    {ok, Req1} = ?reply_bad_request([?SERVER_HEADER], Req0),
    {ok, Req1, State};

handle1({ok,_AccessKeyId}, Req0, ?HTTP_PUT, _,
        #req_params{path = Path0,
                    is_upload = false,
                    upload_id = UploadId,
                    upload_part_num = PartNum0} = Params, State) when UploadId /= <<>>,
                                                                      PartNum0 /= 0 ->
    PartNum1 = list_to_binary(integer_to_list(PartNum0)),
    Key0 = << Path0/binary, ?STR_NEWLINE, UploadId/binary >>, %% for confirmation
    Key1 = << Path0/binary, ?STR_NEWLINE, PartNum1/binary >>, %% for put a part of an object

    {ok, Req1} =
        case leo_gateway_rpc_handler:head(Key0) of
            {ok, _Metadata} ->
                put1(?BIN_EMPTY, Req0, Key1, Params);
            {error, not_found} ->
                ?reply_not_found([?SERVER_HEADER], Req0);
            {error, timeout} ->
                ?reply_timeout([?SERVER_HEADER], Req0);
            {error, ?ERR_TYPE_INTERNAL_ERROR} ->
                ?reply_internal_error([?SERVER_HEADER], Req0)
        end,
    {ok, Req1, State};

handle1({ok,_AccessKeyId}, Req0, ?HTTP_DELETE, _,
        #req_params{path = Path0,
                    upload_id = UploadId}, State) when UploadId /= <<>> ->
    _ = leo_gateway_rpc_handler:put(Path0, <<>>, 0),
    _ = leo_gateway_rpc_handler:delete(Path0),
    {ok, Req1} = ?reply_no_content([?SERVER_HEADER], Req0),
    {ok, Req1, State};

%% For Multipart Upload - Completion
%%
handle1({ok,_AccessKeyId}, Req0, ?HTTP_POST, _,
        #req_params{path = Path,
                    is_upload = false,
                    upload_id = UploadId,
                    upload_part_num = PartNum}, State) when UploadId /= <<>>,
                                                            PartNum  == 0 ->
    Res = cowboy_req:has_body(Req0),
    {ok, Req1} = handle_multi_upload_1(Res, Req0, Path, UploadId),
    {ok, Req1, State};

%% For Regular cases
%%
handle1({ok, AccessKeyId}, Req0, HTTPMethod0, Path, Params, State) ->
    HTTPMethod1 = case HTTPMethod0 of
                      ?HTTP_POST -> ?HTTP_PUT;
                      Other      -> Other
                  end,

    case catch invoke(HTTPMethod1, Req0, Path, Params#req_params{access_key_id = AccessKeyId}) of
        {'EXIT', Cause} ->
            ?error("handle1/6", "path:~s, cause:~p", [binary_to_list(Path), Cause]),
            {ok, Req1} = ?reply_internal_error([?SERVER_HEADER], Req0),
            {ok, Req1, State};
        {ok, Req1} ->
            Req2 = cowboy_req:compact(Req1),
            {ok, Req2, State}
    end.


%% @doc Handle multi-upload processing
%% @private
handle_multi_upload_1(true, Req, Path, UploadId) ->
    Path4Conf = << Path/binary, ?STR_NEWLINE, UploadId/binary >>,

    case leo_gateway_rpc_handler:head(Path4Conf) of
        {ok, _} ->
            _ = leo_gateway_rpc_handler:delete(Path4Conf),

            Ret = cowboy_req:body(Req),
            handle_multi_upload_2(Ret, Req, Path);
        _ ->
            ?reply_forbidden([?SERVER_HEADER], Req)
    end;
handle_multi_upload_1(false, Req,_Path,_UploadId) ->
    ?reply_forbidden([?SERVER_HEADER], Req).

handle_multi_upload_2({ok, Bin, Req0}, _Req, Path0) ->
    {#xmlElement{content = Content},_} = xmerl_scan:string(binary_to_list(Bin)),
    TotalUploadedObjs = length(Content),

    case handle_multi_upload_3(TotalUploadedObjs, Path0, []) of
        {ok, {Len, ETag0}} ->
            case leo_gateway_rpc_handler:put(Path0, <<>>, Len, 0, TotalUploadedObjs, ETag0) of
                {ok, _} ->
                    [Bucket|Path1] = leo_misc:binary_tokens(Path0, ?BIN_SLASH),
                    ETag1 = leo_hex:integer_to_hex(ETag0, 32),
                    XML   = gen_upload_completion_xml(Bucket, Path1, ETag1),
                    Req1  = cowboy_req:set_resp_body(XML, Req0),
                    ?reply_ok([?SERVER_HEADER], Req1);
                {error, Cause} ->
                    ?error("handle_multi_upload_2/2", "path:~s, cause:~p", [binary_to_list(Path0), Cause]),
                    ?reply_internal_error([?SERVER_HEADER], Req0)
            end;
        {error, Cause} ->
            ?error("handle_multi_upload_2/2", "path:~s, cause:~p", [binary_to_list(Path0), Cause]),
            ?reply_internal_error([?SERVER_HEADER], Req0)
    end;
handle_multi_upload_2({error, Cause}, Req, Path) ->
    ?error("handle_multi_upload_2/3", "path:~s, cause:~p", [binary_to_list(Path), Cause]),
    ?reply_internal_error([?SERVER_HEADER], Req).

%% @doc Retrieve Metadatas for uploaded objects (Multipart)
%% @private
-spec(handle_multi_upload_3(integer(), binary(), list(tuple())) ->
             {ok, tuple()} | {error, any()}).
handle_multi_upload_3(0, _, Acc) ->
    Metas = lists:reverse(Acc),
    {Len, ETag0} = lists:foldl(
                     fun({_, {DSize, Checksum}}, {Sum, ETagBin0}) ->
                             ETagBin1 = list_to_binary(leo_hex:integer_to_hex(Checksum, 32)),
                             {Sum + DSize, <<ETagBin0/binary, ETagBin1/binary>>}
                     end, {0, <<>>}, lists:sort(Metas)),
    ETag1 = leo_hex:hex_to_integer(leo_hex:binary_to_hex(crypto:md5(ETag0))),
    {ok, {Len, ETag1}};

handle_multi_upload_3(PartNum, Path, Acc) ->
    PartNumBin = list_to_binary(integer_to_list(PartNum)),
    Key = << Path/binary, ?STR_NEWLINE, PartNumBin/binary  >>,

    case leo_gateway_rpc_handler:head(Key) of
        {ok, #metadata{dsize = Len,
                       checksum = Checksum}} ->
            handle_multi_upload_3(PartNum - 1, Path, [{PartNum, {Len, Checksum}} | Acc]);
        Error ->
            Error
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
%% @doc Retrieve header values from a request
%%      Set request params
%% @private
request_params(Req, Params) ->
    IsUpload = case cowboy_req:qs_val(?HTTP_QS_BIN_UPLOADS, Req) of
                   {undefined, _} -> false;
                   _ -> true
               end,
    UploadId = case cowboy_req:qs_val(?HTTP_QS_BIN_UPLOAD_ID, Req) of
                   {undefined, _} -> <<>>;
                   {Val0,      _} -> Val0
               end,
    PartNum  = case cowboy_req:qs_val(?HTTP_QS_BIN_PART_NUMBER, Req) of
                   {undefined, _} -> 0;
                   {Val1,      _} -> list_to_integer(binary_to_list(Val1))
               end,
    Range    = element(1, cowboy_req:header(?HTTP_HEAD_RANGE, Req)),

    Params#req_params{is_upload       = IsUpload,
                      upload_id       = UploadId,
                      upload_part_num = PartNum,
                      range_header    = Range}.


%% Compile Options:
%%
-compile({inline, [gen_key/1, invoke/4, get_obj_with_etag/4, put1/4, put2/5, put3/3, put4/2,
                   put_small_object/3, put_large_object/4,
                   get_header/2, auth1/4, auth2/4]}).

%% @doc Create a key
%% @private
gen_key(Req) ->
    EndPoints1 = case leo_s3_endpoint:get_endpoints() of
                     {ok, EndPoints0} ->
                         lists:map(fun({endpoint,EP,_}) -> EP end, EndPoints0);
                     _ -> []
                 end,
    {Host,    _} = cowboy_req:host(Req),
    {RawPath, _} = cowboy_req:path(Req),
    Path = cowboy_http:urldecode(RawPath),
    leo_http:key(EndPoints1, Host, Path).


%% ---------------------------------------------------------------------
%% INVALID OPERATION
%% ---------------------------------------------------------------------
%% @doc Constraint violation.
%% @private
invoke(_HTTPMethod, Req,_Key, #req_params{token_length = Len,
                                          max_layers   = Max}) when Len > Max ->
    ?reply_not_found([?SERVER_HEADER], Req);


%% ---------------------------------------------------------------------
%% For BUCKET-OPERATION
%% ---------------------------------------------------------------------
%% @doc GET operation on buckets & Dirs.
%% @private
invoke(?HTTP_GET, Req, Key, #req_params{is_dir        = true,
                                        access_key_id = AccessKeyId,
                                        qs_prefix     = Prefix}) ->
    case leo_gateway_s3_bucket:get_bucket_list(AccessKeyId, Key, none, none, 1000, Prefix) of
        {ok, Meta, XML} when is_list(Meta) == true ->
            Req2 = cowboy_req:set_resp_body(XML, Req),
            ?reply_ok([?SERVER_HEADER,
                       {?HTTP_HEAD_CONTENT_TYPE, ?HTTP_CTYPE_XML}
                      ], Req2);
        {error, not_found} ->
            ?reply_not_found([?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?reply_internal_error([?SERVER_HEADER], Req);
        {error, timeout} ->
            ?reply_timeout([?SERVER_HEADER], Req)
    end;

%% @doc PUT operation on buckets.
%% @private
invoke(?HTTP_PUT, Req, Key, #req_params{token_length  = 1,
                                        access_key_id = AccessKeyId}) ->
    Bucket = case (?BIN_SLASH == binary:part(Key, {byte_size(Key)-1, 1})) of
                 true ->
                     binary:part(Key, {0, byte_size(Key) -1});
                 false ->
                     Key
             end,

    case leo_gateway_s3_bucket:put_bucket(AccessKeyId, Bucket) of
        ok ->
            ?reply_ok([?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?reply_internal_error([?SERVER_HEADER], Req);
        {error, timeout} ->
            ?reply_timeout([?SERVER_HEADER], Req)
    end;

%% @doc DELETE operation on buckets.
%% @private
invoke(?HTTP_DELETE, Req, Key, #req_params{token_length  = 1,
                                           access_key_id = AccessKeyId}) ->
    case leo_gateway_s3_bucket:delete_bucket(AccessKeyId, Key) of
        ok ->
            ?reply_no_content([?SERVER_HEADER], Req);
        not_found ->
            ?reply_not_found([?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?reply_internal_error([?SERVER_HEADER], Req);
        {error, timeout} ->
            ?reply_timeout([?SERVER_HEADER], Req)
    end;

%% @doc HEAD operation on buckets.
%% @private
invoke(?HTTP_HEAD, Req, Key, #req_params{token_length  = 1,
                                         access_key_id = AccessKeyId}) ->
    case leo_gateway_s3_bucket:head_bucket(AccessKeyId, Key) of
        ok ->
            ?reply_ok([?SERVER_HEADER], Req);
        not_found ->
            ?reply_not_found([?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?reply_internal_error([?SERVER_HEADER], Req);
        {error, timeout} ->
            ?reply_timeout([?SERVER_HEADER], Req)
    end;

%% ---------------------------------------------------------------------
%% For OBJECT-OPERATION
%% ---------------------------------------------------------------------
%% @doc GET operation on Object with Range Header.
%% @private
invoke(?HTTP_GET, Req, Key, #req_params{is_dir       = false,
                                        range_header = RangeHeader}) when RangeHeader /= undefined ->
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
            ?reply_bad_range([?SERVER_HEADER], Req);
        _ ->
            case leo_gateway_rpc_handler:get(Key, Start, End) of
                {ok, _Meta, RespObject} ->
                    Mime = leo_mime:guess_mime(Key),
                    Req2 = cowboy_req:set_resp_body(RespObject, Req),
                    ?reply_partial_content([?SERVER_HEADER,
                                            {?HTTP_HEAD_CONTENT_TYPE,  Mime}],
                                           Req2);
                {error, not_found} ->
                    ?reply_not_found([?SERVER_HEADER], Req);
                {error, ?ERR_TYPE_INTERNAL_ERROR} ->
                    ?reply_internal_error([?SERVER_HEADER], Req);
                {error, timeout} ->
                    ?reply_timeout([?SERVER_HEADER], Req)
            end
    end;

%% @doc GET operation on Object if inner cache is enabled.
%% @private
invoke(?HTTP_GET = HTTPMethod, Req, Key, #req_params{is_dir = false,
                                                     is_cached = true,
                                                     has_inner_cache = true} = Params) ->
    case ecache_api:get(Key) of
        not_found ->
            invoke(HTTPMethod, Req, Key, Params#req_params{is_cached = false});
        {ok, CachedObj} ->
            Cached = binary_to_term(CachedObj),
            get_obj_with_etag(Req, Key, Params, Cached)
    end;

%% @doc GET operation on Object.
%% @private
invoke(?HTTP_GET, Req, Key, #req_params{is_dir = false,
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
invoke(?HTTP_HEAD, Req, Key, _Params) ->
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
invoke(?HTTP_DELETE, Req, Key, _Params) ->
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
invoke(?HTTP_PUT, Req, Key, Params) ->
    put1(get_header(Req, ?HTTP_HEAD_X_AMZ_META_DIRECTIVE), Req, Key, Params);

%% @doc invalid request.
%% @private
invoke(_, Req, _, _) ->
    ?reply_bad_request([?SERVER_HEADER], Req).


%% @doc GET operation with Etag
%% @private
get_obj_with_etag(Req, Key, #req_params{is_dir = false, has_inner_cache = true}, Cached) ->
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
    end;

%% @doc POST/PUT operation on Objects. COPY/REPLACE
%% @private
put1(Directive, Req, Key, _Params) ->
    CS = get_header(Req, ?HTTP_HEAD_X_AMZ_COPY_SOURCE),

    %% need to trim head '/' when cooperating with s3fs(-c)
    CS2 = case binary:part(CS, {0, 1}) of
              ?BIN_SLASH ->
                  binary:part(CS, {1, byte_size(CS) -1});
              _ ->
                  CS
          end,

    case leo_gateway_rpc_handler:get(CS2) of
        {ok, Meta, RespObject} ->
            put2(Directive, Req, Key, Meta, RespObject);
        {error, not_found} ->
            ?reply_not_found([?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?reply_internal_error([?SERVER_HEADER], Req);
        {error, timeout} ->
            ?reply_timeout([?SERVER_HEADER], Req)
    end.

%% @doc POST/PUT operation on Objects. COPY
%% @private
put2(Directive, Req, Key, Meta, Bin) ->
    Size = size(Bin),

    case leo_gateway_rpc_handler:put(Key, Bin, Size) of
        {ok, _ETag} when Directive == ?HTTP_HEAD_X_AMZ_META_DIRECTIVE_COPY ->
            resp_copy_obj_xml(Req, Meta);
        {ok, _ETag} when Directive == ?HTTP_HEAD_X_AMZ_META_DIRECTIVE_REPLACE ->
            put3(Req, Key, Meta);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?reply_internal_error([?SERVER_HEADER], Req);
        {error, timeout} ->
            ?reply_timeout([?SERVER_HEADER], Req)
    end.


%% @doc POST/PUT operation on Objects. REPLACE
%% @private
put3(Req, Key, Meta) ->
    KeyList = binary_to_list(Key),
    case KeyList == Meta#metadata.key of
        true  -> resp_copy_obj_xml(Req, Meta);
        false -> put4(Req, Meta)
    end.

put4(Req, Meta) ->
    KeyBin = list_to_binary(Meta#metadata.key),
    case leo_gateway_rpc_handler:delete(KeyBin) of
        ok ->
            resp_copy_obj_xml(Req, Meta);
        {error, not_found} ->
            resp_copy_obj_xml(Req, Meta);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?reply_internal_error([?SERVER_HEADER], Req);
        {error, timeout} ->
            ?reply_timeout([?SERVER_HEADER], Req)
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


%% @doc getter helper function. return "" if specified header is undefined
%% @private
get_header(Req, Key) ->
    case cowboy_req:header(Key, Req) of
        {undefined, _} ->
            ?BIN_EMPTY;
        {Bin, _} ->
            Bin
    end.


%% @doc
%% @private
resp_copy_obj_xml(Req, Meta) ->
    XML = io_lib:format(?XML_COPY_OBJ_RESULT,
                        [leo_http:web_date(Meta#metadata.timestamp),
                         leo_hex:integer_to_hex(Meta#metadata.checksum, 32)]),
    Req2 = cowboy_req:set_resp_body(XML, Req),
    ?reply_ok([?SERVER_HEADER,
               {?HTTP_HEAD_CONTENT_TYPE, ?HTTP_CTYPE_XML}
              ], Req2).


%% @doc Authentication
%% @private
auth1(Req, HTTPMethod, Path, TokenLen) when TokenLen =< 1 ->
    auth2(Req, HTTPMethod, Path, TokenLen);
auth1(Req, ?HTTP_POST = HTTPMethod, Path, TokenLen) when TokenLen > 1 ->
    auth2(Req, HTTPMethod, Path, TokenLen);
auth1(Req, ?HTTP_PUT = HTTPMethod, Path, TokenLen) when TokenLen > 1 ->
    auth2(Req, HTTPMethod, Path, TokenLen);
auth1(Req, ?HTTP_DELETE = HTTPMethod, Path, TokenLen) when TokenLen > 1 ->
    auth2(Req, HTTPMethod, Path, TokenLen);
auth1(_,_,_,_) ->
    {ok, []}.

auth2(Req, HTTPMethod, Path, TokenLen) ->
    %% bucket operations must be needed to auth
    %% AND alter object operations as well
    case cowboy_req:header(?HTTP_HEAD_AUTHORIZATION, Req) of
        {undefined, _} ->
            {error, undefined};
        {AuthorizationBin, _} ->
            Bucket = case (TokenLen >= 1) of
                         true  -> hd(leo_misc:binary_tokens(Path, ?BIN_SLASH));
                         false -> ?BIN_EMPTY
                     end,

            IsCreateBucketOp = (TokenLen == 1 andalso HTTPMethod == ?HTTP_PUT),
            {RawUri,       _} = cowboy_req:path(Req),
            {QueryString0, _} = cowboy_req:qs(Req),
            {Headers,      _} = cowboy_req:headers(Req),

            Len = byte_size(QueryString0),
            QueryString1 =
                case (Len > 0 andalso binary:last(QueryString0) == $=) of
                    true ->
                        binary:part(QueryString0, 0, Len-1);
                    false ->
                        QueryString0
                end,

            QueryString2 = case binary:match(QueryString1, <<"&">>) of
                               nomatch -> QueryString1;
                               _ ->
                                   Ret = lists:foldl(
                                           fun(Q, []) ->
                                                   Q;
                                              (Q, Acc) ->
                                                   lists:append([Acc, "&", Q])
                                           end, [], lists:sort(string:tokens(binary_to_list(QueryString1), "&"))),
                                   list_to_binary(Ret)
                           end,

            URI = case (Len > 0) of
                      true when  QueryString2 == ?HTTP_QS_BIN_UPLOADS ->
                          << RawUri/binary, "?", QueryString2/binary >>;
                      true ->
                          case (nomatch /= binary:match(QueryString2, ?HTTP_QS_BIN_UPLOAD_ID)) of
                              true  -> << RawUri/binary, "?", QueryString2/binary >>;
                              false -> RawUri
                          end;
                      _ ->
                          RawUri
                  end,

            SignParams = #sign_params{http_verb    = HTTPMethod,
                                      content_md5  = get_header(Req, ?HTTP_HEAD_CONTENT_MD5),
                                      content_type = get_header(Req, ?HTTP_HEAD_CONTENT_TYPE),
                                      date         = get_header(Req, ?HTTP_HEAD_DATE),
                                      bucket       = Bucket,
                                      uri          = URI,
                                      query_str    = QueryString2,
                                      amz_headers  = leo_http:get_amz_headers4cow(Headers)},
            leo_s3_auth:authenticate(AuthorizationBin, SignParams, IsCreateBucketOp)
    end.


%% @doc Generate an update-initiate xml
%% @private
-spec(gen_upload_initiate_xml(binary(), list(binary()), string()) ->
             list()).
gen_upload_initiate_xml(Bucket0, Path, UploadId) ->
    Bucket1 = binary_to_list(Bucket0),
    Key = gen_upload_key(Path),
    io_lib:format(?XML_UPLOAD_INITIATION, [Bucket1, Key, UploadId]).


%% @doc Generate an update-completion xml
%% @private
-spec(gen_upload_completion_xml(binary(), list(binary()), string()) ->
             list()).
gen_upload_completion_xml(Bucket0, Path, ETag) ->
    Bucket1 = binary_to_list(Bucket0),
    Key = gen_upload_key(Path),
    io_lib:format(?XML_UPLOAD_COMPLETION, [Bucket1, Key, ETag]).


%% @doc Generate an upload-key
%% @private
gen_upload_key(Path) ->
    Key = lists:foldl(fun(I, [])  -> binary_to_list(I);
                         (I, Acc) -> Acc ++ "/" ++ binary_to_list(I)
                      end, [], Path),
    Key.

