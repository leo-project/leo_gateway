%%======================================================================
%%
%% Leo S3 Handler
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
%% Leo Gateway S3-API
%% @doc
%% @end
%%======================================================================
-module(leo_gateway_s3_api).

-author('Yosuke Hara').
-author('Yoshiyuki Kanno').

-behaviour(leo_gateway_http_behaviour).

-export([start/2, stop/0,
         init/3, handle/2, terminate/3]).
-export([onrequest/1, onresponse/1]).
-export([get_bucket/3, put_bucket/3, delete_bucket/3, head_bucket/3,
         get_object/3, put_object/3, delete_object/3, head_object/3,
         get_object_with_cache/4, range_object/3
        ]).

-include("leo_gateway.hrl").
-include("leo_http.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("leo_s3_libs/include/leo_s3_auth.hrl").
-include_lib("leo_s3_libs/include/leo_s3_bucket.hrl").
-include_lib("leo_s3_libs/include/leo_s3_endpoint.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-compile({inline, [handle/2, handle_1/4, handle_2/6,
                   handle_multi_upload_1/5, handle_multi_upload_2/4, handle_multi_upload_3/3,
                   gen_upload_key/1, gen_upload_initiate_xml/3, gen_upload_completion_xml/4,
                   resp_copy_obj_xml/2, request_params/2, auth/5, auth/7, auth/8,
                   get_bucket_1/6, put_bucket_1/3, delete_bucket_1/2, head_bucket_1/2
                  ]}).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
start(Sup, HttpOptions) ->
    leo_gateway_http_commons:start(Sup, HttpOptions).

stop() ->
    cowboy:stop_listener(?MODULE),
    cowboy:stop_listener(list_to_atom(lists:append([?MODULE_STRING, "_ssl"]))),
    ok.

%% @doc Initializer
init({_Any, http}, Req, Opts) ->
    {ok, Req, Opts}.


%% @doc Handle a request
%% @callback
handle(Req, State) ->
    {Bucket, Key} = gen_key(Req),
    handle_1(Req, State, Bucket, Key).

%% @doc Terminater
terminate(_Reason, _Req, _State) ->
    ok.


%%--------------------------------------------------------------------
%% Callbacks from Cowboy
%%--------------------------------------------------------------------
%% @doc Handle request
%%
onrequest(CacheCondition) ->
    leo_gateway_http_commons:onrequest(CacheCondition, fun gen_key/1).

%% @doc Handle response
%%
onresponse(CacheCondition) ->
    leo_gateway_http_commons:onresponse(CacheCondition, fun gen_key/1).


%% ---------------------------------------------------------------------
%% Callbacks from HTTP-Handler
%% ---------------------------------------------------------------------
%% ---------------------------------------------------------------------
%% For BUCKET-OPERATION
%% ---------------------------------------------------------------------
%% @doc GET buckets and dirs
-spec(get_bucket(any(), binary(), #req_params{}) ->
             {ok, any()}).
get_bucket(Req, Key, #req_params{access_key_id = AccessKeyId,
                                 is_acl        = false,
                                 qs_prefix     = Prefix}) ->
    Marker = case cowboy_req:qs_val(?HTTP_QS_BIN_MARKER, Req) of
                 {undefined, _} -> [];
                 {Val0,      _} -> Val0
             end,
    MaxKeys = case cowboy_req:qs_val(?HTTP_QS_BIN_MAXKEYS, Req) of
                  {undefined, _} -> 1000;
                  {Val1,      _} -> list_to_integer(binary_to_list(Val1))
              end,

    case get_bucket_1(AccessKeyId, Key, none, Marker, MaxKeys, Prefix) of
        {ok, Meta, XML} when is_list(Meta) == true ->
            Header = [?SERVER_HEADER,
                      {?HTTP_HEAD_RESP_CONTENT_TYPE, ?HTTP_CTYPE_XML}],
            ?reply_ok(Header, XML, Req);
        {error, not_found} ->
            ?reply_not_found([?SERVER_HEADER], Key, <<>>, Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?reply_internal_error([?SERVER_HEADER], Key, <<>>, Req);
        {error, timeout} ->
            ?reply_timeout([?SERVER_HEADER], Key, <<>>, Req)
    end;
get_bucket(Req, Bucket1, #req_params{access_key_id = _AccessKeyId,
                                     is_acl        = true}) ->
    Bucket2 = formalize_bucket(Bucket1),
    case leo_s3_bucket:find_bucket_by_name(Bucket2) of
        {ok, BucketInfo} ->
            XML = generate_acl_xml(BucketInfo),
            Header = [?SERVER_HEADER,
                      {?HTTP_HEAD_RESP_CONTENT_TYPE, ?HTTP_CTYPE_XML}],
            ?reply_ok(Header, XML, Req);
        not_found ->
            ?reply_not_found([?SERVER_HEADER], Bucket2, <<>>, Req);
        {error, _Cause} ->
            ?reply_internal_error([?SERVER_HEADER], Bucket2, <<>>, Req)
    end.

%% @doc Put a bucket
-spec(put_bucket(any(), binary(), #req_params{}) ->
             {ok, any()}).
put_bucket(Req, Key, #req_params{access_key_id = AccessKeyId,
                                 is_acl        = false}) ->
    Bucket = formalize_bucket(Key),
    CannedACL = string:to_lower(binary_to_list(?http_header(Req, ?HTTP_HEAD_X_AMZ_ACL))),
    case put_bucket_1(CannedACL, AccessKeyId, Bucket) of
        ok ->
            ?reply_ok([?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?reply_internal_error([?SERVER_HEADER], Key, <<>>, Req);
        {error, timeout} ->
            ?reply_timeout([?SERVER_HEADER], Key, <<>>, Req)
    end;
put_bucket(Req, Key, #req_params{access_key_id = AccessKeyId,
                                 is_acl        = true}) ->
    Bucket = formalize_bucket(Key),
    CannedACL = string:to_lower(binary_to_list(?http_header(Req, ?HTTP_HEAD_X_AMZ_ACL))),
    case put_bucket_acl_1(CannedACL, AccessKeyId, Bucket) of
        ok ->
            ?reply_ok([?SERVER_HEADER], Req);
        {error, not_supported} ->
            ?reply_bad_request([?SERVER_HEADER], ?XML_ERROR_CODE_InvalidArgument,
                               ?XML_ERROR_MSG_InvalidArgument, Key, <<>>, Req);
        {error, invalid_access} ->
            ?reply_bad_request([?SERVER_HEADER], ?XML_ERROR_CODE_AccessDenied,
                               ?XML_ERROR_MSG_AccessDenied, Key, <<>>, Req);
        {error, _} ->
            ?reply_internal_error([?SERVER_HEADER], Key, <<>>, Req)
    end.


%% @doc Remove a bucket
-spec(delete_bucket(any(), binary(), #req_params{}) ->
             {ok, any()}).
delete_bucket(Req, Key, #req_params{access_key_id = AccessKeyId}) ->
    case delete_bucket_1(AccessKeyId, Key) of
        ok ->
            ?reply_no_content([?SERVER_HEADER], Req);
        not_found ->
            ?reply_not_found([?SERVER_HEADER], Key, <<>>, Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?reply_internal_error([?SERVER_HEADER], Key, <<>>, Req);
        {error, timeout} ->
            ?reply_timeout([?SERVER_HEADER], Key, <<>>, Req)
    end.


%% @doc Retrieve a bucket-info
-spec(head_bucket(any(), binary(), #req_params{}) ->
             {ok, any()}).
head_bucket(Req, Key, #req_params{access_key_id = AccessKeyId}) ->
    case head_bucket_1(AccessKeyId, Key) of
        ok ->
            ?reply_ok([?SERVER_HEADER], Req);
        not_found ->
            ?reply_not_found_without_body([?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?reply_internal_error_without_body([?SERVER_HEADER], Req);
        {error, timeout} ->
            ?reply_timeout_without_body([?SERVER_HEADER], Req)
    end.


%% ---------------------------------------------------------------------
%% For OBJECT-OPERATION
%% ---------------------------------------------------------------------
%% @doc GET operation on Objects
-spec(get_object(any(), binary(), #req_params{}) ->
             {ok, any()}).
get_object(Req, Key, Params) ->
    leo_gateway_http_commons:get_object(Req, Key, Params).


%% @doc GET operation on Objects
-spec(get_object_with_cache(any(), binary(), #cache{}, #req_params{}) ->
             {ok, any()}).
get_object_with_cache(Req, Key, CacheObj, Params) ->
    leo_gateway_http_commons:get_object_with_cache(Req, Key, CacheObj,  Params).


%% @doc utility func for getting x-amz-meta-directive correctly
get_x_amz_meta_directive(Req) ->
    Directive = ?http_header(Req, ?HTTP_HEAD_X_AMZ_META_DIRECTIVE),
    get_x_amz_meta_directive(Req, Directive).
get_x_amz_meta_directive(Req, ?BIN_EMPTY) ->
    CS = ?http_header(Req, ?HTTP_HEAD_X_AMZ_COPY_SOURCE),
    case CS of
        ?BIN_EMPTY -> ?BIN_EMPTY;
        _ -> ?HTTP_HEAD_X_AMZ_META_DIRECTIVE_COPY %% default copy
    end;
get_x_amz_meta_directive(_Req, Other) ->
    Other.

%% @doc POST/PUT operation on Objects
-spec(put_object(any(), binary(), #req_params{}) ->
             {ok, any()}).
put_object(Req, Key, Params) ->
    put_object(get_x_amz_meta_directive(Req), Req, Key, Params).

put_object(?BIN_EMPTY, Req, Key, Params) ->
    {Size, _} = cowboy_req:body_length(Req),

    case (Size >= Params#req_params.threshold_of_chunk_len) of
        true when Size >= Params#req_params.max_len_of_obj ->
            ?reply_bad_request([?SERVER_HEADER], ?XML_ERROR_CODE_EntityTooLarge,
                               ?XML_ERROR_MSG_EntityTooLarge, Key, <<>>, Req);
        true when Params#req_params.is_upload == false ->
            leo_gateway_http_commons:put_large_object(Req, Key, Size, Params);
        false ->
            Ret = case cowboy_req:has_body(Req) of
                      true ->
                          case cowboy_req:body(Req) of
                              {ok, Bin, Req1} ->
                                  {ok, {Size, Bin, Req1}};
                              {error, Cause} ->
                                  {error, Cause}
                          end;
                      false ->
                          {ok, {0, ?BIN_EMPTY, Req}}
                  end,
            leo_gateway_http_commons:put_small_object(Ret, Key, Params)
    end;

%% @doc POST/PUT operation on Objects. COPY/REPLACE
%% @private
put_object(Directive, Req, Key, #req_params{handler = ?HTTP_HANDLER_S3} = Params) ->
    CS = ?http_header(Req, ?HTTP_HEAD_X_AMZ_COPY_SOURCE),

    %% need to trim head '/' when cooperating with s3fs(-c)
    CS2 = case binary:part(CS, {0, 1}) of
              ?BIN_SLASH ->
                  binary:part(CS, {1, byte_size(CS) -1});
              _ ->
                  CS
          end,

    case leo_gateway_rpc_handler:get(CS2) of
        {ok, #?METADATA{cnumber = 0} = Meta, RespObject} ->
            put_object_1(Directive, Req, Key, Meta, RespObject);
        {ok, #?METADATA{cnumber = _TotalChunkedObjs} = Meta, _RespObject} ->
            put_large_object_1(Directive, Req, Key, Meta, Params);
        {error, not_found} ->
            ?reply_not_found([?SERVER_HEADER], Key, <<>>, Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?reply_internal_error([?SERVER_HEADER], Key, <<>>, Req);
        {error, timeout} ->
            ?reply_timeout([?SERVER_HEADER], Key, <<>>, Req)
    end.

%% @doc POST/PUT operation on Objects. COPY
%% @private
put_object_1(Directive, Req, Key, Meta, Bin) ->
    Size = size(Bin),

    case leo_gateway_rpc_handler:put(Key, Bin, Size) of
        {ok, _ETag} when Directive == ?HTTP_HEAD_X_AMZ_META_DIRECTIVE_COPY ->
            resp_copy_obj_xml(Req, Meta);
        {ok, _ETag} when Directive == ?HTTP_HEAD_X_AMZ_META_DIRECTIVE_REPLACE ->
            put_object_2(Req, Key, Meta);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?reply_internal_error([?SERVER_HEADER], Key, <<>>, Req);
        {error, timeout} ->
            ?reply_timeout([?SERVER_HEADER], Key, <<>>, Req)
    end.

%% @doc POST/PUT operation on Objects. REPLACE
%% @private
put_object_2(Req, Key, Meta) ->
    case Key == Meta#?METADATA.key of
        true  -> resp_copy_obj_xml(Req, Meta);
        false -> put_object_3(Req, Meta)
    end.

put_object_3(Req, Meta) ->
    case leo_gateway_rpc_handler:delete(Meta#?METADATA.key) of
        ok ->
            resp_copy_obj_xml(Req, Meta);
        {error, not_found} ->
            resp_copy_obj_xml(Req, Meta);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?reply_internal_error([?SERVER_HEADER], Meta#?METADATA.key, <<>>, Req);
        {error, timeout} ->
            ?reply_timeout([?SERVER_HEADER], Meta#?METADATA.key, <<>>, Req)
    end.

%% @doc POST/PUT operation on `Large` Objects. COPY
%% @private
put_large_object_1(Directive, Req, Key, Meta, Params) ->
    case leo_gateway_http_commons:move_large_object(Meta, Key, Params) of
        ok when Directive == ?HTTP_HEAD_X_AMZ_META_DIRECTIVE_COPY ->
            resp_copy_obj_xml(Req, Meta);
        ok when Directive == ?HTTP_HEAD_X_AMZ_META_DIRECTIVE_REPLACE ->
            put_large_object_2(Req, Key, Meta);
        {error, timeout} ->
            ?reply_timeout([?SERVER_HEADER], Key, <<>>, Req);
        {error, _Other} ->
            ?reply_internal_error([?SERVER_HEADER], Key, <<>>, Req)
    end.

%% @doc POST/PUT operation on Objects. REPLACE
%% @private
put_large_object_2(Req, Key, Meta) ->
    case Key == Meta#?METADATA.key of
        true  -> resp_copy_obj_xml(Req, Meta);
        false -> put_large_object_3(Req, Meta)
    end.

put_large_object_3(Req, Meta) ->
    case leo_gateway_large_object_handler:delete_chunked_objects(
           Meta#?METADATA.key, Meta#?METADATA.cnumber) of
        ok ->
            resp_copy_obj_xml(Req, Meta);
        {error, not_found} ->
            resp_copy_obj_xml(Req, Meta);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?reply_internal_error([?SERVER_HEADER], Meta#?METADATA.key, <<>>, Req);
        {error, timeout} ->
            ?reply_timeout([?SERVER_HEADER], Meta#?METADATA.key, <<>>, Req)
    end.

%% @doc DELETE operation on Objects
-spec(delete_object(any(), binary(), #req_params{}) ->
             {ok, any()}).
delete_object(Req, Key, Params) ->
    leo_gateway_http_commons:delete_object(Req, Key, Params).


%% @doc HEAD operation on Objects
-spec(head_object(any(), binary(), #req_params{}) ->
             {ok, any()}).
head_object(Req, Key, Params) ->
    leo_gateway_http_commons:head_object(Req, Key, Params).


%% @doc RANGE-Query operation on Objects
-spec(range_object(any(), binary(), #req_params{}) ->
             {ok, any()}).
range_object(Req, Key, Params) ->
    leo_gateway_http_commons:range_object(Req, Key, Params).


%% ---------------------------------------------------------------------
%% Inner Functions
%% ---------------------------------------------------------------------
%% @doc Create a key
%% @private
gen_key(Req) ->
    EndPoints_2 = case leo_s3_endpoint:get_endpoints() of
                      {ok, EndPoints_1} ->
                          lists:map(fun(#endpoint{endpoint = EP}) ->
                                            EP
                                    end, EndPoints_1);
                      _ -> []
                  end,
    {Host,    _} = cowboy_req:host(Req),
    {RawPath, _} = cowboy_req:path(Req),
    Path = cowboy_http:urldecode(RawPath),
    leo_http:key(EndPoints_2, Host, Path).


%% @doc Handle an http-request
%% @private
handle_1(Req, [{NumOfMinLayers, NumOfMaxLayers}, HasInnerCache, Props] = State, Bucket, Path) ->
    BinPart    = binary:part(Path, {byte_size(Path)-1, 1}),
    TokenLen   = length(binary:split(Path, [?BIN_SLASH], [global, trim])),
    HTTPMethod = cowboy_req:get(method, Req),

    {Prefix, IsDir, Path2, Req2} =
        case cowboy_req:qs_val(?HTTP_HEAD_PREFIX, Req) of
            {undefined, Req1} ->
                {none, (TokenLen == 1 orelse ?BIN_SLASH == BinPart), Path, Req1};
            {BinParam, Req1} ->
                NewPath = case BinPart of
                              ?BIN_SLASH -> Path;
                              _Else      -> <<Path/binary, ?BIN_SLASH/binary>>
                          end,
                {BinParam, true, NewPath, Req1}
        end,

    IsACL = case cowboy_req:qs_val(?HTTP_QS_BIN_ACL, Req2) of
                {undefined, _} -> false;
                _ -> true
            end,

    ReqParams =
        request_params(
          Req2, #req_params{handler           = ?MODULE,
                            path              = Path2,
                            bucket            = Bucket,
                            token_length      = TokenLen,
                            min_layers        = NumOfMinLayers,
                            max_layers        = NumOfMaxLayers,
                            qs_prefix         = Prefix,
                            has_inner_cache   = HasInnerCache,
                            is_cached         = true,
                            is_dir            = IsDir,
                            is_acl            = IsACL,
                            max_chunked_objs  = Props#http_options.max_chunked_objs,
                            max_len_of_obj    = Props#http_options.max_len_of_obj,
                            chunked_obj_len   = Props#http_options.chunked_obj_len,
                            reading_chunked_obj_len = Props#http_options.reading_chunked_obj_len,
                            threshold_of_chunk_len  = Props#http_options.threshold_of_chunk_len}),
    AuthRet = auth(Req2, HTTPMethod, Path2, TokenLen, ReqParams),
    handle_2(AuthRet, Req2, HTTPMethod, Path2, ReqParams, State).

%% @doc Handle a request (sub)
%% @private
handle_2({error, not_found}, Req,_,Key,_,State) ->
    {ok, Req1} = ?reply_not_found([?SERVER_HEADER], Key, <<>>, Req),
    {ok, Req1, State};
handle_2({error, _Cause}, Req,_,Key,_,State) ->
    {ok, Req1} = ?reply_forbidden([?SERVER_HEADER], Key, <<>>, Req),
    {ok, Req1, State};

%% For Multipart Upload - Initiation
%%
handle_2({ok,_AccessKeyId}, Req1, ?HTTP_POST, _, #req_params{path = Path1,
                                                             is_upload = true}, State) ->
    %% remove a registered object with 'touch-command'
    %% from the cache
    _ = leo_cache_api:delete(Path1),
    %% Insert a metadata into the storage-cluster
    NowBin = list_to_binary(integer_to_list(leo_date:now())),
    UploadId    = leo_hex:binary_to_hex(crypto:hash(md5, << Path1/binary, NowBin/binary >>)),
    UploadIdBin = list_to_binary(UploadId),

    {ok, Req2} =
        case leo_gateway_rpc_handler:put(<<Path1/binary, ?STR_NEWLINE, UploadIdBin/binary>>, <<>>, 0) of
            {ok, _ETag} ->
                %% Response xml to a client
                [Bucket|Path2] = leo_misc:binary_tokens(Path1, ?BIN_SLASH),
                XML = gen_upload_initiate_xml(Bucket, Path2, UploadId),

                ?reply_ok([?SERVER_HEADER], XML, Req1);
            {error, timeout} ->
                ?reply_timeout([?SERVER_HEADER], Path1, <<>>, Req1);
            {error, Cause} ->
                ?error("handle_2/6", "path:~s, cause:~p", [binary_to_list(Path1), Cause]),
                ?reply_internal_error([?SERVER_HEADER], Path1, <<>>, Req1)
        end,
    {ok, Req2, State};

%% For Multipart Upload - Upload a part of an object
%%
handle_2({ok,_AccessKeyId}, Req1, ?HTTP_PUT, Key,
         #req_params{upload_id = UploadId,
                     upload_part_num  = PartNum,
                     max_chunked_objs = MaxChunkedObjs}, State) when UploadId /= <<>>,
                                                                     PartNum > MaxChunkedObjs ->
    {ok, Req2} = ?reply_bad_request([?SERVER_HEADER],
                                    ?XML_ERROR_CODE_EntityTooLarge,
                                    ?XML_ERROR_MSG_EntityTooLarge,
                                    Key, <<>>, Req1),
    {ok, Req2, State};

handle_2({ok,_AccessKeyId}, Req1, ?HTTP_PUT, _,
         #req_params{path = Path,
                     is_upload = false,
                     upload_id = UploadId,
                     upload_part_num = PartNum1} = Params, State) when UploadId /= <<>>,
                                                                       PartNum1 /= 0 ->
    PartNum2 = list_to_binary(integer_to_list(PartNum1)),
    Key1 = << Path/binary, ?STR_NEWLINE, UploadId/binary >>, %% for confirmation
    Key2 = << Path/binary, ?STR_NEWLINE, PartNum2/binary >>, %% for put a part of an object

    {ok, Req2} =
        case leo_gateway_rpc_handler:head(Key1) of
            {ok, _Metadata} ->
                put_object(?BIN_EMPTY, Req1, Key2, Params);
            {error, not_found} ->
                ?reply_not_found([?SERVER_HEADER], Path, <<>>, Req1);
            {error, timeout} ->
                ?reply_timeout([?SERVER_HEADER], Path, <<>>, Req1);
            {error, ?ERR_TYPE_INTERNAL_ERROR} ->
                ?reply_internal_error([?SERVER_HEADER], Path, <<>>, Req1)
        end,
    {ok, Req2, State};

handle_2({ok,_AccessKeyId}, Req1, ?HTTP_DELETE, _,
         #req_params{path = Path,
                     upload_id = UploadId}, State) when UploadId /= <<>> ->
    _ = leo_gateway_rpc_handler:put(Path, <<>>, 0),
    _ = leo_gateway_rpc_handler:delete(Path),
    {ok, Req2} = ?reply_no_content([?SERVER_HEADER], Req1),
    {ok, Req2, State};

%% For Multipart Upload - Completion
%%
handle_2({ok,_AccessKeyId}, Req1, ?HTTP_POST, _,
         #req_params{path = Path,
                     chunked_obj_len = CL,
                     is_upload = false,
                     upload_id = UploadId,
                     upload_part_num = PartNum}, State) when UploadId /= <<>>,
                                                             PartNum  == 0 ->
    Res = cowboy_req:has_body(Req1),
    {ok, Req2} = handle_multi_upload_1(Res, Req1, Path, UploadId, CL),
    {ok, Req2, State};

%% For Regular cases
%%
handle_2({ok, AccessKeyId}, Req1, ?HTTP_POST,  Path, Params, State) ->
    handle_2({ok, AccessKeyId}, Req1, ?HTTP_PUT,  Path, Params, State);

handle_2({ok, AccessKeyId}, Req1, HTTPMethod, Path, Params, State) ->
    case catch leo_gateway_http_req_handler:handle(
                 HTTPMethod, Req1, Path, Params#req_params{access_key_id = AccessKeyId}) of
        {'EXIT', Cause} ->
            ?error("handle_2/6", "path:~s, cause:~p", [binary_to_list(Path), Cause]),
            {ok, Req2} = ?reply_internal_error([?SERVER_HEADER], Path, <<>>, Req1),
            {ok, Req2, State};
        {ok, Req2} ->
            Req3 = cowboy_req:compact(Req2),
            {ok, Req3, State}
    end.


%% @doc Handle multi-upload processing
%% @private
handle_multi_upload_1(true, Req, Path, UploadId, CL) ->
    Path4Conf = << Path/binary, ?STR_NEWLINE, UploadId/binary >>,

    case leo_gateway_rpc_handler:head(Path4Conf) of
        {ok, _} ->
            _ = leo_gateway_rpc_handler:delete(Path4Conf),

            Ret = cowboy_req:body(Req),
            handle_multi_upload_2(Ret, Req, Path, CL);
        _ ->
            ?reply_forbidden([?SERVER_HEADER], Path, <<>>, Req)
    end;
handle_multi_upload_1(false, Req, Path,_UploadId, _CL) ->
    ?reply_forbidden([?SERVER_HEADER], Path, <<>>, Req).

handle_multi_upload_2({ok, Bin, Req}, _Req, Path1, CL) ->
    %% trim spaces
    Acc = fun(#xmlText{value = " ", pos = P}, Acc, S) ->
                  {Acc, P, S};
             (X, Acc, S) ->
                  {[X|Acc], S}
          end,
    {#xmlElement{content = Content},_} = xmerl_scan:string(
                                           binary_to_list(Bin),
                                           [{space,normalize}, {acc_fun, Acc}]),
    TotalUploadedObjs = length(Content),

    case handle_multi_upload_3(TotalUploadedObjs, Path1, []) of
        {ok, {Len, ETag1}} ->
            case leo_gateway_rpc_handler:put(Path1, <<>>, Len, CL, TotalUploadedObjs, ETag1) of
                {ok, _} ->
                    [Bucket|Path2] = leo_misc:binary_tokens(Path1, ?BIN_SLASH),
                    ETag2 = leo_hex:integer_to_hex(ETag1, 32),
                    XML   = gen_upload_completion_xml(Bucket, Path2, ETag2, TotalUploadedObjs),
                    ?reply_ok([?SERVER_HEADER], XML, Req);
                {error, Cause} ->
                    ?error("handle_multi_upload_2/4", "path:~s, cause:~p",
                           [binary_to_list(Path1), Cause]),
                    ?reply_internal_error([?SERVER_HEADER], Path1, <<>>, Req)
            end;
        {error, Cause} ->
            ?error("handle_multi_upload_2/4", "path:~s, cause:~p", [binary_to_list(Path1), Cause]),
            ?reply_internal_error([?SERVER_HEADER], Path1, <<>>, Req)
    end;
handle_multi_upload_2({error, Cause}, Req, Path, _CL) ->
    ?error("handle_multi_upload_2/4", "path:~s, cause:~p", [binary_to_list(Path), Cause]),
    ?reply_internal_error([?SERVER_HEADER], Path, <<>>, Req).

%% @doc Retrieve Metadatas for uploaded objects (Multipart)
%% @private
-spec(handle_multi_upload_3(integer(), binary(), list(tuple())) ->
             {ok, tuple()} | {error, any()}).
handle_multi_upload_3(0, _, Acc) ->
    Metas = lists:reverse(Acc),
    {Len, ETag1} = lists:foldl(
                     fun({_, {DSize, Checksum}}, {Sum, ETagBin1}) ->
                             ETagBin2 = leo_hex:integer_to_raw_binary(Checksum),
                             {Sum + DSize, <<ETagBin1/binary, ETagBin2/binary>>}
                     end, {0, <<>>}, lists:sort(Metas)),
    ETag2 = leo_hex:hex_to_integer(leo_hex:binary_to_hex(crypto:hash(md5, ETag1))),
    {ok, {Len, ETag2}};

handle_multi_upload_3(PartNum, Path, Acc) ->
    PartNumBin = list_to_binary(integer_to_list(PartNum)),
    Key = << Path/binary, ?STR_NEWLINE, PartNumBin/binary  >>,

    case leo_gateway_rpc_handler:head(Key) of
        {ok, #?METADATA{dsize = Len,
                        checksum = Checksum}} ->
            handle_multi_upload_3(PartNum - 1, Path, [{PartNum, {Len, Checksum}} | Acc]);
        Error ->
            Error
    end.

%% @doc Generate an upload-key
%% @private
gen_upload_key(Path) ->
    Key = lists:foldl(fun(I, [])  -> binary_to_list(I);
                         (I, Acc) -> Acc ++ "/" ++ binary_to_list(I)
                      end, [], Path),
    Key.

%% @doc Generate an update-initiate xml
%% @private
-spec(gen_upload_initiate_xml(binary(), list(binary()), string()) ->
             list()).
gen_upload_initiate_xml(BucketBin, Path, UploadId) ->
    Bucket = binary_to_list(BucketBin),
    Key = gen_upload_key(Path),
    io_lib:format(?XML_UPLOAD_INITIATION, [Bucket, Key, UploadId]).


%% @doc Generate an update-completion xml
%% @private
-spec(gen_upload_completion_xml(binary(), list(binary()), string(), integer()) ->
             list()).
gen_upload_completion_xml(BucketBin, Path, ETag, Total) ->
    Bucket = binary_to_list(BucketBin),
    TotalStr = integer_to_list(Total),
    Key = gen_upload_key(Path),
    io_lib:format(?XML_UPLOAD_COMPLETION, [Bucket, Key, ETag, TotalStr]).


%% @doc Generate copy-obj's xml
%% @private
resp_copy_obj_xml(Req, Meta) ->
    XML = io_lib:format(?XML_COPY_OBJ_RESULT,
                        [leo_http:web_date(Meta#?METADATA.timestamp),
                         leo_hex:integer_to_hex(Meta#?METADATA.checksum, 32)]),
    ?reply_ok([?SERVER_HEADER,
               {?HTTP_HEAD_RESP_CONTENT_TYPE, ?HTTP_CTYPE_XML}
              ], XML, Req).


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


%% @doc check if bucket is public-read
is_public_read([]) ->
    false;
is_public_read([H|Rest]) ->
    #bucket_acl_info{user_id = UserId, permissions = Permissions} = H,
    case UserId == ?GRANTEE_ALL_USER andalso
        (Permissions == [read] orelse Permissions == [read, write]) of
        true ->
            true;
        false ->
            is_public_read(Rest)
    end.

is_public_read_write([]) ->
    false;
is_public_read_write([H|Rest]) ->
    #bucket_acl_info{user_id = UserId, permissions = Permissions} = H,
    case UserId == ?GRANTEE_ALL_USER andalso
        (Permissions == [read, write]) of
        true ->
            true;
        false ->
            is_public_read_write(Rest)
    end.


%% @doc Authentication
%% @private
auth(Req, HTTPMethod, Path, TokenLen, ReqParams) ->
    Bucket = case (TokenLen >= 1) of
                 true  -> hd(leo_misc:binary_tokens(Path, ?BIN_SLASH));
                 false -> ?BIN_EMPTY
             end,
    case leo_s3_bucket:get_acls(Bucket) of
        {ok, ACLs} ->
            auth(Req, HTTPMethod, Path, TokenLen, Bucket, ACLs, ReqParams);
        not_found ->
            auth(Req, HTTPMethod, Path, TokenLen, Bucket, [], ReqParams);
        {error, Cause} ->
            {error, Cause}
    end.

auth(Req, HTTPMethod, Path, TokenLen, Bucket, ACLs, ReqParams) when TokenLen =< 1 ->
    auth(next, Req, HTTPMethod, Path, TokenLen, Bucket, ACLs, ReqParams);
auth(Req, HTTPMethod, Path, TokenLen, Bucket, ACLs, ReqParams) when TokenLen > 1 andalso
                                                                    (HTTPMethod == ?HTTP_POST orelse
                                                                     HTTPMethod == ?HTTP_PUT orelse
                                                                     HTTPMethod == ?HTTP_DELETE) ->
    case is_public_read_write(ACLs) of
        true ->
            {ok, []};
        false ->
            auth(next, Req, HTTPMethod, Path, TokenLen, Bucket, ACLs, ReqParams)
    end;
auth(Req, HTTPMethod, Path, TokenLen, Bucket, ACLs, ReqParams) when TokenLen > 1 ->
    %% handle HTTP_GET|HTTP_HEAD
    case is_public_read(ACLs) of
        true ->
            {ok, []};
        false ->
            auth(next, Req, HTTPMethod, Path, TokenLen, Bucket, ACLs, ReqParams)
    end.

auth(next, Req, HTTPMethod, _Path, TokenLen, Bucket, _ACLs, #req_params{is_acl = IsACL}) ->
    %% bucket operations must be needed to auth
    %% AND alter object operations as well
    case cowboy_req:header(?HTTP_HEAD_AUTHORIZATION, Req) of
        {undefined, _} ->
            {error, undefined};
        {AuthorizationBin, _} ->
            IsCreateBucketOp = (TokenLen == 1 andalso HTTPMethod == ?HTTP_PUT andalso not IsACL),
            {RawUri,  _} = cowboy_req:path(Req),
            {QStr1,   _} = cowboy_req:qs(Req),
            {Headers, _} = cowboy_req:headers(Req),

            Len = byte_size(QStr1),
            QStr2 = case (Len > 0 andalso binary:last(QStr1) == $=) of
                        true ->
                            binary:part(QStr1, 0, Len-1);
                        false ->
                            QStr1
                    end,
            QStr3 = case binary:match(QStr2, <<"&">>) of
                        nomatch -> QStr2;
                        _ ->
                            Ret = lists:foldl(fun(Q, []) ->
                                                      Q;
                                                 (Q, Acc) ->
                                                      lists:append([Acc, "&", Q])
                                              end, [],
                                              lists:sort(string:tokens(binary_to_list(QStr2), "&"))),
                            list_to_binary(Ret)
                    end,

            SignParams = #sign_params{http_verb    = HTTPMethod,
                                      content_md5  = ?http_header(Req, ?HTTP_HEAD_CONTENT_MD5),
                                      content_type = ?http_header(Req, ?HTTP_HEAD_CONTENT_TYPE),
                                      date         = ?http_header(Req, ?HTTP_HEAD_DATE),
                                      bucket       = Bucket,
                                      uri          = RawUri,
                                      query_str    = QStr3,
                                      amz_headers  = leo_http:get_amz_headers4cow(Headers)},
            leo_s3_auth:authenticate(AuthorizationBin, SignParams, IsCreateBucketOp)
    end.


%% @doc Get bucket list
%% @private
%% @see http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketGET.html
-spec(get_bucket_1(string(), none, char()|none, string()|none, integer(), string()|none) ->
             {ok, list(), string()}|{error, any()}).
get_bucket_1(AccessKeyId, <<>>, Delimiter, Marker, MaxKeys, none) ->
    get_bucket_1(AccessKeyId, <<"/">>, Delimiter, Marker, MaxKeys, none);

get_bucket_1(AccessKeyId, <<"/">>, _Delimiter, _Marker, _MaxKeys, none) ->
    case leo_s3_bucket:find_buckets_by_id(AccessKeyId) of
        {ok, Meta} when is_list(Meta) =:= true ->
            {ok, Meta, generate_bucket_xml(Meta)};
        not_found ->
            {ok, [], generate_bucket_xml([])};
        Error ->
            Error
    end;

get_bucket_1(_AccessKeyId, Bucket, Delimiter, Marker, MaxKeys, Prefix1) ->
    Prefix2 = case Prefix1 of
                  none -> <<>>;
                  _    -> Prefix1
              end,

    {ok, #redundancies{nodes = Redundancies}} =
        leo_redundant_manager_api:get_redundancies_by_key(get, Bucket),
    Key = << Bucket/binary, Prefix2/binary >>,

    case leo_gateway_rpc_handler:invoke(Redundancies,
                                        leo_storage_handler_directory,
                                        find_by_parent_dir,
                                        [Key, Delimiter, Marker, MaxKeys],
                                        []) of
        {ok, Meta} when is_list(Meta) =:= true ->
            {ok, Meta, generate_bucket_xml(Key, Prefix2, Meta, MaxKeys)};
        {ok, _} ->
            {error, invalid_format};
        Error ->
            Error
    end.


%% @doc Put a bucket
%% @private
%% @see http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketPUT.html
-spec(put_bucket_1(string(), string(), string()|none) ->
             ok|{error, any()}).
put_bucket_1([], AccessKeyId, Bucket) ->
    leo_s3_bucket:put(AccessKeyId, Bucket);
put_bucket_1(CannedACL, AccessKeyId, Bucket) ->
    leo_s3_bucket:put(AccessKeyId, Bucket, CannedACL).

%% @doc Put a bucket ACL
%% @private
%% @see http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTacl.html
-spec(put_bucket_acl_1(string(), string(), string()|none) ->
             ok|{error, any()}).
put_bucket_acl_1(?CANNED_ACL_PRIVATE, AccessKeyId, Bucket) ->
    leo_s3_bucket:update_acls2private(AccessKeyId, Bucket);
put_bucket_acl_1(?CANNED_ACL_PUBLIC_READ, AccessKeyId, Bucket) ->
    leo_s3_bucket:update_acls2public_read(AccessKeyId, Bucket);
put_bucket_acl_1(?CANNED_ACL_PUBLIC_READ_WRITE, AccessKeyId, Bucket) ->
    leo_s3_bucket:update_acls2public_read_write(AccessKeyId, Bucket);
put_bucket_acl_1(_, _AccessKeyId, _Bucket) ->
    {error, not_supported}.

%% @doc Delete a bucket
%% @private
%% @see http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketDELETE.html
-spec(delete_bucket_1(string(), string()|none) ->
             ok|{error, any()}).
delete_bucket_1(AccessKeyId, Bucket1) ->
    Bucket2 = formalize_bucket(Bucket1),
    ManagerNodes = ?env_manager_nodes(leo_gateway),
    delete_bucket_2(ManagerNodes, AccessKeyId, Bucket2).

delete_bucket_2([],_,_) ->
    {error, ?ERR_TYPE_INTERNAL_ERROR};
delete_bucket_2([Node|Rest], AccessKeyId, Bucket) ->
    Node_1 = case is_list(Node) of
                 true  -> list_to_atom(Node);
                 false -> Node
             end,
    case rpc:call(Node_1, leo_manager_api, delete_bucket,
                  [AccessKeyId, Bucket], ?DEF_TIMEOUT) of
        ok ->
            ok;
        {error, not_found} ->
            not_found;
        {_, Cause} ->
            ?warn("delete_bucket_2/3", "cause:~p", [Cause]),
            delete_bucket_2(Rest, AccessKeyId, Bucket)
    end.


%% @doc Head a bucket
%% @private
%% @see http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketHEAD.html
-spec(head_bucket_1(string(), string()|none) ->
             ok|{error, any()}).
head_bucket_1(AccessKeyId, Bucket) ->
    leo_s3_bucket:head(AccessKeyId, Bucket).


%% @doc Generate XML from matadata-list
%% @private
generate_bucket_xml(KeyBin, PrefixBin, MetadataList, MaxKeys) ->
    Len    = byte_size(KeyBin),
    Key    = binary_to_list(KeyBin),
    Prefix = binary_to_list(PrefixBin),
    TruncatedStr = case length(MetadataList) =:= MaxKeys of
                       true -> "true";
                       false -> "false"
                   end,

    Fun = fun(#?METADATA{key       = EntryKeyBin,
                         dsize     = Length,
                         timestamp = TS,
                         checksum  = CS,
                         del       = 0} , {Acc, _NextMarker}) ->
                  EntryKey = binary_to_list(EntryKeyBin),

                  case string:equal(Key, EntryKey) of
                      true ->
                          {Acc, _NextMarker};
                      false ->
                          Entry = string:sub_string(EntryKey, Len + 1),
                          case Length of
                              -1 ->
                                  %% directory.
                                  {lists:append([Acc,
                                                 "<CommonPrefixes><Prefix>",
                                                 xmerl_lib:export_text(Prefix),
                                                 xmerl_lib:export_text(Entry),
                                                 "</Prefix></CommonPrefixes>"]),
                                   EntryKeyBin};
                              _ ->
                                  %% file.
                                  {lists:append([Acc,
                                                 "<Contents>",
                                                 "<Key>",
                                                 xmerl_lib:export_text(Prefix),
                                                 xmerl_lib:export_text(Entry),
                                                 "</Key>",
                                                 "<LastModified>", leo_http:web_date(TS),
                                                 "</LastModified>",
                                                 "<ETag>", leo_hex:integer_to_hex(CS, 32),
                                                 "</ETag>",
                                                 "<Size>", integer_to_list(Length),
                                                 "</Size>",
                                                 "<StorageClass>STANDARD</StorageClass>",
                                                 "<Owner>",
                                                 "<ID>leofs</ID>",
                                                 "<DisplayName>leofs</DisplayName>",
                                                 "</Owner>",
                                                 "</Contents>"]),
                                   EntryKeyBin}
                          end
                  end
          end,
    {List, NextMarker}= lists:foldl(Fun, {[], <<>>}, MetadataList),
    io_lib:format(?XML_OBJ_LIST,
                  [xmerl_lib:export_text(Prefix),
                   xmerl_lib:export_text(NextMarker),
                   integer_to_list(MaxKeys), TruncatedStr, List]).

generate_bucket_xml(MetadataList) ->
    Fun = fun(#?BUCKET{name = BucketBin,
                       created_at = CreatedAt} , Acc) ->
                  Bucket = binary_to_list(BucketBin),
                  case string:equal(?STR_SLASH, Bucket) of
                      true ->
                          Acc;
                      false ->
                          lists:append([Acc,
                                        "<Bucket><Name>",
                                        xmerl_lib:export_text(Bucket),
                                        "</Name>",
                                        "<CreationDate>", leo_http:web_date(CreatedAt),
                                        "</CreationDate></Bucket>"])
                  end
          end,
    io_lib:format(?XML_BUCKET_LIST, [lists:foldl(Fun, [], MetadataList)]).

generate_acl_xml(#?BUCKET{access_key_id = ID, acls = ACLs}) ->
    Fun = fun(#bucket_acl_info{user_id     = URI,
                               permissions = Permissions} , Acc) ->
                  lists:foldl(
                    fun(read, Acc2) ->
                            Acc3 = lists:append([
                                                 Acc2,
                                                 io_lib:format(?XML_ACL_GRANT, [URI, ?acl_read])]),
                            lists:append([
                                          Acc3,
                                          io_lib:format(?XML_ACL_GRANT, [URI, ?acl_read_acp])]);
                       (write, Acc2) ->
                            Acc3 = lists:append([
                                                 Acc2,
                                                 io_lib:format(?XML_ACL_GRANT, [URI, ?acl_write])]),
                            lists:append([
                                          Acc3,
                                          io_lib:format(?XML_ACL_GRANT, [URI, ?acl_write_acp])]);
                       (full_control, Acc2) ->
                            lists:append([
                                          Acc2,
                                          io_lib:format(?XML_ACL_GRANT, [URI, ?acl_full_control])])
                    end, Acc, Permissions)
          end,
    io_lib:format(?XML_ACL_POLICY, [ID, ID, lists:foldl(Fun, [], ACLs)]).

formalize_bucket(Bucket1) ->
    case (binary:last(Bucket1) == $/) of
        true ->
            binary:part(Bucket1, {0, byte_size(Bucket1) - 1});
        false ->
            Bucket1
    end.
