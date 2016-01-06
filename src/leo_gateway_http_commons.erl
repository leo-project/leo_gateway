%%======================================================================
%%
%% Leo Gateway
%%
%% Copyright (c) 2012-2015 Rakuten, Inc.
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
%% Leo Gateway - HTTP Commons
%% @doc
%% @end
%%======================================================================
-module(leo_gateway_http_commons).

-author('Yosuke Hara').

-include("leo_gateway.hrl").
-include("leo_http.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start/1, start/2]).
-export([onrequest/2, onresponse/2]).
-export([get_object/3, get_object_with_cache/4,
         put_object/3, put_small_object/3, put_large_object/4, move_large_object/3,
         delete_object/3, head_object/3,
         range_object/3]).

-record(req_large_obj, {
          handler :: pid(),
          bucket  :: binary(),
          key     :: binary(),
          length  :: pos_integer(),
          timeout_for_body     :: pos_integer(),
          chunked_size         :: pos_integer(),
          reading_chunked_size :: pos_integer()
         }).

-record(transport_record, {
          transport :: module(),
          socket    :: inet:socket(),
          sending_chunked_obj_len :: pos_integer()
         }).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
-spec(start(#http_options{}) ->
             ok).
start(#http_options{handler                = Handler,
                    port                   = Port,
                    ssl_port               = SSLPort,
                    ssl_certfile           = SSLCertFile,
                    ssl_keyfile            = SSLKeyFile,
                    num_of_acceptors       = NumOfAcceptors,
                    max_keepalive          = MaxKeepAlive,
                    headers_config_file    = CustomHeaderConf,
                    timeout_for_header     = Timeout4Header,
                    sending_chunked_obj_len= SendChunkLen,
                    cache_method           = CacheMethod,
                    cache_expire           = CacheExpire,
                    cache_max_content_len  = CacheMaxContentLen,
                    cachable_content_type  = CachableContentTypes,
                    cachable_path_pattern  = CachablePathPatterns} = Props) ->
    CustomHeaderSettings = case leo_nginx_conf_parser:parse(CustomHeaderConf) of
                               {ok, Ret} ->
                                   Ret;
                               not_found ->
                                   undefined;
                               {error, enoent} ->
                                   undefined;
                               {error, Reason} ->
                                   ?error("start/1", [{simple_cause, "reading http custom header file failed"},
                                                      {cause, Reason}]),
                                   undefined
                           end,
    InternalCache = (CacheMethod == 'inner'),
    Dispatch      = cowboy_router:compile(
                      [{'_', [{'_', Handler,
                               [?env_layer_of_dirs(), InternalCache, CustomHeaderSettings, Props]}]}]),

    Config = case InternalCache of
                 %% Using inner-cache
                 true ->
                     [{env, [{dispatch, Dispatch}]},
                      {max_keepalive, MaxKeepAlive},
                      {timeout, Timeout4Header}];
                 %% Using http-cache (like a varnish/squid)
                 false ->
                     CacheCondition = #cache_condition{expire          = CacheExpire,
                                                       max_content_len = CacheMaxContentLen,
                                                       content_types   = CachableContentTypes,
                                                       path_patterns   = CachablePathPatterns,
                                                       sending_chunked_obj_len = SendChunkLen},
                     [{env,        [{dispatch, Dispatch}]},
                      {max_keepalive, MaxKeepAlive},
                      {onrequest,     Handler:onrequest(CacheCondition)},
                      {onresponse,    Handler:onresponse(CacheCondition)},
                      {timeout, Timeout4Header}]
             end,

    {ok, _Pid1}= cowboy:start_http(Handler, NumOfAcceptors,
                                   [{port, Port}], Config),
    {ok, _Pid2}= cowboy:start_https(list_to_atom(lists:append([atom_to_list(Handler), "_ssl"])),
                                    NumOfAcceptors,
                                    [{port,     SSLPort},
                                     {certfile, SSLCertFile},
                                     {keyfile,  SSLKeyFile}],
                                    Config),
    ok.

%% @doc Launch http handler
%%
-spec(start(atom(), #http_options{}) ->
             ok).
start(Sup, Options) ->
    %% launch Cowboy
    ChildSpec1 = {cowboy_sup,
                  {cowboy_sup, start_link, []},
                  permanent, ?SHUTDOWN_WAITING_TIME, supervisor, [cowboy_sup]},
    {ok, _} = supervisor:start_child(Sup, ChildSpec1),

    %% launch http-handler(s)
    start(Options).


%% @doc Handle request
%%
-spec(onrequest(#cache_condition{}, function()) ->
             any()).
onrequest(#cache_condition{expire = Expire, sending_chunked_obj_len = SendChunkLen}, FunGenKey) ->
    fun(Req) ->
            Method = cowboy_req:get(method, Req),
            onrequest_1(Method, Req, Expire, FunGenKey, SendChunkLen)
    end.

onrequest_1(?HTTP_GET, Req, Expire, FunGenKey, SendChunkLen) ->
    {_Bucket, Key} = FunGenKey(Req),
    Ret = (catch leo_cache_api:get(Key)),
    onrequest_2(Req, Expire, Key, Ret, SendChunkLen);
onrequest_1(_, Req,_,_,_) ->
    Req.

onrequest_2(Req,_Expire,_Key, not_found, _) ->
    Req;
onrequest_2(Req,_Expire,_Key, {'EXIT', _Cause}, _) ->
    Req;
onrequest_2(Req, Expire, Key, {ok, CachedObj}, SendChunkLen) ->
    #cache{mtime        = MTime,
           content_type = ContentType,
           etag         = Checksum,
           body         = Body,
           size         = Size} = binary_to_term(CachedObj),

    Now = leo_date:now(),
    Diff = Now - MTime,

    case (Diff > Expire) of
        true ->
            _ = (catch leo_cache_api:delete(Key)),
            Req;
        false ->
            LastModified = leo_http:rfc1123_date(MTime),
            Header = [?SERVER_HEADER,
                      {?HTTP_HEAD_RESP_LAST_MODIFIED, LastModified},
                      {?HTTP_HEAD_RESP_CONTENT_TYPE,  ContentType},
                      {?HTTP_HEAD_RESP_AGE,           integer_to_list(Diff)},
                      {?HTTP_HEAD_RESP_ETAG,          ?http_etag(Checksum)},
                      {?HTTP_HEAD_RESP_CACHE_CTRL,    ?httP_cache_ctl(Expire)}],

            IMSSec = case cowboy_req:parse_header(?HTTP_HEAD_IF_MODIFIED_SINCE, Req) of
                         {ok, undefined, _} ->
                             0;
                         {ok, IMSDateTime, _} ->
                             calendar:datetime_to_gregorian_seconds(IMSDateTime)
                     end,
            case IMSSec of
                MTime ->
                    {ok, Req2} = ?reply_not_modified(Header, Req),
                    Req2;
                _ ->
                    BodyFunc = fun(Socket, Transport) ->
                                       leo_net:chunked_send(Transport, Socket, Body, SendChunkLen)
                               end,
                    {ok, Req2} = ?reply_ok([?SERVER_HEADER], {Size, BodyFunc}, Req),
                    Req2
            end
    end.


%% @doc Handle response
%%
-spec(onresponse(#cache_condition{}, function()) ->
             any()).
onresponse(#cache_condition{expire = Expire} = Config, FunGenKey) ->
    fun(?HTTP_ST_OK, Header1, Body, Req) ->
            case cowboy_req:get(method, Req) of
                ?HTTP_GET ->
                    {_Bucket, Key} = FunGenKey(Req),

                    case lists:all(fun(Fun) ->
                                           Fun(Key, Config, Header1, Body)
                                   end, [fun is_cachable_req1/4,
                                         fun is_cachable_req2/4,
                                         fun is_cachable_req3/4]) of
                        true ->
                            Now = leo_date:now(),
                            Bin = term_to_binary(
                                    #cache{mtime = Now,
                                           etag  = leo_hex:raw_binary_to_integer(crypto:hash(md5, Body)),
                                           size  = byte_size(Body),
                                           body  = Body,
                                           content_type = ?http_content_type(Header1)}),
                            _ = (catch leo_cache_api:put(Key, Bin)),

                            Header2 = lists:keydelete(?HTTP_HEAD_LAST_MODIFIED, 1, Header1),
                            Header3 = [{?HTTP_HEAD_RESP_CACHE_CTRL,    ?httP_cache_ctl(Expire)},
                                       {?HTTP_HEAD_RESP_LAST_MODIFIED, leo_http:rfc1123_date(Now)}
                                       |Header2],
                            {ok, Req2} = ?reply_ok(Header3, Req),
                            Req2;
                        false ->
                            cowboy_req:set_resp_body(<<>>, Req)
                    end;
                _ ->
                    cowboy_req:set_resp_body(<<>>, Req)
            end
    end.


%%--------------------------------------------------------------------
%% Commons Request Handlers
%%--------------------------------------------------------------------
%% @doc GET an object
-spec(get_object(cowboy_req:req(), binary(), #req_params{}) ->
             {ok, cowboy_req:req()}).
get_object(Req, Key, #req_params{bucket                 = Bucket,
                                 custom_header_settings = CustomHeaderSettings,
                                 has_inner_cache        = HasInnerCache,
                                 sending_chunked_obj_len= SendChunkLen}) ->
    case leo_gateway_rpc_handler:get(Key) of
        %% For regular case (NOT a chunked object)
        {ok, #?METADATA{cnumber = 0} = Meta, RespObject} ->
            Mime = leo_mime:guess_mime(Key),

            case HasInnerCache of
                true ->
                    Val = term_to_binary(#cache{etag  = Meta#?METADATA.checksum,
                                                mtime = Meta#?METADATA.timestamp,
                                                content_type = Mime,
                                                body = RespObject,
                                                size = byte_size(RespObject)}),
                    catch leo_cache_api:put(Key, Val);
                false ->
                    void
            end,

            ?access_log_get(Bucket, Key, Meta#?METADATA.dsize, ?HTTP_ST_OK),
            Headers = [?SERVER_HEADER,
                       {?HTTP_HEAD_RESP_CONTENT_TYPE,  Mime},
                       {?HTTP_HEAD_RESP_ETAG,          ?http_etag(Meta#?METADATA.checksum)},
                       {?HTTP_HEAD_RESP_LAST_MODIFIED, ?http_date(Meta#?METADATA.timestamp)}],
            {ok, CustomHeaders} = leo_nginx_conf_parser:get_custom_headers(Key, CustomHeaderSettings),
            Headers2 = Headers ++ CustomHeaders,
            BodyFunc = fun(Socket, Transport) ->
                               leo_net:chunked_send(Transport, Socket, RespObject, SendChunkLen)
                       end,
            ?reply_ok(Headers2, {Meta#?METADATA.dsize, BodyFunc}, Req);

        %% For a chunked object.
        {ok, #?METADATA{cnumber = TotalChunkedObjs} = Meta, _RespObject} ->
            Mime = leo_mime:guess_mime(Key),
            Headers = [?SERVER_HEADER,
                       {?HTTP_HEAD_RESP_CONTENT_TYPE,  Mime},
                       {?HTTP_HEAD_RESP_ETAG,          ?http_etag(Meta#?METADATA.checksum)},
                       {?HTTP_HEAD_RESP_LAST_MODIFIED, ?http_date(Meta#?METADATA.timestamp)}],
            {ok, CustomHeaders} = leo_nginx_conf_parser:get_custom_headers(Key, CustomHeaderSettings),
            Headers2 = Headers ++ CustomHeaders,
            BodyFunc = fun(Socket, Transport) ->
                               {ok, Pid} = leo_large_object_get_handler:start_link({Key, Transport, Socket, SendChunkLen}),
                               try
                                   leo_large_object_get_handler:get(Pid, TotalChunkedObjs, Req, Meta),
                                   ok
                               after
                                   ?access_log_get(Bucket, Key, Meta#?METADATA.dsize, 0),
                                   catch leo_large_object_get_handler:stop(Pid)
                               end
                       end,
            cowboy_req:reply(?HTTP_ST_OK, Headers2, {Meta#?METADATA.dsize, BodyFunc}, Req);
        {error, unavailable} ->
            ?reply_service_unavailable_error([?SERVER_HEADER], Key, <<>>, Req);
        {error, not_found} ->
            ?access_log_get(Bucket, Key, 0, ?HTTP_ST_NOT_FOUND),
            ?reply_not_found([?SERVER_HEADER], Key, <<>>, Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?access_log_get(Bucket, Key, 0, ?HTTP_ST_INTERNAL_ERROR),
            ?reply_internal_error([?SERVER_HEADER], Key, <<>>, Req);
        {error, timeout} ->
            ?access_log_get(Bucket, Key, 0, ?HTTP_ST_GATEWAY_TIMEOUT),
            ?reply_timeout([?SERVER_HEADER], Key, <<>>, Req)
    end.


%% @doc GET an object with Etag
-spec(get_object_with_cache(cowboy_req:req(), binary(), #cache{}, #req_params{}) ->
             {ok, cowboy_req:req()}).
get_object_with_cache(Req, Key, CacheObj, #req_params{bucket                 = Bucket,
                                                      custom_header_settings = CustomHeaderSettings,
                                                      sending_chunked_obj_len= SendChunkLen}) ->
    case leo_gateway_rpc_handler:get(Key, CacheObj#cache.etag) of
        %% HIT: get an object from disc-cache
        {ok, match} when CacheObj#cache.file_path /= [] ->
            ?access_log_get(Bucket, Key, CacheObj#cache.size, ?HTTP_ST_OK),
            Headers = [?SERVER_HEADER,
                       {?HTTP_HEAD_RESP_CONTENT_TYPE,   CacheObj#cache.content_type},
                       {?HTTP_HEAD_RESP_ETAG,           ?http_etag(CacheObj#cache.etag)},
                       {?HTTP_HEAD_RESP_LAST_MODIFIED,  leo_http:rfc1123_date(CacheObj#cache.mtime)},
                       {?HTTP_HEAD_X_FROM_CACHE,        <<"True/via disk">>}],
            {ok, CustomHeaders} = leo_nginx_conf_parser:get_custom_headers(Key, CustomHeaderSettings),
            Headers2 = Headers ++ CustomHeaders,
            BodyFunc = fun(Socket, _Transport) ->
                               file:sendfile(CacheObj#cache.file_path, Socket, 0, 0, [{chunk_size, SendChunkLen}]),
                               ok
                       end,
            cowboy_req:reply(?HTTP_ST_OK, Headers2, {CacheObj#cache.size, BodyFunc}, Req);

        %% HIT: get an object from memory-cache
        {ok, match} when CacheObj#cache.file_path == [] ->
            ?access_log_get(Bucket, Key, CacheObj#cache.size, ?HTTP_ST_OK),
            Headers = [?SERVER_HEADER,
                       {?HTTP_HEAD_RESP_CONTENT_TYPE,  CacheObj#cache.content_type},
                       {?HTTP_HEAD_RESP_ETAG,          ?http_etag(CacheObj#cache.etag)},
                       {?HTTP_HEAD_RESP_LAST_MODIFIED, leo_http:rfc1123_date(CacheObj#cache.mtime)},
                       {?HTTP_HEAD_X_FROM_CACHE,       <<"True/via memory">>}],
            {ok, CustomHeaders} = leo_nginx_conf_parser:get_custom_headers(Key, CustomHeaderSettings),
            Headers2 = Headers ++ CustomHeaders,
            BodyFunc = fun(Socket, Transport) ->
                               leo_net:chunked_send(Transport, Socket, CacheObj#cache.body, SendChunkLen)
                       end,
            ?reply_ok(Headers2, {CacheObj#cache.size, BodyFunc}, Req);

        %% MISS: get an object from storage (small-size)
        {ok, #?METADATA{cnumber = 0} = Meta, RespObject} ->
            Mime = leo_mime:guess_mime(Key),
            Val = term_to_binary(#cache{etag  = Meta#?METADATA.checksum,
                                        mtime = Meta#?METADATA.timestamp,
                                        content_type = Mime,
                                        body = RespObject,
                                        size = byte_size(RespObject)
                                       }),
            catch leo_cache_api:put(Key, Val),

            ?access_log_get(Bucket, Key, Meta#?METADATA.dsize, ?HTTP_ST_OK),
            Headers = [?SERVER_HEADER,
                       {?HTTP_HEAD_RESP_CONTENT_TYPE,  Mime},
                       {?HTTP_HEAD_RESP_ETAG,          ?http_etag(Meta#?METADATA.checksum)},
                       {?HTTP_HEAD_RESP_LAST_MODIFIED, ?http_date(Meta#?METADATA.timestamp)}],
            {ok, CustomHeaders} = leo_nginx_conf_parser:get_custom_headers(Key, CustomHeaderSettings),
            Headers2 = Headers ++ CustomHeaders,
            BodyFunc = fun(Socket, Transport) ->
                               leo_net:chunked_send(Transport, Socket, RespObject, SendChunkLen)
                       end,
            ?reply_ok(Headers2, {Meta#?METADATA.dsize, BodyFunc}, Req);

        %% MISS: get an object from storage (large-size)
        {ok, #?METADATA{cnumber = TotalChunkedObjs} = Meta, _RespObject} ->
            Mime = leo_mime:guess_mime(Key),
            Headers = [?SERVER_HEADER,
                       {?HTTP_HEAD_RESP_CONTENT_TYPE,  Mime},
                       {?HTTP_HEAD_RESP_ETAG,          ?http_etag(Meta#?METADATA.checksum)},
                       {?HTTP_HEAD_RESP_LAST_MODIFIED, ?http_date(Meta#?METADATA.timestamp)}],
            {ok, CustomHeaders} = leo_nginx_conf_parser:get_custom_headers(Key, CustomHeaderSettings),
            Headers2 = Headers ++ CustomHeaders,
            BodyFunc = fun(Socket, Transport) ->
                               {ok, Pid} = leo_large_object_get_handler:start_link({Key, Transport, Socket, SendChunkLen}),
                               try
                                   leo_large_object_get_handler:get(Pid, TotalChunkedObjs, Req, Meta)
                               after
                                   ?access_log_get(Bucket, Key, Meta#?METADATA.dsize, 0),
                                   catch leo_large_object_get_handler:stop(Pid)
                               end
                       end,
            cowboy_req:reply(?HTTP_ST_OK, Headers2, {Meta#?METADATA.dsize, BodyFunc}, Req);
        {error, not_found} ->
            ?access_log_get(Bucket, Key, 0, ?HTTP_ST_NOT_FOUND),
            ?reply_not_found([?SERVER_HEADER], Key, <<>>, Req);
        {error, unavailable} ->
            ?reply_service_unavailable_error([?SERVER_HEADER], Key, <<>>, Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?access_log_get(Bucket, Key, 0, ?HTTP_ST_INTERNAL_ERROR),
            ?reply_internal_error([?SERVER_HEADER], Key, <<>>, Req);
        {error, timeout} ->
            ?access_log_get(Bucket, Key, 0, ?HTTP_ST_GATEWAY_TIMEOUT),
            ?reply_timeout([?SERVER_HEADER], Key, <<>>, Req)
    end.

%% @doc MOVE/COPY an object
-spec(move_large_object(#?METADATA{}, binary(), #req_params{}) ->
             ok | {error, any()}).
move_large_object(#?METADATA{key = Key, cnumber = TotalChunkedObjs} = SrcMeta, DstKey, Params) ->
    {ok, ReadHandler} = leo_large_object_move_handler:start_link(Key, 0, TotalChunkedObjs),
    try
        move_large_object(SrcMeta, DstKey, Params, ReadHandler)
    after
        catch leo_large_object_move_handler:stop(ReadHandler)
    end.

move_large_object(#?METADATA{dsize = Size}, DstKey,
                  #req_params{chunked_obj_len = ChunkedSize, bucket = Bucket},
                  ReadHandler) ->
    {ok, WriteHandler}  = leo_large_object_put_handler:start_link(DstKey, ChunkedSize),
    try
        case move_large_object_1(
               leo_large_object_move_handler:get_chunk_obj(ReadHandler),
               #req_large_obj{handler       = WriteHandler,
                              bucket        = Bucket,
                              key           = DstKey,
                              length        = Size,
                              chunked_size  = ChunkedSize}, ReadHandler) of
            ok ->
                ok;
            {error, Cause} ->
                ok = leo_large_object_put_handler:rollback(WriteHandler),
                {error, Cause}
        end
    after
        catch leo_large_object_put_handler:stop(WriteHandler)
    end.

move_large_object_1({ok, Data},
                    #req_large_obj{key = Key,
                                   handler = WriteHandler} = ReqLargeObj, ReadHandler) ->
    case catch leo_large_object_put_handler:put(WriteHandler, Data) of
        ok ->
            move_large_object_1(
              leo_large_object_move_handler:get_chunk_obj(ReadHandler),
              ReqLargeObj, ReadHandler);
        {'EXIT', Cause} ->
            ?error("move_large_object_1/3",
                   [{key, binary_to_list(Key)}, {cause, Cause}]),
            {error, ?ERROR_FAIL_PUT_OBJ};
        {error, Cause} ->
            ?error("move_large_object_1/3", [{key, binary_to_list(Key)},
                                             {cause, Cause}]),
            {error, ?ERROR_FAIL_PUT_OBJ}
    end;
move_large_object_1({error, Cause},
                    #req_large_obj{key = Key}, _) ->
    ?error("move_large_object_1/3", [{key, binary_to_list(Key)},
                                     {cause, Cause}]),
    {error, ?ERROR_FAIL_RETRIEVE_OBJ};
move_large_object_1(done, #req_large_obj{handler = WriteHandler,
                                         bucket = Bucket,
                                         key = Key,
                                         length = Size,
                                         chunked_size = ChunkedSize}, _) ->
    case catch leo_large_object_put_handler:result(WriteHandler) of
        {ok, #large_obj_info{length = TotalSize,
                             num_of_chunks = TotalChunks,
                             md5_context   = Digest}} when Size == TotalSize ->
            Digest_1 = leo_hex:raw_binary_to_integer(Digest),

            case leo_gateway_rpc_handler:put(Key, ?BIN_EMPTY, Size,
                                             ChunkedSize, TotalChunks, Digest_1) of
                {ok, _ETag} ->
                    ?access_log_put(Bucket, Key, Size, ?HTTP_ST_OK),
                    ok;
                {error, timeout = Cause} ->
                    {error, Cause};
                {error, unavailable} ->
                    {error, unavailable};
                {error, _Cause} ->
                    {error, ?ERROR_FAIL_PUT_OBJ}
            end;
        {ok, _} ->
            {error, ?ERROR_NOT_MATCH_LENGTH};
        {_,_Cause} ->
            {error, ?ERROR_FAIL_PUT_OBJ}
    end.

%% @doc PUT an object
-spec(put_object(cowboy_req:req(), binary(), #req_params{}) ->
             {ok, cowboy_req:req()}).
put_object(Req, Key, #req_params{bucket = Bucket,
                                 is_upload = IsUpload,
                                 timeout_for_body = Timeout4Body,
                                 max_len_of_obj = MaxLenForObj,
                                 threshold_of_chunk_len = ThresholdObjLen} = Params) ->
    {Size, _} = cowboy_req:body_length(Req),
    case (Size >= ThresholdObjLen) of
        true when Size >= MaxLenForObj ->
            ?access_log_put(Bucket, Key, 0, ?HTTP_ST_BAD_REQ),
            ?reply_bad_request([?SERVER_HEADER],
                               ?XML_ERROR_CODE_EntityTooLarge,
                               ?XML_ERROR_MSG_EntityTooLarge,
                               Key, <<>>, Req);

        true when IsUpload == false ->
            put_large_object(Req, Key, Size, Params);

        false ->
            Ret = case cowboy_req:has_body(Req) of
                      true ->
                          BodyOpts = [{read_timeout, Timeout4Body}],
                          case cowboy_req:body(Req, BodyOpts) of
                              {ok, Bin0, Req0} ->
                                  {ok, {Size, Bin0, Req0}};
                              {error, Cause} ->
                                  {error, Cause}
                          end;
                      false ->
                          {ok, {0, ?BIN_EMPTY, Req}}
                  end,
            put_small_object(Ret, Key, Params)
    end.

%% @doc check if a specified binary contains a character
%% @private
binary_is_contained(<<>>, _Char) ->
    false;
binary_is_contained(<<C:8, Rest/binary>>, Char) ->
    case C of
        Char ->
            true;
        _ ->
            binary_is_contained(Rest, Char)
    end.

%% @doc Put a small object
%% @private
-spec(put_small_object({ok, any()}|{error, any()}, binary(), #req_params{}) ->
             {ok, any()}).
put_small_object({error, Cause},_,_) ->
    {error, Cause};
put_small_object({ok, {Size, Bin, Req}}, Key, #req_params{bucket = Bucket,
                                                          upload_part_num = UploadPartNum,
                                                          has_inner_cache = HasInnerCache
                                                         }) ->
    case leo_gateway_rpc_handler:put(Key, Bin, Size, UploadPartNum) of
        {ok, ETag} ->
            case (HasInnerCache
                  andalso binary_is_contained(Key, 10) == false) of
                true  ->
                    Mime = leo_mime:guess_mime(Key),
                    Val  = term_to_binary(#cache{etag = ETag,
                                                 mtime = leo_date:now(),
                                                 content_type = Mime,
                                                 body = Bin,
                                                 size = byte_size(Bin)
                                                }),
                    catch leo_cache_api:put(Key, Val);
                false ->
                    void
            end,

            ?access_log_put(Bucket, Key, Size, ?HTTP_ST_OK),
            Header = [?SERVER_HEADER,
                      {?HTTP_HEAD_RESP_ETAG, ?http_etag(ETag)}],
            ?reply_ok(Header, Req);
        {error, unavailable} ->
            ?reply_service_unavailable_error([?SERVER_HEADER], Key, <<>>, Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?access_log_put(Bucket, Key, 0, ?HTTP_ST_INTERNAL_ERROR),
            ?reply_internal_error([?SERVER_HEADER], Key, <<>>, Req);
        {error, timeout} ->
            ?access_log_put(Bucket, Key, 0, ?HTTP_ST_GATEWAY_TIMEOUT),
            ?reply_timeout([?SERVER_HEADER], Key, <<>>, Req)
    end.


%% @doc Put a large-object
%% @private
-spec(put_large_object(cowboy_req:req(), binary(), pos_integer(), #req_params{}) ->
             {ok, cowboy_req:req()}).
put_large_object(Req, Key, Size, #req_params{bucket = Bucket,
                                             timeout_for_body = Timeout4Body,
                                             chunked_obj_len = ChunkedSize,
                                             reading_chunked_obj_len = ReadingChunkedSize})->
    %% launch 'large_object_handler'
    {ok, Handler}  = leo_large_object_put_handler:start_link(Key, ChunkedSize),

    %% remove a registered object with 'touch-command'
    %% from the cache
    catch leo_cache_api:delete(Key),

    %% retrieve an object from the stream,
    %% then put it to the storage-cluster
    BodyOpts = [{length, ReadingChunkedSize},
                {read_timeout, Timeout4Body},
                {read_length, ReadingChunkedSize}],
    Reply = case put_large_object_1(cowboy_req:body(Req, BodyOpts),
                                    #req_large_obj{handler = Handler,
                                                   key     = Key,
                                                   length  = Size,
                                                   timeout_for_body = Timeout4Body,
                                                   chunked_size = ChunkedSize,
                                                   reading_chunked_size = ReadingChunkedSize}) of
                {error, ErrorRet} ->
                    ok = leo_large_object_put_handler:rollback(Handler),
                    {Req_1, Cause} = case (erlang:size(ErrorRet) == 2) of
                                         true  -> ErrorRet;
                                         false -> {Req, ErrorRet}
                                     end,
                    case Cause of
                        timeout ->
                            ?reply_timeout([?SERVER_HEADER], Key, <<>>, Req_1);
                        unavailable ->
                            ?reply_service_unavailable_error([?SERVER_HEADER], Key, <<>>, Req_1);
                        _Other  ->
                            ?reply_internal_error([?SERVER_HEADER], Key, <<>>, Req_1)
                    end;
                Ret ->
                    ?access_log_put(Bucket, Key, Size, 0),
                    Ret
            end,
    catch leo_large_object_put_handler:stop(Handler),
    Reply.

%% @private
put_large_object_1({more, Data, Req},
                   #req_large_obj{key = Key,
                                  handler = Handler,
                                  timeout_for_body = Timeout4Body,
                                  reading_chunked_size = ReadingChunkedSize} = ReqLargeObj) ->
    case catch leo_large_object_put_handler:put(Handler, Data) of
        ok ->
            BodyOpts = [{length, ReadingChunkedSize},
                        {read_timeout, Timeout4Body},
                        {read_length, ReadingChunkedSize}],
            put_large_object_1(cowboy_req:body(Req, BodyOpts), ReqLargeObj);
        {'EXIT', Cause} ->
            ?error("put_large_object_1/2", [{key, binary_to_list(Key)},
                                            {cause, Cause}]),
            {error, {Req, ?ERROR_FAIL_PUT_OBJ}};
        {error, Cause} ->
            ?error("put_large_object_1/2", [{key, binary_to_list(Key)},
                                            {cause, Cause}]),
            {error, {Req, ?ERROR_FAIL_PUT_OBJ}}
    end;

%% An error occurred while reading the body, connection is gone.
%% @private
put_large_object_1({error, Cause}, #req_large_obj{key = Key}) ->
    ?error("put_large_object_1/2", [{key, binary_to_list(Key)},
                                    {cause, Cause}]),
    {error, ?ERROR_FAIL_RETRIEVE_OBJ};

%% @private
put_large_object_1({ok, Data, Req}, #req_large_obj{handler = Handler,
                                                   key = Key,
                                                   length = Size,
                                                   chunked_size = ChunkedSize}) ->
    case catch leo_large_object_put_handler:put(Handler, Data) of
        ok ->
            case catch leo_large_object_put_handler:result(Handler) of
                {ok, #large_obj_info{length = TotalSize,
                                     num_of_chunks = TotalChunks,
                                     md5_context   = Digest}} when Size == TotalSize ->
                    Digest_1 = leo_hex:raw_binary_to_integer(Digest),

                    case leo_gateway_rpc_handler:put(Key, ?BIN_EMPTY, Size,
                                                     ChunkedSize, TotalChunks, Digest_1) of
                        {ok, _ETag} ->
                            Header = [?SERVER_HEADER,
                                      {?HTTP_HEAD_RESP_ETAG, ?http_etag(Digest_1)}],
                            ?reply_ok(Header, Req);
                        {error, timeout = Cause} ->
                            {error, {Req, Cause}};
                        {error, unavailable} ->
                            {error, {Req, unavailable}};
                        {error,_Cause} ->
                            {error, {Req, ?ERROR_FAIL_PUT_OBJ}}
                    end;
                {ok, _} ->
                    {error, {Req, ?ERROR_NOT_MATCH_LENGTH}};
                {_,_Cause} ->
                    {error, {Req, ?ERROR_FAIL_PUT_OBJ}}
            end;
        {'EXIT', Cause} ->
            ?error("put_large_object_1/2", [{key, binary_to_list(Key)},
                                            {cause, Cause}]),
            {error, {Req, ?ERROR_FAIL_PUT_OBJ}};
        {error, Cause} ->
            ?error("put_large_object_1/2", [{key, binary_to_list(Key)},
                                            {cause, Cause}]),
            {error, {Req, ?ERROR_FAIL_PUT_OBJ}}
    end.

%% @doc DELETE an object
-spec(delete_object(cowboy_req:req(), binary(), #req_params{}) ->
             {ok, cowboy_req:req()}).
delete_object(Req, Key, #req_params{bucket = Bucket}) ->
    Size1 = case leo_gateway_rpc_handler:head(Key) of
                {ok, #?METADATA{del = 0, dsize = Size}} ->
                    Size;
                _ ->
                    0
            end,

    case leo_gateway_rpc_handler:delete(Key) of
        ok ->
            ?access_log_delete(Bucket, Key, Size1, ?HTTP_ST_NO_CONTENT),
            ?reply_no_content([?SERVER_HEADER], Req);
        {error, not_found} ->
            ?access_log_delete(Bucket, Key, 0, ?HTTP_ST_NOT_FOUND),
            ?reply_no_content([?SERVER_HEADER], Req);
        {error, unavailable} ->
            ?reply_service_unavailable_error([?SERVER_HEADER], Key, <<>>, Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?access_log_delete(Bucket, Key, 0, ?HTTP_ST_INTERNAL_ERROR),
            ?reply_internal_error([?SERVER_HEADER], Key, <<>>, Req);
        {error, timeout} ->
            ?access_log_delete(Bucket, Key, 0, ?HTTP_ST_GATEWAY_TIMEOUT),
            ?reply_timeout([?SERVER_HEADER], Key, <<>>, Req)
    end.


%% @doc HEAD an object
-spec(head_object(cowboy_req:req(), binary(), #req_params{}) ->
             {ok, cowboy_req:req()}).
head_object(Req, Key, #req_params{bucket = Bucket}) ->
    case leo_gateway_rpc_handler:head(Key) of
        {ok, #?METADATA{del = 0} = Meta} ->
            Timestamp = leo_http:rfc1123_date(Meta#?METADATA.timestamp),
            ?access_log_head(Bucket, Key, ?HTTP_ST_OK),
            Headers   = [?SERVER_HEADER,
                         {?HTTP_HEAD_RESP_CONTENT_TYPE,   leo_mime:guess_mime(Key)},
                         {?HTTP_HEAD_RESP_ETAG,           ?http_etag(Meta#?METADATA.checksum)},
                         {?HTTP_HEAD_RESP_CONTENT_LENGTH, erlang:integer_to_list(Meta#?METADATA.dsize)},
                         {?HTTP_HEAD_RESP_LAST_MODIFIED,  Timestamp}],
            cowboy_req:reply(?HTTP_ST_OK, Headers, fun() -> void end, Req);
        {ok, #?METADATA{del = 1}} ->
            ?access_log_head(Bucket, Key, ?HTTP_ST_NOT_FOUND),
            ?reply_not_found_without_body([?SERVER_HEADER], Req);
        {error, not_found} ->
            ?access_log_head(Bucket, Key, ?HTTP_ST_NOT_FOUND),
            ?reply_not_found_without_body([?SERVER_HEADER], Req);
        {error, unavailable} ->
            ?reply_service_unavailable_error([?SERVER_HEADER], Key, <<>>, Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?access_log_head(Bucket, Key, ?HTTP_ST_INTERNAL_ERROR),
            ?reply_internal_error_without_body([?SERVER_HEADER], Req);
        {error, timeout} ->
            ?access_log_head(Bucket, Key, ?HTTP_ST_GATEWAY_TIMEOUT),
            ?reply_timeout_without_body([?SERVER_HEADER], Req)
    end.


%% @doc Retrieve a part of an object
-spec(range_object(cowboy_req:req(), binary(), #req_params{}) ->
             {ok, cowboy_req:req()}).
range_object(Req, Key, #req_params{bucket = Bucket,
                                   range_header = RangeHeader,
                                   sending_chunked_obj_len = SendChunkLen}) ->
    Range = cowboy_http:range(RangeHeader),
    get_range_object(Req, Bucket, Key, Range, SendChunkLen).

get_range_object(Req, Bucket, Key, {error, badarg}, _) ->
    ?access_log_get(Bucket, Key, 0, ?HTTP_ST_BAD_RANGE),
    ?reply_bad_range([?SERVER_HEADER], Key, <<>>, Req);
get_range_object(Req, Bucket, Key, {_Unit, Range}, SendChunkLen) when is_list(Range) ->
    Mime = leo_mime:guess_mime(Key),

    case get_body_length_1(Key, Range) of
        {ok, Length} ->
            Header = [?SERVER_HEADER,
                      {?HTTP_HEAD_RESP_CONTENT_TYPE,  Mime}
                     ],
            Req2 = cowboy_req:set_resp_body_fun(
                     Length,
                     fun(Socket, Transport) ->
                             get_range_object_1(Req, Bucket, Key, Range, undefined,
                                                #transport_record{transport = Transport,
                                                                  socket    = Socket,
                                                                  sending_chunked_obj_len = SendChunkLen})
                     end,
                     Req),
            ?reply_partial_content(Header, Req2);
        {error, bad_range} ->
            ?reply_bad_range([?SERVER_HEADER], Key, <<>>, Req);
        {error, unavailable} ->
            ?reply_service_unavailable_error([?SERVER_HEADER], Key, <<>>, Req);
        {error, timeout} ->
            ?reply_timeout_without_body([?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?reply_internal_error_without_body([?SERVER_HEADER], Req);
        _ ->
            ?reply_not_found_without_body([?SERVER_HEADER], Req)
    end.

get_body_length_1(Key, Range) ->
    case leo_gateway_rpc_handler:head(Key) of
        {ok, #?METADATA{dsize = ObjectSize}} ->
            get_body_length(Range, ObjectSize, 0);
        {error, Reason} ->
            {error, Reason}
    end.

get_body_length([], _ObjectSize, Acc) ->
    {ok, Acc};
get_body_length([{Start, infinity}|Rest], ObjectSize, Acc) ->
    get_body_length(Rest, ObjectSize, Acc + ObjectSize - Start);
get_body_length([{Start, End}|Rest], ObjectSize, Acc) when End < 0 ->
    get_body_length(Rest, ObjectSize, Acc + ObjectSize - Start);
get_body_length([{Start, End}|Rest], ObjectSize, Acc) when End < ObjectSize ->
    get_body_length(Rest, ObjectSize, Acc + End - Start + 1);
get_body_length([End|Rest], ObjectSize, Acc) when End < 0 ->
    get_body_length(Rest, ObjectSize, Acc + ObjectSize);
get_body_length([End|Rest], ObjectSize, Acc) when End < ObjectSize ->
    get_body_length(Rest, ObjectSize, Acc + End + 1);
get_body_length(_, _, _) ->
    {error, bad_range}.

get_range_object_1(_Req, _Bucket, _Key, _, {error, _Reason}, #transport_record{socket = Socket,
                                                                               transport = Transport}) ->
    Transport:close(Socket);
get_range_object_1(Req,_Bucket,_Key, [], _, _TransportRec) ->
    {ok, Req};
get_range_object_1(Req, Bucket, Key, [{Start, infinity}|Rest], _, TransportRec) ->
    Ret = get_range_object_2(Req, Bucket, Key, Start, 0, TransportRec),
    get_range_object_1(Req, Bucket, Key, Rest, Ret, TransportRec);
get_range_object_1(Req, Bucket, Key, [{Start, End}|Rest], _, TransportRec) ->
    Ret = get_range_object_2(Req, Bucket, Key, Start, End, TransportRec),
    get_range_object_1(Req, Bucket, Key, Rest, Ret, TransportRec);
get_range_object_1(Req, Bucket, Key, [End|Rest], _, TransportRec) ->
    Ret = get_range_object_2(Req, Bucket, Key, 0, End, TransportRec),
    get_range_object_1(Req, Bucket, Key, Rest, Ret, TransportRec).

get_range_object_2(Req, Bucket, Key, Start, End, TransportRec) ->
    case leo_gateway_rpc_handler:head(Key) of
        {ok, #?METADATA{del = 0,
                        cnumber = 0}} ->
            get_range_object_small(Req, Bucket, Key, Start, End, TransportRec);
        {ok, #?METADATA{del = 0,
                        cnumber = N,
                        dsize = ObjectSize,
                        csize = CS}} ->
            %% Retrieve start and end position of the object
            {NewStartPos, NewEndPos} = calc_pos(Start, End, ObjectSize),

            %% Retrieve the grand-child's metadata
            %% to get collect chunk size of the object
            {CurPos, Index} = move_current_pos_to_head(NewStartPos, CS, 0, 0),

            IndexBin = list_to_binary(integer_to_list(1)),
            Key_1 = << Key/binary, ?DEF_SEPARATOR/binary, IndexBin/binary >>,
            {CurPos_1, Index_1} =
                case leo_gateway_rpc_handler:head(Key_1) of
                    {ok, #?METADATA{del = 0,
                                    dsize = ChildObjSize}} ->
                        move_current_pos_to_head(NewStartPos, ChildObjSize, 0, 0);
                    _ ->
                        {CurPos, Index}
                end,
            get_range_object_large(Req, Bucket, Key,
                                   NewStartPos, NewEndPos, N, Index_1, CurPos_1,
                                   TransportRec);
        Error ->
            Error
    end.


%% @doc Retrieve the small object
%% @private
get_range_object_small(_Req, Bucket, Key, Start, End, #transport_record{transport = Transport,
                                                                        socket    = Socket,
                                                                        sending_chunked_obj_len = SendChunkLen}) ->
    case leo_gateway_rpc_handler:get(Key, Start, End) of
        {ok, _Meta, <<>>} ->
            ?access_log_get(Bucket, Key, 0, ?HTTP_ST_OK),
            ok;
        {ok, _Meta, Bin} ->
            ?access_log_get(Bucket, Key, byte_size(Bin), ?HTTP_ST_OK),
            case leo_net:chunked_send(Transport, Socket, Bin, SendChunkLen) of
                ok ->
                    ok;
                {error, Cause} ->
                    {error, Cause}
            end;
        {error, Cause} ->
            {error, Cause}
    end.

%% @doc
%% @private
move_current_pos_to_head(Start, ChunkedSize, CurPos, Idx)
  when (CurPos + ChunkedSize - 1) < Start ->
    move_current_pos_to_head(Start, ChunkedSize, CurPos + ChunkedSize, Idx + 1);
move_current_pos_to_head(_Start, _ChunkedSize, CurPos, Idx) ->
    {CurPos, Idx}.


%% @doc
%% @private
calc_pos(_StartPos, EndPos, ObjectSize) when EndPos < 0 ->
    NewStartPos = ObjectSize + EndPos,
    NewEndPos   = ObjectSize - 1,
    {NewStartPos, NewEndPos};
calc_pos(StartPos, 0, ObjectSize) when StartPos > 0 ->
    {StartPos, ObjectSize - 1};
calc_pos(StartPos, EndPos, _ObjectSize) ->
    {StartPos, EndPos}.


%% @doc Retrieve the large object
%% @private
get_range_object_large(_Req,_Bucket,_Key,_Start,_End, _Total, _Index, {error, _} = Error, _TransportRec) ->
    Error;
get_range_object_large(_Req,_Bucket,_Key,_Start,_End, Total, Total, CurPos, _TransportRec) ->
    {ok, CurPos};
get_range_object_large(_Req,_Bucket,_Key,_Start, End,_Total,_Index, CurPos, _TransportRec) when CurPos > End ->
    {ok, CurPos};
get_range_object_large( Req, Bucket, Key, Start, End, Total, Index, CurPos, TransportRec) ->
    IndexBin = list_to_binary(integer_to_list(Index + 1)),
    Key2 = << Key/binary, ?DEF_SEPARATOR/binary, IndexBin/binary >>,

    case leo_gateway_rpc_handler:head(Key2) of
        {ok, #?METADATA{cnumber = 0,
                        dsize = CS}} ->
            %% get and chunk an object
            NewPos = send_chunk(Req, Bucket, Key2, Start, End, CurPos, CS, TransportRec),
            get_range_object_large(Req, Bucket, Key, Start, End, Total, Index + 1, NewPos, TransportRec);

        {ok, #?METADATA{cnumber = GrandChildNum}} ->
            case get_range_object_large(Req, Bucket, Key2, Start, End, GrandChildNum, 0, CurPos, TransportRec) of
                {ok, NewPos} ->
                    get_range_object_large(Req, Bucket, Key, Start, End, Total, Index + 1, NewPos, TransportRec);
                {error, Cause} ->
                    {error, Cause}
            end;
        {error, Cause} ->
            {error, Cause}
    end.

%% @doc
%% @private
send_chunk(_Req,_,_Key, Start,_End, CurPos, ChunkSize, _TransportRec) when (CurPos + ChunkSize - 1) < Start ->
    %% skip proc
    CurPos + ChunkSize;
send_chunk(_Req,_Bucket, Key, Start, End, CurPos, ChunkSize, #transport_record{transport = Transport,
                                                                               socket    = Socket,
                                                                               sending_chunked_obj_len = SendChunkLen}) when CurPos >= Start andalso
                                                                                                                             (CurPos + ChunkSize - 1) =< End ->
    %% whole get
    case leo_gateway_rpc_handler:get(Key) of
        {ok, _Meta, Bin} ->
            %% @FIXME current impl can't handle a file which consist of grand children
            %% ?access_log_get(Bucket, Key, ChunkSize, ?HTTP_ST_OK),
            case leo_net:chunked_send(Transport, Socket, Bin, SendChunkLen) of
                ok ->
                    CurPos + ChunkSize;
                {error, Cause} ->
                    {error, Cause}
            end;
        Error ->
            Error
    end;
send_chunk(_Req, _Bucket, Key, Start, End, CurPos, ChunkSize, #transport_record{transport = Transport,
                                                                                socket    = Socket,
                                                                                sending_chunked_obj_len = SendChunkLen}) ->
    %% partial get
    StartPos = case Start =< CurPos of
                   true -> 0;
                   false -> Start - CurPos
               end,
    EndPos = case (CurPos + ChunkSize - 1) =< End of
                 true -> ChunkSize - 1;
                 false -> End - CurPos
             end,
    case leo_gateway_rpc_handler:get(Key, StartPos, EndPos) of
        {ok, _Meta, <<>>} ->
            CurPos + ChunkSize;
        {ok, _Meta, Bin} ->
            %% @FIXME current impl can't handle a file which consist of grand childs
            %% ?access_log_get(Bucket, Key, ChunkSize, ?HTTP_ST_OK),
            case leo_net:chunked_send(Transport, Socket, Bin, SendChunkLen) of
                ok ->
                    CurPos + ChunkSize;
                {error, Cause} ->
                    {error, Cause}
            end;
        {error, Cause} ->
            {error, Cause}
    end.


%%--------------------------------------------------------------------
%% INNER Functions
%%--------------------------------------------------------------------
%% @doc Judge cachable request
%% @private
is_cachable_req1(_Key, #cache_condition{max_content_len = MaxLen}, Headers, Body) ->
    HasNOTCacheControl = (false == lists:keyfind(?HTTP_HEAD_CACHE_CTRL, 1, Headers)),
    HasNOTCacheControl  andalso
        is_binary(Body) andalso
        size(Body) > 0  andalso
        size(Body) < MaxLen.

is_cachable_req2(_Key, #cache_condition{path_patterns = []},       _Headers, _Body) -> true;
is_cachable_req2(_Key, #cache_condition{path_patterns = undefined},_Headers, _Body) -> true;
is_cachable_req2( Key, #cache_condition{path_patterns = Patterns}, _Headers, _Body) ->
    Res = lists:any(fun(Path) ->
                            nomatch /= re:run(Key, Path)
                    end, Patterns),
    Res.

is_cachable_req3(_, #cache_condition{content_types = []},       _Headers, _Body) -> true;
is_cachable_req3(_, #cache_condition{content_types = undefined},_Headers, _Body) -> true;
is_cachable_req3(_Key, #cache_condition{content_types = CTypes}, Headers, _Body) ->
    case lists:keyfind(?HTTP_HEAD_CONTENT_TYPE, 1, Headers) of
        false ->
            false;
        {_, ContentType} ->
            lists:member(ContentType, CTypes)
    end.
