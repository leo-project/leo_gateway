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
%% Leo Gateway - HTTP Commons
%% @doc
%% @end
%%======================================================================
-module(leo_gateway_http_commons).

-author('Yosuke Hara').

-include("leo_gateway.hrl").
-include("leo_http.hrl").
-include_lib("leo_dcerl/include/leo_dcerl.hrl").
-include_lib("leo_cache/include/leo_cache.hrl").
-undef(error).
-undef(warn).
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start/1, start/2]).
-export([onrequest/2, onresponse/2]).
-export([get_object/3, get_object_with_cache/4,
         put_object/3, put_small_object/3, put_large_object/4,
         delete_object/3, head_object/3,
         range_object/3]).

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
                    cache_method           = CacheMethod,
                    cache_expire           = CacheExpire,
                    cache_max_content_len  = CacheMaxContentLen,
                    cachable_content_type  = CachableContentTypes,
                    cachable_path_pattern  = CachablePathPatterns} = Props) ->
    InternalCache = (CacheMethod == 'inner'),
    Dispatch      = cowboy_router:compile(
                      [{'_', [{'_', Handler,
                               [?env_layer_of_dirs(), InternalCache, Props]}]}]),

    Config = case InternalCache of
                 %% Using inner-cache
                 true ->
                     [{env, [{dispatch, Dispatch}]}];
                 %% Using http-cache (like a varnish/squid)
                 false ->
                     CacheCondition = #cache_condition{expire          = CacheExpire,
                                                       max_content_len = CacheMaxContentLen,
                                                       content_types   = CachableContentTypes,
                                                       path_patterns   = CachablePathPatterns},
                     [{env,        [{dispatch, Dispatch}]},
                      {onrequest,  Handler:onrequest(CacheCondition)},
                      {onresponse, Handler:onresponse(CacheCondition)}]
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
    %% launch LeoCache
    NumOfCacheWorkers     = Options#http_options.cache_workers,
    CacheRAMCapacity      = Options#http_options.cache_ram_capacity,
    CacheDiscCapacity     = Options#http_options.cache_disc_capacity,
    CacheDiscThresholdLen = Options#http_options.cache_disc_threshold_len,
    CacheDiscDirData      = Options#http_options.cache_disc_dir_data,
    CacheDiscDirJournal   = Options#http_options.cache_disc_dir_journal,
    leo_cache_api:start([{?PROP_RAM_CACHE_NAME,           ?DEF_PROP_RAM_CACHE},
                         {?PROP_RAM_CACHE_WORKERS,        NumOfCacheWorkers},
                         {?PROP_RAM_CACHE_SIZE,           CacheRAMCapacity},
                         {?PROP_DISC_CACHE_NAME,          ?DEF_PROP_DISC_CACHE},
                         {?PROP_DISC_CACHE_WORKERS,       NumOfCacheWorkers},
                         {?PROP_DISC_CACHE_SIZE,          CacheDiscCapacity},
                         {?PROP_DISC_CACHE_THRESHOLD_LEN, CacheDiscThresholdLen},
                         {?PROP_DISC_CACHE_DATA_DIR,      CacheDiscDirData},
                         {?PROP_DISC_CACHE_JOURNAL_DIR,   CacheDiscDirJournal}
                        ]),

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
onrequest(#cache_condition{expire = Expire}, FunGenKey) ->
    fun(Req) ->
            Method = cowboy_req:get(method, Req),
            onrequest_1(Method, Req, Expire, FunGenKey)
    end.

onrequest_1(?HTTP_GET, Req, Expire, FunGenKey) ->
    Key = FunGenKey(Req),
    Ret = leo_cache_api:get(Key),
    onrequest_2(Req, Expire, Key, Ret);
onrequest_1(_, Req,_,_) ->
    Req.

onrequest_2(Req,_Expire,_Key, not_found) ->
    Req;
onrequest_2(Req, Expire, Key, {ok, CachedObj}) ->
    #cache{mtime        = MTime,
           content_type = ContentType,
           etag         = Checksum,
           body         = Body} = binary_to_term(CachedObj),

    Now = leo_date:now(),
    Diff = Now - MTime,

    case (Diff > Expire) of
        true ->
            _ = leo_cache_api:delete(Key),
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
                    {ok, Req2} = ?reply_ok([?SERVER_HEADER], Body, Req),
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
                    Key = FunGenKey(Req),

                    case lists:all(fun(Fun) ->
                                           Fun(Key, Config, Header1, Body)
                                   end, [fun is_cachable_req1/4,
                                         fun is_cachable_req2/4,
                                         fun is_cachable_req3/4]) of
                        true ->
                            Now = leo_date:now(),
                            Bin = term_to_binary(
                                    #cache{mtime        = Now,
                                           etag         = leo_hex:raw_binary_to_integer(crypto:hash(md5, Body)),
                                           content_type = ?http_content_type(Header1),
                                           body         = Body}),
                            _ = leo_cache_api:put(Key, Bin),

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
-spec(get_object(any(), binary(), #req_params{}) ->
             {ok, any()}).
get_object(Req, Key, #req_params{has_inner_cache = HasInnerCache}) ->
    case leo_gateway_rpc_handler:get(Key) of
        %% For regular case (NOT a chunked object)
        {ok, #metadata{cnumber = 0} = Meta, RespObject} ->
            Mime = leo_mime:guess_mime(Key),

            case HasInnerCache of
                true ->
                    Val = term_to_binary(#cache{etag  = Meta#metadata.checksum,
                                                mtime = Meta#metadata.timestamp,
                                                content_type = Mime,
                                                body = RespObject}),
                    leo_cache_api:put(Key, Val);
                false ->
                    void
            end,

            Header = [?SERVER_HEADER,
                      {?HTTP_HEAD_RESP_CONTENT_TYPE,  Mime},
                      {?HTTP_HEAD_RESP_ETAG,          ?http_etag(Meta#metadata.checksum)},
                      {?HTTP_HEAD_RESP_LAST_MODIFIED, ?http_date(Meta#metadata.timestamp)}],
            ?reply_ok(Header, RespObject, Req);

        %% For a chunked object.
        {ok, #metadata{cnumber = TotalChunkedObjs} = Meta, _RespObject} ->
            {ok, Pid}  = leo_gateway_large_object_handler:start_link(Key),
            try
                leo_gateway_large_object_handler:get(Pid, TotalChunkedObjs, Req, Meta)
            after
                catch leo_gateway_large_object_handler:stop(Pid)
            end;
        {error, not_found} ->
            ?reply_not_found([?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?reply_internal_error([?SERVER_HEADER], Req);
        {error, timeout} ->
            ?reply_timeout([?SERVER_HEADER], Req)
    end.


%% @doc GET an object with Etag
-spec(get_object_with_cache(any(), binary(), #cache{}, #req_params{}) ->
             {ok, any()}).
get_object_with_cache(Req, Key, CacheObj,_Params) ->
    case leo_gateway_rpc_handler:get(Key, CacheObj#cache.etag) of
        {ok, match} when CacheObj#cache.file_path /= [] ->
            Header = [?SERVER_HEADER,
                      {?HTTP_HEAD_RESP_CONTENT_TYPE,   CacheObj#cache.content_type},
                      {?HTTP_HEAD_RESP_ETAG,           ?http_etag(CacheObj#cache.etag)},
                      {?HTTP_HEAD_RESP_LAST_MODIFIED,  leo_http:rfc1123_date(CacheObj#cache.mtime)},
                      {?HTTP_HEAD_X_FROM_CACHE,        <<"True/via disk">>}],
            BodyFunc = fun(Socket, _Transport) ->
                               file:sendfile(CacheObj#cache.file_path, Socket)
                       end,
            cowboy_req:reply(?HTTP_ST_OK, Header, {CacheObj#cache.size, BodyFunc}, Req);
        {ok, match} when CacheObj#cache.file_path == [] ->
            Header = [?SERVER_HEADER,
                      {?HTTP_HEAD_RESP_CONTENT_TYPE,  CacheObj#cache.content_type},
                      {?HTTP_HEAD_RESP_ETAG,          ?http_etag(CacheObj#cache.etag)},
                      {?HTTP_HEAD_RESP_LAST_MODIFIED, leo_http:rfc1123_date(CacheObj#cache.mtime)},
                      {?HTTP_HEAD_X_FROM_CACHE,       <<"True/via memory">>}],
            ?reply_ok(Header, CacheObj#cache.body, Req);
        {ok, #metadata{cnumber = 0} = Meta, RespObject} ->
            Mime = leo_mime:guess_mime(Key),
            Val = term_to_binary(#cache{etag  = Meta#metadata.checksum,
                                        mtime = Meta#metadata.timestamp,
                                        content_type = Mime,
                                        body = RespObject}),
            leo_cache_api:put(Key, Val),
            Header = [?SERVER_HEADER,
                      {?HTTP_HEAD_RESP_CONTENT_TYPE,  Mime},
                      {?HTTP_HEAD_RESP_ETAG,          ?http_etag(Meta#metadata.checksum)},
                      {?HTTP_HEAD_RESP_LAST_MODIFIED, ?http_date(Meta#metadata.timestamp)}],
            ?reply_ok(Header, RespObject, Req);
        {ok, #metadata{cnumber = TotalChunkedObjs} = Meta, _RespObject} ->
            {ok, Pid}  = leo_gateway_large_object_handler:start_link(Key),
            try
                leo_gateway_large_object_handler:get(Pid, TotalChunkedObjs, Req, Meta)
            after
                catch leo_gateway_large_object_handler:stop(Pid)
            end;
        {error, not_found} ->
            ?reply_not_found([?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?reply_internal_error([?SERVER_HEADER], Req);
        {error, timeout} ->
            ?reply_timeout([?SERVER_HEADER], Req)
    end.


%% @doc PUT an object
-spec(put_object(any(), binary(), #req_params{}) ->
             {ok, any()}).
put_object(Req, Key, Params) ->
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
put_small_object({ok, {Size, Bin, Req}}, Key, Params) ->
    CIndex = case Params#req_params.upload_part_num of
                 <<>> ->
                     0;
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
            case (Params#req_params.has_inner_cache
                  andalso binary_is_contained(Key, 10) == false) of
                true  ->
                    Mime = leo_mime:guess_mime(Key),
                    Val  = term_to_binary(#cache{etag = ETag,
                                                 mtime = leo_date:now(),
                                                 content_type = Mime,
                                                 body = Bin}),
                    _ = leo_cache_api:put(Key, Val);
                false ->
                    void
            end,

            Header = [?SERVER_HEADER,
                      {?HTTP_HEAD_RESP_ETAG, ?http_etag(ETag)}],
            ?reply_ok(Header, Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?reply_internal_error([?SERVER_HEADER], Req);
        {error, timeout} ->
            ?reply_timeout([?SERVER_HEADER], Req)
    end.


%% @doc Put a large-object
%% @private
-spec(put_large_object(any(), binary(), pos_integer(), #req_params{}) ->
             {ok, any()}).
put_large_object(Req, Key, Size, #req_params{chunked_obj_len=ChunkedSize})->
    {ok, Pid}  = leo_gateway_large_object_handler:start_link(Key),

    %% remove a registered object with 'touch-command'
    %% from the cache
    _ = leo_cache_api:delete(Key),

    Ret2 = case catch put_large_object(cowboy_req:stream_body(ChunkedSize, Req),
                                       Key, Size, ChunkedSize, 0, 1, Pid) of
               {'EXIT', Cause} ->
                   {error, Cause};
               Ret1 ->
                   Ret1
           end,
    catch leo_gateway_large_object_handler:stop(Pid),
    Ret2.

put_large_object({ok, Data, Req}, Key, Size, ChunkedSize, TotalSize, TotalChunks, Pid) ->
    DataSize = byte_size(Data),
    catch leo_gateway_large_object_handler:put(Pid, TotalChunks, DataSize, Data),
    put_large_object(cowboy_req:stream_body(ChunkedSize, Req),
                     Key, Size, ChunkedSize, TotalSize + DataSize, TotalChunks + 1, Pid);

put_large_object({done, Req}, Key, Size, ChunkedSize, TotalSize, TotalChunks, Pid) ->
    TotalChunks1 = TotalChunks -1,
    case catch leo_gateway_large_object_handler:result(Pid) of
        {ok, Digest0} when Size == TotalSize ->
            Digest1 = leo_hex:raw_binary_to_integer(Digest0),
            case leo_gateway_rpc_handler:put(
                   Key, ?BIN_EMPTY, Size, ChunkedSize, TotalChunks1, Digest1) of
                {ok, _ETag} ->
                    Header = [?SERVER_HEADER,
                              {?HTTP_HEAD_RESP_ETAG, ?http_etag(Digest1)}],
                    ?reply_ok(Header, Req);
                {error, ?ERR_TYPE_INTERNAL_ERROR} ->
                    ?reply_internal_error([?SERVER_HEADER], Req);
                {error, timeout} ->
                    ?reply_timeout([?SERVER_HEADER], Req)
            end;
        {_, _Cause} ->
            ok = leo_gateway_large_object_handler:rollback(Pid, TotalChunks1),
            ?reply_internal_error([?SERVER_HEADER], Req)
    end;


%% An error occurred while reading the body, connection is gone.
put_large_object({error, Cause}, Key, _Size, _ChunkedSize, _TotalSize, TotalChunks, Pid) ->
    ?error("put_large_object/7", "key:~s, cause:~p", [binary_to_list(Key), Cause]),
    ok = leo_gateway_large_object_handler:rollback(Pid, TotalChunks).


%% @doc DELETE an object
-spec(delete_object(any(), binary(), #req_params{}) ->
             {ok, any()}).
delete_object(Req, Key,_Params) ->
    case leo_gateway_rpc_handler:delete(Key) of
        ok ->
            ?reply_no_content([?SERVER_HEADER], Req);
        {error, not_found} ->
            ?reply_not_found([?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?reply_internal_error([?SERVER_HEADER], Req);
        {error, timeout} ->
            ?reply_timeout([?SERVER_HEADER], Req)
    end.


%% @doc HEAD an object
-spec(head_object(any(), binary(), #req_params{}) ->
             {ok, any()}).
head_object(Req, Key,_Params) ->
    case leo_gateway_rpc_handler:head(Key) of
        {ok, #metadata{del = 0} = Meta} ->
            Timestamp = leo_http:rfc1123_date(Meta#metadata.timestamp),
            Headers   = [?SERVER_HEADER,
                         {?HTTP_HEAD_RESP_CONTENT_TYPE,   leo_mime:guess_mime(Key)},
                         {?HTTP_HEAD_RESP_ETAG,           ?http_etag(Meta#metadata.checksum)},
                         {?HTTP_HEAD_RESP_CONTENT_LENGTH, erlang:integer_to_list(Meta#metadata.dsize)},
                         {?HTTP_HEAD_RESP_LAST_MODIFIED,  Timestamp}],
            ?reply_ok(Headers, Req);
        {ok, #metadata{del = 1}} ->
            ?reply_not_found([?SERVER_HEADER], Req);
        {error, not_found} ->
            ?reply_not_found([?SERVER_HEADER], Req);
        {error, ?ERR_TYPE_INTERNAL_ERROR} ->
            ?reply_internal_error([?SERVER_HEADER], Req);
        {error, timeout} ->
            ?reply_timeout([?SERVER_HEADER], Req)
    end.

-undef(DEF_SEPARATOR).
-define(DEF_SEPARATOR, <<"\n">>).

%% @doc Retrieve a part of an object
-spec(range_object(any(), binary(), #req_params{}) ->
             {ok, any()}).
range_object(Req, Key, #req_params{range_header = RangeHeader}) ->
    Range = cowboy_http:range(RangeHeader),
    get_range_object(Req, Key, Range).

get_range_object(Req, _Key, {error, badarg}) ->
    ?reply_bad_range([?SERVER_HEADER], Req);
get_range_object(Req, Key, {_Unit, Ranges}) when is_list(Ranges) ->
    Mime = leo_mime:guess_mime(Key),
    Header = [?SERVER_HEADER,
              {?HTTP_HEAD_RESP_CONTENT_TYPE,  Mime}],
    {ok, Req2} = cowboy_req:chunked_reply(?HTTP_ST_OK, Header, Req),
    get_range_object_1(Req2, Key, Ranges, undefined).

get_range_object_1(Req, _Key, [], _) ->
    {ok, Req};
get_range_object_1(Req, _Key, _, {error, _}) ->
    {ok, Req};
get_range_object_1(Req, Key, [{Start, infinity}|Rest], _) ->
    Ret = get_range_object_2(Req, Key, Start, 0),
    get_range_object_1(Req, Key, Rest, Ret);
get_range_object_1(Req, Key, [{Start, End}|Rest], _) ->
    Ret = get_range_object_2(Req, Key, Start, End),
    get_range_object_1(Req, Key, Rest, Ret);
get_range_object_1(Req, Key, [End|Rest], _) ->
    Ret = get_range_object_2(Req, Key, 0, End),
    get_range_object_1(Req, Key, Rest, Ret).

get_range_object_2(Req, Key, Start, End) ->
    case leo_gateway_rpc_handler:head(Key) of
        {ok, #metadata{del = 0, cnumber = 0} = _Meta} ->
            get_range_object_small(Req, Key, Start, End);
        {ok, #metadata{del = 0, cnumber = N, dsize = ObjectSize} = _Meta} ->
            {NewStartPos, NewEndPos} = calc_pos(Start, End, ObjectSize),
            get_range_object_large(Req, Key, NewStartPos, NewEndPos, N, 0, 0);
        _ ->
            {error, not_found}
    end.

get_range_object_small(Req, Key, Start, End) ->
    case leo_gateway_rpc_handler:get(Key, Start, End) of
        {ok, _Meta, <<>>} ->
            ok;
        {ok, _Meta, Bin} ->
            cowboy_req:chunk(Bin, Req);
        {error, Cause} ->
            {error, Cause}
    end.

calc_pos(_StartPos, EndPos, ObjectSize) when EndPos < 0 ->
    NewStartPos = ObjectSize + EndPos,
    NewEndPos   = ObjectSize - 1,
    {NewStartPos, NewEndPos};
calc_pos(StartPos, 0, ObjectSize) ->
    {StartPos, ObjectSize - 1}; 
calc_pos(StartPos, EndPos, _ObjectSize) ->
    {StartPos, EndPos}.

get_range_object_large(_Req, _Key, _Start, _End, Total, Total, CurPos) ->
    {ok, CurPos};
get_range_object_large(_Req, _Key, _Start, End, _Total, _Index, CurPos) when CurPos > End ->
    {ok, CurPos};
get_range_object_large(Req, Key, Start, End, Total, Index, CurPos) ->
    IndexBin = list_to_binary(integer_to_list(Index + 1)),
    Key2 = << Key/binary, ?DEF_SEPARATOR/binary, IndexBin/binary >>,
    case leo_gateway_rpc_handler:head(Key2) of
        {ok, #metadata{cnumber = 0, dsize = CS}} -> % not csize
            %% only children
            %% get and chunk
            NewPos = send_chunk(Req, Key2, Start, End, CurPos, CS),
            get_range_object_large(Req, Key, Start, End, Total, Index + 1, NewPos);
        {ok, #metadata{cnumber = GrandChildNum}} ->
            case get_range_object_large(Req, Key2, Start, End, GrandChildNum, 0, CurPos) of
                {ok, NewPos} ->
                    get_range_object_large(Req, Key, Start, End, Total, Index + 1, NewPos);
                {error, Cause} ->
                    {error, Cause}
            end;
        {error, Cause} ->
            {error, Cause}
    end.

send_chunk(_Req, _Key, Start, _End, CurPos, ChunkSize) when (CurPos + ChunkSize - 1) < Start ->
    % skip
    CurPos + ChunkSize;
send_chunk(Req, Key, Start, End, CurPos, ChunkSize) when CurPos >= Start andalso
                                                         (CurPos + ChunkSize - 1) =< End ->
    % whole get
    case leo_gateway_rpc_handler:get(Key) of
        {ok, _Meta, Bin} ->
            cowboy_req:chunk(Bin, Req),
            CurPos + ChunkSize;
        Error ->
            Error
    end;
send_chunk(Req, Key, Start, End, CurPos, ChunkSize) ->
    % partial get
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
            cowboy_req:chunk(Bin, Req),
            CurPos + ChunkSize;
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
