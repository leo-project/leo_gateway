-module(leo_gateway_nfs_proto3_server).
-include("leo_gateway.hrl").
-include("leo_http.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_s3_libs/include/leo_s3_bucket.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include("leo_gateway_nfs_proto3.hrl").
-include_lib("kernel/include/file.hrl").
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         path_relative2abs/1,
         nfsproc3_null_3/2,
         nfsproc3_getattr_3/3,
         nfsproc3_setattr_3/3,
         nfsproc3_lookup_3/3,
         nfsproc3_access_3/3,
         nfsproc3_readlink_3/3,
         nfsproc3_read_3/3,
         nfsproc3_write_3/3,
         nfsproc3_create_3/3,
         nfsproc3_mkdir_3/3,
         nfsproc3_symlink_3/3,
         nfsproc3_mknod_3/3,
         nfsproc3_remove_3/3,
         nfsproc3_rmdir_3/3,
         nfsproc3_rename_3/3,
         nfsproc3_link_3/3,
         nfsproc3_readdir_3/3,
         nfsproc3_readdirplus_3/3,
         nfsproc3_fsstat_3/3,
         nfsproc3_pathconf_3/3,
         nfsproc3_commit_3/3,
         nfsproc3_fsinfo_3/3]).

-record(state, {
    write_verf :: binary() %% to be consistent during a single boot session
}).

-define(SIMPLENFS_WCC_EMPTY, 
    {
        {false, void},
        {false, void}
    }
).
-define(NFS_DUMMY_FILE4S3DIR, <<"$$_dir_$$">>).
-undef(DEF_SEPARATOR).
-define(DEF_SEPARATOR, <<"\n">>).
-define(NFS_READDIRPLUS_NUM_OF_RESPONSE, 10).

% @doc
% Called only once from a parent rpc server process to initialize this module
% during starting a leo_storage server.
-spec(init(any()) -> {ok, any()}).
init(_Args) ->
    State = #state{write_verf = crypto:rand_bytes(8)},
    {ok, State}.
 
handle_call(Req, _From, S) ->
    ?debug("handle_call", "req:~p from:~p", [Req, _From]),
    {reply, [], S}.
 
handle_cast(Req, S) ->
    ?debug("handle_cast", "req:~p", [Req]),
    {reply, [], S}.
 
handle_info(Req, S) ->
    ?debug("handle_info", "req:~p", [Req]),
    {noreply, S}.
 
terminate(_Reason, _S) ->
    ok.

% @doc
% Returns true if Path refers to a file, and false otherwise.
-spec(is_file(binary()) -> boolean()).
is_file(Path) ->
    case leo_gateway_rpc_handler:head(Path) of
        {ok, #?METADATA{del = 0}} ->
            true;
        {ok, _} ->
            %% deleted(del = 1)
            false;
        _ ->
            false
    end.

% @doc
% Lists file's metadatas stored in Path(directory)
-spec(get_dir_entries(binary()) ->
      {ok, list(#?METADATA{})}|{error, any()}).
get_dir_entries(Path) ->
    {ok, #redundancies{nodes = Redundancies}} =
        leo_redundant_manager_api:get_redundancies_by_key(get, Path),
    leo_gateway_rpc_handler:invoke(Redundancies,
                                   leo_storage_handler_directory,
                                   find_by_parent_dir,
                                   [Path, <<"/">>, <<>>, ?NFS_READDIRPLUS_NUM_OF_RESPONSE],
                                   []).

% @doc
% Returns true if Path refers to a directory, and false otherwise.
-spec(is_dir(binary()) -> boolean()).
is_dir(Path) ->
    case get_dir_entries(Path) of
        {ok, Meta} when is_list(Meta) =:= true andalso length(Meta) > 0 ->
            true;
        _Error ->
            false 
    end.

% @doc
% Returns true if Path refers to a directory which have child files, 
% and false otherwise.
-spec(is_empty_dir(binary()) -> boolean()).
is_empty_dir(Path) ->
    case get_dir_entries(Path) of
        {ok, MetaList} when is_list(MetaList) ->
            FilteredList = [Meta || Meta <- MetaList, filename:basename(Meta#?METADATA.key) =/= ?NFS_DUMMY_FILE4S3DIR],
            length(FilteredList) =:= 0;
        _Error ->
            false 
    end.

% @doc
% Returns list of file's metadatas stored under the Path.
% if the number of files is larger than ?NFS_READDIRPLUS_NUM_OF_RESPONSE, returned list is partial
% so to get all of files, you need to call this function repeatedly with a cookie verifier which returned at first function call
-spec(readdir_get_entry(binary(), binary()) -> 
      {ok, binary(), list(#?METADATA{}), boolean()}).
readdir_get_entry(<<0,0,0,0,0,0,0,0>>, Path) ->
    %<<CookieVerf:8/binary, _/binary>> = erlang:md5(Path),
    CookieVerf = crypto:rand_bytes(8),
    readdir_get_entry(CookieVerf, Path);
readdir_get_entry(CookieVerf, Path) ->
    Marker = case application:get_env(?MODULE, CookieVerf) of
        undefined ->
            <<>>;
        {ok, Ret} ->
            Ret
    end,
    {ok, #redundancies{nodes = Redundancies}} =
        leo_redundant_manager_api:get_redundancies_by_key(get, Path),
    case leo_gateway_rpc_handler:invoke(Redundancies,
                                        leo_storage_handler_directory,
                                        find_by_parent_dir,
                                        [Path, <<"/">>, Marker, ?NFS_READDIRPLUS_NUM_OF_RESPONSE],
                                        []) of
        {ok, []} ->
            {ok, CookieVerf, [], true};
        {ok, Meta} when is_list(Meta) ->
            Last = lists:last(Meta),
            application:set_env(?MODULE, CookieVerf, Last#?METADATA.key),
            EOF = length(Meta) =/= ?NFS_READDIRPLUS_NUM_OF_RESPONSE,
            NewMeta = case EOF of
                true ->
                    %% add current(.) and parent(..) directories
                    CuurentDirKey = << Path/binary, <<".">>/binary >>,
                    CurrentDir = #?METADATA{key = CuurentDirKey, dsize = -1},
                    ParentDirKey = << Path/binary, <<"..">>/binary >>,
                    ParentDir = #?METADATA{key = ParentDirKey, dsize = -1},
                    [CurrentDir, ParentDir|Meta];
                false ->
                    Meta
            end,
            {ok, CookieVerf, NewMeta, EOF};
        _Error ->
            {ok, <<>>, [], true} 
    end.
% @doc
% Delete a cookies verifier which returned by readdir_get_entry
-spec(readdir_del_entry(binary()) -> ok).
readdir_del_entry(CookieVerf) ->
    application:set_env(?MODULE, CookieVerf, undefined).

% @doc
% Create a rpc response for a readdir3 request
-spec(readdir_create_resp(binary(), list(#?METADATA{})) ->
      void | tuple()).
readdir_create_resp(Path, Meta) ->
    readdir_create_resp(Path, Meta, void).
readdir_create_resp(_Path, [], Resp) ->
    Resp;
readdir_create_resp(_Path, [#?METADATA{key = Key, dsize = Size} = Meta|Rest], Resp) ->
    NormalizedKey = case Size of
        -1 ->
            % dir to be normalized(means expand .|.. chars)
            path_relative2abs(Key);
        _ ->
            % file
            Key
    end,
    FileName = filename:basename(Key),
    case FileName of
        ?NFS_DUMMY_FILE4S3DIR ->
            readdir_create_resp(_Path, Rest, Resp);
        _ ->
            {ok, UID} = leo_gateway_nfs_uid_ets:new(NormalizedKey),
            NewResp = {inode(NormalizedKey),
                       FileName,
                       0,
                       {true, %% post_op_attr
                           meta2fattr3(Meta#?METADATA{key = NormalizedKey})
                       },
                       {true, {UID}}, %% post_op_fh3
                       Resp
                      },
            readdir_create_resp(_Path, Rest, NewResp)
    end.

% @doc
% Rename the file SrcKey to DstKey
-spec(rename(binary(), binary()) ->
      ok | {error, any()}).
rename(SrcKey, DstKey) ->
    case leo_gateway_rpc_handler:get(SrcKey) of
        {ok, #?METADATA{cnumber = 0} = Meta, RespObject} ->
            rename_1(DstKey, Meta, RespObject);
        {ok, #?METADATA{cnumber = _TotalChunkedObjs} = Meta, _RespObject} ->
            rename_large_object_1(DstKey, Meta);
        Error ->
            Error %% {error, not_found} | {error, ?ERR_TYPE_INTERNAL_ERROR} | {error, timeout}
    end.
rename_1(Key, Meta, Bin) ->
    Size = size(Bin),
    case leo_gateway_rpc_handler:put(Key, Bin, Size) of
        {ok, _ETag} ->
            rename_2(Meta);
        Error ->
            Error %% {error, ?ERR_TYPE_INTERNAL_ERROR} | {error, timeout}
    end.
rename_2(Meta) ->
    case leo_gateway_rpc_handler:delete(Meta#?METADATA.key) of
        ok ->
            ok;
        {error, not_found} ->
            ok;
        Error ->
            Error %% {error, ?ERR_TYPE_INTERNAL_ERROR} | {error, timeout}
    end.
rename_large_object_1(Key, Meta) ->
    % Params to be applied with configurations about large object
    LargeObjectProp   = ?env_large_object_properties(),
    ChunkedObjLen     = leo_misc:get_value('chunked_obj_len',
                                           LargeObjectProp, ?DEF_LOBJ_CHUNK_OBJ_LEN),
    Params = #req_params{chunked_obj_len = ChunkedObjLen},
    case leo_gateway_http_commons:move_large_object(Meta, Key, Params) of
        ok ->
            rename_large_object_2(Meta);
        Error ->
            Error %% {error, ?ERR_TYPE_INTERNAL_ERROR} | {error, timeout}
    end.
rename_large_object_2(Meta) ->
    case leo_gateway_large_object_handler:delete_chunked_objects(
           Meta#?METADATA.key, Meta#?METADATA.cnumber) of
        ok ->
            catch leo_gateway_rpc_handler:delete(Meta#?METADATA.key),
            ok;
        {error, not_found} ->
            ok;
        Error ->
            Error %% {error, ?ERR_TYPE_INTERNAL_ERROR} | {error, timeout}
    end.

%% functions for write operation
%%

% @doc
% Update a part of the file which start position to be updated is Start 
% and the end position is End
-spec(put_range(binary(), pos_integer(), pos_integer(), binary()) ->
      ok | {error, any()}).
put_range(Key, Start, End, Data) ->
    LargeObjectProp   = ?env_large_object_properties(),
    ChunkedObjLen     = leo_misc:get_value('chunked_obj_len',
                                           LargeObjectProp, ?DEF_LOBJ_CHUNK_OBJ_LEN),
    IsLarge = (End + 1) > ChunkedObjLen,
    case leo_gateway_rpc_handler:get(Key) of
        {ok, #?METADATA{cnumber = 0} = SrcMeta, SrcObj} when IsLarge =:= true ->
            put_range_small2large(Key, Start, End, Data, SrcMeta, SrcObj);
        {ok, #?METADATA{cnumber = 0} = SrcMeta, SrcObj} when IsLarge =:= false ->
            put_range_small2small(Key, Start, End, Data, SrcMeta, SrcObj);
        {ok, #?METADATA{cnumber = _CNum} = SrcMeta, _} ->
            put_range_large2any(Key, Start, End, Data, SrcMeta);
        {error, not_found} when IsLarge =:= true ->
            put_range_nothing2large(Key, Start, End, Data);
        {error, not_found} when IsLarge =:= false ->
            put_range_nothing2small(Key, Start, End, Data);
        {error, Cause} ->
            {error, Cause}
    end.
put_range_small2large(Key, Start, _End, Data, SrcMeta, SrcObj) ->
    % result to be merged with existing data blocks
    Data2 = case Start > SrcMeta#?METADATA.dsize of
        true ->
            <<SrcObj/binary, 0:(8*(Start - SrcMeta#?METADATA.dsize)), Data/binary>>;
        false ->
            <<Head:Start/binary, _/binary>> = SrcObj,
            <<Head/binary, Data/binary>>
    end,
    case large_obj_update(Key, Data2) of
        ok ->
            ok;
        Error ->
            Error
    end.
put_range_small2small(Key, Start, End, Data, SrcMeta, SrcObj) ->
    % result to be merged with existing data blocks
    Data2 = case Start > SrcMeta#?METADATA.dsize of
        true ->
            <<SrcObj/binary, 0:(8*(Start - SrcMeta#?METADATA.dsize)), Data/binary>>;
        false ->
            case (End + 1) < SrcMeta#?METADATA.dsize of
                true ->
                    <<Head:Start/binary, _/binary>> = SrcObj,
                    <<_:End/binary, Tail/binary>> = SrcObj,
                    <<Head/binary, Data/binary, Tail/binary>>;
                false ->
                    <<Head:Start/binary, _/binary>> = SrcObj,
                    <<Head/binary, Data/binary>>
            end
    end,
    case leo_gateway_rpc_handler:put(Key, Data2) of
        {ok, _} ->
            ok;
        Error ->
            Error
    end.
put_range_large2any(Key, Start, End, Data, SrcMeta) ->
    LargeObjectProp   = ?env_large_object_properties(),
    ChunkedObjLen     = leo_misc:get_value('chunked_obj_len', 
                                           LargeObjectProp, ?DEF_LOBJ_CHUNK_OBJ_LEN),
    IndexStart = Start div ChunkedObjLen + 1,
    IndexEnd   = End div ChunkedObjLen + 1,
    case IndexStart =:= IndexEnd of
        true ->
            Offset = Start rem ChunkedObjLen,
            Size = End - Start + 1,
            large_obj_partial_update(Key, Data, IndexStart, Offset, Size);
        false ->
            % head
            HeadOffset = Start rem ChunkedObjLen,
            HeadSize = ChunkedObjLen - HeadOffset,
            <<HeadData:HeadSize/binary, Rest/binary>> = Data,
            large_obj_partial_update(Key, HeadData, IndexStart, HeadOffset, HeadSize),
            % middle
            Rest3 = lists:foldl(
                        fun(Index, <<MidData:ChunkedObjLen/binary, Rest2/binary>>) ->
                            large_obj_partial_update(Key, MidData, Index),
                            Rest2
                        end,
                        Rest, 
                        lists:seq(IndexStart + 1, IndexEnd - 1)),
            % tail
            TailOffset = 0,
            TailSize = End rem ChunkedObjLen + 1,
            large_obj_partial_update(Key, Rest3, IndexEnd, TailOffset, TailSize)
    end,
    NumChunks = erlang:max(IndexEnd, SrcMeta#?METADATA.cnumber),
    large_obj_partial_commit(Key, NumChunks, ChunkedObjLen).

put_range_nothing2large(Key, Start, _End, Data) ->
    Data2 = <<0:(Start*8), Data/binary>>,
    case large_obj_update(Key, Data2) of
        ok ->
            ok;
        Error ->
            Error
    end.
put_range_nothing2small(Key, Start, _End, Data) ->
    % zero pdding to be added until the position reached Start
    Data2 = <<0:(Start*8), Data/binary>>,
    case leo_gateway_rpc_handler:put(Key, Data2) of
        {ok, _} ->
            ok;
        Error ->
            Error
    end.

%% functions for handling a large object
%%

% @doc
% Update the whole file which is handled as a large object in LeoFS
-spec(large_obj_update(binary(), binary()) ->
      ok | {error, any()}).
large_obj_update(Key, Data) ->
    LargeObjectProp   = ?env_large_object_properties(),
    ChunkedObjLen     = leo_misc:get_value('chunked_obj_len', 
                                           LargeObjectProp, ?DEF_LOBJ_CHUNK_OBJ_LEN),
    {ok, Handler} = leo_gateway_large_object_handler:start_link(Key, ChunkedObjLen),
    case catch leo_gateway_large_object_handler:put(Handler, Data) of
        ok ->
            large_obj_commit(Handler, Key, size(Data), ChunkedObjLen);
        {_, Cause} ->
            ok = leo_gateway_large_object_handler:rollback(Handler),
            {error, Cause}
    end.

large_obj_commit(Handler, Key, Size, ChunkedObjLen) ->
    case catch leo_gateway_large_object_handler:result(Handler) of
        {ok, #large_obj_info{length = TotalSize,
                             num_of_chunks = TotalChunks,
                             md5_context   = Digest}} when Size == TotalSize ->
            Digest_1 = leo_hex:raw_binary_to_integer(Digest),
            case leo_gateway_rpc_handler:put(Key, ?BIN_EMPTY, Size,
                                             ChunkedObjLen, TotalChunks, Digest_1) of
                {ok, _ETag} ->
                    ok;
                {error, Cause} ->
                    {error, Cause}
            end;
        {ok, _} ->
            {error, ?ERROR_NOT_MATCH_LENGTH};
        {_, Cause} ->
            {error, Cause}
    end.

% @doc
% Update the chunked file which is a part of a large object in LeoFS
-spec(large_obj_partial_update(binary(), binary(), pos_integer()) ->
      ok | {error, any()}).
large_obj_partial_update(Key, Data, Index) ->
    IndexBin = list_to_binary(integer_to_list(Index)),
    Key2 = << Key/binary, ?DEF_SEPARATOR/binary, IndexBin/binary >>,
    case leo_gateway_rpc_handler:put(Key2, Data, size(Data), Index) of
        {ok, _ETag} ->
            ok;
        {error, Cause} ->
            {error, Cause}
    end.
large_obj_partial_update(Key, Data, Index, Offset, Size) ->
    IndexBin = list_to_binary(integer_to_list(Index)),
    Key2 = << Key/binary, ?DEF_SEPARATOR/binary, IndexBin/binary >>,
    case leo_gateway_rpc_handler:get(Key2) of
        {ok, Meta, Bin} ->
            Data2 = case Offset > Meta#?METADATA.dsize of
                true ->
                    <<Bin/binary, 0:(8*(Offset - Meta#?METADATA.dsize)), Data/binary>>;
                false ->
                    case (Offset + Size + 1) < Meta#?METADATA.dsize of
                        true ->
                            End = Offset + Size,
                            <<Head:Offset/binary, _/binary>> = Bin,
                            <<_:End/binary, Tail/binary>> = Bin,
                            <<Head/binary, Data/binary, Tail/binary>>;
                        false ->
                            <<Head:Offset/binary, _/binary>> = Bin,
                            <<Head/binary, Data/binary>>
                    end
            end,
            case leo_gateway_rpc_handler:put(Key2, Data2, size(Data2), Index) of
                {ok, _ETag} ->
                    ok;
                {error, Cause} ->
                    {error, Cause}
            end;
        {error, not_found} ->
            Data2 = <<0:(Offset*8), Data/binary>>,
            case leo_gateway_rpc_handler:put(Key2, Data2, size(Data2), Index) of
                {ok, _ETag} ->
                    ok;
                {error, Cause} ->
                    {error, Cause}
            end;
        {error, Cause} ->
            {error, Cause}
    end.

% @doc
% Update the metadata of a large file to reflect the total file size
% @todo Chucksum(MD5) also have to be updated
-spec(large_obj_partial_commit(binary(), pos_integer(), pos_integer()) ->
      ok | {error, any()}).
large_obj_partial_commit(Key, NumChunks, ChunkSize) ->
    large_obj_partial_commit(NumChunks, Key, NumChunks, ChunkSize, 0).
large_obj_partial_commit(0, Key, NumChunks, ChunkSize, TotalSize) ->
    case leo_gateway_rpc_handler:put(Key, <<>>, TotalSize, ChunkSize, NumChunks, 0) of
        {ok, _} ->
            ok;
        Error ->
            Error
    end;
large_obj_partial_commit(PartNum, Key, NumChunks, ChunkSize, TotalSize) ->
    PartNumBin = list_to_binary(integer_to_list(PartNum)),
    Key2 = << Key/binary, ?DEF_SEPARATOR/binary, PartNumBin/binary >>,
    case leo_gateway_rpc_handler:head(Key2) of
        {ok, #?METADATA{dsize = Size}} ->
            large_obj_partial_commit(PartNum - 1, Key, NumChunks, ChunkSize, TotalSize + Size);
        Error ->
            Error
    end.

%% functions for read operation
%%

% @doc
% Retrieve a part of the file which start position is Start 
% and the end position is End
-spec(get_range(binary(), pos_integer(), pos_integer()) ->
      {ok, #?METADATA{}, binary()}| {error, any()}).
get_range(Key, Start, End) ->
    case leo_gateway_rpc_handler:head(Key) of
        {ok, #?METADATA{del = 0, cnumber = 0} = _Meta} ->
            get_range_small(Key, Start, End);
        {ok, #?METADATA{del = 0, cnumber = N, dsize = ObjectSize, csize = CS} = Meta} ->
            {NewStartPos, NewEndPos} = calc_pos(Start, End, ObjectSize),
            {CurPos, Index} = move_curpos2head(NewStartPos, CS, 0, 0),
            {ok, _Pos, Bin} = get_range_large(Key, NewStartPos, NewEndPos, N, Index, CurPos, <<>>),
            {ok, Meta, Bin};
        Error ->
            Error
    end.
get_range_small(Key, Start, End) ->
    case leo_gateway_rpc_handler:get(Key, Start, End) of
        {ok, Meta, Bin} ->
            {ok, Meta, Bin};
        {error, Cause} ->
            {error, Cause}
    end.
move_curpos2head(Start, ChunkedSize, CurPos, Idx) when (CurPos + ChunkedSize - 1) < Start ->
    move_curpos2head(Start, ChunkedSize, CurPos + ChunkedSize, Idx + 1);
move_curpos2head(_Start, _ChunkedSize, CurPos, Idx) ->
    {CurPos, Idx}.
calc_pos(_StartPos, EndPos, ObjectSize) when EndPos < 0 ->
    NewStartPos = ObjectSize + EndPos,
    NewEndPos   = ObjectSize - 1,
    {NewStartPos, NewEndPos};
calc_pos(StartPos, 0, ObjectSize) ->
    {StartPos, ObjectSize - 1};
calc_pos(StartPos, EndPos, _ObjectSize) ->
    {StartPos, EndPos}.
get_range_large(_Key,_Start,_End, Total, Total, CurPos, Acc) ->
    {ok, CurPos, Acc};
get_range_large(_Key,_Start, End,_Total,_Index, CurPos, Acc) when CurPos > End ->
    {ok, CurPos, Acc};
get_range_large(Key, Start, End, Total, Index, CurPos, Acc) ->
    IndexBin = list_to_binary(integer_to_list(Index + 1)),
    Key2 = << Key/binary, ?DEF_SEPARATOR/binary, IndexBin/binary >>,
    case leo_gateway_rpc_handler:head(Key2) of
        {ok, #?METADATA{cnumber = 0, dsize = CS}} ->
            {NewPos, Bin} = get_chunk(Key2, Start, End, CurPos, CS),
            get_range_large(Key, Start, End, Total, Index + 1, NewPos, <<Acc/binary, Bin/binary>>);
        {ok, #?METADATA{cnumber = GrandChildNum}} ->
            case get_range_large(Key2, Start, End, GrandChildNum, 0, CurPos, Acc) of
                {ok, NewPos, NewAcc} ->
                    get_range_large(Key, Start, End, Total, Index + 1, NewPos, NewAcc);
                {error, Cause} ->
                    {error, Cause}
            end;
        {error, Cause} ->
            {error, Cause}
    end.
get_chunk(_Key, Start, _End, CurPos, ChunkSize) when (CurPos + ChunkSize - 1) < Start ->
    %% skip proc
    {CurPos + ChunkSize, <<>>};
get_chunk(Key, Start, End, CurPos, ChunkSize) when CurPos >= Start andalso
                                                                  (CurPos + ChunkSize - 1) =< End ->
    %% whole get
    case leo_gateway_rpc_handler:get(Key) of
        {ok, _Meta, Bin} ->
            {CurPos + ChunkSize, Bin};
        Error ->
            Error
    end;
get_chunk(Key, Start, End, CurPos, ChunkSize) ->
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
        {ok, _Meta, Bin} ->
            {CurPos + ChunkSize, Bin};
        {error, Cause} ->
            {error, Cause}
    end.

-define(UNIX_TIME_BASE, 62167219200).
% @doc
% Return the current time with unix time format
-spec(unix_time() ->
      pos_integer()).
unix_time() -> 
    % calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}) = 62167219200
    calendar:datetime_to_gregorian_seconds(calendar:universal_time()) - ?UNIX_TIME_BASE.

% @doc
% Convert from the gregorian second to the unix time
-spec(gs2unix_time(pos_integer()) ->
      pos_integer()).
gs2unix_time(GS) ->
    GS - ?UNIX_TIME_BASE.

% @doc
% Return the inode of Path
% @todo to be replaced with truely unique id supported by LeoFS future works 
-spec(inode(binary()) ->
      integer()).
inode(Path) ->
    <<F8:8/binary, _/binary>> = erlang:md5(Path),
    Hex = leo_hex:binary_to_hex(F8),
    ?debug("inode", "path:~p inode:~p~n", [Path, Hex]),
    leo_hex:hex_to_integer(Hex).

sattr_mode2file_info({0, _})   -> undefined;
sattr_mode2file_info({true, Mode}) -> Mode.

sattr_uid2file_info({0, _})   -> undefined;
sattr_uid2file_info({true, UID}) -> UID.

sattr_gid2file_info({0, _})   -> undefined;
sattr_gid2file_info({true, GID}) -> GID.

sattr_atime2file_info({'DONT_CHANGE', _}) -> undefined;
sattr_atime2file_info({'SET_TO_SERVER_TIME', _}) -> unix_time();
sattr_atime2file_info({_, {ATime, _}})         -> ATime.

sattr_mtime2file_info({'DONT_CHANGE', _}) -> undefined;
sattr_mtime2file_info({'SET_TO_SERVER_TIME', _}) -> unix_time();
sattr_mtime2file_info({_, {MTime, _}})         -> MTime.

% @doc
% Convert from #?METADATA{} format to the format codes erpcgen generated expect
-spec(meta2fattr3(#?METADATA{}) ->
      tuple()).
meta2fattr3(#?METADATA{dsize = -1, key = Key}) ->
    Now = calendar:datetime_to_gregorian_seconds(calendar:universal_time()),
    UT = gs2unix_time(Now),
    {'NF3DIR',
     8#00777,  % @todo determin based on ACL? protection mode bits
     0,   % # of hard links
     0,   % @todo determin base on ACL? uid
     0,   % @todo gid
     4096,  % file size
     4096,  % @todo actual size used at disk(LeoFS should return `body + metadata + header/footer`)
     {0, 0}, % data used for special file(in Linux first is major, second is minor number)
     0, % fsid
     inode(Key),
     {UT, 0}, % last access
     {UT, 0}, % last modification
     {UT, 0}};% last change
meta2fattr3(#?METADATA{key = Key, timestamp = TS, dsize = Size}) ->
    UT = gs2unix_time(TS),
    {'NF3REG',
     8#00666,  % @todo determin based on ACL? protection mode bits
     0,   % # of hard links
     0,   % @todo determin base on ACL? uid
     0,   % @todo gid
     Size,  % file size
     Size,  % @todo actual size used at disk(LeoFS should return `body + metadata + header/footer`)
     {0, 0}, % data used for special file(in Linux first is major, second is minor number)
     0, % fsid
     inode(Key), % @todo Unique ID to be specifed fieldid 
     {UT, 0}, % last access
     {UT, 0}, % last modification
     {UT, 0}}.% last change

% @doc
% Convert from the directory path to the format codes erpcgen generated expect
-spec(s3dir2fattr3(binary()) ->
      tuple()).
s3dir2fattr3(Dir) ->
    Now = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
    UT = gs2unix_time(Now),
    {'NF3DIR',
     8#00777,  % @todo determin based on ACL? protection mode bits
     0,   % # of hard links
     0,   % @todo determin base on ACL? uid
     0,   % @todo gid
     4096,  % @todo how to calc?
     4096,  % @todo actual size used at disk(LeoFS should return `body + metadata + header/footer`)
     {0, 0}, % data used for special file(in Linux first is major, second is minor number)
     0, % fsid
     inode(Dir), % @todo Unique ID to be specifed fieldid 
     {UT, 0}, % last access
     {UT, 0}, % last modification
     {UT, 0}}.% last change

% @doc
% Convert from #?BUCKET to the format codes erpcgen generated expect
-spec(bucket2fattr3(#?BUCKET{}) ->
      tuple()).
bucket2fattr3(Bucket) ->
    UT = gs2unix_time(Bucket#?BUCKET.last_modified_at),
    {'NF3DIR',
     8#00777,  % @todo determin based on ACL? protection mode bits
     0,   % # of hard links
     0,   % @todo determin base on ACL? uid
     0,   % @todo gid
     4096,  % @todo directory size
     4096,  % @todo actual size used at disk
     {0, 0}, % data used for special file(in Linux first is major, second is minor number)
     0, % fsid
     inode(Bucket#?BUCKET.name), % @todo Unique ID to be specifed fieldid 
     {UT, 0}, % last access
     {UT, 0}, % last modification
     {UT, 0}}.% last change

% @doc
% Convert from the file path to the path trailing '/'
-spec(path2dir(binary()) ->
      binary()).
path2dir(Path) ->
    case binary:last(Path) of
        $/ ->
            Path;
        _ ->
            <<Path/binary, "/">>
    end.

% @doc
% Convert from a relative file path to a absolute one
-spec(path_relative2abs(binary()) ->
      binary()).
path_relative2abs(P) ->
    path_relative2abs(binary:split(P, <<"/">>, [global, trim]), []).

path_relative2abs([], []) ->
    <<"/">>;
path_relative2abs([], Acc) ->
    filename:join(lists:reverse(Acc));
path_relative2abs([<<>>|Rest], Acc) ->
    path_relative2abs(Rest, Acc);
path_relative2abs([<<".">>|Rest], Acc) ->
    path_relative2abs(Rest, Acc);
path_relative2abs([<<"..">>|Rest], Acc) ->
    path_relative2abs(Rest, tl(Acc));
path_relative2abs([Segment|Rest], Acc) ->
    path_relative2abs(Rest, [Segment|Acc]).

% @doc
% Return true if the specified binary contain _Char, and false otherwise
-spec(binary_is_contained(binary(), char()) ->
      boolean()).
binary_is_contained(<<>>, _Char) ->
    false;
binary_is_contained(<<Char:8, _Rest/binary>>, Char) ->
    true;
binary_is_contained(<<_Other:8, Rest/binary>>, Char) ->
    binary_is_contained(Rest, Char).

% @doc
% Return total disk usage on LeoFS in byte
-spec(du_get_disk_usage() -> 
      {ok, {Total::pos_integer(), Free::pos_integer()}}| {error, any()}).
du_get_disk_usage() ->
    StorageNodes = case leo_redundant_manager_api:get_members() of
        {ok, Members} ->
            Nodes = [_N || #member{node  = _N,
                                   state = ?STATE_RUNNING} <- Members],
            Nodes;
        not_found ->
            [];
        {error,_Cause} ->
            [] 
    end,
    du_get_disk_usage(StorageNodes, {0, 0}).

du_get_disk_usage([], {Total, Free}) ->
    {ok, {erlang:round(Total * 1024), erlang:round(Free * 1024)}};
du_get_disk_usage([Node|Rest], {SumTotal, SumFree}) ->
    case rpc:call(Node, leo_storage_api, get_disk_usage, [], 5000) of
        {ok, {Total, Free}} ->
            du_get_disk_usage(Rest, {SumTotal + Total, SumFree + Free});
        {badrpc, Cause} ->
            {error, Cause};
        Error ->
            Error
    end.

nfsproc3_null_3(_Clnt, State) ->
    {reply, [], State}.
 
nfsproc3_getattr_3({{UID}} = _1, Clnt, State) ->
    ?debug("nfsproc3_getattr_3", "args:~p client:~p", [_1, Clnt]),
    {ok, Path} = leo_gateway_nfs_uid_ets:get(UID),
    case binary_is_contained(Path, $/) of
        %% object
        true ->
            %% @todo emulate directories
            case leo_gateway_rpc_handler:head(Path) of
                {ok, Meta} ->
                    {reply, 
                    {'NFS3_OK',
                    {
                       % fattr
                       meta2fattr3(Meta)
                    }}, 
                    State};
                {error, not_found} ->
                    Path4S3Dir = path2dir(Path),
                    case is_dir(Path4S3Dir) of
                        true ->
                            {reply, 
                            {'NFS3_OK',
                            {
                              % fattr
                              s3dir2fattr3(Path)
                            }}, 
                            State};
                        false ->
                            {reply, {'NFS3ERR_NOENT', void}, State}
                    end;
                {error, Reason} ->
                    {reply, {'NFS3ERR_IO', Reason}, State}
            end;
        %% bucket 
        false ->
            case leo_s3_bucket:find_bucket_by_name(Path) of
                {ok, Bucket} ->
                    Attr = bucket2fattr3(Bucket),
                    {reply, 
                    {'NFS3_OK',
                    {
                       Attr
                    }}, 
                    State};
                {error, Reason} ->
                    {reply, {'NFS3ERR_IO', Reason}, State};
                not_found ->
                    {reply, {'NFS3ERR_NOENT', void}, State}
            end
    end.
     
% @todo for now do nothing
nfsproc3_setattr_3({{_Path},
                    {_Mode,
                     _UID,
                     _GID,
                     _,
                     _ATime,
                     _MTime
                    },_Guard} = _1, Clnt, State) ->
    ?debug("nfsproc3_setattr_3", "args:~p client:~p", [_1, Clnt]),
    {reply, 
        {'NFS3_OK',
        {
            ?SIMPLENFS_WCC_EMPTY
        }}, 
        State}.
         
nfsproc3_lookup_3({{{UID}, Name}} = _1, Clnt, State) ->
    ?debug("nfsproc3_lookup_3", "args:~p client:~p", [_1, Clnt]),
    {ok, Dir} = leo_gateway_nfs_uid_ets:get(UID),
    Path4S3 = filename:join(Dir, Name),
    %% A path for directory must be trailing with '/'
    Path4S3Dir = path2dir(Path4S3),
    case is_file(Path4S3) orelse is_dir(Path4S3Dir) of
        true ->
            {ok, FileUID} = leo_gateway_nfs_uid_ets:new(Path4S3),
            {reply, 
                {'NFS3_OK',
                {
                    {FileUID}, %% nfs_fh3
                    {false, void}, %% post_op_attr for obj
                    {false, void}  %% post_op_attr for dir
                }}, 
                State};
        false ->
            {reply, 
                {'NFS3ERR_NOENT',
                {
                    {false, void}  %% post_op_attr for dir
                }}, 
                State}
    end.
 
nfsproc3_access_3(_1, Clnt, State) ->
    ?debug("nfsproc3_access_3", "args:~p client:~p", [_1, Clnt]),
    {reply, 
        {'NFS3_OK',
        {
            {false, void}, %% post_op_attr for obj
            63       %% access bits(up all)
        }}, 
        State}.
 
nfsproc3_readlink_3(_1, Clnt, State) ->
    ?debug("nfsproc3_readlink_3", "args:~p client:~p", [_1, Clnt]),
    {reply, 
        {'NFS3_OK',
        {
            {false, void}, %% post_op_attr for obj
            <<"link path">>
        }}, 
        State}.
 
nfsproc3_read_3({{UID}, Offset, Count} =_1, Clnt, State) ->
    ?debug("nfsproc3_read_3", "args:~p client:~p", [_1, Clnt]),
    {ok, Path} = leo_gateway_nfs_uid_ets:get(UID),
    case get_range(Path, Offset, Offset + Count - 1) of
        {ok, Meta, Body} ->
            EOF = Meta#?METADATA.dsize =:= (Offset + Count),
            {reply, 
                {'NFS3_OK',
                {
                    {false, void}, %% post_op_attr for obj
                    Count,         %% count read bytes
                    EOF,           %% eof
                    Body
                }}, 
                State};
        {error, Reason} ->
            io:format(user, "[read]open error reason:~p~n",[Reason]),
            {reply, 
                {'NFS3ERR_IO',
                {
                    {false, void} %% post_op_attr for obj
                }}, 
                State} 
    end.
 
nfsproc3_write_3({{UID}, Offset, Count, _HowStable, Data} = _1, Clnt, State) ->
    ?debug("nfsproc3_write_3", "uid:~p offset:~p count:~p client:~p", 
           [UID, Offset, Count, Clnt]),
    {ok, Path} = leo_gateway_nfs_uid_ets:get(UID),
    case put_range(Path, Offset, Offset + Count - 1, Data) of
        ok ->
            {reply, 
                {'NFS3_OK',
                {
                    ?SIMPLENFS_WCC_EMPTY,
                    Count,
                    'DATA_SYNC',
                    State#state.write_verf
                }}, 
                State};
        {error, Reason} ->
            io:format(user, "[write]error file:~p reason:~p~n",[Path ,Reason]),
            {reply, 
                {'NFS3ERR_IO',
                {
                    ?SIMPLENFS_WCC_EMPTY
                }}, 
                State}
    end.
 
nfsproc3_create_3({{{UID}, Name}, {_CreateMode, _How}} = _1, Clnt, State) ->
    ?debug("nfsproc3_create_3", "args:~p client:~p", [_1, Clnt]),
    {ok, Dir} = leo_gateway_nfs_uid_ets:get(UID),
    FilePath4S3 = filename:join(Dir, Name),
    case leo_gateway_rpc_handler:put(FilePath4S3, <<>>) of
        {ok, _}->
            {ok, FileUID} = leo_gateway_nfs_uid_ets:new(FilePath4S3),
            {reply, 
                {'NFS3_OK',
                {
                    {true, {FileUID}}, %% post_op file handle
                    {false, void},      %% post_op_attr
                    ?SIMPLENFS_WCC_EMPTY
                }}, 
                State};
        {error, Reason} ->
            io:format(user, "[create]error reason:~p~n",[Reason]),
            {reply, 
                {'NFS3ERR_IO',
                {
                    ?SIMPLENFS_WCC_EMPTY
                }}, 
                State}
    end.
 
nfsproc3_mkdir_3({{{UID}, Name}, _How} = _1, Clnt, State) ->
    ?debug("nfsproc3_mkdir_3", "args:~p client:~p", [_1, Clnt]),
    {ok, Dir} = leo_gateway_nfs_uid_ets:get(UID),
    DirPath = filename:join(Dir, Name),
    DummyFile4S3Dir = filename:join(DirPath, ?NFS_DUMMY_FILE4S3DIR),
    case leo_gateway_rpc_handler:put(DummyFile4S3Dir, <<>>) of
        {ok, _}->
            {reply, 
                {'NFS3_OK',
                {
                    {false, void}, %% post_op file handle
                    {false, void}, %% post_op_attr
                    ?SIMPLENFS_WCC_EMPTY
                }}, 
                State};
        {error, Reason} ->
            io:format(user, "[mkdir]error reason:~p~n",[Reason]),
            {reply, 
                {'NFS3ERR_IO',
                {
                    {false, void}, %% post_op file handle
                    {false, void}, %% post_op_attr
                    ?SIMPLENFS_WCC_EMPTY
                }}, 
                State}
    end.
 
nfsproc3_symlink_3(_1, Clnt, State) ->
    ?debug("nfsproc3_symlink_3", "args:~p client:~p", [_1, Clnt]),
    {reply, 
        {'NFS3_OK',
        {
            {false, void}, %% post_op file handle
            {false, void}, %% post_op_attr
            ?SIMPLENFS_WCC_EMPTY
        }}, 
        State}.
 
nfsproc3_mknod_3(_1, Clnt, State) ->
    ?debug("nfsproc3_mknod_3", "args:~p client:~p", [_1, Clnt]),
    {reply, 
        {'NFS3_OK',
        {
            {false, void}, %% post_op file handle
            {false, void}, %% post_op_attr
            ?SIMPLENFS_WCC_EMPTY
        }}, 
        State}.
 
nfsproc3_remove_3({{{UID}, Name}} = _1, Clnt, State) ->
    ?debug("nfsproc3_remove_3", "args:~p client:~p", [_1, Clnt]),
    {ok, Dir} = leo_gateway_nfs_uid_ets:get(UID),
    FilePath4S3 = filename:join(Dir, Name),
    case leo_gateway_rpc_handler:delete(FilePath4S3) of
        ok ->
            {reply, 
                {'NFS3_OK',
                {
                    ?SIMPLENFS_WCC_EMPTY
                }}, 
                State};
        {error, Reason} ->
            io:format(user, "[remove]reason:~p~n",[Reason]),
            {reply, 
                {'NFS3ERR_IO',
                {
                    ?SIMPLENFS_WCC_EMPTY
                }}, 
                State}
    end.
         
nfsproc3_rmdir_3({{{UID}, Name}} = _1, Clnt, State) ->
    ?debug("nfsproc3_rmdir_3", "args:~p client:~p", [_1, Clnt]),
    {ok, Dir} = leo_gateway_nfs_uid_ets:get(UID),
    Path4S3 = filename:join(Dir, Name),
    Path4S3Dir = path2dir(Path4S3),
    case is_empty_dir(Path4S3Dir) of
        true ->
            DummyFile4S3Dir = filename:join(Path4S3Dir, ?NFS_DUMMY_FILE4S3DIR),
            catch leo_gateway_rpc_handler:delete(DummyFile4S3Dir),
            {reply, 
                {'NFS3_OK',
                {
                    ?SIMPLENFS_WCC_EMPTY
                }}, 
                State};
        false ->
            {reply, 
                {'NFS3ERR_NOTEMPTY',
                {
                    ?SIMPLENFS_WCC_EMPTY
                }}, 
                State}
    end.
     
nfsproc3_rename_3({{{SrcUID}, SrcName}, {{DstUID}, DstName}} =_1, Clnt, State) ->
    ?debug("nfsproc3_rename_3", "args:~p client:~p", [_1, Clnt]),
    {ok, SrcDir} = leo_gateway_nfs_uid_ets:get(SrcUID),
    {ok, DstDir} = leo_gateway_nfs_uid_ets:get(DstUID),
    Src4S3 = filename:join(SrcDir, SrcName),
    Dst4S3 = filename:join(DstDir, DstName),
    case rename(Src4S3, Dst4S3) of
        ok ->
            {reply, 
                {'NFS3_OK',
                {
                    ?SIMPLENFS_WCC_EMPTY, %% src
                    ?SIMPLENFS_WCC_EMPTY  %% dst
                }}, 
                State};
        {error, not_found} ->
            {reply, 
                {'NFS3ERR_NOENT',
                {
                    ?SIMPLENFS_WCC_EMPTY, %% src
                    ?SIMPLENFS_WCC_EMPTY  %% dst
                }}, 
                State};
        {error, Reason} ->
            io:format(user, "[rename]reason:~p~n",[Reason]),
            {reply, 
                {'NFS3ERR_IO',
                {
                    ?SIMPLENFS_WCC_EMPTY, %% src
                    ?SIMPLENFS_WCC_EMPTY  %% dst
                }}, 
                State}
    end.
 
nfsproc3_link_3(_1, Clnt, State) ->
    ?debug("nfsproc3_link_3", "args:~p client:~p", [_1, Clnt]),
    {reply, 
        {'NFS3_NG',
        {
            {false, void}, %% post_op_attr
            ?SIMPLENFS_WCC_EMPTY
        }}, 
        State}.
 
nfsproc3_readdir_3(_1, Clnt, State) ->
    ?debug("nfsproc3_readdir_3", "args:~p client:~p", [_1, Clnt]),
    {reply, 
        {'NFS3_OK',
        {
            {false, void}, %% post_op_attr
            <<"12345678">>, %% cookie verfier
            {%% dir_list(empty)
                void, %% pre_op_attr
                true  %% eof
            }
        }}, 
        State}.
 
nfsproc3_readdirplus_3({{UID}, _Cookie, CookieVerf, _DirCnt, _MaxCnt} = _1, Clnt, State) ->
    ?debug("nfsproc3_readdirplus_3", "args:~p client:~p", [_1, Clnt]),
    {ok, Path} = leo_gateway_nfs_uid_ets:get(UID),
    Path4S3Dir = path2dir(Path),
    {ok, NewCookieVerf, ReadDir, EOF} = readdir_get_entry(CookieVerf, Path4S3Dir),
    case ReadDir of
        [] ->
            % empty response
            {reply, 
                {'NFS3_OK',
                {
                    {false, void}, %% post_op_attr
                    <<"00000000">>, %% cookie verfier
                    {%% dir_list(empty)
                        void, %% pre_op_attr
                        true  %% eof
                    }
                }}, 
                State};
        ReadDir ->
            % create response
            % @TODO
            % # of entries should be determinted by _MaxCnt
            Resp = readdir_create_resp(Path4S3Dir, ReadDir),
            ?debug("nfsproc3_readdirplus_3", "resp:~p~n", [Resp]),
            case EOF of
                true -> 
                    readdir_del_entry(NewCookieVerf);
                false ->
                    void
            end,
            {reply,
                {'NFS3_OK',
                {
                    {false, void}, %% post_op_attr
                    NewCookieVerf, %% cookie verfier
                    {
                        Resp, %% pre_op_attr
                        EOF
                    }
                }}, 
                State}
    end.

nfsproc3_fsstat_3(_1, Clnt, State) ->
    ?debug("nfsproc3_fsstat_3", "args:~p client:~p", [_1, Clnt]),
    {ok, {Total, Free}} = du_get_disk_usage(),
    {reply, 
        {'NFS3_OK',
        {
            {false, void}, %% post_op_attr
            Total,         %% total size
            Free,          %% free size
            Free,          %% free size(for auth user)
            16,            %% # of files
            8,             %% # of free file slots
            8,             %% # of free file slots(for auth user)
            10             %% invarsec
        }}, 
        State}.
 
nfsproc3_fsinfo_3(_1, Clnt, State) ->
    ?debug("nfsproc3_fsinfo_3", "args:~p client:~p", [_1, Clnt]),
    {reply, 
        {'NFS3_OK',
        {
            {false, void}, %% post_op_attr
            5242880, %% rtmax
            5242880, %% rtperf(limited at client up to 1024 * 1024)
            8,    %% rtmult
            5242880, %% wtmaxa(limited at client up to 1024 * 1024)
            5242880, %% wtperf
            8,    %% wtmult
            5242880, %% dperf (limited at client up to 32768)
            1024 * 1024 * 1024 * 4, %% max size of a file
            {1, 0}, %% time_delta
            0     %% properties
        }}, 
        State}.
 
nfsproc3_pathconf_3(_1, Clnt, State) ->
    ?debug("nfsproc3_pathconf_3", "args:~p client:~p", [_1, Clnt]),
    {reply, 
        {'NFS3_OK',
        {
            {false, void}, %% post_op_attr
            8,    %% linkmax
            1024, %% name_max
            true, %% no_trunc (mean make a reques error if filename's length was larger than max
            false,%% chown_restricted
            true, %% case_insensitive
            true  %% case_preserving
        }}, 
        State}.
 
nfsproc3_commit_3(_1, Clnt, State) ->
    ?debug("nfsproc3_commit_3", "args:~p client:~p", [_1, Clnt]),
    {reply, 
        {'NFS3_OK',
        {
            ?SIMPLENFS_WCC_EMPTY,
            State#state.write_verf %% write verfier
        }}, 
        State}.

