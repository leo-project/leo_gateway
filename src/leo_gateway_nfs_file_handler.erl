-module(leo_gateway_nfs_file_handler).
-include("leo_gateway.hrl").
-include("leo_http.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_s3_libs/include/leo_s3_bucket.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include("leo_gateway_nfs_proto3.hrl").
-include_lib("kernel/include/file.hrl").
-export([is_file/1, is_dir/1, list_dir/1, list_dir/2, rename/2,
         write/4, read/3, unix_time/0, gs2unix_time/1, trim/2]).
-export([path2dir/1, path_trim_trailing_sep/1, path_relative2abs/1,
         binary_is_contained/2, get_disk_usage/0]).

-undef(DEF_SEPARATOR).
-define(DEF_SEPARATOR, <<"\n">>).
-define(LEOFS_NUM_OF_LIST_DIR, 1000).

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

%% @doc
%% @private
list_dir_append_hidden_files(BasePath, List) ->
    %% add current(.) and parent(..) directories
    CuurentDirKey = << BasePath/binary, <<".">>/binary >>,
    CurrentDir = #?METADATA{key = CuurentDirKey, dsize = -1},
    ParentDirKey = << BasePath/binary, <<"..">>/binary >>,
    ParentDir = #?METADATA{key = ParentDirKey, dsize = -1},
    [CurrentDir, ParentDir|List].

-spec(list_dir(binary()) -> 
      {ok, list(#?METADATA{})}|{error, any()}).
list_dir(Path) ->
    list_dir(Path, true).

-spec(list_dir(binary(), boolean()) -> 
      {ok, list(#?METADATA{})}|{error, any()}).
list_dir(Path, IncludeHiddenFiles) ->
    {ok, #redundancies{nodes = Redundancies}} =
        leo_redundant_manager_api:get_redundancies_by_key(get, Path),
    Modifier = case IncludeHiddenFiles of
        true ->
            fun list_dir_append_hidden_files/2;
        false ->
            void
    end,
    list_dir(Redundancies, Path, <<>>, [], Modifier).
list_dir(Redundancies, Path, Marker, Acc, Modifier) when is_function(Modifier) ->
    case leo_gateway_rpc_handler:invoke(Redundancies,
                                        leo_storage_handler_directory,
                                        find_by_parent_dir,
                                        [Path, <<"/">>, Marker, ?LEOFS_NUM_OF_LIST_DIR],
                                        []) of
        {ok, []} ->
            {ok, Modifier(Path, Acc)};
        {ok, Meta} when is_list(Meta) ->
            Last = lists:last(Meta),
            TrimedKey = path_trim_trailing_sep(Last#?METADATA.key),
            case Marker of
                TrimedKey ->
                    {ok, Modifier(Path, Acc)};
                _Other ->
                    list_dir(Redundancies, Path, TrimedKey, Meta ++ Acc, Modifier)
            end;
        Error ->
            ?error("list_dir", "cause:~p~n", [Error]),
            Error
    end;
list_dir(Redundancies, Path, Marker, _Acc, _Modifier) ->
    leo_gateway_rpc_handler:invoke(Redundancies,
                                   leo_storage_handler_directory,
                                   find_by_parent_dir,
                                   [Path, <<"/">>, Marker, ?LEOFS_NUM_OF_LIST_DIR],
                                   []).
 % @doc
% Returns true if Path refers to a directory, and false otherwise.
-spec(is_dir(binary()) -> boolean()).
is_dir(Path) ->
    case list_dir(Path, false) of
        {ok, Meta} when is_list(Meta) =:= true andalso length(Meta) > 0 ->
            true;
        _Error ->
            false 
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
% Update a part of the file which start position is Start 
% and the end position is End
-spec(write(binary(), pos_integer(), pos_integer(), binary()) ->
      ok | {error, any()}).
write(Key, Start, End, Data) ->
    LargeObjectProp   = ?env_large_object_properties(),
    ChunkedObjLen     = leo_misc:get_value('chunked_obj_len',
                                           LargeObjectProp, ?DEF_LOBJ_CHUNK_OBJ_LEN),
    IsLarge = (End + 1) > ChunkedObjLen,
    case leo_gateway_rpc_handler:get(Key) of
        {ok, #?METADATA{cnumber = 0} = SrcMeta, SrcObj} when IsLarge =:= true ->
            write_small2large(Key, Start, End, Data, SrcMeta, SrcObj);
        {ok, #?METADATA{cnumber = 0} = SrcMeta, SrcObj} when IsLarge =:= false ->
            write_small2small(Key, Start, End, Data, SrcMeta, SrcObj);
        {ok, #?METADATA{cnumber = _CNum} = SrcMeta, _} ->
            write_large2any(Key, Start, End, Data, SrcMeta);
        {error, not_found} when IsLarge =:= true ->
            write_nothing2large(Key, Start, End, Data);
        {error, not_found} when IsLarge =:= false ->
            write_nothing2small(Key, Start, End, Data);
        {error, Cause} ->
            {error, Cause}
    end.
write_small2large(Key, Start, _End, Data, SrcMeta, SrcObj) ->
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
write_small2small(Key, Start, End, Data, SrcMeta, SrcObj) ->
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
write_large2any(Key, Start, End, Data, SrcMeta) ->
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

write_nothing2large(Key, Start, _End, Data) ->
    Data2 = <<0:(Start*8), Data/binary>>,
    case large_obj_update(Key, Data2) of
        ok ->
            ok;
        Error ->
            Error
    end.
write_nothing2small(Key, Start, _End, Data) ->
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
-spec(read(binary(), pos_integer(), pos_integer()) ->
      {ok, #?METADATA{}, binary()}| {error, any()}).
read(Key, Start, End) ->
    case leo_gateway_rpc_handler:head(Key) of
        {ok, #?METADATA{del = 0, cnumber = 0} = _Meta} ->
            read_small(Key, Start, End);
        {ok, #?METADATA{del = 0, cnumber = N, dsize = ObjectSize, csize = CS} = Meta} ->
            {NewStartPos, NewEndPos} = calc_pos(Start, End, ObjectSize),
            {CurPos, Index} = move_curpos2head(NewStartPos, CS, 0, 0),
            {ok, _Pos, Bin} = read_large(Key, NewStartPos, NewEndPos, N, Index, CurPos, <<>>),
            {ok, Meta, Bin};
        Error ->
            Error
    end.
read_small(Key, Start, End) ->
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
read_large(_Key,_Start,_End, Total, Total, CurPos, Acc) ->
    {ok, CurPos, Acc};
read_large(_Key,_Start, End,_Total,_Index, CurPos, Acc) when CurPos > End ->
    {ok, CurPos, Acc};
read_large(Key, Start, End, Total, Index, CurPos, Acc) ->
    IndexBin = list_to_binary(integer_to_list(Index + 1)),
    Key2 = << Key/binary, ?DEF_SEPARATOR/binary, IndexBin/binary >>,
    case leo_gateway_rpc_handler:head(Key2) of
        {ok, #?METADATA{cnumber = 0, dsize = CS}} ->
            {NewPos, Bin} = get_chunk(Key2, Start, End, CurPos, CS),
            read_large(Key, Start, End, Total, Index + 1, NewPos, <<Acc/binary, Bin/binary>>);
        {ok, #?METADATA{cnumber = GrandChildNum}} ->
            case read_large(Key2, Start, End, GrandChildNum, 0, CurPos, Acc) of
                {ok, NewPos, NewAcc} ->
                    read_large(Key, Start, End, Total, Index + 1, NewPos, NewAcc);
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
% Trim a file which size is modified to Size
-spec(trim(binary(), pos_integer()) ->
      ok | {error, any()}).
trim(Key, Size) ->
    LargeObjectProp   = ?env_large_object_properties(),
    ChunkedObjLen     = leo_misc:get_value('chunked_obj_len',
                                           LargeObjectProp, ?DEF_LOBJ_CHUNK_OBJ_LEN),
    IsLarge = Size > ChunkedObjLen,
    case leo_gateway_rpc_handler:get(Key) of
        {ok, #?METADATA{cnumber = 0} = _SrcMeta, SrcObj} when IsLarge =:= false ->
            %% small to small
            %% @todo handle expand case
            <<DstObj:Size/binary, _Rest/binary>> = SrcObj,
            case leo_gateway_rpc_handler:put(Key, DstObj) of
                {ok, _} ->
                    ok;
                Error ->
                    Error
            end;
        {ok, #?METADATA{cnumber = 0} = _SrcMeta, _SrcObj} ->
            %% small to large
            %% @todo handle expand case
            ok;
        {ok, #?METADATA{cnumber = CNum} = _SrcMeta, _} when IsLarge =:= true ->
            %% large to large
            %% @todo handle expand case
            End = Size - 1,
            IndexEnd = End div ChunkedObjLen + 1,
            % Modify the last chunk
            TailSize = End rem ChunkedObjLen + 1,
            case large_obj_partial_trim(Key, IndexEnd, TailSize) of
                ok ->
                    % Update the metadata based on Size and IndexEnd
                    case leo_gateway_rpc_handler:put(Key, <<>>, Size, ChunkedObjLen, IndexEnd, 0) of
                        {ok, _} ->
                            % Remove tail chunks between IndexEnd + 1 to CNum
                            RemovedIdxList = lists:seq(IndexEnd + 1, CNum),
                            large_obj_delete_chunks(Key, RemovedIdxList),
                            ok;
                        Error ->
                            Error
                    end;
                Error ->
                    Error
            end;
        {ok, #?METADATA{cnumber = _CNum} = _SrcMeta, _} ->
            %% large to small
            %% Get the first chunk
            IndexBin = list_to_binary(integer_to_list(1)),
            Key2 = << Key/binary, ?DEF_SEPARATOR/binary, IndexBin/binary >>,
            case leo_gateway_rpc_handler:get(Key2) of
                {ok, _Meta, SrcObj} ->
                    %% Trim the data by Size
                    <<DstObj:Size/binary, _Rest/binary>> = SrcObj,
                    %% Insert the new object as a small object
                    case leo_gateway_rpc_handler:put(Key, DstObj) of
                        {ok, _} ->
                            ok;
                        Error ->
                            Error
                    end;
                Error ->
                    Error
            end
    end.

%% @private
large_obj_partial_trim(Key, Index, Size) ->
    IndexBin = list_to_binary(integer_to_list(Index)),
    Key2 = << Key/binary, ?DEF_SEPARATOR/binary, IndexBin/binary >>,
    case leo_gateway_rpc_handler:get(Key2) of
        {ok, _Meta, SrcObj} ->
            <<DstObj:Size/binary, _Rest/binary>> = SrcObj,
            case leo_gateway_rpc_handler:put(Key2, DstObj) of
                {ok, _} ->
                    ok;
                Error ->
                    Error 
            end; 
        Error ->
            Error
    end.

%% @private
large_obj_delete_chunks(_Key, []) ->
    ok;
large_obj_delete_chunks(Key1, [Index1|Rest]) ->
    Index2 = list_to_binary(integer_to_list(Index1)),
    Key2   = << Key1/binary, ?DEF_SEPARATOR/binary, Index2/binary >>,
    case leo_gateway_rpc_handler:delete(Key2) of
        ok ->
            void;
        {error, Cause} ->
            ?error("large_obj_delete_chunks/2", "key:~s, index:~p, cause:~p",
                   [binary_to_list(Key1), Index1, Cause])
    end,
    large_obj_delete_chunks(Key1, Rest).

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
% Trim the trailing path separator
-spec(path_trim_trailing_sep(binary()) ->
      binary()).
path_trim_trailing_sep(Src) ->
    case binary:last(Src) of
        $/ ->
            binary:part(Src, 0, size(Src) - 1);
        _ ->
            Src
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
-spec(get_disk_usage() -> 
      {ok, {Total::pos_integer(), Free::pos_integer()}}| {error, any()}).
get_disk_usage() ->
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
    get_disk_usage(StorageNodes, {0, 0}).

get_disk_usage([], {Total, Free}) ->
    {ok, {erlang:round(Total * 1024), erlang:round(Free * 1024)}};
get_disk_usage([Node|Rest], {SumTotal, SumFree}) ->
    case rpc:call(Node, leo_storage_api, get_disk_usage, [], 5000) of
        {ok, {Total, Free}} ->
            get_disk_usage(Rest, {SumTotal + Total, SumFree + Free});
        {badrpc, Cause} ->
            {error, Cause};
        Error ->
            Error
    end.

