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

-export([sattr_mode2file_info/1, sattr_uid2file_info/1, sattr_gid2file_info/1,
         sattr_atime2file_info/1, sattr_mtime2file_info/1, sattr_size2file_info/1]).

-record(ongoing_readdir, {
    filelist  :: list(#?METADATA{})
}).

-define(SIMPLENFS_WCC_EMPTY, 
    {
        {false, void},
        {false, void}
    }
).
-define(NFS_DUMMY_FILE4S3DIR, <<"$$_dir_$$">>).
-define(NFS_READDIRPLUS_NUM_OF_RESPONSE, 10).

% @doc
% Called only once from a parent rpc server process to initialize this module
% during starting a leo_storage server.
-spec(init(any()) -> {ok, any()}).
init(_Args) ->
    leo_gateway_nfs_state_ets:add_write_verfier(crypto:rand_bytes(8)),
    {ok, void}.
 
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
% @private
% Returns true if Path refers to a directory which have child files, 
% and false otherwise.
-spec(is_empty_dir(binary()) -> boolean()).
is_empty_dir(Path) ->
    case leo_gateway_nfs_file_handler:list_dir(Path, false) of
        {ok, MetaList} when is_list(MetaList) ->
            FilteredList = [Meta || Meta <- MetaList, filename:basename(Meta#?METADATA.key) =/= ?NFS_DUMMY_FILE4S3DIR],
            length(FilteredList) =:= 0;
        _Error ->
            false 
    end.

% @doc
% @private
% Returns list of file's metadatas stored in the specified Path and
% store that with a verifier to identify and retrieve later.
-spec(readdir_add_entry(binary()) -> 
      {ok, binary(), list(#?METADATA{})}).
readdir_add_entry(Path) ->
    case leo_gateway_nfs_file_handler:list_dir(Path) of
        {ok, FileList}->
            %% gen cookie verfier
            CookieVerf = crypto:rand_bytes(8),
            ReadDir = #ongoing_readdir{filelist = FileList},
            leo_gateway_nfs_state_ets:add_readdir_entry(CookieVerf, ReadDir),
            {ok, CookieVerf, ReadDir};
         Error ->
            Error
    end.

% @doc
% @private
% get resources correspond with a cookies verifier which returned by readdir_get_entry
-spec(readdir_get_entry(binary()) -> ok).
readdir_get_entry(CookieVerf) ->
    case leo_gateway_nfs_state_ets:get_readdir_entry(CookieVerf) of
        not_found ->
            {ok, CookieVerf, []};
        {ok, Ret} ->
            {ok, CookieVerf, Ret}
    end.

% @doc
% @private
% Delete resources correspond with a cookies verifier which returned by readdir_get_entry
-spec(readdir_del_entry(binary()) -> ok).
readdir_del_entry(CookieVerf) ->
    leo_gateway_nfs_state_ets:del_readdir_entry(CookieVerf).

% @doc
% @private
% Create a rpc response for a readdir3 request
-spec(readdir_create_resp(binary(), integer(), #ongoing_readdir{}, integer()) ->
      void | tuple()).
readdir_create_resp(Path, Cookie,
            #ongoing_readdir{filelist = FileList} = ReadDir, NumEntry) ->
    {StartCookie, EOF} = case length(FileList) =< (Cookie + NumEntry) of
        true ->
            {length(FileList), true};
        false ->
            {Cookie + NumEntry, false}
    end,
    readdir_create_resp(Path, 
                        StartCookie, 
                        ReadDir, 
                        Cookie, 
                        EOF,
                        void).

readdir_create_resp(_Path, CurCookie,
                    _ReadDir,
                    Cookie, EOF, Resp)
            when CurCookie =:= Cookie ->
    {Resp, EOF};

readdir_create_resp(Path, CurCookie,
                    #ongoing_readdir{filelist = FileList} = ReadDir, 
                    Cookie, EOF, Resp) ->
    Meta = lists:nth(CurCookie, FileList),
    #?METADATA{key = Key, dsize = Size} = Meta,
    NormalizedKey = case Size of
        -1 ->
            % dir to be normalized(means expand .|.. chars)
            leo_gateway_nfs_file_handler:path_relative2abs(Key);
        _ ->
            % file
            Key
    end,
    FileName = filename:basename(Key),
    case FileName of
        ?NFS_DUMMY_FILE4S3DIR ->
            readdir_create_resp(Path, 
                                CurCookie - 1, 
                                ReadDir,
                                Cookie, 
                                EOF,
                                Resp);
        _ ->
            {ok, UID} = leo_gateway_nfs_state_ets:add_path(NormalizedKey),
            NewResp = {inode(NormalizedKey),
                       FileName,
                       CurCookie,
                       {true, %% post_op_attr
                           meta2fattr3(Meta#?METADATA{key = NormalizedKey})
                       },
                       {true, {UID}}, %% post_op_fh3
                       Resp
                      },
            ?debug("readdir_create_resp", "key:~p cookie:~p~n", 
                   [NormalizedKey, CurCookie]),
            readdir_create_resp(Path, 
                                CurCookie - 1,
                                ReadDir,
                                Cookie,
                                EOF,
                                NewResp)
    end.

% @doc
% @private
% Return the inode of Path
% @todo to be replaced with truely unique id supported by LeoFS future works 
-spec(inode(binary()) ->
      integer()).
inode(Path) ->
    <<F8:8/binary, _/binary>> = erlang:md5(Path),
    Hex = leo_hex:binary_to_hex(F8),
    leo_hex:hex_to_integer(Hex).

sattr_mode2file_info({0, _})   -> undefined;
sattr_mode2file_info({true, Mode}) -> Mode.

sattr_uid2file_info({0, _})   -> undefined;
sattr_uid2file_info({true, UID}) -> UID.

sattr_gid2file_info({0, _})   -> undefined;
sattr_gid2file_info({true, GID}) -> GID.

sattr_size2file_info({true, Size}) -> Size;
sattr_size2file_info({_, _})   -> undefined.

sattr_atime2file_info({'DONT_CHANGE', _}) -> undefined;
sattr_atime2file_info({'SET_TO_SERVER_TIME', _}) -> leo_gateway_nfs_file_handler:unix_time();
sattr_atime2file_info({_, {ATime, _}})         -> ATime.

sattr_mtime2file_info({'DONT_CHANGE', _}) -> undefined;
sattr_mtime2file_info({'SET_TO_SERVER_TIME', _}) -> leo_gateway_nfs_file_handler:unix_time();
sattr_mtime2file_info({_, {MTime, _}})         -> MTime.

% @doc
% @private
% Convert from #?METADATA{} format to the format codes erpcgen generated expect
-spec(meta2fattr3(#?METADATA{}) ->
      tuple()).
meta2fattr3(#?METADATA{dsize = -1, key = Key}) ->
    Now = calendar:datetime_to_gregorian_seconds(calendar:universal_time()),
    UT = leo_gateway_nfs_file_handler:gs2unix_time(Now),
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
    UT = leo_gateway_nfs_file_handler:gs2unix_time(TS),
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
% @private
% Convert from the directory path to the format codes erpcgen generated expect
-spec(s3dir2fattr3(binary()) ->
      tuple()).
s3dir2fattr3(Dir) ->
    Now = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
    UT = leo_gateway_nfs_file_handler:gs2unix_time(Now),
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
% @private
% Convert from #?BUCKET to the format codes erpcgen generated expect
-spec(bucket2fattr3(#?BUCKET{}) ->
      tuple()).
bucket2fattr3(Bucket) ->
    UT = leo_gateway_nfs_file_handler:gs2unix_time(Bucket#?BUCKET.last_modified_at),
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

nfsproc3_null_3(_Clnt, State) ->
    {reply, [], State}.
 
nfsproc3_getattr_3({{UID}} = _1, Clnt, State) ->
    ?debug("nfsproc3_getattr_3", "args:~p client:~p", [_1, Clnt]),
    {ok, Path} = leo_gateway_nfs_state_ets:get_path(UID),
    case leo_gateway_nfs_file_handler:binary_is_contained(Path, $/) of
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
                    Path4S3Dir = leo_gateway_nfs_file_handler:path2dir(Path),
                    case leo_gateway_nfs_file_handler:is_dir(Path4S3Dir) of
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
nfsproc3_setattr_3({{FileUID},
                    {_Mode,
                     _UID,
                     _GID,
                     SattrSize,
                     _ATime,
                     _MTime
                    },_Guard} = _1, Clnt, State) ->
    ?debug("nfsproc3_setattr_3", "args:~p client:~p", [_1, Clnt]),
    Size = sattr_size2file_info(SattrSize),
    case Size of
        undefined ->
            {reply, 
                {'NFS3_OK',
                {
                    ?SIMPLENFS_WCC_EMPTY
                }}, 
                State};
        _ ->
            {ok, Path} = leo_gateway_nfs_state_ets:get_path(FileUID),
            case leo_gateway_nfs_file_handler:trim(Path, Size) of
                ok ->
                    {reply, 
                        {'NFS3_OK',
                        {
                            ?SIMPLENFS_WCC_EMPTY
                        }}, 
                        State};
                {error, Reason}->
                    {reply, {'NFS3ERR_IO', Reason}, State}
            end
    end.

         
nfsproc3_lookup_3({{{UID}, Name}} = _1, Clnt, State) ->
    ?debug("nfsproc3_lookup_3", "args:~p client:~p", [_1, Clnt]),
    {ok, Dir} = leo_gateway_nfs_state_ets:get_path(UID),
    Path4S3 = filename:join(Dir, Name),
    %% A path for directory must be trailing with '/'
    Path4S3Dir = leo_gateway_nfs_file_handler:path2dir(Path4S3),
    case leo_gateway_nfs_file_handler:is_file(Path4S3) orelse 
         leo_gateway_nfs_file_handler:is_dir(Path4S3Dir) of
        true ->
            {ok, FileUID} = leo_gateway_nfs_state_ets:add_path(Path4S3),
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
    {ok, Path} = leo_gateway_nfs_state_ets:get_path(UID),
    case leo_gateway_nfs_file_handler:read(Path, Offset, Offset + Count - 1) of
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
    {ok, Path} = leo_gateway_nfs_state_ets:get_path(UID),
    case leo_gateway_nfs_file_handler:write(Path, Offset, Offset + Count - 1, Data) of
        ok ->
            {ok, WriteVerf} = leo_gateway_nfs_state_ets:get_write_verfier(),
            {reply, 
                {'NFS3_OK',
                {
                    ?SIMPLENFS_WCC_EMPTY,
                    Count,
                    'DATA_SYNC',
                    WriteVerf
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
    {ok, Dir} = leo_gateway_nfs_state_ets:get_path(UID),
    FilePath4S3 = filename:join(Dir, Name),
    case leo_gateway_rpc_handler:put(FilePath4S3, <<>>) of
        {ok, _}->
            {ok, FileUID} = leo_gateway_nfs_state_ets:add_path(FilePath4S3),
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
    {ok, Dir} = leo_gateway_nfs_state_ets:get_path(UID),
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
    {ok, Dir} = leo_gateway_nfs_state_ets:get_path(UID),
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
    {ok, Dir} = leo_gateway_nfs_state_ets:get_path(UID),
    Path4S3 = filename:join(Dir, Name),
    Path4S3Dir = leo_gateway_nfs_file_handler:path2dir(Path4S3),
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
    {ok, SrcDir} = leo_gateway_nfs_state_ets:get_path(SrcUID),
    {ok, DstDir} = leo_gateway_nfs_state_ets:get_path(DstUID),
    Src4S3 = filename:join(SrcDir, SrcName),
    Dst4S3 = filename:join(DstDir, DstName),
    case leo_gateway_nfs_file_handler:rename(Src4S3, Dst4S3) of
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
 
nfsproc3_readdirplus_3({{UID}, Cookie, CookieVerf, _DirCnt, _MaxCnt} = _1, Clnt, State) ->
    ?debug("nfsproc3_readdirplus_3", "args:~p client:~p", [_1, Clnt]),
    {ok, Path} = leo_gateway_nfs_state_ets:get_path(UID),
    Path4S3Dir = leo_gateway_nfs_file_handler:path2dir(Path),
    {ok, NewCookieVerf, ReadDir} = case CookieVerf of
        <<0,0,0,0,0,0,0,0>> ->
            readdir_add_entry(Path4S3Dir);
        CookieVerf ->
            readdir_get_entry(CookieVerf)
    end,
    case ReadDir of
        [] ->
            % empty response
            readdir_del_entry(NewCookieVerf),
            {reply, 
                {'NFS3_OK',
                {
                    {false, void},       %% post_op_attr
                    NewCookieVerf,       %% cookie verfier
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
            {Resp, EOF} = readdir_create_resp(Path4S3Dir, Cookie, ReadDir, ?NFS_READDIRPLUS_NUM_OF_RESPONSE),
            %?debug("nfsproc3_readdirplus_3", "eof:~p resp:~p~n", [EOF, Resp]),
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
    {ok, {Total, Free}} = leo_gateway_nfs_file_handler:get_disk_usage(),
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
    {ok, WriteVerf} = leo_gateway_nfs_state_ets:get_write_verfier(),
    {reply, 
        {'NFS3_OK',
        {
            ?SIMPLENFS_WCC_EMPTY,
            WriteVerf
        }}, 
        State}.

