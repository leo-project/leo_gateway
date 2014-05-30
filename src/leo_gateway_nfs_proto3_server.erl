-module(leo_gateway_nfs_proto3_server).
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_s3_libs/include/leo_s3_bucket.hrl").
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

-record(simplenfs_ongoing_readdir, {
    filelist  :: list(file:filename_all()),
    pos       :: pos_integer()
}).

-record(state, {
    debug      :: boolean(),
    write_verf :: binary() %% to be consistent during a single boot session
}).

-define(SIMPLENFS_WCC_EMPTY, 
    {
        {false, void},
        {false, void}
    }
).

init(_Args) ->
    Debug = case application:get_env(rpc_server, debug) of
        undefined ->
            false;
        {ok, Val} ->
            Val
    end,
    State = #state{debug = Debug,
                   write_verf = crypto:rand_bytes(8)},
    {ok, State}.
 
handle_call(Req, _From, #state{debug = Debug} = S) ->
    case Debug of
        true -> io:format(user, "[handle_call]req:~p from:~p~n",[Req, _From]);
        false -> void
    end,
    {reply, [], S}.
 
handle_cast(Req, #state{debug = Debug} = S) ->
    case Debug of
        true -> io:format(user, "[handle_cast]req:~p~n",[Req]);
        false -> void
    end,
    {reply, [], S}.
 
handle_info(Req, #state{debug = Debug} = S) ->
    case Debug of
        true -> io:format(user, "[handle_info]req:~p~n",[Req]);
        false -> void
    end,
    {noreply, S}.
 
terminate(_Reason, _S) ->
    ok.

%% about readdir
%% dict(CookieVerf::binary(), ReadDir::#simplenfs_ongoing_readdir{}), 
readdir_add_entry(Path) ->
    ReadDirDict = case application:get_env(simplenfs, ongoing_readdir) of
        undefined ->
            dict:new();
        {ok, Val} ->
            Val
    end,
    readdir_add_entry(Path, ReadDirDict).
readdir_add_entry(Path, ReadDirDict) ->
    case file:list_dir(Path) of
        {ok, FileList}->
            %% gen cookie verfier
            %% @TODO must be uniq
            <<CookieVerf:8/binary, _/binary>> = erlang:md5(Path),
            %% store new simplenfs_ongoing_readdir
            ReadDir = #simplenfs_ongoing_readdir{filelist = FileList, pos = 0},
            NewReadDirDict = dict:store(CookieVerf, ReadDir, ReadDirDict),
            application:set_env(simplenfs, ongoing_readdir, NewReadDirDict),
            {ok, CookieVerf, ReadDir};
        {error, _Reason}->
            %% return empty
            %% @TODO error handling
            {ok, <<>>, []} 
    end.

readdir_set_entry(CookieVerf, ReadDir) ->
    ReadDirDict = case application:get_env(simplenfs, ongoing_readdir) of
        undefined ->
            dict:new();
        {ok, Val} ->
            Val
    end,
    readdir_set_entry(CookieVerf, ReadDir, ReadDirDict).
readdir_set_entry(CookieVerf, ReadDir, ReadDirDict) ->
    NewReadDirDict = dict:store(CookieVerf, ReadDir, ReadDirDict),
    application:set_env(simplenfs, ongoing_readdir, NewReadDirDict).

readdir_get_entry(CookieVerf) ->
    case application:get_env(simplenfs, ongoing_readdir) of
        undefined ->
            {ok, CookieVerf, []};
        {ok, ReadDirDict} ->
            case dict:find(CookieVerf, ReadDirDict) of
                {ok, ReadDir} ->
                    {ok, CookieVerf, ReadDir};
                error ->
                    {ok, CookieVerf, []}
            end
    end.

readdir_del_entry(CookieVerf) ->
    case application:get_env(simplenfs, ongoing_readdir) of
        {ok, ReadDirDict} ->
            NewReadDirDict = dict:erase(CookieVerf, ReadDirDict),
            application:set_env(simplenfs, ongoing_readdir, NewReadDirDict);
        undefined ->
            void
    end.

readdir_create_resp(Path, ReadDir, NumEntry) ->
    readdir_create_resp(Path, ReadDir, NumEntry, void).
readdir_create_resp(_Path,
            #simplenfs_ongoing_readdir{filelist = FileList,
                                       pos      = Pos} = ReadDir, NumEntry, Resp)
            when length(FileList) == Pos orelse NumEntry == 0 ->
    {Resp, ReadDir};
readdir_create_resp(Dir,
            #simplenfs_ongoing_readdir{filelist = FileList,
                                       pos      = Pos} = ReadDir, NumEntry, Resp) ->
    Name = lists:nth(Pos + 1, FileList),
    FilePath = filename:join(Dir, Name),
    case file:read_file_info(FilePath, [{time, posix}]) of
        {ok, FileInfo} ->
            NewResp = {FileInfo#file_info.inode,
                     Name,
                     0,
                     {true, %% post_op_attr
                         void % @todo file_info2fattr3(FileInfo)
                     },
                     {true, {FilePath}}, %% post_op_fh3
                     Resp
                    },
            readdir_create_resp(Dir,
                                ReadDir#simplenfs_ongoing_readdir{pos = Pos + 1},
                                NumEntry - 1,
                                NewResp);
        {error, _Reason} ->
            readdir_create_resp(Dir,
                                ReadDir#simplenfs_ongoing_readdir{pos = Pos + 1},
                                NumEntry - 1,
                                Resp)
    end.

-define(UNIX_TIME_BASE, 62167219200).
unix_time() -> 
    % calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}}) = 62167219200
    calendar:datetime_to_gregorian_seconds(calendar:universal_time()) - ?UNIX_TIME_BASE.

gs2unix_time(GS) ->
    GS - ?UNIX_TIME_BASE.

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

meta2fattr3(Meta) ->
    UT = gs2unix_time(Meta#?METADATA.timestamp),
    {'NF3REG',
     8#00666,  % @todo determin based on ACL? protection mode bits
     0,   % # of hard links
     0,   % @todo determin base on ACL? uid
     0,   % @todo gid
     Meta#?METADATA.dsize,  % file size
     Meta#?METADATA.dsize,  % @todo actual size used at disk(LeoFS should return `body + metadata + header/footer`)
     {0, 0}, % data used for special file(in Linux first is major, second is minor number)
     0, % fsid
     0, % @todo Unique ID to be specifed fieldid 
     {UT, 0}, % last access
     {UT, 0}, % last modification
     {UT, 0}}.% last change

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
     0, % @todo Unique ID to be specifed fieldid 
     {UT, 0}, % last access
     {UT, 0}, % last modification
     {UT, 0}}.% last change

binary_is_contained(<<>>, _Char) ->
    false;
binary_is_contained(<<Char:8, _Rest/binary>>, Char) ->
    true;
binary_is_contained(<<_Other:8, Rest/binary>>, Char) ->
    binary_is_contained(Rest, Char).

nfsproc3_null_3(_Clnt, State) ->
    {reply, [], State}.
 
nfsproc3_getattr_3({{<<$/, Path/binary>>}}, Clnt, #state{debug = Debug} = State) ->
    case Debug of
        true -> io:format(user, "[getattr]args:~p client:~p~n",[Path, Clnt]);
        false -> void
    end,
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
                    {reply, {'NFS3ERR_NOENT', void}, State};
                {error, Reason} ->
                    io:format(user, "[debug]read_file_info failed reason:~p~n", [Reason]),
                    {reply, {'NFS3ERR_IO', Reason}, State}
            end;
        %% bucket 
        false ->
            io:format(user, "[debug]find_bucket path:~p~n", [Path]),
            case leo_s3_bucket:find_bucket_by_name(Path) of
                {ok, Bucket} ->
                    Attr = bucket2fattr3(Bucket),
                    io:format(user, "[debug]find_bucket attr:~p~n", [Attr]),
                    {reply, 
                    {'NFS3_OK',
                    {
                       Attr
                    }}, 
                    State};
                {error, Reason} ->
                    io:format(user, "[debug]read_file_info failed reason:~p~n", [Reason]),
                    {reply, {'NFS3ERR_IO', Reason}, State};
                not_found ->
                    {reply, {'NFS3ERR_NOENT', void}, State}
            end
    end.
     
nfsproc3_setattr_3({{Path},
                    {Mode,
                     UID,
                     GID,
                     _,
                     ATime,
                     MTime
                    },_Guard} = _1, Clnt, #state{debug = Debug} = State) ->
    case Debug of
        true -> io:format(user, "[setattr]args:~p client:~p~n",[_1, Clnt]);
        false -> void
    end,
    FileInfo = #file_info{
            mode = sattr_mode2file_info(Mode),
            uid  = sattr_uid2file_info(UID),
            gid  = sattr_gid2file_info(GID),
            atime = sattr_atime2file_info(ATime),
            mtime = sattr_mtime2file_info(MTime)
            },
    case file:write_file_info(Path, FileInfo, [{time, posix}]) of
        ok ->
            {reply, 
                {'NFS3_OK',
                {
                    ?SIMPLENFS_WCC_EMPTY
                }}, 
                State};
        {error, Reason} ->
            io:format(user, "[setattr]error reason:~p~n",[Reason]),
            {reply, 
                {'NFS3ERR_IO',
                {
                    ?SIMPLENFS_WCC_EMPTY
                }}, 
                State}
    end.
 
nfsproc3_lookup_3({{{Dir}, Name}} = _1, Clnt, #state{debug = Debug} = State) ->
    case Debug of
        true -> io:format(user, "[lookup]args:~p client:~p~n",[_1, Clnt]);
        false -> void
    end,
    Path = filename:join(Dir, Name),
    case file:read_file_info(Path, [{time, posix}]) of
        {ok, _FileInfo} ->
            {reply, 
                {'NFS3_OK',
                {
                    {Path}, %% pre_op_attr
                    {true, void}, %% @todo post_op_attr for obj
                    {false, void}  %% post_op_attr for dir
                }}, 
                State};
        {error, _} ->
            {reply, 
                {'NFS3ERR_NOENT',
                {
                    {false, void}  %% post_op_attr for dir
                }}, 
                State}
    end.
 
nfsproc3_access_3(_1, Clnt, #state{debug = Debug} = State) ->
    case Debug of
        true -> io:format(user, "[access]args:~p client:~p~n",[_1, Clnt]);
        false -> void
    end,
    {reply, 
        {'NFS3_OK',
        {
            {false, void}, %% post_op_attr for obj
            63       %% access bits(up all)
        }}, 
        State}.
 
nfsproc3_readlink_3(_1, Clnt, #state{debug = Debug} = State) ->
    case Debug of
        true -> io:format(user, "[readlink]args:~p client:~p~n",[_1, Clnt]);
        false -> void
    end,
    {reply, 
        {'NFS3_OK',
        {
            {false, void}, %% post_op_attr for obj
            <<"link path">>
        }}, 
        State}.
 
nfsproc3_read_3({{Path}, Offset, Count} =_1, Clnt, #state{debug = Debug} = State) ->
    case Debug of
        true -> io:format(user, "[read]args:~p client:~p~n",[_1, Clnt]);
        false -> void
    end,
    case file:open(Path, [read, binary, raw]) of
        {ok, IoDev} ->
            try
                case file:pread(IoDev, Offset, Count) of
                    {ok, Data} ->
                        {reply, 
                            {'NFS3_OK',
                            {
                                {false, void}, %% post_op_attr for obj
                                Count,         %% count read bytes
                                false,         %% eof
                                Data
                            }}, 
                            State};
                    eof ->
                        {reply, 
                            {'NFS3_OK',
                            {
                                {false, void}, %% post_op_attr for obj
                                0,             %% count read bytes
                                true,          %% eof
                                <<>>
                            }}, 
                            State};
                    {error, Reason} ->
                        io:format(user, "[read]error reason:~p~n",[Reason]),
                        {reply, 
                            {'NFS3ERR_IO',
                            {
                                {false, void} %% post_op_attr for obj
                            }}, 
                            State}
                end
            after
                file:close(IoDev)
            end;
        {error, Reason} ->
            io:format(user, "[read]open error reason:~p~n",[Reason]),
            {reply, 
                {'NFS3ERR_IO',
                {
                    {false, void} %% post_op_attr for obj
                }}, 
                State} 
    end.
 
nfsproc3_write_3({{Path}, Offset, Count, _HowStable, Data} = _1, Clnt, #state{debug = Debug} = State) ->
    case Debug of
        true -> io:format(user, "[write]args:~p client:~p~n",[_1, Clnt]);
        false -> void
    end,
    case file:open(Path, [read, write, binary, raw]) of
        {ok, IoDev} ->
            try
                case file:pwrite(IoDev, Offset, Data) of
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
                end
            after
                file:close(IoDev)
            end;
        {error, Reason} ->
            io:format(user, "[write]open error reason:~p~n",[Reason]),
            {reply, 
                {'NFS3ERR_IO',
                {
                    ?SIMPLENFS_WCC_EMPTY
                }}, 
                State}
    end.
    
 
nfsproc3_create_3({{{Dir}, Name}, {CreateMode, _How}} = _1, Clnt, #state{debug = Debug} = State) ->
    case Debug of
        true -> io:format(user, "[create]args:~p client:~p~n",[_1, Clnt]);
        false -> void
    end,
    OpenModes = case CreateMode of
        'UNCHECKED' ->
            [write];
        _ ->
            [write, exclusive]
    end,
    FilePath = filename:join(Dir, Name),
    case file:open(FilePath, OpenModes) of
        {ok, IoDev} ->
            catch file:close(IoDev),
            {reply, 
                {'NFS3_OK',
                {
                    {true, {FilePath}}, %% post_op file handle
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
 
nfsproc3_mkdir_3({{{Dir}, Name}, _How} = _1, Clnt, #state{debug = Debug} = State) ->
    case Debug of
        true -> io:format(user, "[mkdir]args:~p client:~p~n",[_1, Clnt]);
        false -> void
    end,
    DirPath = filename:join(Dir, Name),
    case file:make_dir(DirPath) of
        ok ->
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
 
nfsproc3_symlink_3(_1, Clnt, #state{debug = Debug} = State) ->
    case Debug of
        true -> io:format(user, "[symlink]args:~p client:~p~n",[_1, Clnt]);
        false -> void
    end,
    {reply, 
        {'NFS3_OK',
        {
            {false, void}, %% post_op file handle
            {false, void}, %% post_op_attr
            ?SIMPLENFS_WCC_EMPTY
        }}, 
        State}.
 
nfsproc3_mknod_3(_1, Clnt, #state{debug = Debug} = State) ->
    case Debug of
        true -> io:format(user, "[mknod]args:~p client:~p~n",[_1, Clnt]);
        false -> void
    end,
    {reply, 
        {'NFS3_OK',
        {
            {false, void}, %% post_op file handle
            {false, void}, %% post_op_attr
            ?SIMPLENFS_WCC_EMPTY
        }}, 
        State}.
 
nfsproc3_remove_3({{{Dir}, Name}} = _1, Clnt, #state{debug = Debug} = State) ->
    case Debug of
        true -> io:format(user, "[remove]args:~p client:~p~n",[_1, Clnt]);
        false -> void
    end,
    FilePath = filename:join(Dir, Name),
    case file:delete(FilePath) of
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
         
nfsproc3_rmdir_3({{{Dir}, Name}} = _1, Clnt, #state{debug = Debug} = State) ->
    case Debug of
        true -> io:format(user, "[rmdir]args:~p client:~p~n",[_1, Clnt]);
        false -> void
    end,
    DirPath = filename:join(Dir, Name),
    case file:del_dir(DirPath) of
        ok ->
            {reply, 
                {'NFS3_OK',
                {
                    ?SIMPLENFS_WCC_EMPTY
                }}, 
                State};
        {error, Reason} ->
            io:format(user, "[rmdir]reason:~p~n",[Reason]),
            {reply, 
                {'NFS3ERR_IO',
                {
                    ?SIMPLENFS_WCC_EMPTY
                }}, 
                State}
    end.
 
nfsproc3_rename_3({{{SrcDir}, SrcName}, {{DstDir}, DstName}} =_1, Clnt, #state{debug = Debug} = State) ->
    case Debug of
        true -> io:format(user, "[rename]args:~p client:~p~n",[_1, Clnt]);
        false -> void
    end,
    Src = filename:join(SrcDir, SrcName),
    Dst = filename:join(DstDir, DstName),
    case file:rename(Src, Dst) of
        ok ->
            {reply, 
                {'NFS3_OK',
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
 
nfsproc3_link_3(_1, Clnt, #state{debug = Debug} = State) ->
    case Debug of
        true -> io:format(user, "[link]args:~p client:~p~n",[_1, Clnt]);
        false -> void
    end,
    {reply, 
        {'NFS3_NG',
        {
            {false, void}, %% post_op_attr
            ?SIMPLENFS_WCC_EMPTY
        }}, 
        State}.
 
nfsproc3_readdir_3(_1, Clnt, #state{debug = Debug} = State) ->
    case Debug of
        true -> io:format(user, "[readdir]args:~p client:~p~n",[_1, Clnt]);
        false -> void
    end,
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
 
nfsproc3_readdirplus_3({{Path}, _Cookie, CookieVerf, _DirCnt, _MaxCnt} = _1, Clnt, #state{debug = Debug} = State) ->
    case Debug of
        true -> io:format(user, "[readdirplus]args:~p client:~p~n",[_1, Clnt]);
        false -> void
    end,
    {ok, NewCookieVerf, ReadDir} = case CookieVerf of
        <<0,0,0,0,0,0,0,0>> ->
            readdir_add_entry(Path);
        CookieVerf ->
            readdir_get_entry(CookieVerf)
    end,
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
            {Resp, NewReadDir} = readdir_create_resp(Path, ReadDir, 10),
            #simplenfs_ongoing_readdir{filelist = FileList, pos = Pos} = NewReadDir,
            EOF = (length(FileList) == Pos),
            case EOF of
                true -> 
                    readdir_del_entry(NewCookieVerf);
                false ->
                    readdir_set_entry(NewCookieVerf, NewReadDir)
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

nfsproc3_fsstat_3(_1, Clnt, #state{debug = Debug} = State) ->
    case Debug of
        true -> io:format(user, "[fsstat]args:~p client:~p~n",[_1, Clnt]);
        false -> void
    end,
    {reply, 
        {'NFS3_OK',
        {
            {false, void}, %% post_op_attr
            8192, %% total size
            1024, %% free size
            1024, %% free size(for auth user)
            16,   %% # of files
            8,    %% # of free file slots
            8,    %% # of free file slots(for auth user)
            10    %% invarsec
        }}, 
        State}.
 
nfsproc3_fsinfo_3(_1, Clnt, #state{debug = Debug} = State) ->
    case Debug of
        true -> io:format(user, "[fsinfo]args:~p client:~p~n",[_1, Clnt]);
        false -> void
    end,
    {reply, 
        {'NFS3_OK',
        {
            {false, void}, %% post_op_attr
            32768, %% rtmax
            32768, %% rtperf
            8,    %% rtmult
            32768, %% wtmax
            32768, %% wtperf
            8,    %% wtmult
            4096, %% dperf
            1024 * 1024 * 1024 * 4, %% max size of a file
            {1, 0}, %% time_delta
            0     %% properties
        }}, 
        State}.
 
nfsproc3_pathconf_3(_1, Clnt, #state{debug = Debug} = State) ->
    case Debug of
        true -> io:format(user, "[pathconf]args:~p client:~p~n",[_1, Clnt]);
        false -> void
    end,
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
 
nfsproc3_commit_3(_1, Clnt, #state{debug = Debug} = State) ->
    case Debug of
        true -> io:format(user, "[commit]args:~p client:~p~n",[_1, Clnt]);
        false -> void
    end,
    {reply, 
        {'NFS3_OK',
        {
            ?SIMPLENFS_WCC_EMPTY,
            State#state.write_verf %% write verfier
        }}, 
        State}.

