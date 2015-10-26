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
%%======================================================================
-module(leo_nfs_mount3_server).

-include("leo_nfs_mount3.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([mountproc_null_3/2,
         mountproc_mnt_3/3,
         mountproc_dump_3/2,
         mountproc_umnt_3/3,
         mountproc_umntall_3/2,
         mountproc_export_3/2]).

init(_Args) ->
    io:format(user, "[debug]mountd init called args:~p pid:~p~n",[_Args, self()]),
    leo_nfs_state_ets:init(_Args),
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


%% @doc
mountproc_null_3(_Clnt, State) ->
    ?debug("mountproc_null_3", "clnt:~p", [_Clnt]),
    {reply, [], State}.


%% @doc
mountproc_mnt_3(MountDir0, Clnt, State) ->
    ?debug("mountproc_mnt_3", "mount:~p clnt:~p", [MountDir0, Clnt]),
    %% validate path
    MountDir = formalize_path(MountDir0),
    case is_valid_mount_dir(MountDir) of
        true ->
            {ok, {Addr, _Port}}= nfs_rpc_proto:client_ip(Clnt),
            mount_add_entry(MountDir, Addr),
            <<$/, MountDir4S3/binary>> = MountDir,
            {ok, UID} = leo_nfs_state_ets:add_path(MountDir4S3),
            {reply, {'MNT3_OK', {UID, []}}, State};
        _ ->
            {reply, {'MNT3ERR_NOTDIR', []}, State}
    end.


%% @doc
mountproc_dump_3(Clnt, State) ->
    ?debug("mountproc_dump_3", "clnt:~p", [Clnt]),
    {reply, void, State}.


%% @doc
mountproc_umnt_3(MountDir0, Clnt, State) ->
    ?debug("mountproc_umnt_3", "mount:~p clnt:~p", [MountDir0, Clnt]),
    MountDir = formalize_path(MountDir0),
    {ok, {Addr, _Port}}= nfs_rpc_proto:client_ip(Clnt),
    catch mount_del_entry(MountDir, Addr),
    {reply, void, State}.


%% @doc
mountproc_umntall_3(Clnt, State) ->
    ?debug("mountproc_umntall_3", "clnt:~p", [Clnt]),
    {reply, void, State}.


%% @doc
mountproc_export_3(Clnt, State) ->
    ?debug("mountproc_export_3", "clnt:~p", [Clnt]),
    {reply, void, State}.



%% @private
formalize_path(Path) ->
    case (binary:last(Path) == $/) of
        true ->
            binary:part(Path, {0, byte_size(Path) - 1});
        false ->
            Path
    end.


%% @doc Valid path patterns for LeoFS
%%      1. /$BucketName/
%%      2. /$BucketName
%% @private
is_valid_mount_dir(<<$/,Rest/binary>>) ->
    %% trim last /
    Path = formalize_path(Rest),
    case leo_s3_bucket:find_bucket_by_name(Path) of
        {ok, _Bucket} ->
            true;
        Error ->
            ?error("is_valid_mount_dir", [{cause, Error}]),
            false
    end;
is_valid_mount_dir(_Path) ->
    false.


%% @private
mount_add_entry(MountDir, Addr) ->
    MountPointDict = case application:get_env(?MODULE, mount_point) of
                         undefined ->
                             dict:new();
                         {ok, Val} ->
                             Val
                     end,
    mount_add_entry(MountDir, Addr, MountPointDict).
mount_add_entry(MountDir, Addr, MountPointDict) ->
    case dict:find(MountDir, MountPointDict) of
        {ok, IPList}->
            case lists:member(Addr, IPList) of
                true -> void;
                false ->
                    NewIPList = [Addr|IPList],
                    NewMountPointDict = dict:store(MountDir, NewIPList, MountPointDict),
                    application:set_env(?MODULE, mount_point, NewMountPointDict)
            end;
        error ->
            NewMountPointDict = dict:store(MountDir, [Addr], MountPointDict),
            application:set_env(?MODULE, mount_point, NewMountPointDict)
    end.


%% @private
mount_del_entry(MountDir, Addr) ->
    case application:get_env(?MODULE, mount_point) of
        {ok, MountPointDict} ->
            mount_del_entry(MountDir, Addr, MountPointDict);
        undefined ->
            void
    end.
mount_del_entry(MountDir, Addr, MountPointDict) ->
    case dict:find(MountDir, MountPointDict) of
        {ok, IPList}->
            NewIPList = lists:delete(Addr, IPList),
            NewMountPointDict = dict:store(MountDir, NewIPList, MountPointDict),
            application:set_env(?MODULE, mount_point, NewMountPointDict);
        error ->
            void
    end.
