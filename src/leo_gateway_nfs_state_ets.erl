-module(leo_gateway_nfs_state_ets).
-behaviour(leo_gateway_nfs_state_behaviour).
-export([init/1, 
         add_path/1, get_path/1, del_path/1, get_uid_list/0, get_path_list/0,
         add_readdir_entry/2, get_readdir_entry/1, del_readdir_entry/1,
         add_write_verfier/1, get_write_verfier/0]).

-define(LEO_GW_NFS_UID2PATH_ETS_TBL, uid2path_ets_tbl).
-define(LEO_GW_NFS_PATH2UID_ETS_TBL, path2uid_ets_tbl).
-define(LEO_GW_NFS_READDIR_ENTRY_ETS_TBL, readdir_entry_ets_tbl).
-define(LEO_GW_NFS_WRITE_VERIFIER_ETS_TBL, write_verifier_ets_tbl).

init(_Params) ->
    ets:new(?LEO_GW_NFS_UID2PATH_ETS_TBL, [set, named_table, public]),
    ets:new(?LEO_GW_NFS_PATH2UID_ETS_TBL, [set, named_table, public]),
    ets:new(?LEO_GW_NFS_READDIR_ENTRY_ETS_TBL, [set, named_table, public]),
    ets:new(?LEO_GW_NFS_WRITE_VERIFIER_ETS_TBL, [set, named_table, public]),
    ok.

add_path(Path4S3) ->
    Ret = ets:lookup(?LEO_GW_NFS_PATH2UID_ETS_TBL, Path4S3),
    case Ret of
        [] ->
            NewUID = v4(),
            ets:insert(?LEO_GW_NFS_UID2PATH_ETS_TBL, {NewUID, Path4S3}),
            ets:insert(?LEO_GW_NFS_PATH2UID_ETS_TBL, {Path4S3, NewUID}),
            {ok, NewUID};
        [{_, UID}|_] ->
            %% already exists
            {ok, UID}
    end.

get_path(UID) ->
    Ret = ets:lookup(?LEO_GW_NFS_UID2PATH_ETS_TBL, UID),
    case Ret of
        [] ->
            not_found;
        [{_, Path4S3}|_] ->
            {ok, Path4S3}
    end.

del_path(UID) ->
    Ret = ets:lookup(?LEO_GW_NFS_UID2PATH_ETS_TBL, UID),
    case Ret of
        [] ->
            true;
        [{_, Path4S3}|_] ->
            catch ets:delete(?LEO_GW_NFS_UID2PATH_ETS_TBL, UID),
            ets:delete(?LEO_GW_NFS_PATH2UID_ETS_TBL, Path4S3)
    end.

get_uid_list() ->
    {ok, lists:flatten(ets:match(?LEO_GW_NFS_UID2PATH_ETS_TBL, {'$1', '_'}))}.

get_path_list() ->
    {ok, lists:flatten(ets:match(?LEO_GW_NFS_UID2PATH_ETS_TBL, {'_', '$1'}))}.

add_readdir_entry(CookieVerf, ReadDirEntry) ->
    ets:insert(?LEO_GW_NFS_READDIR_ENTRY_ETS_TBL, 
               {CookieVerf, ReadDirEntry}).

get_readdir_entry(CookieVerf) ->
    Ret = ets:lookup(?LEO_GW_NFS_READDIR_ENTRY_ETS_TBL, CookieVerf),
    case Ret of
        [] ->
            not_found;
        [{_, ReadDirEntry}|_] ->
            {ok, ReadDirEntry}
    end.

del_readdir_entry(CookieVerf) ->
    ets:delete(?LEO_GW_NFS_READDIR_ENTRY_ETS_TBL, CookieVerf).

add_write_verfier(WriteVerf) ->
    ets:insert(?LEO_GW_NFS_READDIR_ENTRY_ETS_TBL, 
               {write_verf, WriteVerf}).

get_write_verfier() ->
    Ret = ets:lookup(?LEO_GW_NFS_READDIR_ENTRY_ETS_TBL, write_verf),
    case Ret of
        [] ->
            not_found;
        [{_, WriteVerf}|_] ->
            {ok, WriteVerf}
    end.

% private
% Generates a random binary UUID.
v4() ->
  v4(crypto:rand_uniform(1, round(math:pow(2, 48))) - 1, crypto:rand_uniform(1, round(math:pow(2, 12))) - 1, crypto:rand_uniform(1, round(math:pow(2, 32))) - 1, crypto:rand_uniform(1, round(math:pow(2, 30))) - 1).

v4(R1, R2, R3, R4) ->
    <<R1:48, 4:4, R2:12, 2:2, R3:32, R4: 30>>.

