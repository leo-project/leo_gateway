-module(leo_gateway_nfs_uid_ets).
-behaviour(leo_gateway_nfs_uid_behaviour).
-export([init/1, new/1, get/1, delete/1, get_uid_list/0, get_path_list/0]).

-define(UID2PATH_ETS_TBL, uid2path_ets_tbl).
-define(PATH2UID_ETS_TBL, path2uid_ets_tbl).

init(_Params) ->
    catch ets:new(?UID2PATH_ETS_TBL, [set, named_table, public]),
    catch ets:new(?PATH2UID_ETS_TBL, [set, named_table, public]),
    ok.

new(Path4S3) ->
    Ret = ets:lookup(?PATH2UID_ETS_TBL, Path4S3),
    case Ret of
        [] ->
            NewUID = v4(),
            ets:insert(?UID2PATH_ETS_TBL, {NewUID, Path4S3}),
            ets:insert(?PATH2UID_ETS_TBL, {Path4S3, NewUID}),
            {ok, NewUID};
        [{_, UID}|_] ->
            %% already exists
            {ok, UID}
    end.

get(UID) ->
    Ret = ets:lookup(?UID2PATH_ETS_TBL, UID),
    case Ret of
        [] ->
            not_found;
        [{_, Path4S3}|_] ->
            {ok, Path4S3}
    end.

delete(UID) ->
    Ret = ets:lookup(?UID2PATH_ETS_TBL, UID),
    case Ret of
        [] ->
            ok;
        [{_, Path4S3}|_] ->
            catch ets:delete(?UID2PATH_ETS_TBL, UID),
            catch ets:delete(?PATH2UID_ETS_TBL, Path4S3),
            ok
    end.

get_uid_list() ->
    {ok, lists:flatten(ets:match(?UID2PATH_ETS_TBL, {'$1', '_'}))}.

get_path_list() ->
    {ok, lists:flatten(ets:match(?UID2PATH_ETS_TBL, {'_', '$1'}))}.

% private
% Generates a random binary UUID.
v4() ->
  v4(crypto:rand_uniform(1, round(math:pow(2, 48))) - 1, crypto:rand_uniform(1, round(math:pow(2, 12))) - 1, crypto:rand_uniform(1, round(math:pow(2, 32))) - 1, crypto:rand_uniform(1, round(math:pow(2, 30))) - 1).

v4(R1, R2, R3, R4) ->
    <<R1:48, 4:4, R2:12, 2:2, R3:32, R4: 30>>.

