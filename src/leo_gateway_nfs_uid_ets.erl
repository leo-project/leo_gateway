-module(leo_gateway_nfs_uid_ets).
-behaviour(leo_gateway_nfs_uid_behaviour).
-export([init/1, new/1, get/1, delete/1, get_uid_list/0, get_path_list/0]).

-define(UID_ETS_TBL, uid_ets_tbl).

init(_Params) ->
    catch ets:new(?UID_ETS_TBL, [set, named_table, public]),
    ok.

new(Path4S3) ->
    UID = v4(),
    ets:insert(?UID_ETS_TBL, {UID, Path4S3}),
    {ok, UID}.

get(UID) ->
    Ret = ets:lookup(?UID_ETS_TBL, UID),
    case Ret of
        [] ->
            not_found;
        [{_, Path4S3}|_] ->
            {ok, Path4S3}
    end.

delete(UID) ->
    ets:delete(?UID_ETS_TBL, UID),
    ok.

get_uid_list() ->
    {ok, lists:flatten(ets:match(?UID_ETS_TBL, {'$1', '_'}))}.

get_path_list() ->
    {ok, lists:flatten(ets:match(?UID_ETS_TBL, {'_', '$1'}))}.

% private
% Generates a random binary UUID.
v4() ->
  v4(crypto:rand_uniform(1, round(math:pow(2, 48))) - 1, crypto:rand_uniform(1, round(math:pow(2, 12))) - 1, crypto:rand_uniform(1, round(math:pow(2, 32))) - 1, crypto:rand_uniform(1, round(math:pow(2, 30))) - 1).

v4(R1, R2, R3, R4) ->
    <<R1:48, 4:4, R2:12, 2:2, R3:32, R4: 30>>.

