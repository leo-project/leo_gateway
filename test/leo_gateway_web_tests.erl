%%====================================================================
%%
%% LeoFS Gateway
%%
%% Copyright (c) 2012
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
%% -------------------------------------------------------------------
%% LeoFS Gateway - S3 domainogics Test
%% @doc
%% @end
%%====================================================================
-module(leo_gateway_web_tests).
-author('Yoshiyuki Kanno').
-vsn('0.9.1').

-include("leo_gateway.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TARGET_HOST, "localhost").

%%--------------------------------------------------------------------
%% TEST
%%--------------------------------------------------------------------
-ifdef(EUNIT).
api_mochiweb_test_() ->
    {setup, fun setup_mochiweb/0, fun teardown/1,
     {with, [
             fun get_bucket_list_error_/1,
             fun get_bucket_list_empty_/1,
             fun get_bucket_list_normal1_/1,
             fun head_object_error_/1,
             fun head_object_notfound_/1,
             fun head_object_normal1_/1,
             fun get_object_error_/1,
             fun get_object_notfound_/1,
             fun get_object_normal1_/1,
             fun delete_object_error_/1,
             fun delete_object_notfound_/1,
             fun delete_object_normal1_/1,
             fun put_object_error_/1,
             fun put_object_normal1_/1
            ]}}.

%% TODO
%% api_cowboy_test_() ->
%%     {setup, fun setup_cowboy/0, fun teardown/1,
%%      {with, [
%%              fun get_bucket_list_error_/1,
%%              fun get_bucket_list_empty_/1,
%%              fun get_bucket_list_normal1_/1,
%%              fun head_object_error_/1,
%%              fun head_object_notfound_/1,
%%              fun head_object_normal1_/1,
%%              fun get_object_error_/1,
%%              fun get_object_notfound_/1,
%%              fun get_object_normal1_/1,
%%              fun delete_object_error_/1,
%%              fun delete_object_notfound_/1,
%%              fun delete_object_normal1_/1,
%%              fun put_object_error_/1,
%%              fun put_object_normal1_/1
%%             ]}}.

setup(InitFun, TermFun) ->
    io:format(user, "cwd:~p~n",[os:cmd("pwd")]),
    [] = os:cmd("epmd -daemon"),
    {ok, Hostname} = inet:gethostname(),
    NetKernelNode = list_to_atom("netkernel_0@" ++ Hostname),
    net_kernel:start([NetKernelNode, shortnames]),
    Args = " -pa ../deps/*/ebin "
        ++ " -kernel error_logger    '{file, \"../kernel.log\"}' ",
    {ok, Node0} = slave:start_link(list_to_atom(Hostname), 'storage_0', Args),
    {ok, Node1} = slave:start_link(list_to_atom(Hostname), 'storage_1', Args),
    meck:new(leo_redundant_manager_api),
    meck:expect(leo_redundant_manager_api, get_redundancies_by_key,
                fun(_Method, _Key) ->
                        {ok, #redundancies{id = 0,
                                           nodes = [{Node0,true}, {Node1,true}],
                                           n = 2, r = 1, w = 1, d = 1}}
                end),
    code:add_path("../cherly/ebin"),
    InitFun(),

    inets:start(),
    [TermFun, Node0, Node1].

setup_mochiweb() ->
    InitFun = fun() -> leo_s3_http_mochi:start([{port,8080}]) end,
    TermFun = fun() -> leo_s3_http_mochi:stop() end,
    setup(InitFun, TermFun).

%% TODO
%% setup_cowboy() ->
%%     InitFun = fun() -> leo_s3_http_cowboy:start([{port,8080},{acceptor_pool_size,32}]) end,
%%     TermFun = fun() -> leo_s3_http_cowboy:stop() end,
%%     setup(InitFun, TermFun).

teardown([TermFun, Node0, Node1]) ->
    inets:stop(),
    TermFun(),
    meck:unload(),
    slave:stop(Node0),
    slave:stop(Node1),
    net_kernel:stop(),
    timer:sleep(250),
    ok.

get_bucket_list_error_([_TermFun, _Node0, Node1]) ->
    timer:sleep(150),

    ok = rpc:call(Node1, meck, new,    [leo_storage_handler_directory, [no_link]]),
    ok = rpc:call(Node1, meck, expect, [leo_storage_handler_directory, find_by_parent_dir, 1, {error, some_error}]),
    try
        {ok, {SC, _Body}} =
            httpc:request(get, {"http://" ++ ?TARGET_HOST ++ ":8080/a/b?prefix=pre", []}, [], [{full_result, false}]),
        ?assertEqual(500, SC)
    catch
        throw:Reason ->
            throw(Reason)
    after
        ok = rpc:call(Node1, meck, unload, [leo_storage_handler_directory])
    end,
    ok.

get_bucket_list_empty_([_TermFun, _Node0, Node1]) ->
    timer:sleep(150),

    ok = rpc:call(Node1, meck, new,    [leo_storage_handler_directory, [no_link]]),
    ok = rpc:call(Node1, meck, expect, [leo_storage_handler_directory, find_by_parent_dir, 1, {ok, []}]),
    try
        {ok, {SC,_Body}} = httpc:request(get, {"http://" ++ ?TARGET_HOST ++ ":8080/a/b?prefix=pre",[]}, [], [{full_result, false}]),
        ?assertEqual(200, SC)

        %% TODO
        %% Xml = io_lib:format(?XML_OBJ_LIST, [""]),
        %% ?assertEqual(erlang:list_to_binary(Xml), erlang:list_to_binary(Body))
    catch
        throw:Reason ->
            throw(Reason)
    after
        ok = rpc:call(Node1, meck, unload, [leo_storage_handler_directory])
    end,
    ok.

get_bucket_list_normal1_([_TermFun, _Node0, Node1]) ->
    timer:sleep(150),

    ok = rpc:call(Node1, meck, new,    [leo_storage_handler_directory, [no_link]]),
    ok = rpc:call(Node1, meck, expect, [leo_storage_handler_directory, find_by_parent_dir, 1, {ok, [
                                                                                                    {metadata, "leofs/a/b/pre", 0, 4, 0, 0, 0, 1, [], 0, 0, 19740926, 0, 0}]}]),
    try
        {ok, {SC,_Body}} = httpc:request(get, {"http://" ++ ?TARGET_HOST ++ ":8080/a/b?prefix=pre",[]}, [], [{full_result, false}]),
        ?assertEqual(500, SC)

        %% TODO
        %% ?assertEqual(200, SC)
        %% {_XmlDoc, Rest} = xmerl_scan:string(Body),
        %% ?assertEqual([], Rest)
    catch
        throw:Reason ->
            throw(Reason)
    after
        ok = rpc:call(Node1, meck, unload, [leo_storage_handler_directory])
    end,
    ok.

head_object_notfound_([_TermFun, Node0, Node1]) ->
    ok = rpc:call(Node0, meck, new,    [leo_storage_handler_object, [no_link]]),
    ok = rpc:call(Node0, meck, expect, [leo_storage_handler_object, head, 2, {error, not_found}]),
    ok = rpc:call(Node1, meck, new,    [leo_storage_handler_object, [no_link]]),
    ok = rpc:call(Node1, meck, expect, [leo_storage_handler_object, head, 2, {error, not_found}]),
    try
        {ok, {SC, _Body}} = httpc:request(head, {"http://" ++ ?TARGET_HOST ++ ":8080/a/b",[]}, [], [{full_result, false}]),
        ?assertEqual(404, SC)
    catch
        throw:Reason ->
            throw(Reason)
    after
        ok = rpc:call(Node0, meck, unload, [leo_storage_handler_object]),
        ok = rpc:call(Node1, meck, unload, [leo_storage_handler_object])
    end,
    ok.

head_object_error_([_TermFun, _Node0, Node1]) ->
    ok = rpc:call(Node1, meck, new,    [leo_storage_handler_object, [no_link]]),
    ok = rpc:call(Node1, meck, expect, [leo_storage_handler_object, head, 2, {error, foobar}]),
    try
        {ok, {SC, _Body}} = httpc:request(head, {"http://" ++ ?TARGET_HOST ++ ":8080/a/b",[]}, [], [{full_result, false}]),
        ?assertEqual(500, SC)
    catch
        throw:Reason ->
            throw(Reason)
    after
        ok = rpc:call(Node1, meck, unload, [leo_storage_handler_object])
    end,
    ok.

head_object_normal1_([_TermFun, _Node0, Node1]) ->
    ok = rpc:call(Node1, meck, new,    [leo_storage_handler_object, [no_link]]),
    ok = rpc:call(Node1, meck, expect, [leo_storage_handler_object, head, 2, {ok,
                                                                              {metadata, "a/b.png", 0, 4, 0, 0, 0, 1, [], 0, 63505750315, 19740926, 0, 0}}]),
    try
        {ok, {SC, _Body}} = httpc:request(head, {"http://" ++ ?TARGET_HOST ++ ":8080/a/b.png",[]}, [], [{full_result, false}]),
        ?assertEqual(200, SC)
    catch
        throw:Reason ->
            throw(Reason)
    after
        ok = rpc:call(Node1, meck, unload, [leo_storage_handler_object])
    end,
    ok.

get_object_notfound_([_TermFun, Node0, Node1]) ->
    ok = rpc:call(Node0, meck, new,    [leo_storage_handler_object, [no_link]]),
    ok = rpc:call(Node0, meck, expect, [leo_storage_handler_object, get, 3, {error, not_found}]),
    ok = rpc:call(Node1, meck, new,    [leo_storage_handler_object, [no_link]]),
    ok = rpc:call(Node1, meck, expect, [leo_storage_handler_object, get, 3, {error, not_found}]),
    try
        {ok, {SC, _Body}} = httpc:request(get, {"http://" ++ ?TARGET_HOST ++ ":8080/a/b.png",[]}, [], [{full_result, false}]),
        ?assertEqual(404, SC)
    catch
        throw:Reason ->
            throw(Reason)
    after
        ok = rpc:call(Node0, meck, unload, [leo_storage_handler_object]),
        ok = rpc:call(Node1, meck, unload, [leo_storage_handler_object])
    end,
    ok.

get_object_error_([_TermFun, _Node0, Node1]) ->
    ok = rpc:call(Node1, meck, new,    [leo_storage_handler_object, [no_link]]),
    ok = rpc:call(Node1, meck, expect, [leo_storage_handler_object, get, 3, {error, foobar}]),
    try
        {ok, {SC, _Body}} = httpc:request(get, {"http://" ++ ?TARGET_HOST ++ ":8080/a/b.png",[]}, [], [{full_result, false}]),
        ?assertEqual(500, SC)
    catch
        throw:Reason ->
            throw(Reason)
    after
        ok = rpc:call(Node1, meck, unload, [leo_storage_handler_object])
    end,
    ok.

get_object_normal1_([_TermFun, _Node0, Node1]) ->
    ok = rpc:call(Node1, meck, new,    [leo_storage_handler_object, [no_link]]),
    TS = calendar:datetime_to_gregorian_seconds(erlang:universaltime()),
    ok = rpc:call(Node1, meck, expect,
                  [leo_storage_handler_object, get, 3,
                   {ok, {metadata, "", 0, 4, 0, 0, 0, 1, [], 0, TS, 19740926, 0, 0}, <<"body">>}]),
    try
        {ok, {{_, SC, _}, Headers, Body}} =
            httpc:request(get, {"http://" ++ ?TARGET_HOST ++ ":8080/a/b.png",[{"connection", "close"}]}, [], []),
        ?assertEqual(200, SC),
        ?assertEqual("body", Body),
        ?assertEqual(undefined, proplists:get_value("X-From-Cache", Headers))
        %% confirm cache
        %% {ok, {{_, SC2, _}, Headers2, Body2}} =
        %%     httpc:request(get, {"http://" ++ ?TARGET_HOST ++ ":8080/a/b.png",[]}, [], []),
        %% ?assertEqual(200, SC2),
        %% ?assertEqual("body", Body2),
        %% ?assertEqual("rue", proplists:get_value("X-From-Cache", Headers2))
    catch
        throw:Reason ->
            throw(Reason)
    after
        ok = rpc:call(Node1, meck, unload, [leo_storage_handler_object])
    end,
    ok.

delete_object_notfound_([_TermFun, Node0, Node1]) ->
    ok = rpc:call(Node0, meck, new,    [leo_storage_handler_object, [no_link]]),
    ok = rpc:call(Node0, meck, expect, [leo_storage_handler_object, delete, 4, {error, not_found}]),
    ok = rpc:call(Node1, meck, new,    [leo_storage_handler_object, [no_link]]),
    ok = rpc:call(Node1, meck, expect, [leo_storage_handler_object, delete, 4, {error, not_found}]),
    try
        {ok, {SC, _Body}} = httpc:request(delete, {"http://" ++ ?TARGET_HOST ++ ":8080/a/b.png",[]}, [], [{full_result, false}]),
        ?assertEqual(404, SC)
    catch
        throw:Reason ->
            throw(Reason)
    after
        ok = rpc:call(Node0, meck, unload, [leo_storage_handler_object]),
        ok = rpc:call(Node1, meck, unload, [leo_storage_handler_object])
    end,
    ok.

delete_object_error_([_TermFun, _Node0, Node1]) ->
    ok = rpc:call(Node1, meck, new,    [leo_storage_handler_object, [no_link]]),
    ok = rpc:call(Node1, meck, expect, [leo_storage_handler_object, delete, 4, {error, foobar}]),
    try
        {ok, {SC, _Body}} = httpc:request(delete, {"http://" ++ ?TARGET_HOST ++ ":8080/a/b.png",[]}, [], [{full_result, false}]),
        ?assertEqual(500, SC)
    catch
        throw:Reason ->
            throw(Reason)
    after
        ok = rpc:call(Node1, meck, unload, [leo_storage_handler_object])
    end,
    ok.

delete_object_normal1_([_TermFun, _Node0, Node1]) ->
    ok = rpc:call(Node1, meck, new,    [leo_storage_handler_object, [no_link]]),
    ok = rpc:call(Node1, meck, expect, [leo_storage_handler_object, delete, 4, ok]),
    try
        {ok, {SC, _Body}} = httpc:request(delete, {"http://" ++ ?TARGET_HOST ++ ":8080/a/b.png",[]}, [], [{full_result, false}]),
        ?assertEqual(204, SC)
    catch
        throw:Reason ->
            throw(Reason)
    after
        ok = rpc:call(Node1, meck, unload, [leo_storage_handler_object])
    end,
    ok.

put_object_error_([_TermFun, _Node0, Node1]) ->
    ok = rpc:call(Node1, meck, new,    [leo_storage_handler_object, [no_link]]),
    ok = rpc:call(Node1, meck, expect, [leo_storage_handler_object, put, 6, {error, foobar}]),
    try
        {ok, {SC, _Body}} = httpc:request(put, {"http://" ++ ?TARGET_HOST ++ ":8080/a/b.png",[], "image/png", "body"}, [], [{full_result, false}]),
        ?assertEqual(500, SC)
    catch
        throw:Reason ->
            throw(Reason)
    after
        ok = rpc:call(Node1, meck, unload, [leo_storage_handler_object])
    end,
    ok.

put_object_normal1_([_TermFun, _Node0, Node1]) ->
    ok = rpc:call(Node1, meck, new,    [leo_storage_handler_object, [no_link]]),
    ok = rpc:call(Node1, meck, expect, [leo_storage_handler_object, put, 6, ok]),
    try
        {ok, {SC, _Body}} = httpc:request(put, {"http://" ++ ?TARGET_HOST ++ ":8080/a/b.png",[], "image/png", "body"}, [], [{full_result, false}]),
        ?assertEqual(200, SC)
    catch
        throw:Reason ->
            throw(Reason)
    after
        ok = rpc:call(Node1, meck, unload, [leo_storage_handler_object])
    end,
    ok.
-endif.
