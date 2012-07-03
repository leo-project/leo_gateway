%%======================================================================
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
%% ---------------------------------------------------------------------
%% LeoFS Gateway -  Mock Server.
%% @doc
%% @end
%%======================================================================
-module(leo_mock_server).

-author('Yosuke Hara').
-vsn('0.9.0').

-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").

-export([start/1, stop/0]).

-define(TEST_KEY_1, "air/on/g/string/music.png").
-define(TEST_META_1, #metadata{key        = ?TEST_KEY_1,
                               addr_id    = 1,
                               clock      = 9,
                               timestamp  = 8,
                               checksum   = 7}).


start({fixed_bin, Size}) ->
    meck:new(leo_manager_api),
    meck:expect(leo_manager_api, notify,
                fun(launched, gateway, _Node, _Checksum) ->
                        ok
                end),
    meck:expect(leo_manager_api,  get_system_config,
                fun() ->
                        {ok, #system_conf{}}
                end),
    meck:expect(leo_manager_api, get_members,
                fun() ->
                        {ok, [#member{node = erlang:node()}]}
                end),
    meck:expect(leo_manager_api, synchronize,
                fun(_Arg0, _Arg1) ->
                        ok
                end),
    meck:expect(leo_manager_api, register,
                fun(_Arg0, _Arg1, _Node, gateway) ->
                        ok
                end),

    %%
    %%
    meck:new(leo_redundant_manager_api),
    meck:expect(leo_redundant_manager_api, checksum,
                fun(_Arg) ->
                        {ok, 0}
                end),

    ok = meck:new(leo_storage_handler_object),
    ok = meck:expect(leo_storage_handler_object, head,
                     fun(_Key, _AddrId) ->
                             {ok, ?TEST_META_1}
                     end),
    ok = meck:expect(leo_storage_handler_object, put,
                     fun(_AddrId, _Key, _Bin, _Size, _ReqId, _Timestamp) ->
                             ok
                     end),
    ok = meck:expect(leo_storage_handler_object, get,
                     fun(AddrId, Key, _ReqId, _Timestamp) ->
                             Bin = crypto:rand_bytes(Size),
                             {ok, #metadata{addr_id  = AddrId,
                                            key      = Key,
                                            dsize    = Size,
                                            checksum = erlang:crc32(Bin),
                                            timestamp = calendar:datetime_to_gregorian_seconds(calendar:universal_time())
                                           }, Bin}
                     end),
    ok = meck:expect(leo_storage_handler_object, delete,
                     fun(_AddrId,_Key,_ReqId,_Timestamp) ->
                             ok
                     end),
    ok;

start(redundant_manager) ->
    meck:new(leo_redundant_manager_api),
    meck:expect(leo_redundant_manager_api, get_redundancies_by_key,
                fun(_Arg) ->
                        {ok, #redundancies{n=1, r=1, w=1, d=1 ,
                                           nodes = [{'mock@127.0.0.1', true}]}}
                end),
    meck:expect(leo_redundant_manager_api, get_redundancies_by_addr_id,
                fun(_Arg) ->
                        {ok, #redundancies{n=1, r=1, w=1, d=1 ,
                                           nodes = [{'mock@127.0.0.1', true}]}}
                end),
    ok.

stop() ->
    meck:unload(),
    halt().

