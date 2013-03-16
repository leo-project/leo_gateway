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

-include("leo_gateway.hrl").
-include("leo_s3_http.hrl").
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
api_cowboy_test_() ->
    {setup, fun setup_cowboy/0, fun teardown/1,
     fun gen_tests/1}.

gen_tests(Arg) ->
    lists:map(fun(Test) -> Test(Arg) end,
              [fun get_bucket_list_error_/1,
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
               %% fun proper_/1
              ]
             ).

-define(SSL_CERT_DATA,
        "-----BEGIN CERTIFICATE-----\n" ++
            "MIIDIDCCAgigAwIBAgIJAJLkNZzERPIUMA0GCSqGSIb3DQEBBQUAMBQxEjAQBgNV\n" ++
            "BAMTCWxvY2FsaG9zdDAeFw0xMDAzMTgxOTM5MThaFw0yMDAzMTUxOTM5MThaMBQx\n" ++
            "EjAQBgNVBAMTCWxvY2FsaG9zdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC\n" ++
            "ggEBAJeUCOZxbmtngF4S5lXckjSDLc+8C+XjMBYBPyy5eKdJY20AQ1s9/hhp3ulI\n" ++
            "8pAvl+xVo4wQ+iBSvOzcy248Q+Xi6+zjceF7UNRgoYPgtJjKhdwcHV3mvFFrS/fp\n" ++
            "9ggoAChaJQWDO1OCfUgTWXImhkw+vcDR11OVMAJ/h73dqzJPI9mfq44PTTHfYtgr\n" ++
            "v4LAQAOlhXIAa2B+a6PlF6sqDqJaW5jLTcERjsBwnRhUGi7JevQzkejujX/vdA+N\n" ++
            "jRBjKH/KLU5h3Q7wUchvIez0PXWVTCnZjpA9aR4m7YV05nKQfxtGd71czYDYk+j8\n" ++
            "hd005jetT4ir7JkAWValBybJVksCAwEAAaN1MHMwHQYDVR0OBBYEFJl9s51SnjJt\n" ++
            "V/wgKWqV5Q6jnv1ZMEQGA1UdIwQ9MDuAFJl9s51SnjJtV/wgKWqV5Q6jnv1ZoRik\n" ++
            "FjAUMRIwEAYDVQQDEwlsb2NhbGhvc3SCCQCS5DWcxETyFDAMBgNVHRMEBTADAQH/\n" ++
            "MA0GCSqGSIb3DQEBBQUAA4IBAQB2ldLeLCc+lxK5i0EZquLamMBJwDIjGpT0JMP9\n" ++
            "b4XQOK2JABIu54BQIZhwcjk3FDJz/uOW5vm8k1kYni8FCjNZAaRZzCUfiUYTbTKL\n" ++
            "Rq9LuIAODyP2dnTqyKaQOOJHvrx9MRZ3XVecXPS0Tib4aO57vCaAbIkmhtYpTWmw\n" ++
            "e3t8CAIDVtgvjR6Se0a1JA4LktR7hBu22tDImvCSJn1nVAaHpani6iPBPPdMuMsP\n" ++
            "TBoeQfj8VpqBUjCStqJGa8ytjDFX73YaxV2mgrtGwPNme1x3YNRR11yTu7tksyMO\n" ++
            "GrmgxNriqYRchBhNEf72AKF0LR1ByKwfbDB9rIsV00HtCgOp\n" ++
            "-----END CERTIFICATE-----\n").
-define(SSL_KEY_DATA,
        "-----BEGIN RSA PRIVATE KEY-----\n" ++
            "MIIEpAIBAAKCAQEAl5QI5nFua2eAXhLmVdySNIMtz7wL5eMwFgE/LLl4p0ljbQBD\n" ++
            "Wz3+GGne6UjykC+X7FWjjBD6IFK87NzLbjxD5eLr7ONx4XtQ1GChg+C0mMqF3Bwd\n" ++
            "Xea8UWtL9+n2CCgAKFolBYM7U4J9SBNZciaGTD69wNHXU5UwAn+Hvd2rMk8j2Z+r\n" ++
            "jg9NMd9i2Cu/gsBAA6WFcgBrYH5ro+UXqyoOolpbmMtNwRGOwHCdGFQaLsl69DOR\n" ++
            "6O6Nf+90D42NEGMof8otTmHdDvBRyG8h7PQ9dZVMKdmOkD1pHibthXTmcpB/G0Z3\n" ++
            "vVzNgNiT6PyF3TTmN61PiKvsmQBZVqUHJslWSwIDAQABAoIBACI8Ky5xHDFh9RpK\n" ++
            "Rn/KC7OUlTpADKflgizWJ0Cgu2F9L9mkn5HyFHvLHa+u7CootbWJOiEejH/UcBtH\n" ++
            "WyMQtX0snYCpdkUpJv5wvMoebGu+AjHOn8tfm9T/2O6rhwgckLyMb6QpGbMo28b1\n" ++
            "p9QiY17BJPZx7qJQJcHKsAvwDwSThlb7MFmWf42LYWlzybpeYQvwpd+UY4I0WXLu\n" ++
            "/dqJIS9Npq+5Y5vbo2kAEAssb2hSCvhCfHmwFdKmBzlvgOn4qxgZ1iHQgfKI6Z3Y\n" ++
            "J0573ZgOVTuacn+lewtdg5AaHFcl/zIYEr9SNqRoPNGbPliuv6k6N2EYcufWL5lR\n" ++
            "sCmmmHECgYEAxm+7OpepGr++K3+O1e1MUhD7vSPkKJrCzNtUxbOi2NWj3FFUSPRU\n" ++
            "adWhuxvUnZgTcgM1+KuQ0fB2VmxXe9IDcrSFS7PKFGtd2kMs/5mBw4UgDZkOQh+q\n" ++
            "kDiBEV3HYYJWRq0w3NQ/9Iy1jxxdENHtGmG9aqamHxNtuO608wGW2S8CgYEAw4yG\n" ++
            "ZyAic0Q/U9V2OHI0MLxLCzuQz17C2wRT1+hBywNZuil5YeTuIt2I46jro6mJmWI2\n" ++
            "fH4S/geSZzg2RNOIZ28+aK79ab2jWBmMnvFCvaru+odAuser4N9pfAlHZvY0pT+S\n" ++
            "1zYX3f44ygiio+oosabLC5nWI0zB2gG8pwaJlaUCgYEAgr7poRB+ZlaCCY0RYtjo\n" ++
            "mYYBKD02vp5BzdKSB3V1zeLuBWM84pjB6b3Nw0fyDig+X7fH3uHEGN+USRs3hSj6\n" ++
            "BqD01s1OT6fyfbYXNw5A1r+nP+5h26Wbr0zblcKxdQj4qbbBZC8hOJNhqTqqA0Qe\n" ++
            "MmzF7jiBaiZV/Cyj4x1f9BcCgYEAhjL6SeuTuOctTqs/5pz5lDikh6DpUGcH8qaV\n" ++
            "o6aRAHHcMhYkZzpk8yh1uUdD7516APmVyvn6rrsjjhLVq4ZAJjwB6HWvE9JBN0TR\n" ++
            "bILF+sREHUqU8Zn2Ku0nxyfXCKIOnxlx/J/y4TaGYqBqfXNFWiXNUrjQbIlQv/xR\n" ++
            "K48g/MECgYBZdQlYbMSDmfPCC5cxkdjrkmAl0EgV051PWAi4wR+hLxIMRjHBvAk7\n" ++
            "IweobkFvT4TICulgroLkYcSa5eOZGxB/DHqcQCbWj3reFV0VpzmTDoFKG54sqBRl\n" ++
            "vVntGt0pfA40fF17VoS7riAdHF53ippTtsovHEsg5tq5NrBl5uKm2g==\n" ++
            "-----END RSA PRIVATE KEY-----\n").

setup(InitFun, TermFun) ->
    ok = leo_logger_client_message:new("./", ?LOG_LEVEL_WARN),
    io:format(user, "cwd:~p~n",[os:cmd("pwd")]),
    [] = os:cmd("epmd -daemon"),
    {ok, Hostname} = inet:gethostname(),
    NetKernelNode = list_to_atom("netkernel_0@" ++ Hostname),
    net_kernel:start([NetKernelNode, shortnames]),
    inets:start(),

    Args = " -pa ../deps/*/ebin "
        ++ " -kernel error_logger    '{file, \"../kernel.log\"}' ",
    {ok, Node0} = slave:start_link(list_to_atom(Hostname), 'storage_0', Args),
    {ok, Node1} = slave:start_link(list_to_atom(Hostname), 'storage_1', Args),

    ok = leo_misc:init_env(),

    meck:new(leo_redundant_manager_api),
    meck:expect(leo_redundant_manager_api, get_redundancies_by_key,
                fun(_Method, _Key) ->
                        {ok, #redundancies{id = 0,
                                           nodes = [{Node0,true}, {Node1,true}],
                                           n = 2, r = 1, w = 1, d = 1}}
                end),
    meck:new(leo_s3_endpoint),
    meck:expect(leo_s3_endpoint, get_endpoints, 0, {ok, [{endpoint, <<"localhost">>, 0}]}),

    code:add_path("../cherly/ebin"),
    ok = file:write_file("./server_cert.pem", ?SSL_CERT_DATA),
    ok = file:write_file("./server_key.pem",  ?SSL_KEY_DATA),

    InitFun(),
    [TermFun, Node0, Node1].

setup_cowboy() ->
    application:start(ecache),
    application:start(crypto),
    application:start(ranch),
    application:start(cowboy),
    %% {ok, Options} = leo_s3_http_api:get_options(cowboy, [{port,8080},{num_of_acceptors,32},
    %%                                                      {ssl_port,8443},
    %%                                                      {ssl_certfile,"./server_cert.pem"},
    %%                                                      {ssl_keyfile, "./server_key.pem"}]),
    {ok, Options} = leo_s3_http_api:get_options(),
    InitFun = fun() -> leo_s3_http_cowboy:start(Options) end,
    TermFun = fun() -> leo_s3_http_cowboy:stop() end,
    setup(InitFun, TermFun).

teardown([TermFun, Node0, Node1]) ->
    inets:stop(),
    net_kernel:stop(),
    slave:stop(Node0),
    slave:stop(Node1),

    meck:unload(),
    TermFun(),
    timer:sleep(250),
    ok.

get_bucket_list_error_([_TermFun, _Node0, Node1]) ->
    fun() ->
            timer:sleep(150),

            ok = rpc:call(Node1, meck, new,    [leo_storage_handler_directory, [no_link]]),
            ok = rpc:call(Node1, meck, expect, [leo_storage_handler_directory, find_by_parent_dir, 1, {error, some_error}]),

            try
                {ok, {SC, _Body}} = httpc:request(get, {lists:append(["http://",
                                                                      ?TARGET_HOST,
                                                                      ":8080/a/b?prefix=pre"]), []}, [], [{full_result, false}]),
                ?assertEqual(500, SC)
            catch
                throw:Reason ->
                    throw(Reason)
            after
                ok = rpc:call(Node1, meck, unload, [leo_storage_handler_directory])
            end,
            ok
    end.

get_bucket_list_empty_([_TermFun, _Node0, Node1]) ->
    fun() ->
            timer:sleep(150),

            ok = rpc:call(Node1, meck, new,    [leo_storage_handler_directory, [no_link]]),
            ok = rpc:call(Node1, meck, expect, [leo_storage_handler_directory, find_by_parent_dir, 4, {ok, []}]),

            try
                {ok, {SC, Body}} = httpc:request(get, {lists:append(["http://",
                                                                     ?TARGET_HOST,
                                                                     ":8080/a/b?prefix=pre"]), []},
                                                 [], [{full_result, false}]),
                ?assertEqual(200, SC),
                Xml = io_lib:format(?XML_OBJ_LIST, ["pre", ""]),
                ?assertEqual(erlang:list_to_binary(Xml), erlang:list_to_binary(Body))
            catch
                throw:Reason ->
                    throw(Reason)
            after
                ok = rpc:call(Node1, meck, unload, [leo_storage_handler_directory])
            end,
            ok
    end.

get_bucket_list_normal1_([_TermFun, _Node0, Node1]) ->
    fun() ->
            timer:sleep(150),

            ok = rpc:call(Node1, meck, new,    [leo_storage_handler_directory, [no_link]]),
            ok = rpc:call(Node1, meck, expect, [leo_storage_handler_directory, find_by_parent_dir,
                                                4, {ok,
                                                    [{metadata, <<"localhost/a/b/pre/test.png">>,
                                                      0, 8, 0, 0,
                                                      0, 0, 0,
                                                      0, 0, 63511805822, 19740926, 0, 0}]}]),
            try
                %% TODO
                {ok, {SC,Body}} = httpc:request(get, {lists:append(["http://",
                                                                    ?TARGET_HOST, ":8080/a/b?prefix=pre"]), []},
                                                [], [{full_result, false}]),
                ?assertEqual(200, SC),

                {_XmlDoc, Rest} = xmerl_scan:string(Body),
                ?assertEqual([], Rest)
            catch
                throw:Reason ->
                    throw(Reason)
            after
                ok = rpc:call(Node1, meck, unload, [leo_storage_handler_directory])
            end,
            ok
    end.

head_object_notfound_([_TermFun, Node0, Node1]) ->
    fun() ->
            ok = rpc:call(Node0, meck, new,    [leo_storage_handler_object, [no_link]]),
            ok = rpc:call(Node0, meck, expect, [leo_storage_handler_object, head, 2, {error, not_found}]),
            ok = rpc:call(Node1, meck, new,    [leo_storage_handler_object, [no_link]]),
            ok = rpc:call(Node1, meck, expect, [leo_storage_handler_object, head, 2, {error, not_found}]),

            try
                {ok, {SC, _Body}} = httpc:request(head, {lists:append(["http://",
                                                                       ?TARGET_HOST,
                                                                       ":8080/a/b"]), []}, [], [{full_result, false}]),
                ?assertEqual(404, SC)
            catch
                throw:Reason ->
                    throw(Reason)
            after
                ok = rpc:call(Node0, meck, unload, [leo_storage_handler_object]),
                ok = rpc:call(Node1, meck, unload, [leo_storage_handler_object])
            end,
            ok
    end.

head_object_error_([_TermFun, _Node0, Node1]) ->
    fun() ->
            ok = rpc:call(Node1, meck, new,    [leo_storage_handler_object, [no_link]]),
            ok = rpc:call(Node1, meck, expect, [leo_storage_handler_object, head, 2, {error, foobar}]),

            try
                {ok, {SC, _Body}} = httpc:request(head, {lists:append(["http://",
                                                                       ?TARGET_HOST,
                                                                       ":8080/a/b"]), []}, [], [{full_result, false}]),
                ?assertEqual(500, SC)
            catch
                throw:Reason ->
                    throw(Reason)
            after
                ok = rpc:call(Node1, meck, unload, [leo_storage_handler_object])
            end,
            ok
    end.

head_object_normal1_([_TermFun, _Node0, Node1]) ->
    fun() ->
            ok = rpc:call(Node1, meck, new,    [leo_storage_handler_object, [no_link]]),
            ok = rpc:call(Node1, meck, expect, [leo_storage_handler_object, head, 2,
                                                {ok, {metadata, <<"a/b.png">>,
                                                      0, 4, 16384, 0,
                                                      0, 0, 0,
                                                      0, 1, 63505750315, 19740926, 0, 0}}]),

            try
                %% TODO
                {ok, {{_, SC, _}, Headers, _Body}} =
                    httpc:request(head, {lists:append(["http://",
                                                       ?TARGET_HOST,
                                                       ":8080/a/b.png"]), [{"connection", "close"}]}, [], []),
                ?assertEqual(200, SC),
                ?assertEqual("16384", proplists:get_value("content-length", Headers))
            catch
                throw:Reason ->
                    throw(Reason)
            after
                ok = rpc:call(Node1, meck, unload, [leo_storage_handler_object])
            end,
            ok
    end.

get_object_error_([_TermFun, _Node0, Node1]) ->
    fun() ->
            ok = rpc:call(Node1, meck, new,    [leo_storage_handler_object, [no_link]]),
            ok = rpc:call(Node1, meck, expect, [leo_storage_handler_object, get, 3, {error, foobar}]),

            try
                {ok, {SC, _Body}} = httpc:request(get, {lists:append(["http://",
                                                                      ?TARGET_HOST,
                                                                      ":8080/a/b.png"]), []}, [], [{full_result, false}]),
                ?assertEqual(500, SC)
            catch
                throw:Reason ->
                    throw(Reason)
            after
                ok = rpc:call(Node1, meck, unload, [leo_storage_handler_object])
            end,
            ok
    end.

get_object_notfound_([_TermFun, Node0, Node1]) ->
    fun() ->
            ok = rpc:call(Node0, meck, new,    [leo_storage_handler_object, [no_link]]),
            ok = rpc:call(Node0, meck, expect, [leo_storage_handler_object, get, 3, {error, not_found}]),
            ok = rpc:call(Node1, meck, new,    [leo_storage_handler_object, [no_link]]),
            ok = rpc:call(Node1, meck, expect, [leo_storage_handler_object, get, 3, {error, not_found}]),

            try
                {ok, {SC, _Body}} = httpc:request(get, {lists:append(["http://",
                                                                      ?TARGET_HOST,
                                                                      ":8080/a/b/c.png"]), []}, [], [{full_result, false}]),
                ?assertEqual(404, SC)
            catch
                throw:Reason ->
                    throw(Reason)
            after
                ok = rpc:call(Node0, meck, unload, [leo_storage_handler_object]),
                ok = rpc:call(Node1, meck, unload, [leo_storage_handler_object])
            end,
            ok
    end.

get_object_normal1_([_TermFun, _Node0, Node1]) ->
    fun() ->
            ok = rpc:call(Node1, meck, new,    [leo_storage_handler_object, [no_link]]),
            ok = rpc:call(Node1, meck, expect,
                          [leo_storage_handler_object, get, 3,
                           {ok, {metadata, "", 
                                 0, 4, 4, 0, 
                                 0, 0, 0, 0, 1,
                                 calendar:datetime_to_gregorian_seconds(erlang:universaltime()),
                                 19740926, 0, 0}, <<"body">>}]),

            try
                %% TODO
                {ok, {{_, SC, _}, Headers, Body}} =
                    httpc:request(get, {lists:append(["http://",
                                                      ?TARGET_HOST,
                                                      ":8080/a/b.png"]), [{"connection", "close"}]}, [], []),
                ?assertEqual(200, SC),
                ?assertEqual("body", Body),
                ?assertEqual(undefined, proplists:get_value("X-From-Cache", Headers))
            catch
                throw:Reason ->
                    throw(Reason)
            after
                ok = rpc:call(Node1, meck, unload, [leo_storage_handler_object])
            end,
            ok
    end.

delete_object_notfound_([_TermFun, Node0, Node1]) ->
    fun() ->
            ok = rpc:call(Node0, meck, new,    [leo_storage_handler_object, [no_link]]),
            ok = rpc:call(Node0, meck, expect, [leo_storage_handler_object, delete, 2, {error, not_found}]),
            ok = rpc:call(Node1, meck, new,    [leo_storage_handler_object, [no_link]]),
            ok = rpc:call(Node1, meck, expect, [leo_storage_handler_object, delete, 2, {error, not_found}]),

            meck:new(leo_s3_auth),
            meck:expect(leo_s3_auth, authenticate, 3, {ok, <<"AccessKey">>}),

            try
                {ok, {SC, _Body}} = httpc:request(delete, {lists:append(["http://",
                                                                         ?TARGET_HOST,
                                                                         ":8080/a/b.png"]),
                                                           [{"Authorization","auth"}]}, [], [{full_result, false}]),
                ?assertEqual(404, SC)
            catch
                throw:Reason ->
                    throw(Reason)
            after
                ok = rpc:call(Node0, meck, unload, [leo_storage_handler_object]),
                ok = rpc:call(Node1, meck, unload, [leo_storage_handler_object]),
                ok = meck:unload(leo_s3_auth)
            end,
            ok
    end.

delete_object_error_([_TermFun, _Node0, Node1]) ->
    fun() ->
            ok = rpc:call(Node1, meck, new,    [leo_storage_handler_object, [no_link]]),
            ok = rpc:call(Node1, meck, expect, [leo_storage_handler_object, delete, 2, {error, foobar}]),

            meck:new(leo_s3_auth),
            meck:expect(leo_s3_auth, authenticate, 3, {ok, <<"AccessKey">>}),

            try
                {ok, {SC, _Body}} = httpc:request(delete, {lists:append(["http://",
                                                                         ?TARGET_HOST,
                                                                         ":8080/a/b.png"]),
                                                           [{"Authorization","auth"}]}, [], [{full_result, false}]),
                ?assertEqual(500, SC)
            catch
                throw:Reason ->
                    throw(Reason)
            after
                ok = rpc:call(Node1, meck, unload, [leo_storage_handler_object]),
                ok = meck:unload(leo_s3_auth)
            end,
            ok
    end.

delete_object_normal1_([_TermFun, _Node0, Node1]) ->
    fun() ->
            ok = rpc:call(Node1, meck, new,    [leo_storage_handler_object, [no_link]]),
            ok = rpc:call(Node1, meck, expect, [leo_storage_handler_object, delete, 2, ok]),

            meck:new(leo_s3_auth),
            meck:expect(leo_s3_auth, authenticate, 3, {ok, <<"AccessKey">>}),

            try
                {ok, {SC, _Body}} = httpc:request(delete, {lists:append(["http://",
                                                                         ?TARGET_HOST,
                                                                         ":8080/a/b.png"]),
                                                           [{"Authorization","auth"}]}, [], [{full_result, false}]),
                ?assertEqual(204, SC)
            catch
                throw:Reason ->
                    throw(Reason)
            after
                ok = rpc:call(Node1, meck, unload, [leo_storage_handler_object]),
                ok = meck:unload(leo_s3_auth)
            end,
            ok
    end.

put_object_error_([_TermFun, _Node0, Node1]) ->
    fun() ->
            ok = rpc:call(Node1, meck, new,    [leo_storage_handler_object, [no_link]]),
            ok = rpc:call(Node1, meck, expect, [leo_storage_handler_object, put, 2, {error, foobar}]),

            meck:new(leo_s3_auth),
            meck:expect(leo_s3_auth, authenticate, 3, {ok, <<"AccessKey">>}),

            try
                {ok, {SC, _Body}} = httpc:request(put, {lists:append(["http://",
                                                                      ?TARGET_HOST,
                                                                      ":8080/a/b.png"]),
                                                        [{"Authorization","auth"}], "image/png", "body"},
                                                  [], [{full_result, false}]),
                ?assertEqual(500, SC)
            catch
                throw:Reason ->
                    throw(Reason)
            after
                ok = rpc:call(Node1, meck, unload, [leo_storage_handler_object]),
                ok = meck:unload(leo_s3_auth)
            end,
            ok
    end.

put_object_normal1_([_TermFun, _Node0, Node1]) ->
    fun() ->
            ok = rpc:call(Node1, meck, new,    [leo_storage_handler_object, [no_link]]),
            ok = rpc:call(Node1, meck, expect, [leo_storage_handler_object, put, 2, {ok, 1}]),

            meck:new(leo_s3_auth),
            meck:expect(leo_s3_auth, authenticate, 3, {ok, <<"AccessKey">>}),

            try
                {ok, {SC, _Body}} = httpc:request(put, {lists:append(["http://",
                                                                      ?TARGET_HOST,
                                                                      ":8080/a/b.png"]),
                                                        [{"Authorization","auth"}], "image/png", "body"},
                                                  [], [{full_result, false}]),
                ?assertEqual(200, SC)
            catch
                throw:Reason ->
                    throw(Reason)
            after
                ok = rpc:call(Node1, meck, unload, [leo_storage_handler_object]),
                ok = meck:unload(leo_s3_auth)
            end,
            ok
    end.

%% proper_([_TermFun, _Node0, _Node1]) ->
%%     {timeout, 600, ?_assertEqual(true, leo_gateway_web_prop:test())}.

-endif.
