%%======================================================================
%%
%% Leo Gateway
%%
%% Copyright (c) 2012 Rakuten, Inc.
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
%% Leo Gateway - HTTP Commons Handler
%% @doc
%% @end
%%======================================================================
-module(leo_gateway_http_handler).

-author('Yosuke Hara').

-include("leo_gateway.hrl").
-include("leo_http.hrl").

-export([start/1, start/2, stop/0]).
-export([onrequest/2, onresponse/2]).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Launch http handler
%%
-spec(start(atom(), #http_options{}) ->
             ok).
start(Sup, Options) ->
    %% for ECache
    NumOfECacheWorkers    = Options#http_options.cache_workers,
    CacheRAMCapacity      = Options#http_options.cache_ram_capacity,
    CacheDiscCapacity     = Options#http_options.cache_disc_capacity,
    CacheDiscThresholdLen = Options#http_options.cache_disc_threshold_len,
    CacheDiscDirData      = Options#http_options.cache_disc_dir_data,
    CacheDiscDirJournal   = Options#http_options.cache_disc_dir_journal,
    ChildSpec0 = {ecache_sup,
                  {ecache_sup, start_link, [NumOfECacheWorkers, CacheRAMCapacity, CacheDiscCapacity,
                                            CacheDiscThresholdLen, CacheDiscDirData, CacheDiscDirJournal]},
                  permanent, ?SHUTDOWN_WAITING_TIME, supervisor, [ecache_sup]},
    {ok, _} = supervisor:start_child(Sup, ChildSpec0),

    %% for Cowboy
    ChildSpec1 = {cowboy_sup,
                  {cowboy_sup, start_link, []},
                  permanent, ?SHUTDOWN_WAITING_TIME, supervisor, [cowboy_sup]},
    {ok, _} = supervisor:start_child(Sup, ChildSpec1),

    %% launch http-handler(s)
    start(Options).


-spec(start(#http_options{}) ->
             ok).
start(#http_options{handler                = Handler,
                    port                   = Port,
                    ssl_port               = SSLPort,
                    ssl_certfile           = SSLCertFile,
                    ssl_keyfile            = SSLKeyFile,
                    num_of_acceptors       = NumOfAcceptors,
                    cache_method           = CacheMethod,
                    cache_expire           = CacheExpire,
                    cache_max_content_len  = CacheMaxContentLen,
                    cachable_content_type  = CachableContentTypes,
                    cachable_path_pattern  = CachablePathPatterns} = Props) ->
    InternalCache = (CacheMethod == 'inner'),
    Dispatch      = cowboy_router:compile(
                      [{'_', [{'_', Handler,
                               [?env_layer_of_dirs(), InternalCache, Props]}]}]),

    Config = case InternalCache of
                 %% Using inner-cache
                 true ->
                     [{env, [{dispatch, Dispatch}]}];
                 %% Using http-cache
                 false ->
                     CacheCondition = #cache_condition{expire          = CacheExpire,
                                                       max_content_len = CacheMaxContentLen,
                                                       content_types   = CachableContentTypes,
                                                       path_patterns   = CachablePathPatterns},
                     [{env,        [{dispatch, Dispatch}]},
                      {onrequest,  Handler:onrequest(CacheCondition)},
                      {onresponse, Handler:onresponse(CacheCondition)}]
             end,

    {ok, _Pid1}= cowboy:start_http(Handler, NumOfAcceptors,
                                   [{port, Port}], Config),
    {ok, _Pid2}= cowboy:start_https(list_to_atom(lists:append([atom_to_list(Handler), "_ssl"])),
                                    NumOfAcceptors,
                                    [{port,     SSLPort},
                                     {certfile, SSLCertFile},
                                     {keyfile,  SSLKeyFile}],
                                    Config),
    ok.


%% @doc Stop proc(s)
%%
-spec(stop() ->
             ok).
stop() ->
    {ok, HttpOption} = leo_gateway_app:get_options(),
    Handler = HttpOption#http_options.handler,
    cowboy:stop_listener(Handler),
    cowboy:stop_listener(list_to_atom(lists:append([atom_to_list(Handler), "_ssl"]))),
    ok.


%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
%% @doc Handle request
%%
onrequest(#cache_condition{expire = Expire}, FunGenKey) ->
    fun(Req) ->
            Method = cowboy_req:get(method, Req),
            onrequest_1(Method, Req, Expire, FunGenKey)
    end.

onrequest_1(?HTTP_GET, Req, Expire, FunGenKey) ->
    Key = FunGenKey(Req),
    Ret = ecache_api:get(Key),
    onrequest_2(Req, Expire, Key, Ret);
onrequest_1(_, Req,_,_) ->
    Req.

onrequest_2(Req,_Expire,_Key, not_found) ->
    Req;
onrequest_2(Req, Expire, Key, {ok, CachedObj}) ->
    #cache{mtime        = MTime,
           content_type = ContentType,
           etag         = Checksum,
           body         = Body} = binary_to_term(CachedObj),

    Now = leo_date:now(),
    Diff = Now - MTime,

    case (Diff > Expire) of
        true ->
            _ = ecache_api:delete(Key),
            Req;
        false ->
            LastModified = leo_http:rfc1123_date(MTime),
            Header = [?SERVER_HEADER,
                      {?HTTP_HEAD_LAST_MODIFIED, LastModified},
                      {?HTTP_HEAD_CONTENT_TYPE,  ContentType},
                      {?HTTP_HEAD_AGE,           integer_to_list(Diff)},
                      {?HTTP_HEAD_ETAG4AWS,      lists:append(["\"",leo_hex:integer_to_hex(Checksum, 32),"\""])},
                      {?HTTP_HEAD_CACHE_CTRL,    lists:append(["max-age=",integer_to_list(Expire)])}
                     ],
            IMSSec = case cowboy_req:parse_header(?HTTP_HEAD_IF_MODIFIED_SINCE, Req) of
                         {ok, undefined, _} ->
                             0;
                         {ok, IMSDateTime, _} ->
                             calendar:datetime_to_gregorian_seconds(IMSDateTime)
                     end,
            case IMSSec of
                MTime ->
                    {ok, Req2} = ?reply_not_modified(Header, Req),
                    Req2;
                _ ->
                    Req2 = cowboy_req:set_resp_body(Body, Req),
                    {ok, Req3} = ?reply_ok([?SERVER_HEADER], Req2),
                    Req3
            end
    end.


%% @doc
%%
onresponse(#cache_condition{expire = Expire} = Config, FunGenKey) ->
    fun(?HTTP_ST_OK, Headers, Body, Req) ->
            case cowboy_req:get(method, Req) of
                ?HTTP_GET ->
                    Key = FunGenKey(Req),

                    case lists:all(fun(Fun) ->
                                           Fun(Key, Config, Headers, Body)
                                   end, [fun is_cachable_req1/4,
                                         fun is_cachable_req2/4,
                                         fun is_cachable_req3/4]) of
                        true ->
                            Now = leo_date:now(),
                            ContentType = case lists:keyfind(?HTTP_HEAD_CONTENT_TYPE, 1, Headers) of
                                              false ->
                                                  ?HTTP_CTYPE_OCTET_STREAM;
                                              {_, Val} ->
                                                  Val
                                          end,

                            Bin = term_to_binary(
                                    #cache{mtime        = Now,
                                           etag         = leo_hex:raw_binary_to_integer(crypto:md5(Body)),
                                           content_type = ContentType,
                                           body         = Body}),
                            _ = ecache_api:put(Key, Bin),

                            Headers2 = lists:keydelete(?HTTP_HEAD_LAST_MODIFIED, 1, Headers),
                            Headers3 = [{?HTTP_HEAD_CACHE_CTRL, lists:append(["max-age=",integer_to_list(Expire)])},
                                        {?HTTP_HEAD_LAST_MODIFIED, leo_http:rfc1123_date(Now)}
                                        |Headers2],
                            {ok, Req2} = ?reply_ok(Headers3, Req),
                            Req2;
                        false ->
                            cowboy_req:set_resp_body(<<>>, Req)
                    end;
                _ ->
                    cowboy_req:set_resp_body(<<>>, Req)
            end
    end.


%%--------------------------------------------------------------------
%% INNER Functions
%%--------------------------------------------------------------------
%% @doc Judge cachable request
%% @private
is_cachable_req1(_Key, #cache_condition{max_content_len = MaxLen}, Headers, Body) ->
    HasNOTCacheControl = (false == lists:keyfind(?HTTP_HEAD_CACHE_CTRL, 1, Headers)),
    HasNOTCacheControl  andalso
        is_binary(Body) andalso
        size(Body) > 0  andalso
        size(Body) < MaxLen.

is_cachable_req2(_Key, #cache_condition{path_patterns = []}, _Headers, _Body) ->
    true;
is_cachable_req2(_Key, #cache_condition{path_patterns = undefined}, _Headers, _Body) ->
    true;
is_cachable_req2( Key, #cache_condition{path_patterns = PathPatterns}, _Headers, _Body) ->
    Res = lists:any(fun(Path) ->
                            nomatch /= re:run(Key, Path)
                    end, PathPatterns),
    Res.

is_cachable_req3(_, #cache_condition{content_types = []}, _Headers, _Body) ->
    true;
is_cachable_req3(_, #cache_condition{content_types = undefined}, _Headers, _Body) ->
    true;
is_cachable_req3(_Key, #cache_condition{content_types = ContentTypeList}, Headers, _Body) ->
    case lists:keyfind(?HTTP_HEAD_CONTENT_TYPE, 1, Headers) of
        false ->
            false;
        {_, ContentType} ->
            lists:member(ContentType, ContentTypeList)
    end.
