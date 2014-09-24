%%======================================================================
%%
%% Leo Gateway
%%
%% Copyright (c) 2012-2014 Rakuten, Inc.
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
%% Leo Gateway Parser for Nginx Configuration File
%%
%% @doc
%% @end
%%======================================================================
-module(leo_gateway_nginx_conf_parser).

%% External exports
-export([parse/1]).


%% regular expressions to retrieve custom header information
-define(REGEX_LOCATION_BLOCK, "location\s+([-_/a-zA-Z0-9]+)\s+{([^}]+)}").
-define(REGEX_KEY_VALUE_PAIR, "\s+([-_/a-zA-Z0-9]+)\s+([^;]+);").

%% @doc Parse a nginx configuration file to get custom header settings.
-spec(parse(FileName) ->
         {ok, list()}|not_found|{error, any()} when FileName::file:name_all()).
parse(FileName) ->
    parse_1(file:read_file(FileName)).

%%--------------------------------------------------------------------
%% Internal Functions.
%%--------------------------------------------------------------------
parse_1({error, Reason}) ->
    {error, Reason};
parse_1({ok, Binary}) ->
    {ok, MP1} = re:compile(?REGEX_LOCATION_BLOCK, [multiline]),
    {ok, MP2} = re:compile(?REGEX_KEY_VALUE_PAIR),
    case re:run(Binary, MP1, [global]) of
        {error, ErrType} ->
            {error, ErrType};
        nomatch ->
            not_found;
        {match, Captured} ->
            parse_2(Captured, Binary, MP2, [])
    end.
parse_2([], _Bin, _MP2, Acc) ->
    Acc;
parse_2([H|T], Bin, MP2, Acc) ->
    case parse_3(H, Bin, MP2) of
        {error, Cause} ->
            {error, Cause};
        not_found ->
            parse_2(T, Bin, MP2, Acc);
        Ret ->
            parse_2(T, Bin, MP2, [Ret|Acc])
    end.
parse_3([_, {KeyPos, KeyLen}, {LocPos, LocLen}|_T], Bin, MP2) ->
    Key = binary:part(Bin, KeyPos, KeyLen),
    Loc = binary:part(Bin, LocPos, LocLen),
    case parse_location_body(Loc, MP2) of
        {error, Cause} ->
            {error, Cause};
        not_found ->
            not_found;
        Ret ->
            {Key, Ret}
    end;
parse_3(_, _Bin, _MP2) ->
    {error, invalid_file_format}.
parse_location_body(Binary, MP2) ->
    case re:run(Binary, MP2, [global]) of
        {error, ErrType} ->
            {error, ErrType};
        nomatch ->
            not_found;
        {match, Captured} ->
            parse_location_body_1(Captured, Binary, [])
    end.
parse_location_body_1([], _Bin, Acc) ->
    Acc;
parse_location_body_1([H|T], Bin, Acc) ->
    case parse_location_body_2(H, Bin) of
        {error, Cause} ->
            {error, Cause};
        not_found ->
            parse_location_body_1(T, Bin, Acc);
        Ret ->
            parse_location_body_1(T, Bin, [Ret|Acc])
    end.
parse_location_body_2([_, {KeyPos, KeyLen}, {ValPos, ValLen}|_T], Bin) ->
    Key = binary:part(Bin, KeyPos, KeyLen),
    Val = binary:part(Bin, ValPos, ValLen),
    {Key, Val};
parse_location_body_2(_, _Bin) ->
    {error, invalid_location_format}.
