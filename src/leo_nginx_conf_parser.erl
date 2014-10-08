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
%% @doc Parser of Nginx configuration file
%% @end
%%======================================================================
-module(leo_nginx_conf_parser).

%% External exports
-export([parse/1, get_custom_headers/2]).

%% regular expressions to retrieve custom header information
-define(REGEX_LOCATION_BLOCK, "location\s+([-~%\._/0-9a-zA-Z]+)\s+{([^}]+)}").
-define(REGEX_KEY_VALUE_PAIR, "\s+([-~%\._/0-9a-zA-Z]+)\s+([^;]+);").


%% @doc Parse a nginx configuration file to get custom header settings.
-spec(parse(FileName) ->
             {ok, list()} |
             not_found |
             {error, any()} when FileName::file:name_all()).
parse(FileName) ->
    parse_1(file:read_file(FileName)).

%% @doc Get custom HTTP headers based on the return value of parse function
-spec(get_custom_headers(Path, ParseResult) ->
             {ok, list()} |
             {error, any()} when Path::binary(), ParseResult::list()).
get_custom_headers(Path, ParseResult) ->
    Ret = match_path_pattern(Path, ParseResult),
    case Ret of
        undefined ->
            {ok, []};
        Value ->
            make_result_headers(Value, [])
    end.

%%--------------------------------------------------------------------
%% Internal Functions.
%%--------------------------------------------------------------------
% @private
match_path_pattern(_Path, []) ->
    undefined;
match_path_pattern(Path, [{Prefix, Value}|T]) ->
    case binary:match(Path, Prefix) of
        nomatch ->
            match_path_pattern(Path, T);
        {0, _} ->
            Value;
        {_, _} ->
            match_path_pattern(Path, T)
    end.

% @private
make_result_headers([], Acc) ->
    Acc;
make_result_headers([{<<"add_header">>, KeyValuePair}|T], Acc) ->
    [Key, Val|_] = binary:split(KeyValuePair, <<" ">>),
    NewAcc = case Key of
        <<"Cache-Control">> ->
            merge_cache_control_values(Val, Acc);
        _ ->
            [{Key, Val}|Acc]
    end,
    make_result_headers(T, NewAcc);
make_result_headers([{<<"expires">>, ExpireVal}|T], Acc) ->
    {_, CCVal}= make_cache_control_header(ExpireVal),
    NewAcc = merge_cache_control_values(CCVal, Acc),
    make_result_headers(T, NewAcc);
make_result_headers([{_, _KeyValuePair}|T], Acc) ->
    make_result_headers(T, Acc).

% @private
make_cache_control_header(ExpireVal) when is_integer(ExpireVal), ExpireVal >= 0 ->
    BinExpireVal = erlang:integer_to_binary(ExpireVal),
    {<<"Cache-Control">>, <<"max-age=", BinExpireVal/binary>>};
make_cache_control_header(ExpireVal) when is_integer(ExpireVal) ->
    {<<"Cache-Control">>, <<"no-cache">>}.

merge_cache_control_values(NewVal, List) ->
    case lists:keyfind(<<"Cache-Control">>, 1, List) of
        false ->
            [{<<"Cache-Control">>, NewVal}|List];
        {_, OldVal} ->
            List2 = lists:keydelete(<<"Cache-Control">>, 1, List),
            [{<<"Cache-Control">>, <<OldVal/binary, <<", ">>/binary, NewVal/binary>>}|List2]
    end.

%% @private
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

%% @private
parse_2([], _Bin, _MP2, Acc) ->
    {ok, Acc};
parse_2([H|T], Bin, MP2, Acc) ->
    case parse_3(H, Bin, MP2) of
        {error, Cause} ->
            {error, Cause};
        not_found ->
            parse_2(T, Bin, MP2, Acc);
        Ret ->
            parse_2(T, Bin, MP2, [Ret|Acc])
    end.

%% @private
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

%% @private
parse_location_body(Binary, MP2) ->
    case re:run(Binary, MP2, [global]) of
        {error, ErrType} ->
            {error, ErrType};
        nomatch ->
            not_found;
        {match, Captured} ->
            parse_location_body_1(Captured, Binary, [])
    end.

%% @private
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

%% @private
parse_location_body_2([_, {KeyPos, KeyLen}, {ValPos, ValLen}|_T], Bin) ->
    Key = binary:part(Bin, KeyPos, KeyLen),
    Val = binary:part(Bin, ValPos, ValLen),
    {Key, Val};
parse_location_body_2(_, _Bin) ->
    {error, invalid_location_format}.
