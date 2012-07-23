%%======================================================================
%%
%% Leo S3 HTTP
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
%% Leo S3 HTTP - S3 Bucket-related
%% @doc
%% @end
%%======================================================================
-module(leo_s3_http_bucket).

-author('Yosuke Hara').
-author('Yoshiyuki Kanno').

-export([get_bucket_list/1, get_bucket_list/5]).

-include("leo_gateway.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(STR_SLASH,       "/").

%%--------------------------------------------------------------------
%% S3 Compatible Bucket APIs
%%--------------------------------------------------------------------
%% @doc get bucket
%% @see http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketGET.html
-spec(get_bucket_list(string()|none) ->
             {ok, list(), string()}|{error, any()}).
get_bucket_list(Bucket) ->
    get_bucket_list(Bucket, none, none, 1000, none).

-spec(get_bucket_list(string()|none, char()|none, string()|none, integer(), string()|none) ->
             {ok, list(), string()}|{error, any()}).
get_bucket_list(Bucket, _Delimiter, _Marker, _MaxKeys, Prefix) ->
    {ok, #redundancies{nodes = Redundancies}} =
        leo_redundant_manager_api:get_redundancies_by_key(get, Bucket),

    Key = case Bucket of
              none ->
                  ?STR_SLASH;
              Base when Prefix =:= none ->
                  Base;
              Base when Prefix /= none ->
                  Base ++ ?STR_SLASH ++ Prefix
          end,
    case leo_gateway_rpc_handler:invoke(Redundancies,
                                        leo_storage_handler_directory,
                                        find_by_parent_dir,
                                        [Key],
                                        []) of
        {ok, Meta} when is_list(Meta) =:= true andalso Bucket =:= none ->
            {ok, Meta, makeMyBucketsXML(Meta)};
        {ok, Meta} when is_list(Meta) =:= true ->
            {ok, Meta, makeBucketsXML(Key, Meta)};
        Error ->
            Error
    end.

makeBucketsXML(Dir, Buckets) ->
    DirLen = string:len(Dir),
    Fun = fun(#metadata{key       = EntryKey,
                        dsize     = Length,
                        timestamp = TS,
                        checksum  = CS,
                        del       = 0} , Acc) ->
                  case string:equal(Dir, EntryKey) of
                      true ->
                          Acc;
                      false ->
                          Entry = string:sub_string(EntryKey, DirLen + 1),
                          case Length of
                              -1 ->
                                  %% directory.
                                  Acc ++ "<CommonPrefixes><Prefix>" ++ Entry ++ "</Prefix></CommonPrefixes>";
                              _ ->
                                  %% file.
                                  Acc ++ "<Contents>"
                                      ++ "<Key>" ++ Entry ++ "</Key>"
                                      ++ "<LastModified>" ++ leo_utils:date_format(TS) ++ "</LastModified>"
                                      ++ "<ETag>" ++ leo_hex:integer_to_hex(CS) ++ "</ETag>"
                                      ++ "<Size>" ++ integer_to_list(Length) ++ "</Size>"
                                      ++ "<StorageClass>STANDARD</StorageClass>"
                                      ++ "</Contents>"
                          end
                  end
          end,
    io_lib:format(?XML_OBJ_LIST, [lists:foldl(Fun, [], Buckets)]).

makeMyBucketsXML(Buckets) ->
    Fun = fun(#metadata{key=EntryKey, dsize=Length, timestamp=TS, del=0} , Acc) ->
                  case string:equal(?STR_SLASH, EntryKey) of
                      true ->
                          Acc;
                      false ->
                          Entry = string:sub_string(EntryKey, 2),
                          case Length of
                              -1 ->
                                  Acc ++ "<Bucket><Name>" ++ Entry ++ "</Name><CreationDate>" ++
                                      leo_utils:date_format(TS) ++ "</CreationDate></Bucket>";
                              _ ->
                                  Acc
                          end
                  end
          end,
    io_lib:format(?XML_BUCKET_LIST, [lists:foldl(Fun, [], Buckets)]).

