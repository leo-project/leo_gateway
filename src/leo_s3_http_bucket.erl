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

-export([get_bucket_list/2, get_bucket_list/6, put_bucket/2, delete_bucket/2, head_bucket/2]).

-include("leo_gateway.hrl").
-include("leo_s3_http.hrl").
-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_s3_libs/include/leo_s3_bucket.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% S3 Compatible Bucket APIs
%%--------------------------------------------------------------------
%% @doc get bucket
%% @see http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketGET.html
-spec(get_bucket_list(string(), string()|none) ->
             {ok, list(), string()}|{error, any()}).
get_bucket_list(AccessKeyId, Bucket) ->
    get_bucket_list(AccessKeyId, Bucket, none, none, 1000, none).

-spec(get_bucket_list(string(), none, char()|none, string()|none, integer(), string()|none) ->
             {ok, list(), string()}|{error, any()}).
get_bucket_list(AccessKeyId, <<>>, Delimiter, Marker, MaxKeys, none) ->
    get_bucket_list(AccessKeyId, <<"/">>, Delimiter, Marker, MaxKeys, none);

get_bucket_list(AccessKeyId, <<"/">>, _Delimiter, _Marker, _MaxKeys, none) ->
    case leo_s3_bucket:find_buckets_by_id(AccessKeyId) of
        {ok, Meta} when is_list(Meta) =:= true ->
            {ok, Meta, generate_xml(Meta)};
        not_found ->
            {ok, [], generate_xml([])};
        Error ->
            Error
    end;

get_bucket_list(_AccessKeyId, Bucket, Delimiter, Marker, MaxKeys, Prefix0) ->
    Prefix1 = case Prefix0 of
                  none -> <<>>;
                  _    -> Prefix0
              end,

    {ok, #redundancies{nodes = Redundancies}} =
        leo_redundant_manager_api:get_redundancies_by_key(get, Bucket),
    Key = << Bucket/binary, Prefix1/binary >>,

    case leo_gateway_rpc_handler:invoke(Redundancies,
                                        leo_storage_handler_directory,
                                        find_by_parent_dir,
                                        [Key, Delimiter, Marker, MaxKeys],
                                        []) of
        {ok, Meta} when is_list(Meta) =:= true ->
            {ok, Meta, generate_xml(Key, Prefix1, Meta)};
        {ok, _} ->
            {error, invalid_format};
        Error ->
            Error
    end.


%% @doc put bucket
%% @see http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketPUT.html
-spec(put_bucket(string(), string()|none) ->
             ok|{error, any()}).
put_bucket(AccessKeyId, Bucket) ->
    leo_s3_bucket:put(AccessKeyId, Bucket).


%% @doc delete bucket
%% @see http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketDELETE.html
-spec(delete_bucket(string(), string()|none) ->
             ok|{error, any()}).
delete_bucket(AccessKeyId, Bucket0) ->
    Bucket1 = case (binary:last(Bucket0) == $/) of
                  true  -> binary:part(Bucket0, {0, byte_size(Bucket0) - 1});
                  false -> Bucket0
              end,

    case leo_redundant_manager_api:get_members_by_status(?STATE_RUNNING) of
        {ok, Members} ->
            Nodes = lists:map(fun(#member{node = Node}) ->
                                      Node
                              end, Members),
            spawn(fun() ->
                          _ = rpc:multicall(Nodes, leo_storage_handler_directory, delete_objects_in_parent_dir,
                                            [Bucket1], ?DEF_REQ_TIMEOUT),
                          ok
                  end),
            leo_s3_bucket:delete(AccessKeyId, Bucket1);
        {error, Cause} ->
            {error, Cause}
    end.


%% @doc head bucket
%% @see http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketHEAD.html
-spec(head_bucket(string(), string()|none) ->
             ok|{error, any()}).
head_bucket(AccessKeyId, Bucket) ->
    leo_s3_bucket:head(AccessKeyId, Bucket).


%% @doc Generate XML from matadata-list
%% @private
generate_xml(Key, Prefix, MetadataList) ->
    Len = byte_size(Key),
    KeyStr    = binary_to_list(Key),
    PrefixStr = binary_to_list(Prefix),

    Fun = fun(#metadata{key       = EntryKey,
                        dsize     = Length,
                        timestamp = TS,
                        checksum  = CS,
                        del       = 0} , Acc) ->
                  EntryKeyStr = binary_to_list(EntryKey),

                  case string:equal(KeyStr, EntryKeyStr) of
                      true ->
                          Acc;
                      false ->
                          Entry = string:sub_string(EntryKeyStr, Len + 1),

                          case Length of
                              -1 ->
                                  %% directory.
                                  lists:append([Acc,
                                                "<CommonPrefixes><Prefix>",
                                                PrefixStr,
                                                Entry,
                                                "</Prefix></CommonPrefixes>"]);
                              _ ->
                                  %% file.
                                  lists:append([Acc,
                                                "<Contents>",
                                                "<Key>", PrefixStr, Entry, "</Key>",
                                                "<LastModified>", leo_http:web_date(TS), "</LastModified>",
                                                "<ETag>", leo_hex:integer_to_hex(CS), "</ETag>",
                                                "<Size>", integer_to_list(Length), "</Size>",
                                                "<StorageClass>STANDARD</StorageClass>",
                                                "<Owner>",
                                                "<ID>leofs</ID>",
                                                "<DisplayName>leofs</DisplayName>",
                                                "</Owner>","</Contents>"])
                          end
                  end
          end,
    io_lib:format(?XML_OBJ_LIST, [PrefixStr, lists:foldl(Fun, [], MetadataList)]).

generate_xml(MetadataList) ->
    Fun = fun(#bucket{name = Name,
                      created_at = CreatedAt} , Acc) ->
                  BucketStr = binary_to_list(Name),
                  case string:equal(?STR_SLASH, BucketStr) of
                      true ->
                          Acc;
                      false ->
                          lists:append([Acc,
                                        "<Bucket><Name>", BucketStr, "</Name>",
                                        "<CreationDate>", leo_http:web_date(CreatedAt), "</CreationDate></Bucket>"])
                  end
          end,
    io_lib:format(?XML_BUCKET_LIST, [lists:foldl(Fun, [], MetadataList)]).

