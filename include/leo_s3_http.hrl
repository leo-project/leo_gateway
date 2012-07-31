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
%% Leo S3 HTTP - Mochiweb
%% @doc
%% @end
%%======================================================================
-define(HTTP_GET,        'GET').
-define(HTTP_POST,       'POST').
-define(HTTP_PUT,        'PUT').
-define(HTTP_DELETE,     'DELETE').
-define(HTTP_HEAD,       'HEAD').
-define(SERVER_HEADER,   {"Server","LeoFS"}).
-define(QUERY_PREFIX,    "prefix").
-define(QUERY_DELIMITER, "delimiter").
-define(QUERY_MAX_KEYS,  "max-keys").
-define(STR_SLASH,       "/").

-define(ERR_TYPE_INTERNAL_ERROR, internal_server_error).

-define(HTTP_HEAD_MD5,          "Content-MD5").
-define(HTTP_HEAD_CONTENT_TYPE, "Content-Type").
-define(HTTP_HEAD_DATE,         "Date").


%% s3 response xmls
-define(XML_BUCKET_LIST,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        ++ "<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01\">"
        ++ "<Owner><ID>LeoFS</ID><DisplayName>webfile</DisplayName></Owner><Buckets>"
        ++ "~s"
        ++ "</Buckets></ListAllMyBucketsResult>").

-define(XML_OBJ_LIST,
        "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
        ++ "<Name>standalone</Name>"
        ++ "<Prefix></Prefix><Marker></Marker>"
        ++ "<MaxKeys>50</MaxKeys><Delimiter>/</Delimiter>"
        ++ "<IsTruncated>false</IsTruncated>"
        ++ "~s"
        ++ "</ListBucketResult>").


-record(req_params, {
          access_key_id     :: string(),
          token_length      :: integer(),
          min_layers        :: integer(),
          max_layers        :: integer(),
          is_dir = false    :: boolean(),
          qs_prefix         :: string(),
          has_inner_cache   :: boolean(),
          is_cached         :: boolean()
         }).

-record(cache, {
          etag         = 0    :: integer(), % actual value is checksum
          mtime        = 0    :: integer(), % gregorian_seconds
          content_type = ""   :: list(),    % from a Content-Type header
          body         = <<>> :: binary()
         }).
