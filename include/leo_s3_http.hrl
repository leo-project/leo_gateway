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
%% Leo S3 HTTP
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
-define(QUERY_ACL,       "acl").
-define(STR_SLASH,       "/").
-define(BIN_SLASH,       <<"/">>).

-define(ERR_TYPE_INTERNAL_ERROR, internal_server_error).

%% @deplicate
-define(HTTP_HEAD_RANGE,         "Range").
-define(HTTP_HEAD_MD5,           "Content-MD5").
-define(HTTP_HEAD_HOST,          "Host").
-define(HTTP_HEAD_EXPECT,        "Expect").
-define(HTTP_HEAD_100_CONTINUE,  "100-continue").

%% http-header key
-define(HTTP_HEAD_ACL,                <<"acl">>).
-define(HTTP_HEAD_AGE,                <<"Age">>).
-define(HTTP_HEAD_CACHE_CTRL,         <<"Cache-Control">>).
-define(HTTP_HEAD_CONTENT_LENGTH,     <<"Content-Length">>).
-define(HTTP_HEAD_CONTENT_TYPE,       <<"Content-Type">>).
-define(HTTP_HEAD_DATE,               <<"Date">>).
-define(HTTP_HEAD_ETAG,               <<"ETag">>).
-define(HTTP_HEAD_LAST_MODIFIED,      <<"Last-Modified">>).
-define(HTTP_HEAD_PREFIX,             <<"prefix">>).

-define(HTTP_HEAD_X_AMZ_META_DIRECTIVE,         <<"x-amz-metadata-directive">>).
-define(HTTP_HEAD_X_AMZ_COPY_SOURCE,            <<"x-amz-copy-source">>).
-define(HTTP_HEAD_X_AMZ_META_DIRECTIVE_COPY,    <<"COPY">>).
-define(HTTP_HEAD_X_AMZ_META_DIRECTIVE_REPLACE, <<"REPLACE">>).
-define(HTTP_HEAD_X_FROM_CACHE,                 <<"X-From-Cache">>).


%% s3 response xmls
-define(XML_BUCKET_LIST,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        ++ "<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01\">"
        ++ "<Owner><ID>LeoFS</ID><DisplayName>webfile</DisplayName></Owner><Buckets>"
        ++ "~s"
        ++ "</Buckets></ListAllMyBucketsResult>").

-define(XML_OBJ_LIST,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        ++ "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
        ++ "<Name>standalone</Name>"
        ++ "<Prefix>~s</Prefix>"
        ++ "<Marker></Marker>"
        ++ "<MaxKeys>1000</MaxKeys>"
        ++ "<Delimiter>/</Delimiter>"
        ++ "<IsTruncated>false</IsTruncated>"
        ++ "~s"
        ++ "</ListBucketResult>").

-define(XML_COPY_OBJ_RESULT,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        ++ "<CopyObjectResult>"
        ++ "<LastModified>~s</LastModified>"
        ++ "<ETag>\"~s\"</ETag>"
        ++ "</CopyObjectResult>").


-record(http_options, {
          port = 0                   :: integer(),
          ssl_port = 0               :: integer(),
          ssl_certfile = []          :: string(),
          ssl_keyfile = []           :: string(),
          num_of_acceptors = 0       :: integer(),
          s3_api = true              :: boolean(),
          cache_plugin = []          :: string(),
          cache_expire = 0           :: integer(),
          cache_max_content_len = 0  :: integer(),
          cachable_content_type = [] :: list(),
          cachable_path_pattern = [] :: list(),
          chunked_obj_size = 0       :: integer(),
          threshold_obj_size = 0     :: integer()
         }).

-record(req_params, {
          access_key_id     :: string(),
          token_length      :: integer(),
          min_layers        :: integer(),
          max_layers        :: integer(),
          is_dir = false    :: boolean(),
          qs_prefix         :: string(),
          has_inner_cache   :: boolean(),
          range_header      :: string(),
          is_cached         :: boolean(),
          chunked_obj_size   :: integer(),
          threshold_obj_size :: integer()
         }).

-record(cache, {
          etag         = 0    :: integer(), % actual value is checksum
          mtime        = 0    :: integer(), % gregorian_seconds
          content_type = ""   :: list(),    % from a Content-Type header
          body         = <<>> :: binary()
         }).
