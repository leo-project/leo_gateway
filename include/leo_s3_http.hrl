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
%%
%% HTTP METHODS
%%
-define(HTTP_GET,        'GET').
-define(HTTP_POST,       'POST').
-define(HTTP_PUT,        'PUT').
-define(HTTP_DELETE,     'DELETE').
-define(HTTP_HEAD,       'HEAD').

%%
%% HTTP-RELATED
%%
-define(SERVER_HEADER,   {"Server","LeoFS"}).
-define(STR_NEWLINE,     "\n").
-define(STR_SLASH,       "/").
-define(BIN_SLASH,       <<"/">>).
-define(BIN_EMPTY,       <<>>).

-define(ERR_TYPE_INTERNAL_ERROR, internal_server_error).

%% HTTP HEADER
-define(HTTP_HEAD_ATOM_AGE,                'Age').
-define(HTTP_HEAD_ATOM_AUTHORIZATION,      'Authorization').
-define(HTTP_HEAD_ATOM_CACHE_CTRL,         'Cache-Control').
-define(HTTP_HEAD_ATOM_CONTENT_LENGTH,     'Content-Length').
-define(HTTP_HEAD_ATOM_CONTENT_MD5,        'Content-Md5').
-define(HTTP_HEAD_ATOM_CONTENT_TYPE,       'Content-Type').
-define(HTTP_HEAD_ATOM_DATE,               'Date').

-define(HTTP_HEAD_ATOM_ETAG,               'Etag').
-define(HTTP_HEAD_ATOM_IF_MODIFIED_SINCE,  'If-Modified-Since').
-define(HTTP_HEAD_ATOM_LAST_MODIFIED,      'Last-Modified').
-define(HTTP_HEAD_ATOM_RANGE,              'Range').

-define(HTTP_HEAD_BIN_CACHE_CTRL,                   <<"Cache-Control">>).
-define(HTTP_HEAD_BIN_CONTENT_TYPE,                 <<"Content-Type">>).
-define(HTTP_HEAD_BIN_ETAG4AWS,                     <<"ETag">>).
-define(HTTP_HEAD_BIN_LAST_MODIFIED,                <<"Last-Modified">>).
-define(HTTP_HEAD_BIN_PREFIX,                       <<"prefix">>).
-define(HTTP_HEAD_BIN_X_AMZ_META_DIRECTIVE,         <<"X-Amz-Metadata-Directive">>).
-define(HTTP_HEAD_BIN_X_AMZ_COPY_SOURCE,            <<"X-Amz-Copy-Source">>).
-define(HTTP_HEAD_BIN_X_AMZ_ID_2,                   <<"X-Amz-Id-2">>).
-define(HTTP_HEAD_BIN_X_AMZ_REQ_ID,                 <<"X-Amz-Request-Id">>).
-define(HTTP_HEAD_BIN_X_AMZ_META_DIRECTIVE_COPY,    <<"COPY">>).
-define(HTTP_HEAD_BIN_X_AMZ_META_DIRECTIVE_REPLACE, <<"REPLACE">>).
-define(HTTP_HEAD_BIN_X_FROM_CACHE,                 <<"X-From-Cache">>).

-define(HTTP_CTYPE_OCTET_STREAM, <<"application/octet-stream">>).
-define(HTTP_CTYPE_XML,          <<"application/xml">>).

-define(HTTP_QS_BIN_ACL,         <<"acl">>).
-define(HTTP_QS_BIN_UPLOADS,     <<"uploads">>).
-define(HTTP_QS_BIN_UPLOAD_ID,   <<"uploadId">>).
-define(HTTP_QS_BIN_PART_NUMBER, <<"partNumber">>).

-define(HTTP_ST_OK,                  200).
-define(HTTP_ST_NO_CONTENT,          204).
-define(HTTP_ST_NOT_MODIFIED,        304).
-define(HTTP_ST_BAD_REQ,             400).
-define(HTTP_ST_FORBIDDEN,           403).
-define(HTTP_ST_NOT_FOUND,           404).
-define(HTTP_ST_INTERNAL_ERROR,      500).
-define(HTTP_ST_SERVICE_UNAVAILABLE, 503).
-define(HTTP_ST_GATEWAY_TIMEOUT,     504).

-define(CACHE_HTTP,  'http').
-define(CACHE_INNER, 'inner').
-type(cache_method() :: ?CACHE_HTTP | ?CACHE_INNER).


%%
%% S3 RESPONSE XML
%%
-define(XML_BUCKET_LIST,
        lists:append(["<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
                      "<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01\">",
                      "<Owner>",
                      "  <ID>LeoFS</ID>",
                      "  <DisplayName>webfile</DisplayName>",
                      "</Owner>",
                      "<Buckets>",
                      "~s",
                      "</Buckets></ListAllMyBucketsResult>"])).

-define(XML_OBJ_LIST,
        lists:append(["<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
                      "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
                      "  <Name>standalone</Name>",
                      "  <Prefix>~s</Prefix>",
                      "  <Marker></Marker>",
                      "  <MaxKeys>1000</MaxKeys>",
                      "  <Delimiter>/</Delimiter>",
                      "  <IsTruncated>false</IsTruncated>",
                      "~s",
                      "</ListBucketResult>"])).

-define(XML_COPY_OBJ_RESULT,
        lists:append(["<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
                      "<CopyObjectResult>",
                      "  <LastModified>~s</LastModified>",
                      "  <ETag>\"~s\"</ETag>",
                      "</CopyObjectResult>"])).

-define(XML_UPLOAD_INITIATION,
        lists:append(["<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
                      "<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                      "  <Bucket>~s</Bucket>"
                      "  <Key>~s</Key>"
                      "  <UploadId>~s</UploadId>"
                      "</InitiateMultipartUploadResult>"])).

-define(XML_UPLOAD_COMPLETION,
        lists:append(["<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
                      "<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
                      "  <Location>http://Example-Bucket.s3.amazonaws.com/Example-Object</Location>",
                      "  <Bucket>~s</Bucket>",
                      "  <Key>~s</Key>",
                      "  <ETag>\"~s\"</ETag>",
                      "</CompleteMultipartUploadResult>"])).


-record(http_options, {
          port = 0                   :: pos_integer(),  %% http port number
          ssl_port = 0               :: pos_integer(),  %% ssl port number
          ssl_certfile = []          :: string(),       %% ssl cert file name
          ssl_keyfile = []           :: string(),       %% ssk key file name
          num_of_acceptors = 0       :: pos_integer(),  %% # of acceptors (http server's workers)
          s3_api = true              :: boolean(),      %% use s3-api?
          %% for cache
          cache_method               :: cache_method(), %% cahce method: [http | inner]
          cache_workers = 0          :: pos_integer(),
          cache_capacity = 0         :: pos_integer(),
          cache_expire = 0           :: pos_integer(),  %% cache expire time (sec)
          cache_max_content_len = 0  :: pos_integer(),  %% cache max content length (byte)
          cachable_content_type = [] :: list(),         %% cachable content types
          cachable_path_pattern = [] :: list(),         %% cachable path patterns
          %% for large-object
          max_chunked_objs = 0       :: pos_integer(),  %% max chunked objects
          max_len_for_obj = 0        :: pos_integer(),  %% max length a object (byte)
          chunked_obj_len = 0        :: pos_integer(),  %% chunked object length for large object (byte)
          threshold_obj_len = 0      :: pos_integer()   %% threshold object length for large object (byte)
         }).

-record(req_params, {
          path = <<>>                :: binary(),       %% path (uri)
          access_key_id = []         :: string(),       %% s3's access-key-id
          token_length = 0           :: pos_integer(),  %% length of tokened path
          min_layers = 0             :: pos_integer(),  %% acceptable # of min layers
          max_layers = 0             :: pos_integer(),  %% acceptable # of max layers
          qs_prefix = []             :: string(),       %% query string
          range_header               :: string(),       %% range header
          has_inner_cache = false    :: boolean(),      %% has inner-cache?
          is_cached = false          :: boolean(),      %% is cached?
          is_dir = false             :: boolean(),      %% is directory?
          %% For large-object
          is_upload = false          :: boolean(),      %% is upload operation? (for multipart upload)
          upload_id = <<>>           :: binary(),       %% upload id for multipart upload
          upload_part_num = 0        :: pos_integer(),  %% upload part number for multipart upload
          max_chunked_objs = 0       :: pos_integer(),  %% max chunked objects
          max_len_for_multipart = 0  :: pos_integer(),  %% max length a multipart object (byte)
          max_len_for_obj = 0        :: pos_integer(),  %% max length a object (byte)
          chunked_obj_len = 0        :: pos_integer(),  %% chunked object length for large-object (byte)
          threshold_obj_len = 0      :: pos_integer()   %% threshold object length for large-object (byte)
         }).

-record(cache, {
          etag         = 0    :: pos_integer(), %% actual value is checksum
          mtime        = 0    :: pos_integer(), %% gregorian_seconds
          content_type = []   :: string(),      %% from a Content-Type header
          body         = <<>> :: binary()       %% body (value)
         }).


