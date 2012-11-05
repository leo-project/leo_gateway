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
-define(QUERY_PREFIX,    "prefix").
-define(QUERY_DELIMITER, "delimiter").
-define(QUERY_MAX_KEYS,  "max-keys").
-define(QUERY_ACL,       "acl").
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

-define(HTTP_HEAD_BIN_ACL,                          <<"acl">>).
-define(HTTP_HEAD_BIN_CACHE_CTRL,                   <<"Cache-Control">>).
-define(HTTP_HEAD_BIN_CONTENT_TYPE,                 <<"Content-Type">>).
-define(HTTP_HEAD_BIN_ETAG4AWS,                     <<"ETag">>).
-define(HTTP_HEAD_BIN_LAST_MODIFIED,                <<"Last-Modified">>).
-define(HTTP_HEAD_BIN_PREFIX,                       <<"prefix">>).
-define(HTTP_HEAD_BIN_X_AMZ_META_DIRECTIVE,         <<"X-Amz-Metadata-Directive">>).
-define(HTTP_HEAD_BIN_X_AMZ_COPY_SOURCE,            <<"X-Amz-Copy-Source">>).
-define(HTTP_HEAD_BIN_X_AMZ_META_DIRECTIVE_COPY,    <<"COPY">>).
-define(HTTP_HEAD_BIN_X_AMZ_META_DIRECTIVE_REPLACE, <<"REPLACE">>).
-define(HTTP_HEAD_BIN_X_FROM_CACHE,                 <<"X-From-Cache">>).

-define(HTTP_CTYPE_OCTET_STREAM, <<"application/octet-stream">>).
-define(HTTP_CTYPE_XML,          <<"application/xml">>).


-define(HTTP_ST_OK,           200).
-define(HTTP_ST_NO_CONTENT,   204).
-define(HTTP_ST_NOT_MODIFIED, 304).
-define(HTTP_ST_BAD_REQ,      400).
-define(HTTP_ST_FORBIDDEN,    403).
-define(HTTP_ST_NOT_FOUND,    404).
-define(HTTP_ST_INTERNAL_ERROR,      500).
-define(HTTP_ST_SERVICE_UNAVAILABLE, 503).
-define(HTTP_ST_GATEWAY_TIMEOUT,     504).

%%
%% S3 RESPONSE XML
%%
-define(XML_BUCKET_LIST, lists:append(["<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
                                       "<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01\">",
                                       "<Owner><ID>LeoFS</ID><DisplayName>webfile</DisplayName></Owner><Buckets>",
                                       "~s",
                                       "</Buckets></ListAllMyBucketsResult>"])).

-define(XML_OBJ_LIST, lists:append(["<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
                                    "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">",
                                    "<Name>standalone</Name>",
                                    "<Prefix>~s</Prefix>",
                                    "<Marker></Marker>",
                                    "<MaxKeys>1000</MaxKeys>",
                                    "<Delimiter>/</Delimiter>",
                                    "<IsTruncated>false</IsTruncated>",
                                    "~s",
                                    "</ListBucketResult>"])).

-define(XML_COPY_OBJ_RESULT, lists:append(["<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
                                           "<CopyObjectResult>",
                                           "<LastModified>~s</LastModified>",
                                           "<ETag>\"~s\"</ETag>",
                                           "</CopyObjectResult>"])).


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
          acceptable_max_obj_len = 0 :: integer(),
          chunked_obj_len = 0        :: integer(),
          threshold_obj_len = 0      :: integer()
         }).

-record(req_params, {
          access_key_id = []         :: string(),
          token_length = 0           :: integer(),
          min_layers = 0             :: integer(),
          max_layers = 0             :: integer(),
          is_dir = false             :: boolean(),
          qs_prefix = []             :: string(),
          has_inner_cache = false    :: boolean(),
          range_header               :: string(),
          is_cached = false          :: boolean(),
          acceptable_max_obj_len = 0 :: integer(),
          chunked_obj_len        = 0 :: integer(),
          threshold_obj_len      = 0 :: integer()
         }).

-record(cache, {
          etag         = 0    :: integer(), %% actual value is checksum
          mtime        = 0    :: integer(), %% gregorian_seconds
          content_type = []   :: string(),  %% from a Content-Type header
          body         = <<>> :: binary()
         }).


%%
%% S3 RESPONSE ERRORS
%%
-define(S3_ERR_IMCOPLETE_BODY,      'imcomplete_body').
-define(S3_ERR_INTERNAL_ERROR,      'internal_error').
-define(S3_ERR_INVALID_ARG,         'invalid_argument').
-define(S3_ERR_INVALID_URL,         'invalid_uri').
-define(S3_ERR_KEY_TOO_LONG,        'key_too_long').
-define(S3_ERR_MISSING_CONTENT_LEN, 'missing_content_length').
-define(S3_ERR_MISSING_REQ_BODY,    'missing_request_body_error').
-define(S3_ERR_SLOW_DOWN,           'slow_down').

-type(s3_errors() :: ?S3_ERR_IMCOPLETE_BODY |
                     ?S3_ERR_INTERNAL_ERROR |
                     ?S3_ERR_INVALID_ARG    |
                     ?S3_ERR_INVALID_URL    |
                     ?S3_ERR_KEY_TOO_LONG   |
                     ?S3_ERR_MISSING_CONTENT_LEN |
                     ?S3_ERR_MISSING_REQ_BODY    |
                     ?S3_ERR_SLOW_DOWN).

-define(error_resp(Type),
        case Type of
            imcomplete_body ->
                #error_code{code = 'ImcompleteBody',
                            description = "You did not provide the number of bytes specified by the Content-Length HTTP header",
                            http_status_code = 400};
            internal_error ->
                #error_code{code = 'InnternalError',
                            description = "We encountered an internal error. Please try again.",
                            http_status_code = 500};
            invalid_argument ->
                #error_code{code = 'InvalidArgument',
                            description = "Invalid Argument.",
                            http_status_code = 400};
            invalid_uri ->
                #error_code{code = 'InvalidURI',
                            description = "Couldn't pare the specified URI.",
                            http_status_code = 400};
            key_too_long ->
                #error_code{code = 'KeyTooLong',
                            description = "Your key is long",
                            http_status_code = 400};

            missing_content_length ->
                #error_code{code = 'MissingContentLength',
                            description = "You must provide the Content-Length HTTP header.",
                            http_status_code = 411};

            missing_request_body_error ->
                #error_code{code = 'MissingRequestBodyError',
                            description = "This happens when the user sends an empty data as a request. Request body is empty.",
                            http_status_code = 400};

            request_timeout ->
                #error_code{code = 'RequestTimeout',
                            description = "Your socket connection to the server was not read from or written to within the timeout period.",
                            http_status_code = 400};

            slow_down ->
                #error_code{code = 'SlowDown',
                            description = "Please reduce your request rate.",
                            http_status_code = 503}
        end).

