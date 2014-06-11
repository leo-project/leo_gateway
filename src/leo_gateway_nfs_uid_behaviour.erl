-module(leo_gateway_nfs_uid_behaviour).
-callback(init(Params::any()) ->
           ok | {error, any()}).
-callback(new(Path4S3::binary()) ->
          {ok, binary()}| {error, any()}).
-callback(get(UID::binary()) ->
          {ok, binary()}| {error, any()}).
-callback(delete(UID::binary()) ->
          ok | {error, any()}).
-callback(get_uid_list() ->
          {ok, list(binary())}| {error, any()}).
-callback(get_path_list() ->
          {ok, list(binary())}| {error, any()}).
