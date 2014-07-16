-module(leo_gateway_nfs_state_behaviour).
-callback(init(Params::any()) ->
           ok | {error, any()}).
-callback(add_path(Path4S3::binary()) ->
          {ok, binary()}| {error, any()}).
-callback(get_path(UID::binary()) ->
          {ok, binary()}| not_found).
-callback(del_path(UID::binary()) ->
          true).
-callback(get_uid_list() ->
          {ok, list(binary())}| {error, any()}).
-callback(get_path_list() ->
          {ok, list(binary())}| {error, any()}).
-callback(add_readdir_entry(CookieVerf::binary(), ReadDirEntry::list()) ->
          true).
-callback(get_readdir_entry(CookieVerf::binary()) ->
          {ok, list()}| not_found).
-callback(del_readdir_entry(CookieVerf::binary()) ->
          true).
-callback(add_write_verfier(WriteVerf::binary()) ->
          true).
-callback(get_write_verfier(WriteVerf::binary()) ->
          {ok, binary()}| not_found).
