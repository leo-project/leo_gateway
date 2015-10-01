-module(leo_nlm_lock_behaviour).

-include("leo_nlm_lock.hrl").

-callback(test(FileHandler::binary(), Lock::#lock_record{}) ->
                ok | {error, #lock_record{}} | {error, any()}).

-callback(lock(FileHandler::binary(), Lock::#lock_record{}) ->
                ok | {error, #lock_record{}} | {error, any()}).

-callback(unlock(FileHandler::binary(), Owner::binary(), Start::non_neg_integer(), End::integer()) ->
                ok).
