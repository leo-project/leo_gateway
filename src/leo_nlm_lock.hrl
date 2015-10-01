-record(lock_record,{
          start :: non_neg_integer(),
          till  :: integer(),
          len   :: integer(),
          owner :: binary(),
          uppid :: integer(),
          excl  :: boolean()
         }).

-define(NLM_LOCK_ETS, nlm_lock_ets).
