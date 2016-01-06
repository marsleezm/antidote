-record(txn_metadata, {
        num_to_prepare = 0 :: non_neg_integer(),
        num_specula_prepared = 0 :: non_neg_integer(),
        prepare_time = 0 :: non_neg_integer(),
        updated_parts = dict:new() :: dict(),
        read_set = [] :: [],
        prepare_stat = [],
        read_stat = [],
        index :: pos_integer()
        }).

-record(repl_state, {
        pending_log :: dict(),
        replicas :: [atom()],
        local_rep_set :: set(),
        except_replicas :: dict(),
  
        mode :: atom(),
        repl_factor :: non_neg_integer(),
        delay :: non_neg_integer(),
        my_name :: atom()
        }).

-type txn_metadata() :: #txn_metadata{}.
