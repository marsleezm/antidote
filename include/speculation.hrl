-define(REPL_FACTOR, 3).
-define(QUORUM, 2).


-record(txn_metadata, {
        read_dep :: non_neg_integer(),
        num_updated :: non_neg_integer(),
        num_specula_prepared = 0 :: non_neg_integer(),
        num_prepared = 0 :: non_neg_integer(),
        prepare_time = 0 :: non_neg_integer(),
        final_committed = false :: boolean(),
        updated_parts = dict:new() :: dict(),
        read_set :: [],
        index :: pos_integer()
        }).

-type txn_metadata() :: #txn_metadata{}.
