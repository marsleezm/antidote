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

-type txn_metadata() :: #txn_metadata{}.
-define(NUM_VERSION, 10).
