-define(BUCKET, <<"antidote">>).
-define(MASTER, antidote_vnode_master).
-define(LOGGING_MASTER, logging_vnode_master).
-define(CLOCKSI_MASTER, clocksi_vnode_master).
-define(SPECULA_MASTER, specula_vnode_master).
-define(CLOCKSI_GENERATOR_MASTER,
        clocksi_downstream_generator_vnode_master).
-define(CLOCKSI, clocksi).
-define(REPMASTER, antidote_rep_vnode_master).
-define(N, 1).
-define(OP_TIMEOUT, infinity).
-define(COORD_TIMEOUT, infinity).
-define(COMM_TIMEOUT, infinity).
-define(NUM_W, 2).
-define(NUM_R, 2).
%% Allow read concurrency on shared ets tables
%% These are the tables that store materialized objects
%% and information about live transactions, so the assumption
%% is there will be several more reads than writes
-define(TABLE_CONCURRENCY, {read_concurrency,true}).
%% The read concurrency is the maximum number of concurrent
%% readers per vnode.  This is so shared memory can be used
%% in the case of keys that are read frequently.  There is
%% still only 1 writer per vnode
-define(READ_CONCURRENCY, 20).
%% This can be used for testing, so that transactions start with
%% old snapshots to avoid clock-skew.
%% This can break the tests is not set to 0
-define(OLD_SS_MICROSEC,0).
%% The number of supervisors that are responsible for
%% supervising transaction coorinator fsms
-define(NUM_SUP, 100).
%% Threads will sleep for this length when they have to wait
%% for something that is not ready after which they
%% wake up and retry. I.e. a read waiting for
%% a transaction currently in the prepare state that is blocking
%% that read.
-define(SPIN_WAIT, 10).
%% At commit, if this is set to true, the logging vnode
%% will ensure that the transaction record is written to disk
-define(SYNC_LOG, true).
-record (payload, {key:: key(), type :: type(), op_param, actor}).

%% Used by the replication layer
-record(operation, {op_number, payload :: payload()}).
-type operation() :: #operation{}.
-type vectorclock() :: dict().


%% The way records are stored in the log.
-record(log_record, {tx_id :: txid(),
                     op_type:: update | prepare | commit | abort | noop,
                     op_payload}).

%% Clock SI

%% MIN is Used for generating the timeStamp of a new snapshot
%% in the case that a client has already seen a snapshot time
%% greater than the current time at the replica it is starting
%% a new transaction.
-define(MIN, 1).

%% DELTA has the same meaning as in the clock-SI paper.
-define(DELTA, 10000).

-define(CLOCKSI_TIMEOUT, 1000).

-record(tx_id, {snapshot_time, server_pid :: pid(), client_pid :: pid(), txn_seq :: non_neg_integer()}).
-record(clocksi_payload, {key :: key(),
                          type :: type(),
                          op_param :: op(),
                          snapshot_time :: snapshot_time(),
                          commit_time :: commit_time(),
                          txid :: txid()}).
-record(transaction, {snapshot_time :: snapshot_time(),
                      server_pid :: pid(), 
                      vec_snapshot_time, 
                      txn_id :: txid()}).

%%---------------------------------------------------------------------
-type client_op() :: {update, {key(), type(), op()}} | {read, {key(), type()}} | {prepare, term()} | commit.
-type key() :: term().
-type op()  :: {term(), term()}.
-type crdt() :: term().
-type val() :: term().
-type reason() :: atom().
%%chash:index_as_int() is the same as riak_core_apl:index().
%%If it is changed in the future this should be fixed also.
-type index_node() :: {chash:index_as_int(), node()}.
-type preflist() :: riak_core_apl:preflist().
-type log() :: term().
-type op_id() :: {non_neg_integer(), node()}.
-type payload() :: term().
-type partition_id()  :: non_neg_integer().
-type log_id() :: [partition_id()].
-type type() :: atom().
-type snapshot() :: term().
-type snapshot_time() ::  vectorclock:vectorclock().
-type commit_time() ::  {dcid(), non_neg_integer()}.
-type txid() :: #tx_id{}.
-type clocksi_payload() :: #clocksi_payload{}.
-type dcid() :: term().
-type tx() :: #transaction{}.
-type dc_address():: {inet:ip_address(),inet:port_number()}.
-type cache_id() :: ets:tid().

-export_type([key/0, op/0, crdt/0, val/0, reason/0, preflist/0, log/0, op_id/0, payload/0, operation/0, partition_id/0, type/0, snapshot/0, txid/0, tx/0,
             dc_address/0]).

%%---------------------------------------------------------------------
%% @doc Data Type: state
%% where:
%%    from: the pid of the calling process.
%%    txid: transaction id handled by this fsm, as defined in src/antidote.hrl.
%%    updated_partitions: the partitions where update operations take place.
%%    num_to_ack: when sending prepare_commit,
%%                number of partitions that have acked.
%%    prepare_time: transaction prepare time.
%%    commit_time: transaction commit time.
%%    state: state of the transaction: {active|prepared|committing|committed}
%%----------------------------------------------------------------------

-record(tx_coord_state, {
      from :: {pid(), term()},
      transaction :: tx(),
      updated_partitions :: list(),
      num_to_ack :: non_neg_integer(),
      prepare_time :: non_neg_integer(),
      commit_time :: non_neg_integer(),
      commit_protocol :: term(),
      state :: active | prepared | committing | committed | undefined | aborted
         | committed_read_only,
      operations :: list(),
      read_set :: list(),
      is_static :: boolean(),
      full_commit :: boolean()}).
