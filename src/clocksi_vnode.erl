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
%% -------------------------------------------------------------------
-module(clocksi_vnode).
-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(NUM_VERSION, 10).
-define(SPECULA_THRESHOLD, 10).

-export([start_vnode/1,
	    read_data_item/3,
        debug_read/3,
	    relay_read/5,
        %set_prepared/5,
        get_table/1,
        clean_data/2,
        async_send_msg/3,

        set_debug/2,
        do_reply/2,
        debug_prepare/4,
        prepare/3,
        prepare/4,
        commit/3,
        single_commit/2,
        append_value/5,
        append_values/4,
        abort/2,

        init/1,
        terminate/2,
        handle_command/3,
        is_empty/1,
        delete/1,
    
        check_prepared/5]).

-export([
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).


-ignore_xref([start_vnode/1]).

%%---------------------------------------------------------------------
%% @doc Data Type: state
%%      where:
%%          partition: the partition that the vnode is responsible for.
%%          prepared_txs: a list of prepared transactions.
%%          committed_tx: a list of committed transactions.
%%          downstream_set: a list of the downstream operations that the
%%              transactions generate.
%%          write_set: a list of the write sets that the transactions
%%              generate.
%%----------------------------------------------------------------------
-record(state, {partition :: non_neg_integer(),
                prepared_txs :: cache_id(),
                committed_txs :: cache_id(),
                if_certify :: boolean(),
                if_replicate :: boolean(),
                if_specula :: boolean(),
                fast_reply :: boolean(),
                inmemory_store :: cache_id(),
                dep_dict :: dict(),
                %l_abort_dict :: dict(),
                %r_abort_dict :: dict(),
                %Statistics
                %max_ts :: non_neg_integer(),
                debug = false :: boolean()
                %total_time :: non_neg_integer(),
                %prepare_count :: non_neg_integer(),
		        %relay_read :: {non_neg_integer(), non_neg_integer()},
                %num_specula_read :: non_neg_integer(),
                %num_aborted :: non_neg_integer(),
                %num_blocked :: non_neg_integer(),
                %num_cert_fail :: non_neg_integer(),
                %blocked_time :: non_neg_integer(),
                %num_committed :: non_neg_integer()
                }).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

set_debug(Node, Debug) ->
    riak_core_vnode_master:sync_command(Node,
                           {set_debug, Debug},
                           ?CLOCKSI_MASTER, infinity).

do_reply(Node, TxId) ->
    riak_core_vnode_master:sync_command(Node,
                           {do_reply, TxId},
                           ?CLOCKSI_MASTER, infinity).

get_table(Node) ->
    riak_core_vnode_master:sync_command(Node,
                                   {get_table},
                                   ?CLOCKSI_MASTER, infinity).

%% @doc Sends a read request to the Node that is responsible for the Key
read_data_item(Node, Key, TxId) ->
    lager:warning("Trying to read ~w in node ~w", [Key, Node]),
    riak_core_vnode_master:sync_command(Node,
                                   {read, Key, TxId},
                                   ?CLOCKSI_MASTER, infinity).

debug_read(Node, Key, TxId) ->
    lager:warning("Trying to read ~w in node ~w", [Key, Node]),
    riak_core_vnode_master:sync_command(Node,
                                   {debug_read, Key, TxId},
                                   ?CLOCKSI_MASTER, infinity).

clean_data(Node, From) ->
    riak_core_vnode_master:command(Node,
                                   {clean_data, From}, self(),
                                   ?CLOCKSI_MASTER).

relay_read(Node, Key, TxId, Reader, From) ->
    riak_core_vnode_master:command(Node,
                                   {relay_read, Key, TxId, Reader, From}, self(),
                                   ?CLOCKSI_MASTER).

%% @doc Sends a prepare request to a Node involved in a tx identified by TxId
prepare(Updates, TxId, Type) ->
    ProposeTime = TxId#tx_id.snapshot_time+1,
    lists:foreach(fun({Node, WriteSet}) ->
        riak_core_vnode_master:command(Node,
                           {prepare, TxId, WriteSet, Type, ProposeTime},
                           self(),
                           ?CLOCKSI_MASTER)
    end, Updates).

prepare(Updates, CollectedTS, TxId, Type) ->
    ProposeTS = max(TxId#tx_id.snapshot_time+1, CollectedTS),
    lists:foreach(fun({Node, WriteSet}) ->
			riak_core_vnode_master:command(Node,
						       {prepare, TxId, WriteSet, Type, ProposeTS},
                               self(),
						       ?CLOCKSI_MASTER)
		end, Updates).

debug_prepare(Updates, TxId, Type, Sender) ->
    ProposeTime = TxId#tx_id.snapshot_time+1,
    lists:foreach(fun({Node, WriteSet}) ->
			riak_core_vnode_master:command(Node,
						       {prepare, TxId, WriteSet, Type, ProposeTime},
                               Sender,
						       ?CLOCKSI_MASTER)
		end, Updates).

%% @doc Sends prepare+commit to a single partition
%%      Called by a Tx coordinator when the tx only
%%      affects one partition
single_commit(Node, WriteSet) ->
    riak_core_vnode_master:sync_command(Node,
                                   {single_commit, WriteSet},
                                   ?CLOCKSI_MASTER, infinity).

append_value(Node, Key, Value, CommitTime, ToReply) ->
    riak_core_vnode_master:command(Node,
                                   {append_value, Key, Value, CommitTime},
                                   ToReply,
                                   ?CLOCKSI_MASTER).

append_values(Node, KeyValues, CommitTime, _ToReply) ->
    riak_core_vnode_master:sync_command(Node,
                                   {append_values, KeyValues, CommitTime},
                                   ?CLOCKSI_MASTER, infinity).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
commit(UpdatedParts, TxId, CommitTime) ->
    lists:foreach(fun(Node) ->
			riak_core_vnode_master:command(Node,
						       {commit, TxId, CommitTime},
						       {server, undefined, self()},
						       ?CLOCKSI_MASTER)
		end, UpdatedParts).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
abort(UpdatedParts, TxId) ->
    lists:foreach(fun(Node) ->
			riak_core_vnode_master:command(Node,
						       {abort, TxId},
						       {server, undefined, self()},
						       ?CLOCKSI_MASTER)
		end, UpdatedParts).

%% @doc Initializes all data structures that vnode needs to track information
%%      the transactions it participates on.
init([Partition]) ->
    lager:info("Initing partition ~w", [Partition]),
    PreparedTxs = tx_utilities:open_table(Partition, prepared),
    CommittedTxs = tx_utilities:open_table(Partition, committed),
    InMemoryStore = tx_utilities:open_table(Partition, inmemory_store),
    DepDict = dict:new(),
    %DepDict1 = dict:store(success_wait, 0, DepDict),
    DepDict1 = dict:store(commit_diff, {0,0}, DepDict),
    %DepDict3 = dict:store(fucked_by_commit, {0,0}, DepDict2),
    %DepDict4 = dict:store(fucked_by_badprep, {0,0}, DepDict3),
    IfReplicate = antidote_config:get(do_repl), 
    IfSpecula = antidote_config:get(do_specula), 
    FastReply = antidote_config:get(fast_reply),
    _ = case IfReplicate of
                    true ->
                        repl_fsm_sup:start_fsm(Partition);
                    false ->
                        ok
                end,
    %LD = dict:new(),
    %RD = dict:new(),
    {ok, #state{partition=Partition,
                committed_txs=CommittedTxs,
                prepared_txs=PreparedTxs,
		        %relay_read={0,0},
                %l_abort_dict=LD,
                %r_abort_dict=RD,
                if_replicate = IfReplicate,
                if_specula = IfSpecula,
                fast_reply = FastReply,
                inmemory_store=InMemoryStore,
                dep_dict = DepDict1
                %total_time = 0, 
                %prepare_count = 0, 
                %num_aborted = 0,
                %num_blocked = 0,
                %blocked_time = 0,
                %num_specula_read = 0,
                %num_cert_fail = 0,
                %num_committed = 0
                }}.

handle_command({set_debug, Debug},_Sender,SD0=#state{partition=_Partition}) ->
    %lager:info("~w: Setting debug to be ~w", [Partition, Debug]),
    {reply, ok, SD0#state{debug=Debug}};

handle_command({get_table}, _Sender, SD0=#state{inmemory_store=InMemoryStore}) ->
    {reply, InMemoryStore, SD0};

handle_command({verify_table, Repl},_Sender,SD0=#state{inmemory_store=InMemoryStore}) ->
    R = helper:handle_verify_table(Repl, InMemoryStore),
    {reply, R, SD0};

handle_command({clean_data, Sender}, _Sender, SD0=#state{inmemory_store=OldInMemoryStore,
        prepared_txs=OldPreparedTxs, committed_txs=OldCommittedTxs, partition=Partition}) ->
    ets:delete(OldInMemoryStore),
    ets:delete(OldPreparedTxs),
    ets:delete(OldCommittedTxs),
    PreparedTxs = tx_utilities:open_table(Partition, prepared),
    CommittedTxs = tx_utilities:open_table(Partition, committed),
    InMemoryStore = tx_utilities:open_table(Partition, inmemory_store),
    DepDict = dict:new(),
    %DepDict1 = dict:store(success_wait, 0, DepDict),
    DepDict1 = dict:store(commit_diff, {0,0}, DepDict),
    %DepDict3 = dict:store(fucked_by_commit, {0,0}, DepDict2),
    %DepDict4 = dict:store(fucked_by_badprep, {0,0}, DepDict3),
    %LD = dict:new(),
    %RD = dict:new(),
    Sender ! cleaned,
    {noreply, SD0#state{partition=Partition,
                committed_txs=CommittedTxs,
                prepared_txs=PreparedTxs, %relay_read={0,0},
                %l_abort_dict=LD,
                %r_abort_dict=RD,
                inmemory_store=InMemoryStore,
                dep_dict = DepDict1}};
                %max_ts=0, total_time = 0, prepare_count = 0, num_aborted = 0, num_blocked = 0,
                %blocked_time = 0,
                %num_specula_read = 0,
                %num_cert_fail = 0,
                %num_committed = 0}};

handle_command({relay_read_stat},_Sender,SD0) -> %=#state{relay_read=RelayRead}) ->
    %R = helper:handle_relay_read_stat(RelayRead),
    {reply, 0, SD0};

handle_command({num_specula_read},_Sender,SD0) -> %=#state{num_specula_read=NumSpeculaRead}) ->
    {reply, 0, SD0};

handle_command({check_key_record, Key, Type},_Sender,SD0=#state{prepared_txs=PreparedTxs, committed_txs=CommittedTxs}) ->
    R = helper:handle_check_key_record(Key, Type, PreparedTxs, CommittedTxs),
    {reply, R, SD0};

handle_command({check_top_aborted, _},_Sender,SD0=#state{dep_dict=DepDict}) -> %=#state{l_abort_dict=LAbortDict, r_abort_dict=RAbortDict, dep_dict=DepDict}) ->
    R = helper:handle_check_top_aborted(DepDict),
    {reply, R, SD0};

handle_command({do_reply, TxId}, _Sender, SD0=#state{prepared_txs=PreparedTxs, if_replicate=IfReplicate}) ->
    [{{pending, TxId}, Result}] = ets:lookup(PreparedTxs, {pending, TxId}),
    ets:delete(PreparedTxs, {pending, TxId}),
    case IfReplicate of
        true ->
            lager:warning("Start replicate ~w", [TxId]),
            {Sender, _RepMode, _WriteSet, PrepareTime} = Result,
            gen_server:cast(Sender, {prepared, TxId, PrepareTime}),
            {reply, ok, SD0};
        false ->
            lager:warning("Start replying ~w", [TxId]),
            {From, Reply} = Result,
            gen_server:cast(From, Reply),
            {reply, ok, SD0}
    end;

handle_command({if_prepared, TxId, Keys}, _Sender, SD0=#state{prepared_txs=PreparedTxs}) ->
    lager:warning("Checking if prepared of ~w for ~w", [TxId, Keys]),
    Result = helper:handle_if_prepared(TxId, Keys, PreparedTxs), 
    {reply, Result, SD0};

handle_command({check_tables_ready},_Sender,SD0=#state{partition=Partition}) ->
    Result = helper:handle_check_tables_ready(Partition), 
    {reply, Result, SD0};

handle_command({print_stat},_Sender,SD0=#state{partition=_Partition %num_aborted=NumAborted, blocked_time=BlockedTime,
                    %num_committed=NumCommitted, num_cert_fail=NumCertFail, num_blocked=NumBlocked, total_time=A6, prepare_count=A7}) ->
                        }) ->
    %R = helper:handle_print_stat(Partition, NumAborted, BlockedTime,
    %                NumCommitted, NumCertFail, NumBlocked, A6, A7),
    {reply, ok, SD0};
    
handle_command({check_prepared_empty},_Sender,SD0=#state{prepared_txs=PreparedTxs}) ->
    R = helper:handle_command_check_prepared_empty(PreparedTxs),
    {reply, R, SD0};

handle_command({check_servers_ready},_Sender,SD0) ->
    {reply, true, SD0};

handle_command({debug_read, Key, TxId}, _Sender, SD0=#state{
            inmemory_store=InMemoryStore, partition=_Partition}) ->
    %MaxTS1 = max(TxId#tx_id.snapshot_time, MaxTS), 
    %clock_service:update_ts(TxId#tx_id.snapshot_time),
    Result = read_value(Key, TxId, InMemoryStore),
    lager:warning("~w, Reading ~w value is ~w", [TxId, Key, Result]),
    {reply, Result, SD0};

handle_command({read, Key, TxId}, Sender, SD0=#state{%num_blocked=NumBlocked, 
            prepared_txs=PreparedTxs, inmemory_store=InMemoryStore, partition=_Partition}) ->
   %lager:info("Got read for ~w", [Key]),
    %clock_service:update_ts(TxId#tx_id.snapshot_time),
    case ready_or_block(TxId, Key, PreparedTxs, Sender) of
        not_ready->
            %lager:info("Not ready for ~w", [Key]),
            {noreply, SD0}; %num_blocked=NumBlocked+1
        ready ->
            Result = read_value(Key, TxId, InMemoryStore),
            %lager:info("Got value for ~w, ~w", [Key, Result]),
            {reply, Result, SD0}
    end;

handle_command({relay_read, Key, TxId, Reader, From}, _Sender, SD0=#state{
            prepared_txs=PreparedTxs, inmemory_store=InMemoryStore}) ->
    %{NumRR, AccRR} = RelayRead,
  %lager:error("~w relay read ~p", [TxId, Key]),
    %clock_service:update_ts(TxId#tx_id.snapshot_time),
    %T1 = os:timestamp(),
    case From of
        no_specula ->
            case ready_or_block(TxId, Key, PreparedTxs, {relay, Reader}) of
                not_ready->
                    {noreply, SD0};
                ready ->
                    Result = read_value(Key, TxId, InMemoryStore),
                    gen_server:reply(Reader, Result), 
    		        %T2 = os:timestamp(),
                    {noreply, SD0}%i, relay_read={NumRR+1, AccRR+get_time_diff(T1, T2)}}}
            end;
        specula ->
             lager:warning("Specula read!!"),
            case specula_read(TxId, Key, PreparedTxs, {relay, Reader}) of
                not_ready->
                    {noreply, SD0};
                {specula, Value} ->
                    gen_server:reply(Reader, {ok, Value}), 
    		        %T2 = os:timestamp(),
                  lager:warning("Specula read finished: ~w, ~p", [TxId, Key]),
                    {noreply, SD0};
				        %relay_read={NumRR+1, AccRR+get_time_diff(T1, T2)}}};
                ready ->
                    Result = read_value(Key, TxId, InMemoryStore),
                    gen_server:reply(Reader, Result), 
    		        %T2 = os:timestamp(),
                    {noreply, SD0}
				        %relay_read={NumRR+1, AccRR+get_time_diff(T1, T2)}}}
            end
    end;

handle_command({prepare, TxId, WriteSet, RepMode, ProposedTs}, RawSender,
               State = #state{partition=Partition,
                              if_replicate=IfReplicate,
                              committed_txs=CommittedTxs,
                              prepared_txs=PreparedTxs,
                              fast_reply=FastReply,
                              dep_dict=DepDict,
                              debug=Debug
                              }) ->
    Sender = case RawSender of {debug, RS} -> RS; _ -> RawSender end,
    Result = prepare(TxId, WriteSet, CommittedTxs, PreparedTxs, ProposedTs),
    case Result of
        {ok, PrepareTime} ->
            %UsedTime = tx_utilities:now_microsec() - PrepareTime,
             lager:warning("~w: ~w certification check prepred with ~w", [Partition, TxId, PrepareTime]),
            case IfReplicate of
                true ->
                    case Debug of
                        false ->
                            PendingRecord = {Sender, RepMode, WriteSet, PrepareTime},
                            gen_server:cast(Sender, {prepared, TxId, PrepareTime}),
                            repl_fsm:repl_prepare(Partition, prepared, TxId, PendingRecord),
                            {noreply, State};
                        true ->
                            PendingRecord = {Sender, RepMode, WriteSet, PrepareTime},
                            repl_fsm:repl_prepare(Partition, prepared, TxId, PendingRecord),
                    %        lager:warning("Inserting pending log for replicate and debug ~w:~w", [TxId, PendingRecord]),
                            ets:insert(PreparedTxs, {{pending, TxId}, PendingRecord}),
                            {noreply, State}
                    end;
                false ->
                    case Debug of
                        false ->
                            lager:warning("~w prepared with ~w", [TxId, PrepareTime]),
                            gen_server:cast(Sender, {prepared, TxId, PrepareTime}),
                            {noreply, State};
                        true ->
                            ets:insert(PreparedTxs, {{pending, TxId}, {Sender, {prepared, TxId, PrepareTime, RepMode}}}),
                            {noreply, State}
                    end 
            end;
        {wait, NumDeps, PrepareTime} ->
            NewDepDict = case (FastReply == true) and (RepMode == local) of
                                %% local_aggr
                        true ->  gen_server:cast(Sender, {pending_prepared, TxId, PrepareTime}),
                                 PendingRecord = {Sender, pending_prepared, WriteSet, PrepareTime},
                                 repl_fsm:repl_prepare(Partition, prepared, TxId, PendingRecord),
                                 dict:store(TxId, {NumDeps, PrepareTime, Sender, local_aggr}, DepDict);
                        false -> dict:store(TxId, {NumDeps, PrepareTime, Sender, RepMode}, DepDict)
                    end, 
            {noreply, State#state{dep_dict=NewDepDict}};
        {error, write_conflict} ->
            lager:warning("~w: ~w cerfify abort", [Partition, TxId]),
            case Debug of
                false ->
                    case RepMode of 
                        local ->
                            gen_server:cast(Sender, {aborted, TxId, {Partition, node()}}),
                    %        LAbortDict1 = case Reason of 
                    %                        {prep, Diff} -> dict:update(Key, fun({CAN, CAT, PAN, PAT}) -> {CAN, CAT, PAN+1, Diff+PAT} end, {0,0,1,Diff}, LAbortDict);
                    %                        {comm, Diff} -> dict:update(Key, fun({CAN, CAT, PAN, PAT}) -> {CAN+1, CAT+Diff, PAN, PAT} end, {1,Diff,0,0}, LAbortDict)
                    %                      end,
                            {noreply, State};
                        _ ->
                            gen_server:cast(Sender, {aborted, TxId, {Partition, node()}}),
                            %RAbortDict1 = case Reason of 
                            %                {prep, Diff} -> dict:update(Key, fun({CAN, CAT, PAN, PAT}) -> {CAN, CAT, PAN+1, Diff+PAT} end, {0,0,1,Diff}, RAbortDict);
                            %                {comm, Diff} -> dict:update(Key, fun({CAN, CAT, PAN, PAT}) -> {CAN+1, CAT+Diff, PAN, PAT} end, {1,Diff,0,0}, RAbortDict)
                            %              end,
                            {noreply, State}
                    end;
                true ->
                    ets:insert(PreparedTxs, {{pending, TxId}, {Sender, {aborted, TxId, RepMode}}}),
                    {noreply, State}
            end 
    end;

handle_command({single_commit, WriteSet}, Sender,
               State = #state{partition=Partition,
                              if_replicate=IfReplicate,
                              committed_txs=CommittedTxs,
                              prepared_txs=PreparedTxs,
                              inmemory_store=InMemoryStore
                              %num_cert_fail=NumCertFail,
                              %num_committed=NumCommitted
                              }) ->
    lager:warning("Got single commit for ~p", [WriteSet]),
    TxId = tx_utilities:create_tx_id(0),
    Result = prepare_and_commit(TxId, WriteSet, CommittedTxs, PreparedTxs, InMemoryStore), 
    case Result of
        {ok, {committed, CommitTime}} ->
            case IfReplicate of
                true ->
                    PendingRecord = {Sender, WriteSet, CommitTime},
                    lager:warning("Trying to replicate single commit for ~p", [TxId]),
                    %% Always fast reply in prepare
                    repl_fsm:repl_prepare(Partition, single_commit, TxId, PendingRecord),
                    {reply, {ok, {committed, CommitTime}}, State};
                false ->
                    %gen_server:cast(Sender, {committed, CommitTime}),
                    %Sender ! {ok, {committed, CommitTime}},
                    {reply, {ok, {committed, CommitTime}}, State}
            end;
        {error, write_conflict} ->
            gen_server:cast(Sender, {aborted, TxId, {Partition, node()}}),
            {noreply, State}
    end;

handle_command({append_value, Key, Value, CommitTime}, Sender,
               State = #state{committed_txs=CommittedTxs,
                              inmemory_store=InMemoryStore
                              }) ->
    ets:insert(CommittedTxs, {Key, CommitTime}),
    case ets:lookup(InMemoryStore, Key) of
        [] ->
            true = ets:insert(InMemoryStore, {Key, [{CommitTime, Value}]});
        [{Key, ValueList}] ->
            {RemainList, _} = lists:split(min(?NUM_VERSION,length(ValueList)), ValueList),
            true = ets:insert(InMemoryStore, {Key, [{CommitTime, Value}|RemainList]})
    end,
    gen_server:reply(Sender, {ok, {committed, CommitTime}}),
    {noreply, State};

handle_command({append_values, KeyValues, CommitTime}, _Sender,
               State = #state{committed_txs=CommittedTxs,
                              inmemory_store=InMemoryStore
                              }) ->
     %lager:info("Got msg from ~p", [ToReply]),
    lists:foreach(fun({Key, Value}) ->
            ets:insert(CommittedTxs, {Key, CommitTime}),
            true = ets:insert(InMemoryStore, {Key, [{CommitTime, Value}]})
            end, KeyValues),
     %lager:info("Sending msg back to ~p", [ToReply]),
    %gen_fsm:reply(ToReply, {ok, {committed, CommitTime}}),
    {reply, {ok, committed}, State};

handle_command({commit, TxId, TxCommitTime}, _Sender,
               #state{partition=Partition,
                      committed_txs=CommittedTxs,
                      if_replicate=IfReplicate,
                      prepared_txs=PreparedTxs,
                      inmemory_store=InMemoryStore,
                      dep_dict = DepDict,
                      %num_committed=NumCommitted,
                      if_specula=IfSpecula
                      } = State) ->
     lager:warning("~w: Got commit req for ~w with ~w", [Partition, TxId, TxCommitTime]),
    Result = 
        case IfReplicate of
            true ->
                commit(TxId, TxCommitTime, CommittedTxs, PreparedTxs, InMemoryStore, DepDict, Partition, IfSpecula);
            false -> 
                commit(TxId, TxCommitTime, CommittedTxs, PreparedTxs, InMemoryStore, DepDict, ignore, IfSpecula)
        end,
    case Result of
        {ok, committed, DepDict1} ->
            %case IfReplicate of
            %    true ->
            %        PendingRecord = {commit, Sender, 
            %            false, {TxId, TxCommitTime}},
                    %repl_fsm:replicate(Partition, {TxId, PendingRecord}),
            %        {noreply, State#state{
            %                num_committed=NumCommitted+1}};
            %    false ->
                    {noreply, State#state{dep_dict=DepDict1}};
            %end;
        {error, no_updates} ->
            {reply, no_tx_record, State}
    end;

handle_command({abort, TxId}, _Sender,
               State = #state{partition=Partition, prepared_txs=PreparedTxs, inmemory_store=InMemoryStore,
                dep_dict=DepDict, if_replicate=IfReplicate, if_specula=IfSpecula}) ->
   lager:warning("~w: Aborting ~w", [Partition, TxId]),
    case ets:lookup(PreparedTxs, TxId) of
        [{TxId, {waiting, WriteSet}}] ->
            Keys = [Key || {Key, _} <- WriteSet],
            case IfSpecula of
                true -> specula_utilities:deal_abort_deps(TxId);
                false -> ok 
            end,
            lager:warning("Found write set"),
            DepDict1 = dict:erase(TxId, DepDict),
            DepDict2 = case IfReplicate of
                    true ->
                        clean_abort_prepared(PreparedTxs, Keys, TxId, InMemoryStore, DepDict1, Partition);
                    false ->
                        clean_abort_prepared(PreparedTxs, Keys, TxId, InMemoryStore, DepDict1, ignore)
                end,
            true = ets:delete(PreparedTxs, TxId),
            {noreply, State#state{dep_dict=DepDict2}};
        [{TxId, Keys}] ->
            case IfSpecula of
                true -> specula_utilities:deal_abort_deps(TxId);
                false -> ok
            end,
            lager:warning("Found key set"),
            true = ets:delete(PreparedTxs, TxId),
            DepDict1 = case IfReplicate of
                        true ->
                            clean_abort_prepared(PreparedTxs, Keys, TxId, InMemoryStore, DepDict, Partition);
                        false ->
                            clean_abort_prepared(PreparedTxs, Keys, TxId, InMemoryStore, DepDict, ignore)
                    end,
            {noreply, State#state{dep_dict=DepDict1}};
        [] ->
              lager:warning("No key set at all for ~w", [TxId]),
            {noreply, State}
    end;

handle_command({start_read_servers}, _Sender, State) ->
    {reply, ok, State};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(StatName, Val) ->
    term_to_binary({StatName,Val}).

is_empty(State) ->
    {true,State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, #state{partition=Partition} = _State) ->
    ets:delete(tx_utilities:get_table_name(Partition,prepared)),
    ets:delete(tx_utilities:get_table_name(Partition,committed)),
    ets:delete(tx_utilities:get_table_name(Partition,inmemory_store)),
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================
async_send_msg(Delay, Msg, To) ->
    timer:sleep(Delay),
    riak_core_vnode_master:command(To, Msg, To, ?CLOCKSI_MASTER).

prepare(TxId, TxWriteSet, CommittedTxs, PreparedTxs, InitPrepTime)->
    KeySet = [K || {K, _} <- TxWriteSet],
    case certification_check(InitPrepTime, TxId, KeySet, CommittedTxs, PreparedTxs, 0) of
        false ->
            %lists:foreach(fun(K) -> ets:delete(PreparedTxs, K) end, InsertedKeys),
            %lists:foreach(fun(K) -> 
            %    case ets:lookup(PreparedTxs, K) of
            %        [{K, Record}] -> ets:insert(PreparedTxs, {K, droplast(Record)})
            %    end end, WaitingKeys),
	        {error, write_conflict};
        {0, PrepareTime} ->
             lager:warning("~w passed prepare with ~w", [TxId, PrepareTime]),
            lists:foreach(fun({K, V}) ->
                    ets:insert(PreparedTxs, {K, [{TxId, PrepareTime, PrepareTime, PrepareTime, V, []}]})
                    end, TxWriteSet),
            true = ets:insert(PreparedTxs, {TxId, KeySet}),
		    {ok, PrepareTime};
        {N, PrepareTime} ->
            lager:warning("~w passed but has ~w deps, prepare with ~w", [TxId, N, PrepareTime]),
		    %KeySet = [K || {K, _} <- TxWriteSet],  % set_prepared(PreparedTxs, TxWriteSet, TxId,PrepareTime, []),
            lists:foreach(fun({K, V}) ->
                          case ets:lookup(PreparedTxs, K) of
                          [] ->
                              ets:insert(PreparedTxs, {K, [{TxId, PrepareTime, PrepareTime, PrepareTime, V, []}]});
                          [{K, [{PrepTxId, OldPPTime, _, _, PrepValue, RWaiter}|PWaiter]}] ->
                                      ets:insert(PreparedTxs, {K, [{PrepTxId, OldPPTime, PrepareTime, PrepareTime, PrepValue, RWaiter}|
                                               (PWaiter++[{TxId, PrepareTime, V}])]});
                          _ -> ets:insert(PreparedTxs, {K, [{TxId, PrepareTime, PrepareTime, PrepareTime, V, []}]})
                          end
                    end, TxWriteSet),
            true = ets:insert(PreparedTxs, {TxId, {waiting, TxWriteSet}}),
            {wait, N, PrepareTime}
    end.

%prepare_and_commit(TxId, [{Key, Value}], CommittedTxs, _, InMemoryStore)->
%    SnapshotTime = TxId#tx_id.snapshot_time,
%    CommitTime = increment_ts(SnapshotTime, 0),
%    ets:insert(CommittedTxs, {Key, CommitTime}),
%    case ets:lookup(InMemoryStore, Key) of
%        [] ->
%            true = ets:insert(InMemoryStore, {Key, [{CommitTime, Value}]});
%        [{Key, ValueList}] ->
%            {RemainList, _} = lists:split(min(?NUM_VERSION,length(ValueList)), ValueList),
%            true = ets:insert(InMemoryStore, {Key, [{CommitTime, Value}|RemainList]})
%    end,
%    {ok, {committed, CommitTime}};
prepare_and_commit(TxId, [{Key, Value}], CommittedTxs, PreparedTxs, InMemoryStore)->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(CommittedTxs, Key) of
          [{Key, CommitTime}] ->
             lager:warning("~w: There is committed! ~w", [TxId, CommitTime]),
              case CommitTime > SnapshotTime of
                  true ->
                  lager:warning("~w: False because there is committed", [TxId]),
                    {error, write_conflict};
                  false ->
                      case ets:lookup(PreparedTxs, Key) of
                          [] ->
                            MyCommitTime = increment_ts(SnapshotTime, 0),
                            case ets:lookup(InMemoryStore, Key) of
                                [] ->
                                    true = ets:insert(InMemoryStore, {Key, [{MyCommitTime, Value}]});
                                [{Key, ValueList}] ->
                                    {RemainList, _} = lists:split(min(?NUM_VERSION,length(ValueList)), ValueList),
                                    true = ets:insert(InMemoryStore, {Key, [{MyCommitTime, Value}|RemainList]})
                            end,
                            true = ets:insert(CommittedTxs, {Key, MyCommitTime}),
                            {ok, {committed, MyCommitTime}};
                          _ ->
                            {error, write_conflict}
                      end
              end;
          [] ->
              case ets:lookup(PreparedTxs, Key) of
                  [] ->
                    MyCommitTime = increment_ts(TxId#tx_id.snapshot_time, 0),
                    case ets:lookup(InMemoryStore, Key) of
                        [] ->
                            true = ets:insert(InMemoryStore, {Key, [{MyCommitTime, Value}]});
                        [{Key, ValueList}] ->
                            {RemainList, _} = lists:split(min(?NUM_VERSION,length(ValueList)), ValueList),
                            true = ets:insert(InMemoryStore, {Key, [{MyCommitTime, Value}|RemainList]})
                    end,
                    true = ets:insert(CommittedTxs, {Key, MyCommitTime}),
                    {ok, {committed, MyCommitTime}};
                  _ ->
                    {error, write_conflict}
              end
      end.

%set_prepared(_PreparedTxs,[],_TxId,_Time, KeySet) ->
%    KeySet;
%set_prepared(PreparedTxs,[{Key, Value} | Rest],TxId,Time, KeySet) ->
%    true = ets:insert(PreparedTxs, {Key, {TxId, Time, Value, [], []}}),
%    set_prepared(PreparedTxs,Rest,TxId,Time, [Key|KeySet]).

commit(TxId, TxCommitTime, CommittedTxs, PreparedTxs, InMemoryStore, DepDict, Partition, IfSpecula)->
    lager:warning("Before commit ~w", [TxId]),
    case ets:lookup(PreparedTxs, TxId) of
        [{TxId, Keys}] ->
            case IfSpecula of
                true -> specula_utilities:deal_commit_deps(TxId, TxCommitTime);
                false -> ok
            end,
            lager:warning("Before update store!"),
            DepDict1 = update_store(Keys, TxId, TxCommitTime, InMemoryStore, CommittedTxs, PreparedTxs, DepDict, Partition, 0),
            lager:warning("After commit ~w", [TxId]),
            true = ets:delete(PreparedTxs, TxId),
            {ok, committed, DepDict1};
        [] ->
            lager:error("Prepared record of ~w has disappeared!", [TxId]),
            error
    end.

%% @doc clean_and_notify:
%%      This function is used for cleanning the state a transaction
%%      stores in the vnode while it is being procesed. Once a
%%      transaction commits or aborts, it is necessary to clean the 
%%      prepared record of a transaction T. There are three possibility
%%      when trying to clean a record:
%%      1. The record is prepared by T (with T's TxId).
%%          If T is being committed, this is the normal. If T is being 
%%          aborted, it means T successfully prepared here, but got 
%%          aborted somewhere else.
%%          In both cases, we should remove the record.
%%      2. The record is empty.
%%          This can only happen when T is being aborted. What can only
%%          only happen is as follows: when T tried to prepare, someone
%%          else has already prepared, which caused T to abort. Then 
%%          before the partition receives the abort message of T, the
%%          prepared transaction gets processed and the prepared record
%%          is removed.
%%          In this case, we don't need to do anything.
%%      3. The record is prepared by another transaction M.
%%          This can only happen when T is being aborted. We can not
%%          remove M's prepare record, so we should not do anything
%%          either. 
%%
clean_abort_prepared(_PreparedTxs, [], _TxId, _InMemoryStore, DepDict, _) ->
    DepDict; 
clean_abort_prepared(PreparedTxs, [Key | Rest], TxId, InMemoryStore, DepDict, Partition) ->
    MyNode = {Partition, node()},
    case ets:lookup(PreparedTxs, Key) of
        [{Key, [{TxId, _, LastReaderTime, LastPPTime, _, []}| PrepDeps]}] ->
			%% 0 for commit time means that the first prepared txs will just be prepared
            {PPTxId, Record, DepDict1} = deal_with_prepare_deps(PrepDeps, 0, DepDict, LastReaderTime, LastPPTime, MyNode),
            case PPTxId of
                ignore ->
					true = ets:insert(PreparedTxs, {Key, LastReaderTime}),
                    clean_abort_prepared(PreparedTxs,Rest,TxId, InMemoryStore, DepDict1, Partition);
                _ ->
                    DepDict2 = unblock_prepare(PPTxId, DepDict1, PreparedTxs, Partition),
					true = ets:insert(PreparedTxs, {Key, Record}),
					clean_abort_prepared(PreparedTxs,Rest,TxId, InMemoryStore, DepDict2, Partition)
            end;
        [{Key, [{TxId, _, LastReaderTime, LastPPTime, _, PendingReaders}|PrepDeps]}] ->
            lager:warning("Clean abort: for key ~p, readers are ~p, prep deps are ~w", [Key, PendingReaders, PrepDeps]),
			{PPTxId, Record, DepDict1} = deal_with_prepare_deps(PrepDeps, 0, DepDict, LastReaderTime, LastPPTime, MyNode),
            Value = case ets:lookup(InMemoryStore, Key) of
		                [{Key, ValueList}] ->
		                    {_, V} = hd(ValueList),
		                    V;
		                [] ->
							[]
            		end,
			case Record of
                ignore ->
					lists:foreach(fun({_, Sender}) -> 
								reply(Sender, {ok,Value}) end, PendingReaders),
					%true = ets:delete(PreparedTxs, Key),
                    true = ets:insert(PreparedTxs, {Key, LastReaderTime}),
                    clean_abort_prepared(PreparedTxs,Rest,TxId, InMemoryStore, DepDict1, Partition);
                [{PPTxId, PPTime, LastReaderTime, LastPPTime, PValue, []}|Remaining] ->
                    DepDict2 = unblock_prepare(PPTxId, DepDict1, PreparedTxs, Partition),
					StillPReaders = lists:foldl(fun({SnapshotTime, Sender}, PReaders) -> 
										case SnapshotTime >= PPTime of
											true -> [{SnapshotTime, Sender}|PReaders];
											false ->  reply(Sender, {ok,Value}), PReaders
										end end, [], PendingReaders),
					true = ets:insert(PreparedTxs, {Key, [{PPTxId, PPTime, LastReaderTime, LastPPTime, PValue, StillPReaders}|Remaining]}),
					clean_abort_prepared(PreparedTxs,Rest,TxId, InMemoryStore, DepDict2, Partition)
            end;
        [{Key, [Preped|PrepDeps]}] ->
            %% Make TxId invalid so the txn coord can notice this later. Instead of going to delete one by one in the list.
            PrepDeps1 = lists:keydelete(TxId, 1, PrepDeps), 
            lager:warning("~w for ~w is not prepared! PrepDeps are ~p, after is ~p", [Key, TxId, PrepDeps, PrepDeps]),
            ets:insert(PreparedTxs, {Key, [Preped|PrepDeps1]}),
            clean_abort_prepared(PreparedTxs,Rest,TxId, InMemoryStore, DepDict, Partition);
        _ ->
            clean_abort_prepared(PreparedTxs,Rest,TxId, InMemoryStore, DepDict, Partition)
    end.

certification_check(FinalPrepTime, _, [], _, _, NumBlocked) ->
    {NumBlocked, FinalPrepTime};
certification_check(PrepareTime, TxId, [Key|T], CommittedTxs, PreparedTxs, NumBlocked) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(CommittedTxs, Key) of
        [{Key, CommitTime}] ->
            case CommitTime > SnapshotTime of
                true ->
                     lager:warning("False for committed key ~p, Snapshot is ~w, diff with commit ~w", [Key, TxId#tx_id.snapshot_time, CommitTime-TxId#tx_id.snapshot_time]),
                    false;
                false ->
                    case check_prepared(whatever, TxId, PreparedTxs, Key, whatever) of
                        {true, NewPrepTime} ->
                            certification_check(max(NewPrepTime, PrepareTime), TxId, T, CommittedTxs, PreparedTxs, NumBlocked);
                        {wait, NewPrepTime} ->
                            certification_check(max(NewPrepTime, PrepareTime), TxId, T, CommittedTxs, PreparedTxs, NumBlocked+1);
                        false ->
                            false
                    end
            end;
        [] ->
            case check_prepared(whatever, TxId, PreparedTxs, Key, whatever) of
                {true, NewPrepTime} ->
                    certification_check(max(NewPrepTime, PrepareTime), TxId, T, CommittedTxs, PreparedTxs, NumBlocked); 
                {wait, NewPrepTime} ->
                    certification_check(max(NewPrepTime, PrepareTime), TxId, T, CommittedTxs, PreparedTxs, NumBlocked+1);
                false -> 
                     lager:warning("~w: False of prepared for ~p", [TxId, Key]),
                    false
            end
    end.

check_prepared(_, TxId, PreparedTxs, Key, _Value) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(PreparedTxs, Key) of
        [] ->
            %ets:insert(PreparedTxs, {Key, [{TxId, PPTime, PPTime, PPTime, Value, []}]}),
            {true, 1};
        [{Key, [{PrepTxId, PrepareTime, LastReaderTime, LastPPTime, _PrepValue, _RWaiter}|PWaiter]}] ->
            case LastPPTime > SnapshotTime of
                true ->
                     lager:warning("~w fail LastPPTime is ~w, last reader time is ~w", [TxId, LastPPTime, LastReaderTime]),
                    false;
                false ->
                    %ToPrepTime = max(LastReaderTime+1, PPTime),
                      lager:warning("~p: ~w waits for ~w with ~w, which is ~p", [Key, TxId, PrepTxId, PrepareTime, PWaiter]),
                    %ets:insert(PreparedTxs, {Key, [{PrepTxId, PrepareTime, ToPrepTime, PPTime, PrepValue, RWaiter}|
                    %         (PWaiter++[{TxId, ToPrepTime, Value}])]}),
                    {wait, LastReaderTime+1}
            end;
        [{Key, LastReaderTime}] ->
            %ToPrepTime = max(LastReaderTime+1, PPTime),
            %ets:insert(PreparedTxs, {Key, [{TxId, ToPrepTime, ToPrepTime, PPTime, Value, []}]}),
            {true, LastReaderTime+1}
    end.

-spec update_store(Keys :: [{key()}],
                          TxId::txid(),TxCommitTime:: {term(), term()},
                                InMemoryStore :: cache_id(), CommittedTxs :: cache_id(),
                                PreparedTxs :: cache_id(), DepDict :: dict(), 
                        Partition :: integer(), PrepareTime :: integer() ) -> ok.
update_store([], _TxId, TxCommitTime, _InMemoryStore, _CommittedTxs, _PreparedTxs, DepDict, _Partition, PrepareTime) ->
    %DepDict;
    dict:update(commit_diff, fun({Diff, Cnt}) -> {Diff+TxCommitTime-PrepareTime, Cnt+1} end, DepDict);
update_store([Key|Rest], TxId, TxCommitTime, InMemoryStore, CommittedTxs, PreparedTxs, DepDict, Partition, PTime) ->
      lager:warning("Trying to insert key ~p with for ~w, commit time is ~w", [Key, TxId, TxCommitTime]),
    MyNode = {Partition, node()},
    case ets:lookup(PreparedTxs, Key) of
        [{Key, [{TxId, PrepareTime, PrepareTime, LastPPTime, Value, []}|Deps] }] ->		
           lager:warning("Trying to insert key ~p with for ~w, preptime is ~w, diff with commit~w", [Key, TxId, PrepareTime, TxCommitTime-PrepareTime]),
           lager:warning("~w No pending reader! Waiter is ~p", [TxId, Deps]),
            case ets:lookup(InMemoryStore, Key) of
                [] ->
                    true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, Value}]});
                [{Key, ValueList}] ->
                    {RemainList, _} = lists:split(min(?NUM_VERSION,length(ValueList)), ValueList),
                    true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, Value}|RemainList]})
            end,
			true = ets:insert(CommittedTxs, {Key, TxCommitTime}),
            {PPTxId, Record, DepDict1} = deal_with_prepare_deps(Deps, TxCommitTime, DepDict, TxCommitTime, LastPPTime, MyNode),
            case PPTxId of
                ignore ->
                    lager:warning("No record!"),
					true = ets:insert(PreparedTxs, {Key, TxCommitTime}),
                    update_store(Rest, TxId, TxCommitTime, InMemoryStore, CommittedTxs, 
								 PreparedTxs, DepDict1, Partition, PrepareTime);
                _ ->
                    lager:warning("Record is ~p!", [Record]),
					true = ets:insert(PreparedTxs, {Key, Record}),
                    DepDict2 = unblock_prepare(PPTxId, DepDict1, PreparedTxs, Partition),
					update_store(Rest, TxId, TxCommitTime, InMemoryStore, CommittedTxs, 
						PreparedTxs, DepDict2, Partition, PrepareTime)
            end;
        [{Key, [{TxId, PrepareTime, LastReaderTime, LastPPTime, Value, PendingReaders}|Deps]}] ->
           lager:warning("Trying to insert key ~p with for ~w, preptime is ~w, diff with commit~w", [Key, TxId, PrepareTime, TxCommitTime-PrepareTime]),
           lager:warning("~w Pending readers are ~w! Pending writers are ~p", [TxId, PendingReaders, Deps]),
            ets:insert(CommittedTxs, {Key, TxCommitTime}),
            Values = case ets:lookup(InMemoryStore, Key) of
                        [] ->
                            true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, Value}]}),
                            [[], Value];
                        [{Key, ValueList}] ->
                            {RemainList, _} = lists:split(min(?NUM_VERSION,length(ValueList)), ValueList),
                            [{_CommitTime, First}|_] = RemainList,
                            true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, Value}|RemainList]}),
                            [First, Value]
                    end,
            {PPTxId, Record, DepDict1} = deal_with_prepare_deps(Deps, TxCommitTime, DepDict, LastReaderTime, LastPPTime, MyNode),
            case Record of
                ignore ->
                    %% Can safely reply value
                    lists:foreach(fun({SnapshotTime, Sender}) ->
                            case SnapshotTime >= TxCommitTime of
                                true ->
                                     %lager:info("Replying to ~w of second value", [Sender]),
                                    reply(Sender, {ok, lists:nth(2,Values)});
                                false ->
                                     %lager:info("Replying to ~w of first value", [Sender]),
                                    reply(Sender, {ok, hd(Values)})
                            end end,
                        PendingReaders),
                    true = ets:insert(PreparedTxs, {Key, max(TxCommitTime, LastReaderTime)}),
                    update_store(Rest, TxId, TxCommitTime, InMemoryStore, CommittedTxs, PreparedTxs, 
                        DepDict1, Partition, PrepareTime);
                [{PPTxId, PPTime, LLReaderTime, LastPPTime, PPValue, []}|Remaining] ->
                    DepDict2 = unblock_prepare(PPTxId, DepDict1, PreparedTxs, Partition),
                    NewPendingReaders = lists:foldl(fun({SnapshotTime, Sender}, PReaders) ->
                            case SnapshotTime >= TxCommitTime of
                                true ->
                                    %% Larger snapshot means that the read is not safe.
                                    case SnapshotTime >= PPTime of
                                        true ->
                                           lager:warning("Can not reply for ~w, pptime is ~w", [SnapshotTime, PPTime]),
                                            [{SnapshotTime, Sender}| PReaders];
                                        false ->
                                            reply(Sender, {ok, lists:nth(2,Values)}),
                                            PReaders
                                    end;
                                false ->
                                    reply(Sender, {ok, hd(Values)}),
                                    PReaders
                            end end,
                        [], PendingReaders),
                    true = ets:insert(PreparedTxs, {Key, [{PPTxId, PPTime, LLReaderTime, LastPPTime, PPValue, NewPendingReaders}|Remaining]}),
                    update_store(Rest, TxId, TxCommitTime, InMemoryStore, CommittedTxs, PreparedTxs, 
                        DepDict2, Partition, PrepareTime)
            end;
         [] ->
            %[{TxId, Keys}] = ets:lookup(PreparedTxs, TxId),
            lager:error("Something is wrong!!! A txn updated two same keys ~p!", [Key]),
            update_store(Rest, TxId, TxCommitTime, InMemoryStore, CommittedTxs, PreparedTxs, DepDict, Partition, PTime);
          R ->
            lager:error("Record is ~p", [R]),
            error
    end.

ready_or_block(TxId, Key, PreparedTxs, Sender) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(PreparedTxs, Key) of
        [] ->
            ets:insert(PreparedTxs, {Key, SnapshotTime}),
            ready;
        [{Key, [{PreparedTxId, PrepareTime, LastReaderTime, LastPPTime, Value, PendingReader}| PendingPrepare]}] ->
           lager:warning("~p Not ready.. ~w waits for ~w with ~w, others are ~w", [Key, TxId, PreparedTxId, PrepareTime, PendingReader]),
            case PrepareTime =< SnapshotTime of
                true ->
                    ets:insert(PreparedTxs, {Key, [{PreparedTxId, PrepareTime, LastReaderTime, LastPPTime, Value,
                        [{TxId#tx_id.snapshot_time, Sender}|PendingReader]}| PendingPrepare]}),
                    %lager:error("~w non_specula reads ~p is blocked by ~w! PrepareTime is ~w", [TxId, Key, PreparedTxId, PrepareTime]),
                    not_ready;
                false ->
                    ready
            end;
        [{Key, LastReaderTime}] ->
            ets:insert(PreparedTxs, {Key, max(SnapshotTime, LastReaderTime)}),
            ready
    end.

%% TODO: allowing all speculative read now! Maybe should not be so aggressive
specula_read(TxId, Key, PreparedTxs, Sender) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(PreparedTxs, Key) of
        [] ->
              lager:warning("Nothing prepared!!"),
            ets:insert(PreparedTxs, {Key, SnapshotTime}),
            ready;
        [{Key, [{PreparedTxId, PrepareTime, LastReaderTime, LastPPTime, Value, PendingReader}| PendingPrepare]}] ->
             lager:warning("~p Not ready.. ~w waits for ~w with ~w, lastpp time is ~w, others are ~w",[Key, TxId, PreparedTxId, PrepareTime, LastPPTime, PendingReader]),
            case PrepareTime =< SnapshotTime of
                true ->
                    %% The read is not ready, may read from speculative version 
                    Result =
                        find_appr_version(LastPPTime, SnapshotTime, PendingPrepare),
                    NextReaderTime = max(SnapshotTime, LastReaderTime),
                    lager:warning("Result is ~w", [Result]),
                    case Result of
                        first ->
                            %% There is more than one speculative version
                            case prepared_by_local(PreparedTxId) of
                                true ->
                                    add_read_dep(TxId, PreparedTxId, Key),
                                    case SnapshotTime > LastReaderTime of
                                        true -> ets:insert(PreparedTxs, [{Key, [{PreparedTxId, PrepareTime, SnapshotTime, LastPPTime, Value, PendingReader}| PendingPrepare]}]);
                                        false -> ok
                                    end,
                                    {specula, Value};
                                false ->
                                    ets:insert(PreparedTxs, {Key, [{PreparedTxId, PrepareTime, NextReaderTime, LastPPTime, Value,
                                        [{TxId#tx_id.snapshot_time, Sender}|PendingReader]}| PendingPrepare]}),
                                lager:warning("~w specula reads ~p is blocked by ~w ! PrepareTime is ~w", [TxId, Key, PreparedTxId, PrepareTime]),
                                    not_ready
                            end;
                        {ApprTxId, _, ApprPPValue} ->
                            %% There is more than one speculative version
                            case prepared_by_local(ApprTxId) of
                                true ->
                                    add_read_dep(TxId, ApprTxId, Key),
                                    case SnapshotTime > LastReaderTime of
                                        true -> ets:insert(PreparedTxs, [{Key, [{PreparedTxId, PrepareTime, SnapshotTime, LastPPTime, Value, PendingReader}| PendingPrepare]}]);
                                        false -> ok
                                    end,
                                    {specula, ApprPPValue};
                                false ->
                                    ets:insert(PreparedTxs, {Key, [{PreparedTxId, PrepareTime, NextReaderTime, LastPPTime, Value, [{TxId#tx_id.snapshot_time, Sender}|PendingReader]}| PendingPrepare]}),
                                lager:warning("~w specula reads ~p is blocked by ~w! PrepareTime is ~w", [TxId, Key, ApprTxId, PrepareTime]),
                                    not_ready
                            end;
                        not_ready ->
                            lager:warning("Wait as pending reader"),
                            ets:insert(PreparedTxs, {Key, [{PreparedTxId, PrepareTime, NextReaderTime, LastPPTime, Value,
                                [{TxId#tx_id.snapshot_time, Sender}|PendingReader]}| PendingPrepare]}),
                        lager:warning("~w specula reads ~p is blocked by whatever, maybe ~w! PrepareTime is ~w", [TxId, Key, PreparedTxId, PrepareTime]),
                            not_ready
                    end; 
                false ->
                    ready
            end;
        [{Key, LastReaderTime}] ->
            ets:insert(PreparedTxs, {Key, max(SnapshotTime, LastReaderTime)}),
            ready
    end.

add_read_dep(ReaderTx, WriterTx, Key) ->
    lager:warning("Inserting anti_dep from ~w to ~w for ~p", [ReaderTx, WriterTx, Key]),
    ets:insert(dependency, {WriterTx, ReaderTx}),
    ets:insert(anti_dep, {ReaderTx, WriterTx}).

%% Abort all transactions that can not prepare and return the record that should be inserted
deal_with_prepare_deps([], _, DepDict, _LastReaderTime, _LastPPTime, _) ->
    {ignore, ignore, DepDict};
deal_with_prepare_deps([{TxId, PPTime, Value}|PWaiter], TxCommitTime, DepDict, LastReaderTime, LastPPTime, MyNode) ->
      lager:warning("Dealing with ~p, ~p, commit time is ~w", [TxId, PPTime, TxCommitTime]),
    case TxCommitTime > TxId#tx_id.snapshot_time of
        true ->
            %% Abort the current transaction if it is not aborted yet.. (if dep dict still has its record)
            %% Check the preparedtxs table to delete all its inserted keys and reply abort to sender
            %% But the second step can be done by the coordinator..
            case dict:find(TxId, DepDict) of
                {ok, {_, _, Sender, _Type}} ->
                    NewDepDict = dict:erase(TxId, DepDict),
                      lager:warning("Prepare not valid anymore! For ~w, abort to ~w", [TxId, Sender]),
                    gen_server:cast(Sender, {aborted, TxId, MyNode}),
                    %% Abort should be fast, so send abort to myself directly.. Coord won't send abort to me again.
                    abort([MyNode], TxId),
                    %DepDict1 = dict:update(fucked_by_commit, fun({T, Cnt}) ->  {T+TxCommitTime-TxId#tx_id.snapshot_time, Cnt+1} end, DepDict),
                    deal_with_prepare_deps(PWaiter, TxCommitTime, NewDepDict, LastReaderTime, LastPPTime, MyNode);
                error ->
                %  lager:warning("Prepare not valid anymore! For ~w, but it's aborted already", [TxId]),
                    %specula_utilities:deal_abort_deps(TxId),
                    deal_with_prepare_deps(PWaiter, TxCommitTime, DepDict, LastReaderTime, LastPPTime, MyNode)
            end;
        false ->
            case dict:find(TxId, DepDict) of
                error ->
                %    %% This transaction has been aborted already.
                %    specula_utilities:deal_abort_deps(TxId),
                    deal_with_prepare_deps(PWaiter, TxCommitTime, DepDict, LastReaderTime, LastPPTime, MyNode);
                _ ->
                    {NewDepDict, Remaining} = abort_others(PPTime, PWaiter, DepDict, MyNode),
                    %lager:error("Returning record of ~w with prepare ~w, remaining is ~w, dep is ~w", [TxId, PPTime, Remaining, NewDepDict]),
                    {TxId, [{TxId, PPTime, LastReaderTime, LastPPTime, Value, []}|Remaining], NewDepDict}
            end
    end.

abort_others(_, [], DepDict, _MyNode) ->
    {DepDict, []};
abort_others(PPTime, [{TxId, _PTime, _Value}|Rest]=NonAborted, DepDict, MyNode) ->
    case PPTime > TxId#tx_id.snapshot_time of
        true ->
            case dict:find(TxId, DepDict) of
                {ok, {_, _, Sender, _}} ->
                    NewDepDict = dict:erase(TxId, DepDict),
                    gen_server:cast(Sender, {aborted, TxId, MyNode}),
                      lager:warning("Aborting ~w, because PPTime ~w is larger", [TxId, PPTime]),
                    abort([MyNode], TxId),
                    abort_others(PPTime, Rest, NewDepDict, MyNode);
                error ->
                    lager:warning("~w aborted already", [TxId]),
                    abort_others(PPTime, Rest, DepDict, MyNode)
            end;
        false ->
            {DepDict, NonAborted}
    end.
        
%% Update its entry in DepDict.. If the transaction can be prepared already, prepare it
%% (or just replicate it).. Otherwise just update and do nothing. 
unblock_prepare(TxId, DepDict, PreparedTxs, Partition) ->
  lager:warning("Unblocking transaction ~w", [TxId]),
    case dict:find(TxId, DepDict) of
        {ok, {1, PrepareTime, Sender, RepMode}} ->
            gen_server:cast(Sender, {prepared, TxId, PrepareTime}), 
            case Partition of
                ignore ->
                    ok;
                _ ->
                   lager:warning("~w unblocked, replicating writeset", [TxId]),
                    [{TxId, {waiting, WriteSet}}] = ets:lookup(PreparedTxs, TxId),
                    ets:insert(PreparedTxs, {TxId, [K|| {K, _} <-WriteSet]}),
                    case RepMode of
                        local_aggr -> ok;
                        _ -> 
                           %ning("Non local fast, Repmode is ~w", [RepMode]),
                            PendingRecord = {Sender, RepMode, WriteSet, PrepareTime},
                            repl_fsm:repl_prepare(Partition, prepared, TxId, PendingRecord) 
                    end
            end,
            DepDict1 = dict:update_counter(success_wait, 1, DepDict), 
            dict:erase(TxId, DepDict1);
        {ok, {N, PrepareTime, Sender, Type}} ->
            lager:warning("~w updates dep to ~w", [TxId, N-1]),
            dict:store(TxId, {N-1, PrepareTime, Sender, Type}, DepDict)
    end.  

%% @doc return:
%%  - Reads and returns the log of specified Key using replication layer.
read_value(Key, TxId, InMemoryStore) ->
    case ets:lookup(InMemoryStore, Key) of
        [] ->
             lager:warning("Nothing in store!!"),
            {ok, []};
        [{Key, ValueList}] ->
             lager:warning("Value list is ~p", [ValueList]),
            MyClock = TxId#tx_id.snapshot_time,
            find_version(ValueList, MyClock)
    end.

find_version([],  _SnapshotTime) ->
    {ok, []};
find_version([{TS, Value}|Rest], SnapshotTime) ->
    case SnapshotTime >= TS of
        true ->
            {ok, Value};
        false ->
            find_version(Rest, SnapshotTime)
    end.

reply({relay, Sender}, Result) ->
    lager:warning("Replying ~p to ~w", [Result, Sender]),
    gen_server:reply(Sender, Result);
reply(Sender, Result) ->
    riak_core_vnode:reply(Sender, Result).

increment_ts(SnapshotTS, MaxTS) ->
    max(SnapshotTS, MaxTS) + 1.

find_appr_version(LastPPTime, SnapshotTime, PendingPrepare) ->
    case PendingPrepare of
        [] ->
            first;
        _ ->
          lager:warning("LastPPTime is ~w, PendingPrepare is ~p, SnapshotTime is ~w", [LastPPTime, PendingPrepare, SnapshotTime]),
            case SnapshotTime >= LastPPTime + ?SPECULA_THRESHOLD of
                true ->
                    lists:last(PendingPrepare);
                false ->
                    find(SnapshotTime, PendingPrepare, first)
            end
    end.

find(SnapshotTime, [], ToReturn) ->
     lager:warning("Now in last version. Snapshot time is ~w, ToReturn is ~w", [SnapshotTime, ToReturn]),
    case ToReturn of
                first ->
                    first;
                {_, RTime, _} ->
                    case SnapshotTime >= RTime + ?SPECULA_THRESHOLD of
                        true ->
                            ToReturn;
                        false ->
                            not_ready
                    end
    end;
find(SnapshotTime, [{TxId, Time, Value}|Rest], ToReturn) ->
    case SnapshotTime < Time of
        true ->
            case ToReturn of
                first ->
                    first;
                {_, RTime, _} ->
                    case SnapshotTime >= RTime + ?SPECULA_THRESHOLD of
                        true ->
                            ToReturn;
                        false ->
                            not_ready
                    end
            end;
        _ ->
            find(SnapshotTime, Rest, {TxId, Time, Value}) 
    end.

%get_time_diff({A1, B1, C1}, {A2, B2, C2}) ->
%    ((A2-A1)*1000000+ (B2-B1))*1000000+ C2-C1.

prepared_by_local(TxId) ->
    node(TxId#tx_id.server_pid) == node().
