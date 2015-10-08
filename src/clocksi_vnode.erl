%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
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
%% -------------------------------------------------------------------
-module(clocksi_vnode).
-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(NUM_VERSION, 20).

-export([start_vnode/1,
	    read_data_item/3,
        set_prepared/4,
        async_send_msg/3,

        prepare/3,
        commit/3,
        single_commit/2,
        abort/2,

        init/1,
        terminate/2,
        handle_command/3,
        is_empty/1,
        delete/1,
    
        check_prepared/3,
	    check_tables_ready/0,
	    check_prepared_empty/0,
        print_stat/0]).

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
                inmemory_store :: cache_id(),
                %Statistics
                total_time :: non_neg_integer(),
                prepare_count :: non_neg_integer(),
                num_aborted :: non_neg_integer(),
                num_blocked :: non_neg_integer(),
                num_cert_fail :: non_neg_integer(),
                blocked_time :: non_neg_integer(),
                num_committed :: non_neg_integer()}).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Sends a read request to the Node that is responsible for the Key
read_data_item(Node, Key, TxId) ->
    riak_core_vnode_master:sync_command(Node,
                                   {read, Key, TxId},
                                   ?CLOCKSI_MASTER, infinity).

%% @doc Sends a prepare request to a Node involved in a tx identified by TxId
prepare(ListofNodes, TxId, Type) ->
    lists:foreach(fun({Node,WriteSet}) ->
			riak_core_vnode_master:command(Node,
						       {prepare, TxId, WriteSet, Type},
                               self(),
						       ?CLOCKSI_MASTER)
		end, ListofNodes).

%% @doc Sends prepare+commit to a single partition
%%      Called by a Tx coordinator when the tx only
%%      affects one partition
single_commit([{Node,WriteSet}], TxId) ->
    riak_core_vnode_master:command(Node,
                                   {single_commit, TxId,WriteSet},
                                   self(),
                                   ?CLOCKSI_MASTER).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
commit(ListofNodes, TxId, CommitTime) ->
    dict:fold(fun(Node,WriteSet,_) ->
			riak_core_vnode_master:command(Node,
						       {commit, TxId, CommitTime, WriteSet},
						       self(),
						       ?CLOCKSI_MASTER)
		end, ok, ListofNodes).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
abort(ListofNodes, TxId) ->
    dict:fold(fun(Node,WriteSet,_) ->
			riak_core_vnode_master:command(Node,
						       {abort, TxId, WriteSet},
						       {server, undefined, self()},
						       ?CLOCKSI_MASTER)
		end, ok, ListofNodes).

%% @doc Initializes all data structures that vnode needs to track information
%%      the transactions it participates on.
init([Partition]) ->
    PreparedTxs = tx_utilities:open_table(Partition, prepared),
    CommittedTxs = tx_utilities:open_table(Partition, committed),
    InMemoryStore = tx_utilities:open_table(Partition, inmemory_store),

    IfCertify = antidote_config:get(do_cert),
    IfReplicate = antidote_config:get(do_repl),

    _ = case IfReplicate of
                    true ->
                        repl_fsm_sup:start_fsm(Partition);
                    false ->
                        ok
                end,

    {ok, #state{partition=Partition,
                committed_txs=CommittedTxs,
                prepared_txs=PreparedTxs,
                if_certify = IfCertify,
                if_replicate = IfReplicate,
                inmemory_store=InMemoryStore,
                total_time = 0, 
                prepare_count = 0, 
                num_aborted = 0,
                num_blocked = 0,
                blocked_time = 0,
                num_cert_fail = 0,
                num_committed = 0}}.

check_tables_ready() ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    PartitionList = chashbin:to_list(CHBin),
    check_table_ready(PartitionList).

check_table_ready([]) ->
    true;
check_table_ready([{Partition,Node}|Rest]) ->
    Result = riak_core_vnode_master:sync_command({Partition,Node},
						 {check_tables_ready},
						 ?CLOCKSI_MASTER,
						 infinity),
    case Result of
	true ->
	    check_table_ready(Rest);
	false ->
	    false
    end.

print_stat() ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    PartitionList = chashbin:to_list(CHBin),
    print_stat(PartitionList, {0,0,0,0,0,0,0}).

print_stat([], {CommitAcc, AbortAcc, CertFailAcc, BlockedAcc, TimeAcc, CntAcc, BlockedTime}) ->
    lager:info("Total number committed is ~w, total number aborted is ~w, cer fail is ~w, num blocked is ~w,Avg time is ~w, Avg blocked time is ~w", [CommitAcc, AbortAcc, CertFailAcc, BlockedAcc, TimeAcc div max(1,CntAcc), BlockedTime div max(1,BlockedAcc)]),
    {CommitAcc, AbortAcc, CertFailAcc, BlockedAcc, TimeAcc, CntAcc, BlockedTime};
print_stat([{Partition,Node}|Rest], {CommitAcc, AbortAcc, CertFailAcc, BlockedAcc, TimeAcc, CntAcc, BlockedTime}) ->
    {Commit, Abort, Cert, BlockedA, TimeA, CntA, BlockedTimeA} = riak_core_vnode_master:sync_command({Partition,Node},
						 {print_stat},
						 ?CLOCKSI_MASTER,
						 infinity),
	print_stat(Rest, {CommitAcc+Commit, AbortAcc+Abort, CertFailAcc+Cert, BlockedAcc+BlockedA, TimeAcc+TimeA, CntAcc+CntA, BlockedTimeA+BlockedTime}).

check_prepared_empty() ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    PartitionList = chashbin:to_list(CHBin),
    check_prepared_empty(PartitionList).

check_prepared_empty([]) ->
    ok;
check_prepared_empty([{Partition,Node}|Rest]) ->
    Result = riak_core_vnode_master:sync_command({Partition,Node},
						 {check_prepared_empty},
						 ?CLOCKSI_MASTER,
						 infinity),
    case Result of
	    true ->
            ok;
	    false ->
            lager:warning("Prepared not empty!")
    end,
	check_prepared_empty(Rest).

handle_command({check_tables_ready},_Sender,SD0=#state{partition=Partition}) ->
    Result = case ets:info(tx_utilities:get_table_name(Partition,prepared)) of
		 undefined ->
		     false;
		 _ ->
		     true
	     end,
    {reply, Result, SD0};

handle_command({print_stat},_Sender,SD0=#state{partition=Partition, num_aborted=NumAborted, blocked_time=BlockedTime,
                    num_committed=NumCommitted, num_cert_fail=NumCertFail, num_blocked=NumBlocked, total_time=A6, prepare_count=A7}) ->
    lager:info("~w: committed is ~w, aborted is ~w, num cert fail ~w, num blocked ~w, avg blocked time ~w",[Partition, 
            NumCommitted, NumAborted, NumCertFail, NumBlocked, BlockedTime div max(1,NumBlocked)]),
    {reply, {NumCommitted, NumAborted, NumCertFail, NumBlocked, A6, A7, BlockedTime}, SD0};
    
handle_command({check_prepared_empty},_Sender,SD0=#state{prepared_txs=PreparedTxs}) ->
    PreparedList = ets:tab2list(PreparedTxs),
    case length(PreparedList) of
		 0 ->
            {reply, true, SD0};
		 _ ->
            lager:warning("Not empty!! ~w", [PreparedList]),
            {reply, false, SD0}
    end;

handle_command({check_servers_ready},_Sender,SD0) ->
    {reply, true, SD0};

handle_command({read, Key, TxId}, Sender, SD0=#state{num_blocked=NumBlocked,
            prepared_txs=PreparedTxs, inmemory_store=InMemoryStore}) ->
    clock_service:update_ts(TxId#tx_id.snapshot_time),
    case ready_or_block(TxId, Key, PreparedTxs, Sender) of
        not_ready->
            {noreply, SD0#state{num_blocked=NumBlocked+1}};
        ready ->
            Result = read_value(Key, TxId, InMemoryStore),
            {reply, Result, SD0}
    end;

handle_command({prepare, TxId, WriteSet, Type}, Sender,
               State = #state{partition=Partition,
                              if_replicate=IfReplicate,
                              committed_txs=CommittedTxs,
                              if_certify=IfCertify,
                              total_time=TotalTime,
                              prepare_count=PrepareCount,
                              num_cert_fail=NumCertFail,
                              prepared_txs=PreparedTxs
                              }) ->
    %lager:info("Got prep req for ~w", [TxId]),
    Result = prepare(TxId, WriteSet, CommittedTxs, PreparedTxs, IfCertify),
    case Result of
        {ok, PrepareTime} ->
            UsedTime = tx_utilities:now_microsec() - PrepareTime,
            case IfReplicate of
                true ->
                    PendingRecord = {prepare, Sender, 
                            {prepared, TxId, PrepareTime, Type}, {TxId, WriteSet}},
                    repl_fsm:replicate(Partition, {TxId, PendingRecord}),
                    {noreply, State#state{total_time=TotalTime+UsedTime, prepare_count=PrepareCount+1}};
                false ->
                    %riak_core_vnode:reply(OriginalSender, {prepared, TxId, PrepareTime, Type}),
                    %lager:info("~w done, reply", [TxId]),
                    gen_server:cast(Sender, {prepared, TxId, PrepareTime, Type}),
                    {noreply,  
                    State#state{total_time=TotalTime+UsedTime, prepare_count=PrepareCount+1}} 
            end;
        {error, write_conflict} ->
            %lager:info("~w done, abort", [TxId]),
            gen_server:cast(Sender, {abort, TxId, Type}),
            {noreply, State#state{num_cert_fail=NumCertFail+1, prepare_count=PrepareCount+1}}
    end;

handle_command({single_commit, TxId, WriteSet}, Sender,
               State = #state{partition=Partition,
                              if_replicate=IfReplicate,
                              if_certify=IfCertify,
                              committed_txs=CommittedTxs,
                              prepared_txs=PreparedTxs,
                              inmemory_store=InMemoryStore,
                              num_cert_fail=NumCertFail,
                              num_committed=NumCommitted
                              }) ->
    Result = prepare_and_commit(TxId, WriteSet, CommittedTxs, PreparedTxs, InMemoryStore, IfCertify), 
    case Result of
        {ok, {committed, CommitTime}} ->
            case IfReplicate of
                true ->
                    PendingRecord = {commit, Sender, 
                        {committed, CommitTime}, {TxId, WriteSet}},
                    repl_fsm:replicate(Partition, {TxId, PendingRecord}),
                    {noreply, State#state{ 
                            num_committed=NumCommitted+1}};
                false ->
                    gen_server:cast(Sender, {committed, CommitTime}),
                    {noreply, State#state{
                            num_committed=NumCommitted+1}}
            end;
        {error, write_conflict} ->
            gen_server:cast(Sender, {abort, TxId}),
            {noreply, State#state{num_cert_fail=NumCertFail+1}}
    end;

handle_command({commit, TxId, TxCommitTime, Updates}, Sender,
               #state{partition=Partition,
                      committed_txs=CommittedTxs,
                      if_replicate=IfReplicate,
                      prepared_txs=PreparedTxs,
                      inmemory_store=InMemoryStore,
                      num_committed=NumCommitted
                      } = State) ->
    %lager:info("Got commit req for ~w", [TxId]),
    Result = commit(TxId, TxCommitTime, Updates, CommittedTxs, PreparedTxs, InMemoryStore),
    case Result of
        {ok, committed} ->
            case IfReplicate of
                true ->
                    PendingRecord = {commit, Sender, 
                        false, {TxId, TxCommitTime, Updates}},
                    repl_fsm:replicate(Partition, {TxId, PendingRecord}),
                    {noreply, State#state{
                            num_committed=NumCommitted+1}};
                false ->
                    {noreply, State#state{
                            num_committed=NumCommitted+1}}
            end;
        {error, no_updates} ->
            {reply, no_tx_record, State}
    end;

handle_command({abort, TxId, Updates}, _Sender,
               #state{partition=_Partition, prepared_txs=PreparedTxs, inmemory_store=InMemoryStore,
                num_aborted=NumAborted} = State) ->
    case Updates of
        [] ->
            {reply, {error, no_tx_record}, State};
        _ -> 
            clean_abort_prepared(PreparedTxs,Updates,TxId,InMemoryStore),
            {noreply, State#state{num_aborted=NumAborted+1}}
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

prepare(TxId, TxWriteSet, CommittedTxs, PreparedTxs, IfCertify)->
    case certification_check(TxId, TxWriteSet, CommittedTxs, PreparedTxs, IfCertify) of
        true ->
            PrepareTime = clock_service:increment_ts(TxId#tx_id.snapshot_time),
		    set_prepared(PreparedTxs, TxWriteSet, TxId,PrepareTime),
		    {ok, PrepareTime};
	    false ->
	        {error, write_conflict};
        wait ->
            {error,  wait_more}
    end.

prepare_and_commit(TxId, TxWriteSet, CommittedTxs, PreparedTxs, InMemoryStore, IfCertify)->
    case certification_check(TxId, TxWriteSet, CommittedTxs, PreparedTxs, IfCertify) of
        true ->
            CommitTime = clock_service:increment_ts(TxId#tx_id.snapshot_time),
            update_store(TxWriteSet, TxId, CommitTime, InMemoryStore, CommittedTxs, PreparedTxs),
            {ok, {committed, CommitTime}};
	    false ->
	        {error, write_conflict};
        wait ->
            {error,  wait_more}
    end.

set_prepared(_PreparedTxs,[],_TxId,_Time) ->
    ok;
set_prepared(PreparedTxs,[{Key, _Value} | Rest],TxId,Time) ->
    true = ets:insert(PreparedTxs, {Key, {TxId, Time, []}}),
    set_prepared(PreparedTxs,Rest,TxId,Time).

commit(TxId, TxCommitTime, Updates, CommittedTxs, 
                                PreparedTxs, InMemoryStore)->
    update_store(Updates, TxId, TxCommitTime, InMemoryStore, CommittedTxs, PreparedTxs),
    {ok, committed}.

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
clean_abort_prepared(_PreparedTxs,[],_TxId,_InMemoryStore) ->
    ok;
clean_abort_prepared(PreparedTxs,[{Key, _Value} | Rest],TxId, InMemoryStore) ->
    case ets:lookup(PreparedTxs, Key) of
        [{Key, {TxId, _Time, PendingReaders}}] ->
            case ets:lookup(InMemoryStore, Key) of
                [{Key, ValueList}] ->
                    {_, Value} = hd(ValueList),
                    lists:foreach(fun(Sender) -> riak_core_vnode:reply(Sender, {ok,Value}) end, PendingReaders);
                [] ->
                    lists:foreach(fun(Sender) -> riak_core_vnode:reply(Sender, {ok,nil}) end, PendingReaders)
            end,
            true = ets:delete(PreparedTxs, Key);
        _ ->
            ok
    end,
    clean_abort_prepared(PreparedTxs,Rest,TxId, InMemoryStore).

%% @doc Performs a certification check when a transaction wants to move
%%      to the prepared state.
certification_check(_, _, _, _, false) ->
    true;
certification_check(_, [], _, _, true) ->
    true;
certification_check(TxId, [H|T], CommittedTxs, PreparedTxs, true) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    {Key, _Value} = H,
    case ets:lookup(CommittedTxs, Key) of
        [{Key, CommitTime}] ->
            case CommitTime > SnapshotTime of
                true ->
                    false;
                false ->
                    case check_prepared(TxId, PreparedTxs, Key) of
                        true ->
                            certification_check(TxId, T, CommittedTxs, PreparedTxs, true);
                        false ->
                            false
                    end
            end;
        [] ->
            case check_prepared(TxId, PreparedTxs, Key) of
                true ->
                    certification_check(TxId, T, CommittedTxs, PreparedTxs, true); 
                false ->
                    false
            end
    end.

check_prepared(TxId, PreparedTxs, Key) ->
    _SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(PreparedTxs, Key) of
        [] ->
            true;
        _ ->
            false
    end.

-spec update_store(KeyValues :: [{key(), term()}],
                          TxId::txid(),TxCommitTime:: {term(), term()},
                                InMemoryStore :: cache_id(), CommittedTxs :: cache_id(),
                                PreparedTxs :: cache_id()) -> ok.
update_store([], _TxId, _TxCommitTime, _InMemoryStore, _CommittedTxs, _PreparedTxs) ->
    ok;
update_store([{Key, Value}|Rest], TxId, TxCommitTime, InMemoryStore, CommittedTxs, PreparedTxs) ->
    Values= case ets:lookup(InMemoryStore, Key) of
                [] ->
                    true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, Value}]}),
                    [[], Value];
                [{Key, ValueList}] ->
                    {RemainList, _} = lists:split(min(?NUM_VERSION,length(ValueList)), ValueList),
                    [{_CommitTime, First}|_] = RemainList,
                    true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, Value}|RemainList]}),
                    [First, Value]
            end,
    case ets:lookup(PreparedTxs, Key) of
        [{Key, {TxId, _Time, PendingReaders}}] ->
            lists:foreach(fun({SnapshotTime, Sender}) ->
                    case SnapshotTime >= TxCommitTime of
                        true ->
                            riak_core_vnode:reply(Sender, {ok, lists:nth(2,Values)});
                        false ->
                            riak_core_vnode:reply(Sender, {ok, hd(Values)})
                    end end,
                PendingReaders),
            true = ets:delete(PreparedTxs, Key);
         [] ->
            ok
    end,
    ets:insert(CommittedTxs, {Key, TxCommitTime}),
    update_store(Rest, TxId, TxCommitTime, InMemoryStore, CommittedTxs, PreparedTxs).

ready_or_block(TxId, Key, PreparedTxs, Sender) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(PreparedTxs, Key) of
        [] ->
            ready;
        [{Key, {PreparedTxId, PrepareTime, PendingReader}}] ->
            case PrepareTime =< SnapshotTime of
                true ->
                    ets:insert(PreparedTxs, {Key, {PreparedTxId, PrepareTime,
                        [{TxId#tx_id.snapshot_time, Sender}|PendingReader]}}),
                    not_ready;
                false ->
                    ready
            end
    end.

%% @doc return:
%%  - Reads and returns the log of specified Key using replication layer.
read_value(Key, TxId, InMemoryStore) ->
    case ets:lookup(InMemoryStore, Key) of
        [] ->
            {ok, []};
        [{Key, ValueList}] ->
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
