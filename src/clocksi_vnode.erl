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

-export([start_vnode/1,
	    read_data_item/4,
	    async_read_data_item/4,
	    get_cache_name/2,
        set_prepared/4,
        async_send_msg/3,
        update_store/4,

        check_prepared/3,
        certification_check/5,
        prepare/2,
        commit/3,
        single_commit/2,
        abort/2,
        now_microsec/1,
        init/1,
        terminate/2,
        handle_command/3,
        is_empty/1,
        delete/1,
        open_table/2,
    
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
                committed_tx :: dict(),
                if_certify :: boolean(),
                if_replicate :: boolean(),
                inmemory_store :: cache_id(),
                %Statistics
                total_time :: non_neg_integer(),
                prepare_count :: non_neg_integer(),
                num_aborted :: non_neg_integer(),
                num_cert_fail :: non_neg_integer(),
                num_committed :: non_neg_integer()}).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Sends a read request to the Node that is responsible for the Key
read_data_item(Node, Key, Type, TxId) ->
    riak_core_vnode_master:sync_command(Node,
                                   {read, Key, Type, TxId},
                                   ?CLOCKSI_MASTER, infinity).

%% @doc Sends a read request to the Node that is responsible for the Key
async_read_data_item(Node, Key, Type, TxId) ->
    Self = {fsm, undefined, self()},
    riak_core_vnode_master:command(Node,
                                   {async_read, Key, Type, TxId, Self},
                                   Self,
                                   ?CLOCKSI_MASTER).

%% @doc Sends a prepare request to a Node involved in a tx identified by TxId
prepare(ListofNodes, TxId) ->
    Self = {fsm, undefined, self()},
    dict:fold(fun(Node,WriteSet,_Acc) ->
			riak_core_vnode_master:command(Node,
						       {prepare, TxId,WriteSet, Self},
                                Self,
						       ?CLOCKSI_MASTER)
		end, ok, ListofNodes).


%% @doc Sends prepare+commit to a single partition
%%      Called by a Tx coordinator when the tx only
%%      affects one partition
single_commit([{Node,WriteSet}], TxId) ->
    Self = {fsm, undefined, self()},
    riak_core_vnode_master:command(Node,
                                   {single_commit, TxId,WriteSet, Self},
                                   Self,
                                   ?CLOCKSI_MASTER).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
commit(ListofNodes, TxId, CommitTime) ->
    dict:fold(fun(Node,WriteSet,_Acc) ->
			riak_core_vnode_master:command(Node,
						       {commit, TxId, CommitTime, WriteSet},
						       {fsm, undefined, self()},
						       ?CLOCKSI_MASTER)
		end, ok, ListofNodes).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
abort(ListofNodes, TxId) ->
    dict:fold(fun(Node,WriteSet,_Acc) ->
			riak_core_vnode_master:command(Node,
						       {abort, TxId, WriteSet},
						       {fsm, undefined, self()},
						       ?CLOCKSI_MASTER)
		end, ok, ListofNodes).


get_cache_name(Partition,Base) ->
    list_to_atom(atom_to_list(Base) ++ "-" ++ integer_to_list(Partition)).


%% @doc Initializes all data structures that vnode needs to track information
%%      the transactions it participates on.
init([Partition]) ->
    PreparedTxs = open_table(Partition, prepared),
    CommittedTx = dict:new(),
    %%true = ets:insert(PreparedTxs, {committed_tx, dict:new()}),
    InMemoryStore = open_table(Partition, inmemory_store),

    IfCertify = antidote_config:get(do_cert),
    IfReplicate = antidote_config:get(do_repl),

    _ = case IfReplicate of
                    true ->
                        repl_fsm_sup:start_fsm(Partition);
                    false ->
                        ok
                end,

    {ok, #state{partition=Partition,
                committed_tx=CommittedTx,
                prepared_txs=PreparedTxs,
                if_certify = IfCertify,
                if_replicate = IfReplicate,
                inmemory_store=InMemoryStore,
                total_time = 0, 
                prepare_count = 0, 
                num_aborted = 0,
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
    print_stat(PartitionList, {0,0,0,0,0}).

print_stat([], {CommitAcc, AbortAcc, CertFailAcc, TimeAcc, CntAcc}) ->
    lager:info("Total number committed is ~w, total number aborted is ~w, cer fail is ~w, Avg time is ~w",
                [CommitAcc, AbortAcc, CertFailAcc, TimeAcc div CntAcc]);
print_stat([{Partition,Node}|Rest], {CommitAcc, AbortAcc, CertFailAcc, TimeAcc, CntAcc}) ->
    {Commit, Abort, Cert, TimeA, CntA} = riak_core_vnode_master:sync_command({Partition,Node},
						 {print_stat},
						 ?CLOCKSI_MASTER,
						 infinity),
	print_stat(Rest, {CommitAcc+Commit, AbortAcc+Abort, CertFailAcc+Cert, TimeAcc+TimeA, CntAcc+CntA}).

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
            lager:info("Prepared not empty!")
    end,
	check_prepared_empty(Rest).

open_table(Partition, Name) ->
    try
	ets:new(get_cache_name(Partition,Name),
		[set,protected,named_table,?TABLE_CONCURRENCY])
    catch
	_:_Reason ->
	    %% Someone hasn't finished cleaning up yet
	    open_table(Partition, Name)
    end.

handle_command({check_tables_ready},_Sender,SD0=#state{partition=Partition}) ->
    Result = case ets:info(get_cache_name(Partition,prepared)) of
		 undefined ->
		     false;
		 _ ->
		     true
	     end,
    {reply, Result, SD0};

handle_command({print_stat},_Sender,SD0=#state{partition=Partition, num_aborted=NumAborted,
                    num_committed=NumCommitted, num_cert_fail=NumCertFail, total_time=A6, prepare_count=A7}) ->
    lager:info("~w: committed is ~w, aborted is ~w",[Partition, 
            NumCommitted, NumAborted, NumCertFail]),
    {reply, {NumCommitted, NumAborted, NumCertFail, A6, A7}, SD0};
    
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

handle_command({read, Key, Type, TxId}, Sender, SD0=#state{
            prepared_txs=PreparedTxs, inmemory_store=InMemoryStore, partition=Partition}) ->
    %tx_utilities:update_ts(TxId#tx_id.snapshot_time),
    %case clocksi_readitem:check_prepared(Key, TxId, PreparedTxs) of
    case clocksi_readitem:check_clock(Key, TxId, PreparedTxs) of
        {not_ready, Delay} ->
            spawn(clocksi_vnode, async_send_msg, [Delay, {async_read, Key, Type, TxId,
                         Sender}, {Partition, node()}]),
            %lager:info("Not ready for key ~w ~w, reader is ~w",[Key, TxId, Sender]),
            {noreply, SD0};
        ready ->
            Result = clocksi_readitem:return(Key, Type, TxId, InMemoryStore),
            {reply, Result, SD0}
    end;


handle_command({async_read, Key, Type, TxId, OrgSender}, _Sender,SD0=#state{
            prepared_txs=PreparedTxs, inmemory_store=InMemoryStore, partition=Partition}) ->
    %lager:info("Got async read request for key ~w of tx ~w",[Key, TxId]),
    %tx_utilities:update_ts(TxId#tx_id.snapshot_time),
    case clocksi_readitem:check_clock(Key, TxId, PreparedTxs) of
        {not_ready, Delay} ->
            spawn(clocksi_vnode, async_send_msg, [Delay, {async_read, Key, Type, TxId,
                         OrgSender}, {Partition, node()}]),
            {noreply, SD0};
        ready ->
            Result = clocksi_readitem:return(Key, Type, TxId, InMemoryStore),
            riak_core_vnode:reply(OrgSender, Result),
            {noreply, SD0}
    end;


handle_command({prepare, TxId, WriteSet, OriginalSender}, _Sender,
               State = #state{partition=Partition,
                              if_replicate=IfReplicate,
                              committed_tx=CommittedTx,
                              if_certify=IfCertify,
                              total_time=TotalTime,
                              prepare_count=PrepareCount,
                              num_cert_fail=NumCertFail,
                              prepared_txs=PreparedTxs
                              }) ->
    %[{committed_tx, CommittedTx}] = ets:lookup(PreparedTxs, committed_tx),
    Result = prepare(TxId, WriteSet, CommittedTx, PreparedTxs, IfCertify),
    case Result of
        {ok, PrepareTime} ->
            UsedTime = now_microsec(erlang:now()) - PrepareTime,
            case IfReplicate of
                true ->
                    PendingRecord = {prepare, OriginalSender, 
                            {prepared, TxId, PrepareTime}, {TxId, WriteSet}},
                    repl_fsm:replicate(Partition, {TxId, PendingRecord}),
                    {noreply, State};
                false ->
                    riak_core_vnode:reply(OriginalSender, {prepared, TxId, PrepareTime}),
                    {noreply, State#state{total_time=TotalTime+UsedTime, prepare_count=PrepareCount+1}} 
            end;
        {error, wait_more} ->
            spawn(clocksi_vnode, async_send_msg, [2, {prepare, TxId, 
                        WriteSet, OriginalSender}, {Partition, node()}]),
            {noreply, State};
        {error, write_conflict} ->
            riak_core_vnode:reply(OriginalSender, {abort, TxId}),
            %gen_fsm:send_event(OriginalSender, abort),
            {noreply, State#state{num_cert_fail=NumCertFail+1, prepare_count=PrepareCount+1}}
    end;

handle_command({single_commit, TxId, WriteSet, OriginalSender}, _Sender,
               State = #state{partition=Partition,
                              if_replicate=IfReplicate,
                              if_certify=IfCertify,
                              committed_tx=CommittedTx,
                              prepared_txs=PreparedTxs,
                              num_cert_fail=NumCertFail,
                              num_committed=NumCommitted
                              }) ->
    %[{committed_tx, CommittedTx}] = ets:lookup(PreparedTxs, committed_tx),
    Result = prepare(TxId, WriteSet, CommittedTx, PreparedTxs, IfCertify), 
    case Result of
        {ok, PrepareTime} ->
            ResultCommit = commit(TxId, PrepareTime, WriteSet, CommittedTx, State),
            case ResultCommit of
                {ok, {committed, NewCommittedTx}} ->
                    case IfReplicate of
                        true ->
                            PendingRecord = {commit, OriginalSender, 
                                {committed, PrepareTime}, {TxId, WriteSet}},
                            %ets:insert(PreparedTxs, {committed_tx, NewCommittedTx}),
                            repl_fsm:replicate(Partition, {TxId, PendingRecord}),
                            %{noreply, State};
                            {noreply, State#state{committed_tx=NewCommittedTx, 
                                    num_committed=NumCommitted+1}};
                        false ->
                            %ets:insert(PreparedTxs, {committed_tx, NewCommittedTx}),
                            riak_core_vnode:reply(OriginalSender, {committed, PrepareTime}),
                            %{noreply, State}
                            {noreply, State#state{committed_tx=NewCommittedTx,
                                    num_committed=NumCommitted+1}}
                        end;
                {error, no_updates} ->
                    %gen_fsm:send_event(OriginalSender, no_tx_record),
                    riak_core_vnode:reply(OriginalSender, no_tx_record),
                    {noreply, State}
            end;
        {error, wait_more}->
            spawn(clocksi_vnode, async_send_msg, [2, {prepare, TxId, 
                        WriteSet, OriginalSender}, {Partition, node()}]),
            {noreply, State};
        {error, write_conflict} ->
            riak_core_vnode:reply(OriginalSender, {abort, TxId}),
            %gen_fsm:send_event(OriginalSender, abort),
            {noreply, State#state{num_cert_fail=NumCertFail+1}}
    end;

%% TODO: sending empty writeset to clocksi_downstream_generatro
%% Just a workaround, need to delete downstream_generator_vnode
%% eventually.
handle_command({commit, TxId, TxCommitTime, Updates}, Sender,
               #state{partition=Partition,
                      committed_tx=CommittedTx,
                      if_replicate=IfReplicate,
                      num_committed=NumCommitted
                      } = State) ->
    %[{committed_tx, CommittedTx}] = ets:lookup(PreparedTxs, committed_tx),
    Result = commit(TxId, TxCommitTime, Updates, CommittedTx, State),
    case Result of
        {ok, {committed,NewCommittedTx}} ->
            case IfReplicate of
                true ->
                    PendingRecord = {commit, Sender, 
                        false, {TxId, TxCommitTime, Updates}},
                    %ets:insert(PreparedTxs, {committed_tx, NewCommittedTx}),
                    repl_fsm:replicate(Partition, {TxId, PendingRecord}),
                    %{noreply, State};
                    {noreply, State#state{committed_tx=NewCommittedTx,
                            num_committed=NumCommitted+1}};
                false ->
                    %%ets:insert(PreparedTxs, {committed_tx, NewCommittedTx}),
                    %{reply, committed, State}
                    {noreply, State#state{committed_tx=NewCommittedTx,
                            num_committed=NumCommitted+1}}
            end;
        %{error, materializer_failure} ->
        %    {reply, {error, materializer_failure}, State};
        %{error, timeout} ->
        %    {reply, {error, timeout}, State};
        {error, no_updates} ->
            {reply, no_tx_record, State}
    end;

handle_command({abort, TxId, Updates}, _Sender,
               #state{partition=_Partition, num_aborted=NumAborted} = State) ->
    case Updates of
        [] ->
            {reply, {error, no_tx_record}, State};
        _ -> 
            clean_and_notify(TxId, Updates, State),
            {noreply, State#state{num_aborted=NumAborted+1}}
            %%{reply, ack_abort, State};
    end;

%% @doc Return active transactions in prepare state with their preparetime
handle_command({get_active_txns}, _Sender,
               #state{prepared_txs=Prepared} = State) ->
    ActiveTxs = ets:lookup(Prepared, active),
    {reply, {ok, ActiveTxs}, State};

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
    ets:delete(get_cache_name(Partition,prepared)),
    ets:delete(get_cache_name(Partition,inmemory_store)),
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================
async_send_msg(Delay, Msg, To) ->
    timer:sleep(Delay),
    riak_core_vnode_master:command(To, Msg, To, ?CLOCKSI_MASTER).

prepare(TxId, TxWriteSet, CommittedTx, PreparedTxs, IfCertify)->
    case certification_check(TxId, TxWriteSet, CommittedTx, PreparedTxs, IfCertify) of
        true ->
            %case TxWriteSet of 
            %    [{Key, Type, Op} | Rest] -> 

            %PrepareTime = tx_utilities:increment_ts(TxId#tx_id.snapshot_time),
            PrepareTime = clocksi_vnode:now_microsec(now()),
		    set_prepared(PreparedTxs, TxWriteSet, TxId,PrepareTime),

		    %LogRecord = #log_record{tx_id=TxId,
			%		    op_type=prepare,
			%		    op_payload=NewPrepare},
            %        LogId = log_utilities:get_logid_from_key(Key),
            %        [Node] = log_utilities:get_preflist_from_key(Key),
		    %        NewUpdates = write_set_to_logrecord(TxId,TxWriteSet),
            %        Result = logging_vnode:append_group(Node,LogId,NewUpdates ++ [LogRecord]),
		    {ok, PrepareTime};
	    false ->
	        {error, write_conflict};
        wait ->
            {error,  wait_more}
    end.


set_prepared(_PreparedTxs,[],_TxId,_Time) ->
    ok;
set_prepared(PreparedTxs,[{Key, _Type, _Op} | Rest],TxId,Time) ->
    true = ets:insert(PreparedTxs, {Key, {TxId, Time}}),
    set_prepared(PreparedTxs,Rest,TxId,Time).

commit(TxId, TxCommitTime, Updates, CommittedTx, 
                                State=#state{inmemory_store=InMemoryStore})->
    case Updates of
        [{Key, _Type, _Value} | _Rest] -> 
            update_store(Updates, TxId, TxCommitTime, InMemoryStore),
            NewDict = dict:store(Key, TxCommitTime, CommittedTx),
            clean_and_notify(TxId,Updates,State),
            {ok, {committed, NewDict}};
        _ -> 
            {error, no_updates}
    end.




%% @doc clean_and_notify:
%%      This function is used for cleanning the state a transaction
%%      stores in the vnode while it is being procesed. Once a
%%      transaction commits or aborts, it is necessary to:
%%      1. notify all read_fsms that are waiting for this transaction to finish
%%      2. clean the state of the transaction. Namely:
%%      a. ActiteTxsPerKey,
%%      b. PreparedTxs
%%
clean_and_notify(TxId, Updates, #state{
			      prepared_txs=PreparedTxs}) ->
    clean_prepared(PreparedTxs,Updates,TxId).


clean_prepared(_PreparedTxs,[],_TxId) ->
    ok;
clean_prepared(PreparedTxs,[{Key, _Type, _Op} | Rest],TxId) ->
    case ets:lookup(PreparedTxs, Key) of
        [{Key, {TxId, _Time}}] ->
            true = ets:delete(PreparedTxs, Key);
        _ ->
            ok
    end,   
    clean_prepared(PreparedTxs,Rest,TxId).


%% @doc converts a tuple {MegaSecs,Secs,MicroSecs} into microseconds
now_microsec({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.

%% @doc Performs a certification check when a transaction wants to move
%%      to the prepared state.
%certification_check(_TxId, _H, _CommittedTx, _PreparedTxs) ->
%    true.
certification_check(_, _, _, _, false) ->
    true;
certification_check(_, [], _, _, true) ->
    true;
certification_check(TxId, [H|T], CommittedTx, PreparedTxs, true) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    {Key, _Type, _} = H,
    case dict:find(Key, CommittedTx) of
        {ok, CommitTime} ->
            case CommitTime > SnapshotTime of
                true ->
                    false;
                false ->
                    case check_prepared(TxId, PreparedTxs, Key) of
                        true ->
                            certification_check(TxId, T, CommittedTx, PreparedTxs, true);
                        false ->
                            false;
                        wait ->
                            wait
                    end
            end;
        error ->
            case check_prepared(TxId, PreparedTxs, Key) of
                true ->
                    certification_check(TxId, T, CommittedTx, PreparedTxs, true); 
                false ->
                    false;
                wait ->
                    wait
            end
    end.

check_prepared(TxId, PreparedTxs, Key) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(PreparedTxs, Key) of
        [] ->
            true;
        [{Key, {_TxId1, PrepareTime}}] ->
            case PrepareTime > SnapshotTime of
                true ->
                    %lager:info("Has to abort for Key ~w", [Key]),
                    false;
                false ->
                    %case random:uniform(2) of
                    %    1 ->
                            false
                    %    2 ->
                    %        wait  
                    %end
            end
    end.

%% check_keylog(_, [], _) ->
%%     false;
%% check_keylog(TxId, [H|T], CommittedTx)->
%%     {_Key, _Type, ThisTxId}=H,
%%     case ThisTxId > TxId of
%%         true ->
%%             CommitInfo = ets:lookup(CommittedTx, ThisTxId),
%%             case CommitInfo of
%%                 [{_, _CommitTime}] ->
%%                     true;
%%                 [] ->
%%                     check_keylog(TxId, T, CommittedTx)
%%             end;
%%         false ->
%%             check_keylog(TxId, T, CommittedTx)
%%     end.

-spec update_store(KeyValues :: [{key(), atom(), term()}],
                          TxId::txid(),TxCommitTime:: {term(), term()},
                                InMemoryStore :: cache_id()) -> ok.
update_store([], _TxId, _TxCommitTime, _InMemoryStore) ->
    ok;
update_store([{Key, Type, {Param, Actor}}|Rest], TxId, TxCommitTime, InMemoryStore) ->
    %lager:info("Store ~w",[InMemoryStore]),
    case ets:lookup(InMemoryStore, Key) of
        [] ->
     %       lager:info("Wrote ~w to key ~w",[Value, Key]),
            Init = Type:new(),
            {ok, NewSnapshot} = Type:update(Param, Actor, Init),
            %lager:info("Updateing store for key ~w, value ~w firstly", [Key, Type:value(NewSnapshot)]),
            true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, NewSnapshot}]}), 
            update_store(Rest, TxId, TxCommitTime, InMemoryStore);
        [{Key, ValueList}] ->
      %      lager:info("Wrote ~w to key ~w with ~w",[Value, Key, ValueList]),
            {RemainList, _} = lists:split(min(20,length(ValueList)), ValueList),
            [{_CommitTime, First}|_] = RemainList,
            {ok, NewSnapshot} = Type:update(Param, Actor, First),
            %lager:info("Updateing store for key ~w, value ~w", [Key, Type:value(NewSnapshot)]),
            true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, NewSnapshot}|RemainList]}),
            update_store(Rest, TxId, TxCommitTime, InMemoryStore)
    end,
    ok.

