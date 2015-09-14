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
	    read_data_item/4,
	    async_read_data_item/4,
	    get_cache_name/2,
        set_prepared/4,
        async_send_msg/3,

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

        check_prepared/3,
	    check_tables_ready/0,
        check_prepared_empty/0,
        check_tables/0,
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

                specula_dep :: cache_id(),
                specula_timeout :: non_neg_integer(),

                total_time :: non_neg_integer(),
                prepare_count :: non_neg_integer(),
                num_committed :: non_neg_integer(),
                committed_diff :: non_neg_integer(),
                num_specula_read :: non_neg_integer(),
                num_aborted :: non_neg_integer(),
                num_read_abort :: non_neg_integer(),
                num_cert_fail :: non_neg_integer(),
                num_read_invalid :: non_neg_integer()}).

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
    InMemoryStore = open_table(Partition, inmemory_store),
    SpeculaDep = open_table(Partition, specula_dep),
    
    IfCertify = antidote_config:get(do_cert),
    IfReplicate = antidote_config:get(do_repl),
    SpeculaTimeout = antidote_config:get(specula_timeout),

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
                specula_dep=SpeculaDep,
                inmemory_store=InMemoryStore,
                specula_timeout=SpeculaTimeout,
                total_time=0, prepare_count=0, num_specula_read=0, committed_diff=0,
                num_committed=0, num_cert_fail=0, num_aborted=0, num_read_invalid=0, num_read_abort=0}}.


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
    print_stat(PartitionList, {0,0,0,0,0,0,0,0,0}).

print_stat([], {Acc1, Acc2, Acc3, Acc4, Acc5, Acc55, Acc56, Acc6, Acc7}) ->

    lager:info("In total: committed ~w, aborted ~w, cert fail ~w, read invalid ~w, read abort ~w, specula read ~w, avg diff ~w, prepare time ~w",
                [Acc1, Acc2, Acc3, Acc4, Acc5, Acc55, Acc56 div max(1,Acc1), Acc6 div max(1,Acc7)]),
    {Acc1, Acc2, Acc3, Acc4, Acc5, Acc55, Acc56, Acc6, Acc7};
print_stat([{Partition,Node}|Rest], {Acc1, Acc2, Acc3, Acc4, Acc5, Acc55, Acc56, Acc6, Acc7}) ->
    {Add1, Add2, Add3, Add4, Add5, Add55, Add56, Add6, Add7} = riak_core_vnode_master:sync_command({Partition,Node},
						            {print_stat},
						            ?CLOCKSI_MASTER,
						            infinity),
    print_stat(Rest, {Acc1+Add1, Acc2+Add2, Acc3+Add3, Acc4+Add4, Acc5+Add5, Acc55+Add55, Acc56+Add56, Acc6+Add6, Acc7+Add7}).

check_tables() ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    PartitionList = chashbin:to_list(CHBin),
    check_all_tables(PartitionList).

check_all_tables([]) ->
    ok;
check_all_tables([{Partition,Node}|Rest]) ->
    riak_core_vnode_master:sync_command({Partition,Node},
                 {check_tables_empty},
                 ?CLOCKSI_MASTER,
                 infinity),
    check_all_tables(Rest).


open_table(Partition, Name) ->
    try
	ets:new(get_cache_name(Partition,Name),
		[set,protected,named_table,?TABLE_CONCURRENCY])
    catch
	_:_Reason ->
	    %% Someone hasn't finished cleaning up yet
	    open_table(Partition, Name)
    end.

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

handle_command({check_tables_empty},_Sender,SD0=#state{specula_dep=S1, prepared_txs=S3,
            partition=Partition}) ->
    lager:warning("Partition ~w: Dep is ~w, Store is ~w, Prepared is ~w", [Partition, ets:tab2list(S1), ets:tab2list(S3)]),
    {reply, ok, SD0};

handle_command({print_stat},_Sender,SD0=#state{num_committed=A1, num_aborted=A2, num_cert_fail=A3, num_specula_read=A55, 
         committed_diff=A56, num_read_invalid=A4, num_read_abort=A5, total_time=A6, prepare_count=A7, partition=Partition}) ->
    lager:info("~w: committed ~w, aborted ~w, cert fail ~w, read invalid ~w, read abort ~w, num specularead ~w, avg diff ~w, ~w, ~w", [Partition, A1, A2, A3, A4, A5, A55, A56 div max(1, A1), A6, A7]),
    {reply, {A1, A2, A3, A4, A5, A55, A56, A6, A7}, SD0};

handle_command({check_tables_ready},_Sender,SD0=#state{partition=Partition}) ->
    Result = case ets:info(get_cache_name(Partition,prepared)) of
		 undefined ->
		     false;
		 _ ->
		     true
	     end,
    {reply, Result, SD0};
    
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

handle_command({read, Key, Type, TxId}, Sender, SD0=#state{num_specula_read=NumSpeculaRead,
            prepared_txs=PreparedTxs, inmemory_store=InMemoryStore, specula_timeout=SpeculaTimeout, 
            specula_dep=SpeculaDep, partition=Partition}) ->
    Tables = {PreparedTxs, InMemoryStore, SpeculaDep},
    clock_service:update_ts(TxId#tx_id.snapshot_time),
    case clocksi_readitem:check_prepared(Key, TxId, Tables, SpeculaTimeout) of
        {not_ready, Delay} ->
            spawn(clocksi_vnode, async_send_msg, [Delay, {async_read, Key, Type, TxId,
                         Sender}, {Partition, node()}]),
            {noreply, SD0};
        {specula, Value} ->
            {reply, {specula, Value}, SD0#state{num_specula_read=NumSpeculaRead+1}};
        ready ->
            Result = clocksi_readitem:return(Key, Type, TxId, InMemoryStore),
            {reply, Result, SD0}
    end;

handle_command({async_read, Key, Type, TxId, OrgSender}, _Sender,SD0=#state{num_specula_read=NumSpeculaRead,
            prepared_txs=PreparedTxs, inmemory_store=InMemoryStore, specula_timeout=SpeculaTimeout, 
            specula_dep=SpeculaDep, partition=Partition}) ->
    clock_service:update_ts(TxId#tx_id.snapshot_time),
    Tables = {PreparedTxs, InMemoryStore, SpeculaDep},
    case clocksi_readitem:check_prepared(Key, TxId, Tables, SpeculaTimeout) of
        {not_ready, Delay} ->
            spawn(clocksi_vnode, async_send_msg, [Delay, {async_read, Key, Type, TxId,
                         OrgSender}, {Partition, node()}]),
            {noreply, SD0};
        {specula, Value} ->
            riak_core_vnode:reply(OrgSender, {specula, Value}),
            {noreply, SD0#state{num_specula_read=NumSpeculaRead+1}};
        ready ->
            Result = clocksi_readitem:return(Key, Type, TxId, InMemoryStore),
            riak_core_vnode:reply(OrgSender, Result),
            {noreply, SD0}
    end;


handle_command({prepare, TxId, WriteSet, OriginalSender}, _Sender,
                              State=#state{
                               partition=Partition,
                              if_replicate=IfReplicate,
                              committed_tx=CommittedTx,
                              if_certify=IfCertify,
                              total_time=TotalTime,
                              prepare_count=PrepareCount,
                              prepared_txs=PreparedTxs,
                              num_cert_fail=NumCertFail
                              }) ->
    Result = prepare(TxId, WriteSet, CommittedTx, PreparedTxs, IfCertify),
    case Result of
        {ok, PrepareTime} ->
            lager:info("~w: PrepareTime is ~w", [TxId, PrepareTime]),
            UsedTime = now_microsec(erlang:now()) - PrepareTime,
            case IfReplicate of
                true ->
                    PendingRecord = {prepare, OriginalSender, 
                            {prepared, TxId, PrepareTime}, {TxId, WriteSet}},
                    repl_fsm:replicate(Partition, {TxId, PendingRecord}),
                    {noreply, State#state{total_time=TotalTime+UsedTime, prepare_count=PrepareCount+1}};
                false ->
                    riak_core_vnode:reply(OriginalSender, {prepared, TxId, PrepareTime}),
                    {noreply, State#state{total_time=TotalTime+UsedTime, prepare_count=PrepareCount+1}}
            end;
        {error, wait_more} ->
            spawn(clocksi_vnode, async_send_msg, [50, {prepare, TxId, 
                        WriteSet, OriginalSender}, {Partition, node()}]),
            {noreply, State};
        {error, write_conflict} ->
            riak_core_vnode:reply(OriginalSender, {abort, TxId}),
            {noreply, State#state{num_cert_fail=NumCertFail+1, prepare_count=PrepareCount+1}}
    end;

handle_command({single_commit, TxId, WriteSet, OriginalSender}, _Sender,
               State = #state{partition=Partition,
                              if_replicate=IfReplicate,
                              if_certify=IfCertify,
                              committed_tx=CommittedTx,
                              prepared_txs=PreparedTxs,
                              inmemory_store=InMemoryStore,
                              num_cert_fail=NumCertFail,
                              num_committed=NumCommitted,
                              specula_dep=SpeculaDep,
                              num_read_invalid=NumInvalid
                              }) ->
    Result = prepare(TxId, WriteSet, CommittedTx, PreparedTxs, IfCertify), 
    case Result of
        {ok, PrepareTime} ->
            ResultCommit = commit(TxId, PrepareTime, WriteSet, CommittedTx, 
                        InMemoryStore, PreparedTxs, SpeculaDep),
            case ResultCommit of
                {ok, {committed, _, NewCommittedTx}} ->
                    NewInvalid = specula_utilities:finalize_dependency(NumInvalid, TxId, 
                            PrepareTime, SpeculaDep, commit),
                    case IfReplicate of
                        true ->
                            PendingRecord = {commit, OriginalSender, 
                                {committed, PrepareTime}, {TxId, WriteSet}},
                            repl_fsm:replicate(Partition, {TxId, PendingRecord}),
                            {noreply, State#state{committed_tx=NewCommittedTx,
                                    num_committed=NumCommitted+1, num_read_invalid=NewInvalid}};
                        false ->
                            riak_core_vnode:reply(OriginalSender, {committed, PrepareTime}),
                            {reply, {committed, PrepareTime}, State#state{committed_tx=NewCommittedTx,
                                    num_committed=NumCommitted+1, num_read_invalid=NewInvalid}}
                        end;
                {error, no_updates} ->
                    riak_core_vnode:reply(OriginalSender, no_tx_record),
                    {noreply, State}
            end;
        {error, wait_more}->
            spawn(clocksi_vnode, async_send_msg, [50, {prepare, TxId, 
                        WriteSet, OriginalSender}, {Partition, node()}]),
            {noreply, State};
        {error, write_conflict} ->
            riak_core_vnode:reply(OriginalSender, abort),
            {noreply, State#state{num_cert_fail=NumCertFail+1}}
    end;

handle_command({commit, TxId, TxCommitTime, Updates}, Sender,
               #state{partition=Partition,
                      committed_tx=CommittedTx,
                      if_replicate=IfReplicate,
                      prepared_txs=PreparedTxs,
                      inmemory_store=InMemoryStore,
                      num_committed=NumCommitted,
                      committed_diff=CommittedDiff,
                      specula_dep=SpeculaDep,
                      num_read_invalid=NumInvalid
                      } = State) ->
    Result = commit(TxId, TxCommitTime, Updates, CommittedTx, InMemoryStore, PreparedTxs, SpeculaDep),
    case Result of
        {ok, {committed, Diff, NewCommittedTx}} ->
            NewInvalid = specula_utilities:finalize_dependency(NumInvalid, TxId, 
                    TxCommitTime, SpeculaDep, commit),
            case IfReplicate of
                true ->
                    PendingRecord = {commit, Sender, false, {TxId, TxCommitTime, Updates}},
                    repl_fsm:replicate(Partition, {TxId, PendingRecord}),
                    {noreply, State#state{committed_tx=NewCommittedTx, committed_diff=CommittedDiff+Diff, 
                                num_committed=NumCommitted+1, num_read_invalid=NewInvalid}};
                false ->
                    {noreply, State#state{committed_tx=NewCommittedTx, committed_diff=CommittedDiff+Diff, 
                                num_committed=NumCommitted+1, num_read_invalid=NewInvalid}}
            end;
        {error, no_updates} ->
            {reply, no_tx_record, State}
    end;

handle_command({abort, TxId, Updates}, _Sender, State=#state{prepared_txs=PreparedTxs, num_read_abort=NumRAbort,
                    num_aborted=NumAborted, specula_dep=SpeculaDep}) ->
    case Updates of
        [] ->
            {reply, {error, no_tx_record}, State};
        _ -> 
            clean_prepared(PreparedTxs, TxId, Updates),
            NewRAbort = specula_utilities:finalize_dependency(NumRAbort,TxId, ignore, SpeculaDep, abort),
            {noreply, State#state{num_aborted=NumAborted+1, num_read_abort=NewRAbort}}
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
            %PrepareTime = tx_utilities:increment_ts(TxId#tx_id.snapshot_time),
            PrepareTime = clock_service:increment_ts(TxId#tx_id.snapshot_time),
		    set_prepared(PreparedTxs, TxWriteSet, TxId, PrepareTime),
		    {ok, PrepareTime};
        specula_prepared ->
            %%lager:info("~w: Certification check returns specula_prepared", [TxId]),
            %PrepareTime = tx_utilities:increment_ts(TxId#tx_id.snapshot_time),
            PrepareTime = clock_service:increment_ts(TxId#tx_id.snapshot_time),
		    set_prepared(PreparedTxs, TxWriteSet, TxId, PrepareTime),
		    {specula_prepared, PrepareTime};
	    false ->
            %%lager:info("~w: write conflcit", [TxId]),
	        {error, write_conflict};
        wait ->
            {error,  wait_more}
    end.

set_prepared(_PreparedTxs, [], _TxId, _Time) ->
    ok;
set_prepared(PreparedTxs,[{Key, Type, Op}|Rest], TxId, Time) ->
    true = ets:insert(PreparedTxs, {Key, {TxId, Time, Type, Op}}),
    set_prepared(PreparedTxs,Rest,TxId, Time).

commit(TxId, TxCommitTime, Updates, CommittedTx, InMemoryStore, PreparedTxs, SpeculaDep)->
    case Updates of
        [{Key, _Type, _Value} | _Rest] -> 
            Diff = update_and_clean(Updates, TxId, TxCommitTime, InMemoryStore, 
                        PreparedTxs, SpeculaDep, 0),
            NewDict = dict:store(Key, TxCommitTime, CommittedTx),
            {ok, {committed, Diff, NewDict}};
        _ -> 
            {error, no_updates}
    end.


%% @doc clean_all_prepared:
%%      This function is used for cleanning the state a transaction
%%      stores in the vnode while it is being procesed. Once a
%%      transaction commits or aborts, it is necessary to:
%%      1. notify all read_fsms that are waiting for this transaction to finish
%%      2. clean the state of the transaction. Namely:
%%      a. ActiteTxsPerKey,
%%      b. PreparedTxs
%%
clean_prepared(_PreparedTxs, _TxId, []) ->
    ok;
clean_prepared(PreparedTxs, TxId, [{Key, _, _}|Rest]) ->
    case ets:lookup(PreparedTxs, Key) of
        [{Key, {TxId, _Time, _Type, _Op}}] ->
            ets:delete(PreparedTxs, Key);
        [{Key, {TxId, _Time, _SpeculaValue}}] ->
            ets:delete(PreparedTxs, Key);
        _ ->
            ok
    end,
    clean_prepared(PreparedTxs, TxId, Rest).


%% @doc converts a tuple {MegaSecs,Secs,MicroSecs} into microseconds
now_microsec({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.

%% @doc Performs a certification check when a transaction wants to move
%%      to the prepared state.
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
                    case check_prepared(TxId, Key, PreparedTxs) of
                        true ->
                            certification_check(TxId, T, CommittedTx, PreparedTxs, true);
                        false ->
                            false
                    end
            end;
        error ->
            case check_prepared(TxId, Key, PreparedTxs) of
                true ->
                    certification_check(TxId, T, CommittedTx, PreparedTxs, true); 
                false ->
                    false
            end
    end.

check_prepared(TxId, Key, PreparedTxs) -> 
    _SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(PreparedTxs, Key) of
        [] ->
            true;
        _ ->
            false
    end.


-spec update_and_clean(KeyValues :: [{key(), atom(), term()}],
                          TxId::txid(),TxCommitTime:: {term(), term()}, InMemoryStore :: cache_id(), 
                            PreparedTxs :: cache_id(), SpeculaDep :: cache_id(), PrepareTime :: non_neg_integer()) -> ok.
update_and_clean([], _TxId, TxCommitTime, _, _, _, PrepareTime) ->
    lager:info("Ct ~w, Pt ~w, diff ~w", [TxCommitTime, PrepareTime, TxCommitTime-PrepareTime]),
    TxCommitTime-PrepareTime;
update_and_clean([{Key, Type, {Param, Actor}}|Rest], TxId, TxCommitTime, InMemoryStore, 
                PreparedTxs, SpeculaDep, _) ->
    case ets:lookup(PreparedTxs, Key) of
        [{Key, {TxId, PrepareTime, _Type, _Op}}] ->
            case ets:lookup(InMemoryStore, Key) of
                [] ->
                    Init = Type:new(),
                    {ok, NewSnapshot} = Type:update(Param, Actor, Init),
                    true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, NewSnapshot}]});
                [{Key, ValueList}] ->
                    {RemainList, _} = lists:split(min(?NUM_VERSION,length(ValueList)), ValueList),
                    [{_CommitTime, First}|_] = RemainList,
                    {ok, NewSnapshot} = Type:update(Param, Actor, First),
                    true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, NewSnapshot}|RemainList]})
            end,
            ets:delete(PreparedTxs, Key),
            update_and_clean(Rest, TxId, TxCommitTime, InMemoryStore, PreparedTxs, SpeculaDep, PrepareTime);
        [{Key, {TxId, PrepareTime, SpeculaValue}}] ->
            ets:delete(PreparedTxs, Key),
            specula_utilities:make_specula_version_final(Key, TxCommitTime, SpeculaValue, InMemoryStore),
            update_and_clean(Rest, TxId, TxCommitTime, InMemoryStore, PreparedTxs, SpeculaDep, PrepareTime);
        Record ->
            lager:warning("My prepared record disappeard! Record is ~w", [Record]),
            0
    end.

