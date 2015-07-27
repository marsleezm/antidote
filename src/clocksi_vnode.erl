%%
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
        check_prepared_empty/0,
        check_tables/0,
        prepare/2,
        commit/3,
        async_send_msg/2,
        single_commit/2,
        abort/2,

        set_prepared/5,
        now_microsec/1,
        init/1,
        terminate/2,
        handle_command/3,
        is_empty/1,
        delete/1,
        open_table/2,
	    check_tables_ready/0]).

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
                quorum :: non_neg_integer(),
                specula_store :: cache_id(),
                specula_dep :: cache_id(),
                inmemory_store :: cache_id()}).

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
    SpeculaStore = open_table(Partition, specula_store),
    SpeculaDep = open_table(Partition, specula_dep),
    
    IfCertify = antidote_config:get(do_cert),
    IfReplicate = antidote_config:get(do_repl),
    Quorum = antidote_config:get(quorum),

    _ = case IfReplicate of
                    true ->
                        repl_fsm_sup:start_fsm(Partition);
                    false ->
                        ok
                end,

    {ok, #state{partition=Partition,
                committed_tx=CommittedTx,
                prepared_txs=PreparedTxs,
                quorum = Quorum,
                if_certify = IfCertify,
                if_replicate = IfReplicate,
                specula_store=SpeculaStore,
                specula_dep=SpeculaDep,
                inmemory_store=InMemoryStore}}.


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
            lager:wanring("Prepared not empty!")
    end,
	check_prepared_empty(Rest).

handle_command({check_tables_empty},_Sender,SD0=#state{specula_dep=S1, specula_store=S2, prepared_txs=S3,
            partition=Partition}) ->
    lager:warning("Partition ~w: Dep is ~w, Store is ~w, Prepared is ~w", [Partition, ets:tab2list(S1), ets:tab2list(S2), ets:tab2list(S3)]),
    {reply, ok, SD0};

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

handle_command({read, Key, Type, TxId}, Sender, SD0=#state{
            prepared_txs=PreparedTxs, inmemory_store=InMemoryStore, 
            specula_store=SpeculaStore, specula_dep=SpeculaDep, partition=Partition}) ->
    Pid = get_pid(Sender),
    Tables = {PreparedTxs, InMemoryStore, SpeculaStore, SpeculaDep},
    case clocksi_readitem:check_clock(Key, TxId, Tables, Pid) of
        not_ready ->
            spawn(clocksi_vnode, async_send_msg, [{async_read, Key, Type, TxId,
                         Sender}, {Partition, node()}]),
            %lager:info("Not ready for key ~w ~w, reader is ~w",[Key, TxId, Sender]),
            {noreply, SD0};
        {specula, Value} ->
            %lager:info("Replying specula value for key ~w of tx ~w",[Key, TxId]),
            {reply, {specula, Value}, SD0};
        ready ->
            Result = clocksi_readitem:return(Pid, Key, Type, TxId, Tables),
            {reply, Result, SD0}
    end;

handle_command({async_read, Key, Type, TxId, OrgSender}, _Sender,SD0=#state{
            prepared_txs=PreparedTxs, inmemory_store=InMemoryStore, 
            specula_store=SpeculaStore, specula_dep=SpeculaDep, partition=Partition}) ->
    %lager:info("Got async read request for key ~w of tx ~w",[Key, TxId]),
    Pid = get_pid(OrgSender),
    Tables = {PreparedTxs, InMemoryStore, SpeculaStore, SpeculaDep},
    case clocksi_readitem:check_clock(Key, TxId, Tables, Pid) of
        not_ready ->
            spawn(clocksi_vnode, async_send_msg, [{async_read, Key, Type, TxId,
                         OrgSender}, {Partition, node()}]),
            %lager:info("Not ready for key ~w ~w",[Key, TxId]),
            {noreply, SD0};
        {specula, Value} ->
            %lager:info("Async: repling specula value for key ~w of tx ~w to ~w",[Key, TxId, OrgSender]),
            riak_core_vnode:reply(OrgSender, {specula, Value}),
            %%OrgSender ! {specula, Value},
            %gen_fsm:reply(OrgSender, {specula, Value}),
            {noreply, SD0};
        ready ->
            Result = clocksi_readitem:return(Pid, Key, Type, TxId, Tables),
            %lager:info("Ready!! ~w ~w, Result is ~w",[Key, TxId, Result]),
            riak_core_vnode:reply(OrgSender, Result),
            {noreply, SD0}
    end;


handle_command({prepare, TxId, WriteSet, OriginalSender}, _Sender,
               State = #state{partition=Partition,
                              if_replicate=IfReplicate,
                              committed_tx=CommittedTx,
                              if_certify=IfCertify,
                              quorum=Quorum,
                              inmemory_store=InMemoryStore,
                              specula_store=SpeculaStore,
                              specula_dep=SpeculaDep,
                              prepared_txs=PreparedTxs
                              }) ->
    PrepareTime = now_microsec(erlang:now()),
    %[{committed_tx, CommittedTx}] = ets:lookup(PreparedTxs, committed_tx),
    Pid = get_pid(OriginalSender),
    Tables = {PreparedTxs, InMemoryStore, SpeculaStore, SpeculaDep},
    Result = prepare(TxId, WriteSet, CommittedTx, PrepareTime, Tables, Pid, IfCertify),
    case Result of
        {ok, NewPrepare} ->
            case IfReplicate of
                true ->
                    PendingRecord = {prepare, Quorum-1, OriginalSender, 
                            {prepared, TxId, NewPrepare}, {TxId, WriteSet}},
                    repl_fsm:replicate(Partition, {TxId, PendingRecord}),
                    {noreply, State};
                false ->
                    %lager:info("Returning prepare of ~w ~w",[TxId, NewPrepare]),
                    riak_core_vnode:reply(OriginalSender, {prepared, TxId, NewPrepare}),
                    {noreply, State}
            end;
        {specula_prepared, _NewPrepare} ->
            %% TODO: do nothing here... maybe should reply? I don't know...
            %%riak_core_vnode:reply(OriginalSender, {specula_prepared, TxId, NewPrepare}),
            {noreply, State};
        {error, wait_more} ->
            spawn(clocksi_vnode, async_send_msg, [{prepare, TxId, 
                        WriteSet, OriginalSender}, {Partition, node()}]),
            {noreply, State};
        {error, write_conflict} ->
            riak_core_vnode:reply(OriginalSender, abort),
            %gen_fsm:send_event(OriginalSender, abort),
            {noreply, State}
    end;

handle_command({single_commit, TxId, WriteSet, OriginalSender}, _Sender,
               State = #state{partition=Partition,
                              if_replicate=IfReplicate,
                              if_certify=IfCertify,
                              quorum=Quorum,
                              committed_tx=CommittedTx,
                              inmemory_store=InMemoryStore,
                              specula_store=SpeculaStore,
                              specula_dep=SpeculaDep,
                              prepared_txs=PreparedTxs
                              }) ->
    PrepareTime = now_microsec(erlang:now()),
    Pid = get_pid(OriginalSender),
    Tables = {PreparedTxs, InMemoryStore, SpeculaStore, SpeculaDep},
    %[{committed_tx, CommittedTx}] = ets:lookup(PreparedTxs, committed_tx),
    Result = prepare(TxId, WriteSet, CommittedTx, PrepareTime, Tables, Pid, IfCertify), 
    case Result of
        {ok, NewPrepare} ->
            ResultCommit = commit(TxId, NewPrepare, WriteSet, CommittedTx, State),
            case ResultCommit of
                {ok, {committed, NewCommittedTx}} ->
                    case IfReplicate of
                        true ->
                            PendingRecord = {commit, Quorum-1, OriginalSender, 
                                {committed, NewPrepare}, {TxId, WriteSet}},
                            %ets:insert(PreparedTxs, {committed_tx, NewCommittedTx}),
                            repl_fsm:replicate(Partition, {TxId, PendingRecord}),
                            %{noreply, State};
                            {noreply, State#state{committed_tx=NewCommittedTx}};
                        false ->
                            %ets:insert(PreparedTxs, {committed_tx, NewCommittedTx}),
                            riak_core_vnode:reply(OriginalSender, {committed, NewPrepare}),
                            %{noreply, State}
                            {noreply, State#state{committed_tx=NewCommittedTx}}
                        end;
                {error, no_updates} ->
                    %gen_fsm:send_event(OriginalSender, no_tx_record),
                    riak_core_vnode:reply(OriginalSender, no_tx_record),
                    {noreply, State}
            end;
        {error, wait_more}->
            spawn(clocksi_vnode, async_send_msg, [{prepare, TxId, 
                        WriteSet, OriginalSender}, {Partition, node()}]),
            {noreply, State};
        {error, write_conflict} ->
            riak_core_vnode:reply(OriginalSender, abort),
            %gen_fsm:send_event(OriginalSender, abort),
            {noreply, State}
    end;

%% TODO: sending empty writeset to clocksi_downstream_generatro
%% Just a workaround, need to delete downstream_generator_vnode
%% eventually.
handle_command({commit, TxId, TxCommitTime, Updates}, Sender,
               #state{partition=Partition,
                      quorum=Quorum,
                      committed_tx=CommittedTx,
                      if_replicate=IfReplicate
                      } = State) ->
    %[{committed_tx, CommittedTx}] = ets:lookup(PreparedTxs, committed_tx),
    %lager:info("Received commit operation"),
    Result = commit(TxId, TxCommitTime, Updates, CommittedTx, State),
    case Result of
        {ok, {committed,NewCommittedTx}} ->
            case IfReplicate of
                true ->
                    PendingRecord = {commit, Quorum-1, Sender, 
                        committed, {TxId, TxCommitTime, Updates}},
                    %ets:insert(PreparedTxs, {committed_tx, NewCommittedTx}),
                    repl_fsm:replicate(Partition, {TxId, PendingRecord}),
                    %{noreply, State};
                    {noreply, State#state{committed_tx=NewCommittedTx}};
                false ->
                    %%ets:insert(PreparedTxs, {committed_tx, NewCommittedTx}),
                    %{reply, committed, State}
                    {noreply, State#state{committed_tx=NewCommittedTx}}
            end;
        %{error, materializer_failure} ->
        %    {reply, {error, materializer_failure}, State};
        %{error, timeout} ->
        %    {reply, {error, timeout}, State};
        {error, no_updates} ->
            {reply, no_tx_record, State}
    end;

handle_command({abort, TxId, Updates}, _Sender,
               #state{prepared_txs=PreparedTxs, specula_store=SpeculaStore, specula_dep=SpeculaDep} = State) ->
    case Updates of
        [_Something] -> 
            abort_clean(PreparedTxs, SpeculaStore, SpeculaDep, TxId, Updates),
            {noreply, State};
            %%{reply, ack_abort, State};
        [] ->
            {reply, {error, no_tx_record}, State}
    end;

%% @doc Return active transactions in prepare state with their preparetime
handle_command({get_active_txns}, _Sender,
               #state{prepared_txs=Prepared} = State) ->
    ActiveTxs = ets:lookup(Prepared, active),
    {reply, {ok, ActiveTxs}, State};

handle_command({start_read_servers}, _Sender,
               #state{partition=Partition} = State) ->
    clocksi_readitem_fsm:stop_read_servers(Partition),
    clocksi_readitem_fsm:start_read_servers(Partition),
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
    clocksi_readitem_fsm:stop_read_servers(Partition),    
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================
async_send_msg(Msg, To) ->
    SleepTime = random:uniform(150)+50,
    timer:sleep(SleepTime),
    riak_core_vnode_master:command(To, Msg, To, ?CLOCKSI_MASTER).

prepare(TxId, TxWriteSet, CommittedTx, PrepareTime, Tables, Sender, IfCertify)->
    case certification_check(TxId, TxWriteSet, CommittedTx, Tables, Sender, IfCertify) of
        true ->
            {PreparedTxs, _, _, _} = Tables,
		    set_prepared(PreparedTxs, TxWriteSet, TxId, Sender, PrepareTime),
		    {ok, PrepareTime};
        specula_prepared ->
            %%lager:info("Certification check returns specula_prepared"),
            {PreparedTxs, _, _, _} = Tables,
		    set_prepared(PreparedTxs, TxWriteSet, TxId, Sender, PrepareTime),
		    {specula_prepared, PrepareTime};
	    false ->
	        {error, write_conflict};
        wait ->
            {error,  wait_more}
    end.


set_prepared(_PreparedTxs, [], _TxId, _Sender, _Time) ->
    ok;
set_prepared(PreparedTxs,[{Key, Type, Op}|Rest], TxId, Sender, Time) ->
    true = ets:insert(PreparedTxs, {Key, {TxId, Time, Type, Op, Sender}}),
    set_prepared(PreparedTxs,Rest,TxId, Sender, Time).


commit(TxId, TxCommitTime, Updates, CommittedTx, #state{specula_store=SpeculaStore, 
                inmemory_store=InMemoryStore, prepared_txs=PreparedTxs, specula_dep=SpeculaDep})->
    case Updates of
        [{Key, _Type, _Value} | _Rest] -> 
            update_and_clean(Updates, TxId, TxCommitTime, InMemoryStore, 
                SpeculaStore, PreparedTxs, SpeculaDep),
            NewDict = dict:store(Key, TxCommitTime, CommittedTx),
            {ok, {committed, NewDict}};
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


abort_clean(_PreparedTxs, _SpeculaStore, _SpeculaDep, _TxId, []) ->
    ok;
abort_clean(PreparedTxs, SpeculaStore, SpeculaDep, TxId, [{Key, _, _}|Rest]) ->
    case specula_utilities:abort_specula_committed(TxId, Key, SpeculaStore, SpeculaDep) of
        true ->
            abort_clean(PreparedTxs, SpeculaStore, SpeculaDep, TxId, Rest);
        false ->
            clean_prepared(PreparedTxs, Key, TxId),
            abort_clean(PreparedTxs, SpeculaStore, SpeculaDep, TxId, Rest)
    end. 

clean_prepared(PreparedTxs, Key, TxId) ->
    case ets:lookup(PreparedTxs, Key) of
        [{Key, {TxId, _Time, _Type, _Op, _}}] ->
            ets:delete(PreparedTxs, Key);
        [] ->
            ok
    end.


%% @doc converts a tuple {MegaSecs,Secs,MicroSecs} into microseconds
now_microsec({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.

%% @doc Performs a certification check when a transaction wants to move
%%      to the prepared state.
%certification_check(_TxId, _H, _CommittedTx, _PreparedTxs) ->
%    true.
certification_check(_, _, _, _, _, false) ->
    true;
certification_check(_, [], _, _, _, true) ->
    true;
certification_check(TxId, [H|T], CommittedTx, Tables, Sender, true) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    {Key, _Type, _} = H,
    case dict:find(Key, CommittedTx) of
        {ok, CommitTime} ->
            case CommitTime > SnapshotTime of
                true ->
                    false;
                false ->
                    case check_prepared(TxId, Key, Tables, Sender) of
                        true ->
                            certification_check(TxId, T, CommittedTx, Tables, Sender, true);
                        false ->
                            false;
                        %specula_prepared ->
                        %    certification_check(TxId, T, CommittedTx, Tables, true),
                        %    specula_prepared;
                        wait ->
                            wait
                    end
            end;
        error ->
            case check_prepared(TxId, Key, Tables, Sender) of
                true ->
                    certification_check(TxId, T, CommittedTx, Tables, Sender, true); 
                false ->
                    false;
                specula_prepared ->
                    specula_prepared;
                wait ->
                    wait
            end
    end.

check_prepared(TxId, Key, Tables, Sender) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    {PreparedTxs, InMemoryStore, SpeculaStore, SpeculaDep} = Tables,
    case ets:lookup(PreparedTxs, Key) of
        [] ->
            true;
        [{Key, {PreparedTxId, PrepareTime, Type, Op, PreparedSender}}] ->
            case PrepareTime > SnapshotTime of
                true ->
                    %%lager:info("Has to abort for Key ~w", [Key]),
                    false;
                false ->
                    %% TODO: this part should be tested..
                    %% Can I prepare if some updates on this key is specula_committed, or I should always wait?
                    case specula_utilities:should_specula(PrepareTime, SnapshotTime) of
                        true ->
                            _ = specula_utilities:make_prepared_specula(Key, 
                                {PreparedTxId, PrepareTime, Type, Op, PreparedSender},
                                    PreparedTxs, InMemoryStore, SpeculaStore, SpeculaDep),
                            specula_utilities:add_specula_meta(SpeculaDep, PreparedTxId, TxId, 
                                TxId#tx_id.snapshot_time, update, Sender),
                            specula_prepared;
                        false ->
                            false
                    end
            end
    end.


-spec update_and_clean(KeyValues :: [{key(), atom(), term()}],
                          TxId::txid(),TxCommitTime:: {term(), term()}, InMemoryStore :: cache_id(), 
                            SpeculaStore :: cache_id(), PreparedTxs :: cache_id(), SpeculaDep :: cache_id()) -> ok.
update_and_clean([], _TxId, _TxCommitTime, _, _, _, _) ->
    ok;
update_and_clean([{Key, Type, {Param, Actor}}|Rest], TxId, TxCommitTime, InMemoryStore, 
                SpeculaStore, PreparedTxs, SpeculaDep) ->
    %lager:info("Storing key ~w for ~w",[Key, TxId]),
    case specula_utilities:make_specula_final(TxId, Key, TxCommitTime, SpeculaStore, InMemoryStore,
                PreparedTxs, SpeculaDep) of
        true -> %% The prepared value was made speculative and it is true
            ok;
        false -> %% There is no speculative version
            case ets:lookup(InMemoryStore, Key) of
                [] ->
                    Init = Type:new(),
                    {ok, NewSnapshot} = Type:update(Param, Actor, Init),
                    true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, NewSnapshot}]});
                [{Key, ValueList}] ->
                    {RemainList, _} = lists:split(min(20,length(ValueList)), ValueList),
                    [{_CommitTime, First}|_] = RemainList,
                    {ok, NewSnapshot} = Type:update(Param, Actor, First),
                    true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, NewSnapshot}|RemainList]})
            end,
            clean_prepared(PreparedTxs, Key, TxId),
            update_and_clean(Rest, TxId, TxCommitTime, InMemoryStore, SpeculaStore, PreparedTxs, SpeculaDep)
    end.

%write_set_to_logrecord(TxId, WriteSet) ->
%    lists:foldl(fun({Key,Type,Op}, Acc) ->
%			Acc ++ [#log_record{tx_id=TxId, op_type=update,
%					    op_payload={Key, Type, Op}}]
%		end,[],WriteSet).

%% Internal functions
filter_updates_per_key(Updates, Key) ->
    FilterMapFun = fun ({KeyPrime, _Type, Op}) ->
        case KeyPrime == Key of
            true  -> {true, Op};
            false -> false
        end
    end,
    lists:filtermap(FilterMapFun, Updates).

get_pid({_, _, {Pid, _}}) ->
    Pid;
get_pid({_, _, Pid}) ->
    Pid.


-ifdef(TEST).

%% @doc Testing filter_updates_per_key.
filter_updates_per_key_test()->
    Op1 = {update, {{increment,1}, actor1}},
    Op2 = {update, {{increment,2}, actor1}},
    Op3 = {update, {{increment,3}, actor1}},
    Op4 = {update, {{increment,4}, actor1}},

    ClockSIOp1 = {a, crdt_pncounter, Op1},
    ClockSIOp2 = {b, crdt_pncounter, Op2},
    ClockSIOp3 = {c, crdt_pncounter, Op3},
    ClockSIOp4 = {a, crdt_pncounter, Op4},

    ?assertEqual([Op1, Op4], 
        filter_updates_per_key([ClockSIOp1, ClockSIOp2, ClockSIOp3, ClockSIOp4], a)).

-endif.
