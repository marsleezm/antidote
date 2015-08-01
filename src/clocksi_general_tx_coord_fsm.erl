%% -------------------------------------------------------------------
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
%% @doc The coordinator for a given Clock SI general tx_id.
%%      It handles the state of the tx and executes the operations sequentially
%%      by sending each operation to the responsible clockSI_vnode of the
%%      involved key. when a tx is finalized (committed or aborted, the fsm
%%      also finishes.

-module(clocksi_general_tx_coord_fsm).

-behavior(gen_fsm).

-include("antidote.hrl").
-include("speculation.hrl").

-define(SPECULA_TIMEOUT, 1).
-define(DUMB_TIMEOUT, 50).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(LOG_UTIL, mock_partition_fsm).
-define(CLOCKSI_VNODE, mock_partition_fsm).
-define(CLOCKSI_DOWNSTREAM, mock_partition_fsm).
-else.
-define(LOG_UTIL, log_utilities).
-define(CLOCKSI_VNODE, clocksi_vnode).
-define(CLOCKSI_DOWNSTREAM, clocksi_downstream).
-endif.


%% API
-export([start_link/3, start_link/2]).

%% Callbacks
-export([init/1,
         stop/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).

%% States
-export([
         start_processing/2,
         finish_op/3,
         receive_reply/2,
         single_committing/2]).

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

-record(state, {
      %% Metadata for all transactions
	  from :: {pid(), term()},
      txn_id_list :: [txid()],
      all_txn_ops :: [],
      specula_meta :: dict(),
      num_txns :: non_neg_integer(),
      current_txn_index :: non_neg_integer(),
      num_committed_txn :: non_neg_integer(),
      %% Metadata for a single txn
	  tx_id :: txid(),
      current_txn_meta :: txn_metadata(),
      causal_clock :: non_neg_integer(),
      %%Stat
      num_committed,
      num_cert_final,
      num_read_invalid}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(From, Clientclock, Operations) ->
    gen_fsm:start_link(?MODULE, [From, Clientclock, Operations], []).

start_link(From, Operations) ->
    gen_fsm:start_link(?MODULE, [From, ignore, Operations], []).

finish_op(From, Key,Result) ->
    gen_fsm:send_event(From, {Key, Result}).

stop(Pid) -> gen_fsm:sync_send_all_state_event(Pid,stop).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state.
init([From, ClientClock, Txns]) ->
    random:seed(now()),
    SD = #state{
            all_txn_ops = Txns,
            num_txns = length(Txns),
            current_txn_index = 1,
            specula_meta = dict:new(),
            txn_id_list = [],
            causal_clock = ClientClock,
            num_committed_txn = 0,
            from = From
           },
    %io:format(user, "Sending msg to myself ~w, from is ~w~n", [Self, From]),
    {ok, start_processing, SD, 0}.

%% @doc Contact the leader computed in the prepare state for it to execute the
%%      operation, wait for it to finish (synchronous) and go to the prepareOP
%%       to execute the next operation.
start_processing(timeout, SD) ->
    process_txs(SD).

process_txs(SD=#state{causal_clock=CausalClock,  
        all_txn_ops=AllTxnOps, current_txn_index=CurrentTxnIndex}) ->
    TxId = tx_utilities:create_transaction_record(CausalClock),
    MyOperations = lists:nth(CurrentTxnIndex, AllTxnOps),
    TxnMeta = process_operations(TxId, MyOperations,
                {dict:new(), [], dict:new(), [], true}, CurrentTxnIndex),
    %lager:info("In processing txn ~w, final commit is ~w", [TxId, CanCommit]),
    case TxnMeta#txn_metadata.final_committed of
        true -> %%TODO: has to find some way to deal with read-only transaction
            proceed_txn(SD#state{current_txn_meta=TxnMeta, 
                        tx_id=TxId});
        false ->
            {next_state, receive_reply, SD#state{
                     tx_id=TxId, current_txn_meta=TxnMeta}, ?SPECULA_TIMEOUT}
    end.

            %%lager:info("Write set is empty.. ~w", [TxId]),
        %1->
        %    UpdatedPart = dict:to_list(WriteSet),
        %    ?CLOCKSI_VNODE:single_commit(UpdatedPart, TxId),
        %    TxnMetadata1 = TxnMetadata#txn_metadata{num_updated=1},
        %    {next_state, single_committing, SD#state{state=normal, current_txn_meta=TxnMetadata1, tx_id=TxId}};

%% @doc in this state, the fsm waits for prepare_time from each updated
%%      partitions in order to compute the final tx timestamp (the maximum
%%      of the received prepare_time).
receive_reply(timeout,
                 S0=#state{current_txn_meta=CurrentTxnMeta, tx_id=_TxId}) ->
    case specula_utilities:coord_should_specula(CurrentTxnMeta) of
        true ->
            %lager:info("Timeouted, coordinator proceed.. ~w", [TxId]),
            proceed_txn(S0);
        false ->
            %lager:info("Timeouted, coordinator still not proceeding.. ~w", [TxId]),
            {next_state, receive_reply, S0}
    end;

receive_reply({specula_prepared, TxId, ReceivedPrepareTime},
                 S0=#state{current_txn_meta=CurrentTxnMeta,
                            tx_id=CurrentTxId
                            }) ->
    %lager:info("Got specula prepared.. ~w, current tx is ~w", [TxId, CurrentTxId]),
    case TxId of
        CurrentTxId -> %% Got reply to current Tx
            MaxPrepareTime = max(CurrentTxnMeta#txn_metadata.prepare_time, ReceivedPrepareTime),
            NumSpeculaPrepared = CurrentTxnMeta#txn_metadata.num_specula_prepared,
            CurrentTxnMeta1 = CurrentTxnMeta#txn_metadata{prepare_time=MaxPrepareTime,
                         num_specula_prepared=NumSpeculaPrepared+1},
            case specula_utilities:coord_should_specula(CurrentTxnMeta1) of
                true ->
                    proceed_txn(S0#state{current_txn_meta=CurrentTxnMeta1});
                false ->
                    {next_state, receive_reply,
                     S0#state{current_txn_meta=CurrentTxnMeta1}}
            end;
        _ ->
            ok
    end;

receive_reply({Type, TxId, Param2},
                 S0=#state{tx_id=CurrentTxId,
                           num_committed_txn = NumCommittedTxn,
                           txn_id_list=TxIdList,
                           specula_meta=SpeculaMeta,
                           current_txn_meta=CurrentTxnMeta}) ->
    case TxId of
        CurrentTxId ->
            %lager:info("Got ~w of txn ~w",[Type, CurrentTxId]),
            CurrentTxnMeta1 = update_txn_meta(CurrentTxnMeta, Type, Param2),
            case can_commit(CurrentTxId, CurrentTxnMeta1, NumCommittedTxn) of
                true ->
                    CurrentTxnMeta2 = commit_tx(CurrentTxId, CurrentTxnMeta1),
                    proceed_txn(S0#state{current_txn_meta=CurrentTxnMeta2, 
                        num_committed_txn=NumCommittedTxn+1});
                false ->
                    case specula_utilities:coord_should_specula(CurrentTxnMeta1) of
                        true ->
                            proceed_txn(S0#state{current_txn_meta=CurrentTxnMeta1});
                        false ->
                            {next_state, receive_reply,
                             S0#state{current_txn_meta=CurrentTxnMeta1}}
                    end
            end;
        _ ->
            case dict:find(TxId, SpeculaMeta) of
                {ok, TxnMeta} ->
                    %lager:info("Got ~w of previous tx ~w", [Type, TxId]),
                    TxnMeta1 = update_txn_meta(TxnMeta, Type, Param2),
                    SpeculaMeta1 = dict:store(TxId, TxnMeta1, SpeculaMeta),
                    case can_commit(TxId, TxnMeta1, NumCommittedTxn) of
                        true -> 
                            {NewNumCommitted, SpeculaMeta2} = 
                                    cascading_commit_tx(TxId, TxnMeta1, SpeculaMeta1, TxIdList),
                            case can_commit(TxId, CurrentTxnMeta, NewNumCommitted) of
                                true ->
                                    CurrentTxnMeta1 = commit_tx(CurrentTxId, CurrentTxnMeta),
                                    proceed_txn(S0#state{current_txn_meta=CurrentTxnMeta1, 
                                            specula_meta=SpeculaMeta2, num_committed_txn=NewNumCommitted+1});
                                false ->
                                    {next_state, receive_reply, 
                                        S0#state{specula_meta=SpeculaMeta2, num_committed_txn=NewNumCommitted}} 
                            end;
                        false ->
                            %lager:info("Can not commit ~w", [TxId]),
                            {next_state, receive_reply, S0#state{specula_meta=SpeculaMeta1}} 
                    end;
                error ->
                    {next_state, receive_reply, S0}
            end
    end;


%% Abort due to invalid read or invalid prepare
receive_reply({abort, TxId}, S0=#state{tx_id=CurrentTxId, specula_meta=SpeculaMeta,
                 current_txn_meta=CurrentTxnMeta}) ->
    %lager:info("Receive aborted for Tx ~w, current tx is ~w", [TxId, CurrentTxId]),
    case TxId of
        CurrentTxId ->
            ?CLOCKSI_VNODE:abort(CurrentTxnMeta#txn_metadata.updated_parts, CurrentTxId),
            timer:sleep(random:uniform(?DUMB_TIMEOUT)),
            %% Restart from current transaction.
            process_txs(S0);
        _ ->
            case dict:find(TxId, SpeculaMeta) of
                {ok, AbortTxnMeta} ->
                    S1 = cascading_abort(AbortTxnMeta, S0),
                    timer:sleep(random:uniform(?DUMB_TIMEOUT)),
                    process_txs(S1);
                error ->
                    %lager:warning("Can't find txn wants to abort!!! ~w",[TxId]),
                    {next_state, receive_reply, S0}
            end
    end.

%% Abort because prepare failed (concurrent txs has committed or prepared).
%receive_reply(abort, S0=#state{tx_id=CurrentTxId, current_txn_meta=CurrentTxnMeta}) ->
%    %lager:info("Receive aborted for current tx is ~w", [CurrentTxId]),
%    ?CLOCKSI_VNODE:abort(CurrentTxnMeta#txn2_metadata.updated_parts, CurrentTxId),
%    proceed_txn(S0#state{state=aborted}).

%% TODO: need to define better this case: is specula_committed allowed, or not?
%% Why this case brings doubt???
single_committing({committed, CommitTime}, S0=#state{from=_From, current_txn_meta=CurrentTxnMeta}) ->
    proceed_txn(S0#state{current_txn_meta=CurrentTxnMeta#txn_metadata{prepare_time=CommitTime}});
    
single_committing(abort, S0=#state{from=_From}) ->
    proceed_txn(S0).


update_txn_meta(TxnMeta, Type, Param) ->
    case Type of
        prepared ->
            MaxPrepareTime = max(TxnMeta#txn_metadata.prepare_time, Param),
            NumPrepared = TxnMeta#txn_metadata.num_prepared,
            TxnMeta#txn_metadata{prepare_time=MaxPrepareTime,
                         num_prepared=NumPrepared+1};
        read_valid ->
            ReadDep = TxnMeta#txn_metadata.read_dep,
            ReadDep1 = lists:delete(Param, ReadDep),
            TxnMeta#txn_metadata{read_dep=ReadDep1}
    end.

%% @doc proceed_txn is called when timeout has expired, a transaction is aborted or 
%%      the current transaction is committed.
proceed_txn(S0=#state{from=From, tx_id=TxId, txn_id_list=TxIdList, current_txn_index=CurrentTxnIndex,
            current_txn_meta=CurrentTxnMeta, specula_meta=SpeculaMeta, num_txns=NumTxns}) ->
    CommitTime = CurrentTxnMeta#txn_metadata.prepare_time,
    case CurrentTxnIndex of
        NumTxns ->
            case CurrentTxnMeta#txn_metadata.final_committed of 
                true -> %%All transactions must have finished
                    %%lager:info("Finishing txn"),
                    AllReadSet = get_readset(TxIdList, SpeculaMeta, []),
                    AllReadSet1 = [CurrentTxnMeta#txn_metadata.read_set|AllReadSet],
                    %lager:info("Transaction finished, read set is ~w",[RevReadSet1]),
                    From ! {ok, {TxId, lists:reverse(lists:flatten(AllReadSet1)), 
                        CommitTime}},
                    {stop, normal, S0};
                false -> %%This is the last transaction, but not finally committed..
                    %lager:info("Last transaction but can't proceed"),
                    {next_state, receive_reply, S0}
            end;
        _ -> %%Start the next transaction
            SpeculaMeta1 = dict:store(TxId, CurrentTxnMeta, SpeculaMeta),
            TxIdList1 = TxIdList ++ [TxId],
            process_txs(S0#state{specula_meta=SpeculaMeta1, current_txn_index=CurrentTxnIndex+1, 
                txn_id_list=TxIdList1, causal_clock=CommitTime})
    end.

%% =============================================================================

handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(stop,_From,_StateName, StateData) ->
    {stop,normal,ok, StateData};

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%%%%% Private function %%%%%%%%%%

process_operations(TxId, [], {WriteSet, ReadSet, _, ReadDep, CanCommit}, Index) ->
    case dict:size(WriteSet) of
        0 ->
            #txn_metadata{read_dep=ReadDep, read_set=ReadSet, index=Index,
                    final_committed=CanCommit, prepare_time=TxId#tx_id.snapshot_time};
        N ->
            ?CLOCKSI_VNODE:prepare(WriteSet, TxId),
            #txn_metadata{read_dep=ReadDep, updated_parts=WriteSet, num_updated=N,
              read_set=ReadSet, index=Index, final_committed=CanCommit}
    end;
process_operations(TxId, [{read, Key, Type}|Rest], {UpdatedParts, RSet, Buffer, ReadDep, CanCommit}, Index) ->
    Reply = case dict:find(Key, Buffer) of
                    error ->
                        Preflist = ?LOG_UTIL:get_preflist_from_key(Key),
                        IndexNode = hd(Preflist),
                        ?CLOCKSI_VNODE:read_data_item(IndexNode, Key, Type, TxId);
                    {ok, SnapshotState} ->
                        {ok, {Type,SnapshotState}}
            end,
    %lager:info("Read got reply ~w",[Reply]),
    {NewReadDep, CanCommit1} = case Reply of
                                    {specula, {_Type, _Snapshot}} ->
                                        {[Key|ReadDep], false};
                                    _ ->
                                        {ReadDep, CanCommit}
                               end,
    %io:format(user, "~nType is ~w, Key is ~w~n",[Type, Key]),
    {_, {Type, KeySnapshot}} = Reply,
    Buffer1 = dict:store(Key, KeySnapshot, Buffer),
    process_operations(TxId, Rest, {UpdatedParts, 
            [Type:value(KeySnapshot)|RSet], Buffer1, NewReadDep, CanCommit1}, Index);
process_operations(TxId, [{update, Key, Type, Op}|Rest], {UpdatedParts, RSet, Buffer, ReadDep, _}, Index) ->
    Preflist = ?LOG_UTIL:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    UpdatedParts1 = case dict:is_key(IndexNode, UpdatedParts) of
                        false ->
                            dict:store(IndexNode, [{Key, Type, Op}], UpdatedParts);
                        true ->
                            dict:append(IndexNode, {Key, Type, Op}, UpdatedParts)
                    end,
    Buffer1 = case dict:find(Key, Buffer) of
                error ->
                    Init = Type:new(),
                    {Param, Actor} = Op,
                    {ok, NewSnapshot} = Type:update(Param, Actor, Init),
                    dict:store(Key, NewSnapshot, Buffer);
                {ok, Snapshot} ->
                    {Param, Actor} = Op,
                    {ok, NewSnapshot} = Type:update(Param, Actor, Snapshot),
                    dict:store(Key, NewSnapshot, Buffer)
                end,
    process_operations(TxId, Rest, {UpdatedParts1, RSet, Buffer1, ReadDep, false}, Index).

cascading_abort(AbortTxnMeta, #state{tx_id=CurrentTxId, current_txn_meta=CurrentTxnMeta,
                             specula_meta=SpeculaMeta,txn_id_list=TxIdList}=S0) ->
    AbortIndex = AbortTxnMeta#txn_metadata.index,
    AbortTxList = lists:nthtail(AbortIndex-1, TxIdList),
    AbortFun = fun(Id, Dict) -> 
                                TMeta = dict:fetch(Id, Dict),  
                                ?CLOCKSI_VNODE:abort(TMeta#txn_metadata.updated_parts, Id),
                                dict:erase(Id, Dict)
                end, 
    SpeculaMeta1 = lists:foldl(AbortFun, SpeculaMeta, AbortTxList),
    
    %Abort current transaction
    CurrentUpdatedParts = CurrentTxnMeta#txn_metadata.updated_parts, 
    %lager:info("Aborting current txn in cascading"),
    ?CLOCKSI_VNODE:abort(CurrentUpdatedParts, CurrentTxId),

    S0#state{txn_id_list=lists:sublist(TxIdList, AbortIndex-1),
                current_txn_index=AbortIndex, specula_meta=SpeculaMeta1}.

cascading_commit_tx(TxId, TxnMeta, SpeculaMeta, TxIdList) ->
    %lager:info("Doing cascading commit ~w",[TxId]),
    TxnMeta1 = commit_tx(TxId, TxnMeta),
    Index = TxnMeta1#txn_metadata.index,
    SpeculaMeta1 = dict:store(TxId, TxnMeta1, SpeculaMeta),
    try_commit_successors(lists:nthtail(Index, TxIdList), SpeculaMeta1, Index).

commit_tx(TxId, TxnMeta) ->
    UpdatedParts = TxnMeta#txn_metadata.updated_parts,
    ?CLOCKSI_VNODE:commit(UpdatedParts, TxId, TxnMeta#txn_metadata.prepare_time),
    TxnMeta#txn_metadata{final_committed=true}. 

try_commit_successors([], SpeculaMetadata, Index) ->
    {Index, SpeculaMetadata};
try_commit_successors([TxId|Rest], SpeculaMetadata, Index) ->
    TxnMeta = dict:fetch(TxId, SpeculaMetadata),
    ReadDep = TxnMeta#txn_metadata.read_dep,
    case ReadDep of
        [] ->
            NumUpdated = TxnMeta#txn_metadata.num_updated,
            case TxnMeta#txn_metadata.num_prepared of
                NumUpdated ->
                    UpdatedParts = TxnMeta#txn_metadata.updated_parts,
                    ?CLOCKSI_VNODE:commit(UpdatedParts, TxId, TxnMeta#txn_metadata.prepare_time),
                    SpeculaMetadata1 = dict:store(TxId, 
                        TxnMeta#txn_metadata{final_committed=true}, SpeculaMetadata),
                    try_commit_successors(Rest, SpeculaMetadata1, Index+1);
                _ ->
                    {Index, SpeculaMetadata}
            end;
        _ ->
            {Index, SpeculaMetadata}
    end.

can_commit(_TxId, TxnMeta, NumCommittedTxn) ->
    NumberUpdated = TxnMeta#txn_metadata.num_updated,
    case TxnMeta#txn_metadata.num_prepared of
        NumberUpdated ->
            case TxnMeta#txn_metadata.read_dep of
                [] -> %%No read dependency
                    NumCommittedTxn == TxnMeta#txn_metadata.index - 1;
                _ ->
                    %lager:info("~w: Can not commit due to unprepared", [TxId]),
                    false
            end;
        _ ->
            false
    end.

get_readset([], _SpeculaMeta, Acc) ->
    Acc;
get_readset([H|T], SpeculaMeta, Acc) ->
    Tx = dict:fetch(H, SpeculaMeta),
    get_readset(T, SpeculaMeta, [Tx#txn_metadata.read_set|Acc]).


-ifdef(TEST).
process_op_test() ->
    %% Common read
    TxId = tx_utilities:create_transaction_record(clocksi_vnode:now_microsec(now())),
    Key1 = {counter, 1},
    Key2 = {counter, 2},
    Type = riak_dt_gcounter,
    Op1 = {increment, haha},
    Operations = [{read, Key1, Type}, {update, Key1, Type, Op1}, {read, Key1, Type}, {read, Key2, Type},
                {read, Key2, Type}],
    TxnMeta = process_operations(TxId, Operations,
            {dict:new(), [], dict:new(), [], true}, 1),

    ?assertEqual([2, 3, 2, 2], lists:reverse(TxnMeta#txn_metadata.read_set)),
    ?assertEqual([], TxnMeta#txn_metadata.read_dep),
    ?assertEqual(1, TxnMeta#txn_metadata.num_updated),

    %% Read with dependency
    Key3 = {specula, 1, 10},
    Operations1 = [{read, Key1, Type}, {update, Key1, Type, Op1}, {read, Key1, Type}, {read, Key2, Type},
                {read, Key2, Type}, {read, Key3, Type}],
    TxnMeta2 = process_operations(TxId, Operations1,
            {dict:new(), [], dict:new(), [], true}, 1),
   
    ?assertEqual([2, 3, 2, 2, 2], lists:reverse(TxnMeta2#txn_metadata.read_set)),
    ?assertEqual([Key3], TxnMeta2#txn_metadata.read_dep),
    ?assertEqual(1, TxnMeta2#txn_metadata.num_updated).

    
cascading_abort_test() ->
    State = generate_specula_meta(4, 3, 1, 1),
    [TxId1, TxId2, _] = State#state.txn_id_list,
    TxId2Meta = dict:fetch(TxId2, State#state.specula_meta),
    State1 = cascading_abort(TxId2Meta, State),
    TxnIndex = State1#state.current_txn_index,
    RemainingTxId = State1#state.txn_id_list,
    ?assertEqual(TxnIndex, 2),
    ?assertEqual(RemainingTxId, [TxId1]).

cascading_commit_test() ->
    State = generate_specula_meta(5, 3, 4, 0),
    [TxId1, TxId2, TxId3] = State#state.txn_id_list,
    SpeculaMeta = State#state.specula_meta,

    Tx1Meta = dict:fetch(TxId1, SpeculaMeta),
    {_, SpeculaMeta2} = cascading_commit_tx(TxId1, Tx1Meta, SpeculaMeta, [TxId1, TxId2, TxId3]),
    FinalCommitted = lists:map(fun(X) -> Meta=dict:fetch(X, SpeculaMeta2), 
                                Meta#txn_metadata.final_committed end, [TxId1, TxId2, TxId3]),
    ?assertEqual([true, true, true], FinalCommitted),

    %% Without committing the first..
    Tx2Meta = dict:fetch(TxId2, SpeculaMeta),
    {_, SpeculaMeta02} = cascading_commit_tx(TxId2, Tx2Meta, SpeculaMeta, [TxId1, TxId2, TxId3]),
    FinalCommitted0 = lists:map(fun(X) -> Meta=dict:fetch(X, SpeculaMeta02), 
                                Meta#txn_metadata.final_committed end, [TxId1, TxId2, TxId3]),
    ?assertEqual([false, true, true], FinalCommitted0),

    State1 = generate_specula_meta(5, 4, 2, 1),
    TxnList1 = State1#state.txn_id_list,
    [_TxId11, TxId12, _TxId13, _Txn14] = TxnList1, 
    SpeculaMeta1 = State1#state.specula_meta,

    TxMeta12 = dict:fetch(TxId12, SpeculaMeta1),
    {_, SpeculaMeta12} = cascading_commit_tx(TxId12, TxMeta12, SpeculaMeta1, TxnList1),
    FinalCommitted1 = lists:map(fun(X) -> Meta=dict:fetch(X, SpeculaMeta12), 
                                Meta#txn_metadata.final_committed end, TxnList1),
    ?assertEqual([true, true, false, false], FinalCommitted1).

    
can_commit_test() ->
    State = generate_specula_meta(4,3,2,1),
    SpeculaMeta = State#state.specula_meta,
    [TxId1, TxId2, TxId3] = State#state.txn_id_list,
    _TxMeta1 = dict:fetch(TxId1, SpeculaMeta), 
    _TxMeta2 = dict:fetch(TxId2, SpeculaMeta), 
    _TxMeta3 = dict:fetch(TxId3, SpeculaMeta), 
%    ?assertEqual(true, can_commit(TxMeta1, SpeculaMeta, State#state.txn_id_list)),
%    ?assertEqual(true, can_commit(TxMeta2, SpeculaMeta, State#state.txn_id_list)),
%    ?assertEqual(false, can_commit(TxMeta3, SpeculaMeta, State#state.txn_id_list)).
    ok.

generate_specula_meta(NumTotalTxn, NumSpeculaTxn, NumCanCommit, NumCommitted) ->
    Seq0 = lists:seq(1, NumTotalTxn),
    AllTxnOps = lists:map(fun(_) -> generate_random_op(1, 1, []) end, Seq0),

    %% TxIds of specually committed transaction
    Seq1 = lists:seq(1, NumSpeculaTxn),
    TxnIdList = lists:map(fun(_) -> tx_utilities:create_transaction_record(
                        clocksi_vnode:now_microsec(now())) end, Seq1),

    %% Metadatas of txns that can finally commit
    Seq2 = lists:seq(1, NumCanCommit),
    CanCommitTxn0 = lists:map(fun(_) -> generate_txn_meta(false, false) end, Seq2),
    CanCommitTxn = make_commit(NumCommitted, CanCommitTxn0, []),

    {TxnList, [CurrentTxn]} = case NumSpeculaTxn >= NumCanCommit of
                        true ->
                            PendingTxn = lists:map(fun(_) -> generate_txn_meta(false, true) end,
                                lists:seq(1, NumSpeculaTxn - NumCanCommit+1)),
                            {PendingTxn1, Txn2} = lists:split(length(PendingTxn) - 1, PendingTxn),
                            {CanCommitTxn++PendingTxn1, Txn2};
                        false ->
                            lists:split(length(CanCommitTxn) - 1, CanCommitTxn)
                        end,

    Zipped = lists:zip(TxnIdList, TxnList),
    {SpeculaMeta, _} = lists:foldl(fun({Id, Txn}, {Dict, Acc}) ->
                {dict:store(Id, Txn#txn_metadata{index=Acc}, Dict), Acc+1} end, {dict:new(),1}, Zipped),

    CurrentTxId = tx_utilities:create_transaction_record(clocksi_vnode:now_microsec(now())),

    #state{all_txn_ops=AllTxnOps, txn_id_list=TxnIdList, tx_id=CurrentTxId,
        current_txn_meta=CurrentTxn#txn_metadata{index=NumSpeculaTxn+1}, specula_meta=SpeculaMeta}.

generate_txn_meta(false, false) ->
    #txn_metadata{read_dep=[], num_prepared=0, num_updated=0};
generate_txn_meta(PendingRead, PendingPrepare) ->
    ReadDep = case PendingRead of true -> [1,2]; false -> [] end,
    NumUpdated = case PendingPrepare of true -> 1; false -> 0 end,
    #txn_metadata{read_dep=ReadDep, num_prepared=0, num_updated=NumUpdated}.


make_commit(0, H, Acc) ->
    lists:reverse(Acc)++H;
make_commit(Num, [H|T], Acc) ->
    make_commit(Num-1, T, [H#txn_metadata{final_committed=true}|Acc]).

generate_random_op(0, 0, Acc) ->
    Acc;
generate_random_op(0, N, Acc) ->
    Key = {counter, random:uniform(10)},
    generate_random_op(0, N-1,
            [{update, Key, riak_dt_gcounter, {increment, random:uniform(100)}}|Acc]);
generate_random_op(N, 0, Acc) ->
    Key = {counter, random:uniform(10)},
    generate_random_op(N-1, 0, [{read, Key, riak_dt_gcounter}|Acc]);
generate_random_op(NumRead, NumWrite, Acc) ->
    Key = {counter, random:uniform(10)},
    case random:uniform(10) rem 2 of
        0 ->
            generate_random_op(NumRead-1, NumWrite, [{read, Key, riak_dt_gcounter}|Acc]);
        1 ->
            generate_random_op(NumRead, NumWrite-1,
                    [{update, Key, riak_dt_gcounter, {increment, random:uniform(100)}}|Acc])
    end.


-endif.
