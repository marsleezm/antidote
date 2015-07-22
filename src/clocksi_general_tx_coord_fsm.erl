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

-define(SPECULA_TIMEOUT, 1).
-define(DUMB_TIMEOUT, 5).

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
         finish_op/3,
         receive_reply/2,
         single_committing/2,
         abort/2,
         proceed_txn/1]).

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
-record(tx_metadata, {
        read_dep :: non_neg_integer(),
        num_updated :: non_neg_integer(),
        num_specula_prepared = 0 :: non_neg_integer(),
        num_prepared = 0 :: non_neg_integer(),
        prepare_time :: non_neg_integer(),
        final_committed = false :: boolean(),
        updated_parts :: dict(),
        read_set :: [],
        index :: pos_integer()
        }).

-type tx_metadata() :: #tx_metadata{}.

-record(state, {
      %% Metadata for all transactions
	  from :: {pid(), term()},
      tx_id_list :: [txid()],
      all_txn_ops :: [],
      pending_txn_ops :: [],
      specula_meta :: dict(),
      %% Metadata for a single txn
	  tx_id :: txid(),
      current_tx_meta :: tx_metadata(),
      causal_clock :: non_neg_integer(),
	  state :: active | prepared | committing | committed | undefined | aborted}).

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
    SD = #state{
            all_txn_ops = Txns,
            pending_txn_ops = Txns,
            specula_meta = dict:new(),
            tx_id_list = [],
            causal_clock = ClientClock,
            from = From
           },
    gen_fsm:send_event(self(), process_tx),
    {ok, receive_reply, SD, 0}.

%% @doc Contact the leader computed in the prepare state for it to execute the
%%      operation, wait for it to finish (synchronous) and go to the prepareOP
%%       to execute the next operation.
receive_reply(process_tx, 
           SD=#state{causal_clock=CausalClock,
                    tx_id_list=TxIdList,
                    pending_txn_ops=PendingTxnOps
		      }) ->
    TxId = tx_utilities:create_transaction_record(CausalClock),
    [CurrentTxn|_RestTxn] = PendingTxnOps,
    {WriteSet, ReadSet, _, ReadDep} = process_operations(TxId, CurrentTxn,
                {dict:new(), [], dict:new(), []}),
    TxMetadata = #tx_metadata{read_dep=ReadDep, updated_parts=WriteSet, num_updated=dict:size(WriteSet), 
            read_set=ReadSet, index=length(TxIdList)},
    case dict:size(WriteSet) of
        0-> %%TODO: has to find some way to deal with read-only transaction
            CommitTime = clocksi_vnode:now_microsec(now()),
            TxMetadata1 = TxMetadata#tx_metadata{prepare_time=CommitTime},
            proceed_txn(SD#state{state=committed, current_tx_meta=TxMetadata1, causal_clock=CommitTime});
        1->
            UpdatedPart = dict:to_list(WriteSet),
            ?CLOCKSI_VNODE:single_commit(UpdatedPart, TxId),
            TxMetadata1 = TxMetadata#tx_metadata{num_updated=1},
            {next_state, single_committing, SD#state{state=committing, current_tx_meta=TxMetadata1}};
        N->
            ?CLOCKSI_VNODE:prepare(WriteSet, TxId),
            TxMetadata1 = TxMetadata#tx_metadata{num_updated=N},
            {next_state, receive_reply, SD#state{state=prepared, current_tx_meta=TxMetadata1}, ?SPECULA_TIMEOUT}
    end;

%% @doc in this state, the fsm waits for prepare_time from each updated
%%      partitions in order to compute the final tx timestamp (the maximum
%%      of the received prepare_time).
receive_reply(timeout,
                 S0=#state{current_tx_meta=CurrentTxMeta}) ->
    case specula_utilities:coord_should_specula(CurrentTxMeta) of
        true ->
            proceed_txn(S0);
        false ->
            {next_state, receive_reply, S0}
    end;

receive_reply({specula_prepared, TxId, ReceivedPrepareTime},
                 S0=#state{current_tx_meta=CurrentTxMeta,
                            tx_id=CurrentTxId
                            }) ->
    case TxId of
        CurrentTxId -> %% Got reply to current Tx
            MaxPrepareTime = max(CurrentTxMeta#tx_metadata.prepare_time, ReceivedPrepareTime),
            NumSpeculaPrepared = CurrentTxMeta#tx_metadata.num_specula_prepared,
            CurrentTxMeta1 = CurrentTxMeta#tx_metadata{prepare_time=MaxPrepareTime,
                         num_specula_prepared=NumSpeculaPrepared+1},
            case specula_utilities:coord_should_specula(CurrentTxMeta) of
                true ->
                    proceed_txn(S0#state{current_tx_meta=CurrentTxMeta1});
                false ->
                    {next_state, receive_reply,
                     S0#state{current_tx_meta=CurrentTxMeta1}}
            end;
        _ ->
            ok
    end;

receive_reply({prepared, TxId, ReceivedPrepareTime},
                 S0=#state{tx_id=CurrentTxId,
                           tx_id_list=TxIdList,
                           specula_meta=SpeculaMeta,
                           current_tx_meta=CurrentTxMeta}) ->
    case TxId of
        CurrentTxId ->
            MaxPrepareTime = max(CurrentTxMeta#tx_metadata.prepare_time, ReceivedPrepareTime),
            NumPrepared = CurrentTxMeta#tx_metadata.num_prepared,
            CurrentTxMeta1 = CurrentTxMeta#tx_metadata{prepare_time=MaxPrepareTime,
                         num_prepared=NumPrepared+1},
            case can_commit(CurrentTxMeta, SpeculaMeta) of
                true ->
                    CurrentTxMeta1 = commit_tx(CurrentTxId, CurrentTxMeta),
                    proceed_txn(S0#state{current_tx_meta=CurrentTxMeta1});
                false ->
                    case specula_utilities:coord_should_specula(CurrentTxMeta1) of
                        true ->
                            proceed_txn(S0#state{current_tx_meta=CurrentTxMeta1});
                        false ->
                            {next_state, receive_reply,
                             S0#state{current_tx_meta=CurrentTxMeta1}}
                    end
            end;
        _ ->
            TxMeta = dict:find(TxId, SpeculaMeta),
            MaxPrepareTime = max(TxMeta#tx_metadata.prepare_time, ReceivedPrepareTime),
            NumPrepared1 = TxMeta#tx_metadata.num_prepared + 1,
            TxMeta1 = TxMeta#tx_metadata{num_prepared=NumPrepared1, prepare_time=MaxPrepareTime},
            SpeculaMeta1 = dict:store(TxId, TxMeta1, SpeculaMeta),
            case can_commit(TxMeta1, SpeculaMeta1) of
                true -> 
                    SpeculaMeta2 = cascading_commit_tx(TxId, TxMeta#tx_metadata.index, SpeculaMeta1, TxIdList),
                    {next_state, receive_reply, S0#state{specula_meta=SpeculaMeta2}}; 
                false ->
                    {next_state, receive_reply, S0#state{specula_meta=SpeculaMeta1}} 
            end
    end;

receive_reply({read_valid, TxId, Key},
                 S0=#state{specula_meta=SpeculaMeta,
                           tx_id=CurrentTxId,
                           tx_id_list=TxIdList,
                           current_tx_meta=CurrentTxMeta}) ->
    case TxId of
        CurrentTxId ->
            ReadDep = CurrentTxMeta#tx_metadata.read_dep,
            ReadDep1 = lists:delete(Key, ReadDep),
            CurrentTxMeta1 = CurrentTxMeta#tx_metadata{read_dep=ReadDep1},
            case specula_utilities:coord_should_specula(CurrentTxMeta1) of
                true ->
                    proceed_txn(S0#state{current_tx_meta=CurrentTxMeta1});
                false ->
                    {next_state, receive_reply,
                     S0#state{current_tx_meta=CurrentTxMeta1}}
            end;
        _ ->
            TxMeta = dict:find(TxId, SpeculaMeta),
            ReadDep = TxMeta#tx_metadata.read_dep,
            ReadDep1 = lists:delete(Key, ReadDep),
            TxMeta1 = TxMeta#tx_metadata{read_dep=ReadDep1},
            SpeculaMeta1 = dict:store(TxId, TxMeta1, SpeculaMeta),
            case can_commit(TxMeta1, SpeculaMeta1) of
                true ->
                    SpeculaMeta2 = cascading_commit_tx(TxId, TxMeta#tx_metadata.index, SpeculaMeta1, TxIdList),
                    {next_state, receive_reply, S0#state{specula_meta=SpeculaMeta2}}; 
                _ -> %%Still have prepare to wait for
                   {next_state, receive_reply, S0#state{specula_meta=SpeculaMeta1}} 
            end
    end;

receive_reply({abort, TxId}, S0) ->
    cascading_abort(TxId, S0);

receive_reply(abort, S0) ->
    {next_state, abort, S0, 0}.

%% TODO: need to define better this case: is specula_committed allowed, or not?
%% Why this case brings doubt???
single_committing({committed, CommitTime}, S0=#state{from=_From, current_tx_meta=CurrentTxMeta}) ->
    proceed_txn(S0#state{current_tx_meta=CurrentTxMeta#tx_metadata{prepare_time=CommitTime},
             state=committed});
    
single_committing(abort, S0=#state{from=_From}) ->
    proceed_txn(S0#state{state=aborted}).


%% @doc when an error occurs or an updated partition 
%% does not pass the certification check, the transaction aborts.
abort(timeout, SD0=#state{tx_id = TxId,
                        current_tx_meta=CurrentTxMeta}) ->
    ?CLOCKSI_VNODE:abort(CurrentTxMeta#tx_metadata.updated_parts, TxId),
    proceed_txn(SD0#state{state=aborted});

abort(abort, SD0=#state{tx_id = TxId,
                        current_tx_meta=CurrentTxMeta}) ->
    ?CLOCKSI_VNODE:abort(CurrentTxMeta#tx_metadata.updated_parts, TxId),
    proceed_txn(SD0#state{state=aborted});

abort({prepared, _}, SD0=#state{tx_id=TxId,
                        current_tx_meta=CurrentTxMeta}) ->
    ?CLOCKSI_VNODE:abort(CurrentTxMeta#tx_metadata.updated_parts, TxId),
    proceed_txn(SD0#state{state=aborted}).

%% @doc when the transaction has committed or aborted,
%%       a reply is sent to the client that started the tx_id.
proceed_txn(S0=#state{from=From, tx_id=TxId, state=TxState, tx_id_list=TxIdList, 
            current_tx_meta=CurrentTxMeta, specula_meta=SpeculaMeta, pending_txn_ops=PendingTxnOps}) ->
    CommitTime = CurrentTxMeta#tx_metadata.prepare_time,
    case TxState of
        committed ->
            case length(PendingTxnOps) of
                1 ->
                    case CurrentTxMeta#tx_metadata.final_committed of 
                        true -> %%All transactions must have finished
                            %lager:info("Finishing txn"),
                            ReverseReadSet = get_readset(TxIdList, SpeculaMeta, []),
                            RevReadSet1 = [CurrentTxMeta#tx_metadata.read_set|ReverseReadSet],
                            From ! {ok, {TxId, lists:flatten(lists:reverse(RevReadSet1)), 
                                CommitTime}},
                            {stop, normal, S0};
                        false -> %%This is the last transaction, but not finally committed..
                            {next_state, receive_reply, S0}
                    end;
                _ -> %%Start the next transaction
                    SpeculaMeta1 = dict:store(TxId, CurrentTxMeta, SpeculaMeta),
                    [_ExecutedTxn|RestTxns] = PendingTxnOps,
                    TxIdList1 = TxIdList ++ [TxId],
                    gen_fsm:send_event(self(), process_tx),
                    {next_state, receive_reply, S0#state{pending_txn_ops=RestTxns,
                        specula_meta=SpeculaMeta1, tx_id_list=TxIdList1, causal_clock=CommitTime}}
            end;
        aborted -> %%Try current transaction again
            gen_fsm:send_event(self(), process_tx),
            {next_state, receive_reply, S0}
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

process_operations(_TxId, [], Acc) ->
    Acc;
process_operations(TxId, [{read, Key, Type}|Rest], {UpdatedParts, RSet, Buffer, ReadDep}) ->
    Reply = case dict:find(Key, Buffer) of
                    error ->
                        Preflist = ?LOG_UTIL:get_preflist_from_key(Key),
                        IndexNode = hd(Preflist),
                        ?CLOCKSI_VNODE:read_data_item(IndexNode, Key, Type, TxId);
                    {ok, SnapshotState} ->
                        {ok, {Type,SnapshotState}}
            end,
    NewReadDep = case Reply of
                    {specula, {_Type, _Snapshot}} ->
                        [Key|ReadDep];
                    _ ->
                        ReadDep
                end,
    {_, {Type, KeySnapshot}} = Reply,
    Buffer1 = dict:store(Key, KeySnapshot, Buffer),
    process_operations(TxId, Rest, {UpdatedParts, [Type:value(KeySnapshot)|RSet], Buffer1, NewReadDep});
process_operations(TxId, [{update, Key, Type, Op}|Rest], {UpdatedParts, RSet, Buffer, ReadDep}) ->
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
    process_operations(TxId, Rest, {UpdatedParts1, RSet, Buffer1, ReadDep}).

cascading_abort(TxId, #state{all_txn_ops=AllTxn, tx_id=TxId, current_tx_meta=CurrentTxMeta,
                             specula_meta=SpeculaMeta,tx_id_list=TxIdList}=S0) ->
    TxMeta = dict:find(TxId, SpeculaMeta),
    ToAbortIndex = TxMeta#tx_metadata.index-1,
    AbortTxList = lists:nthtail(ToAbortIndex, TxIdList),
    AbortFun = fun(Id, Dict) -> 
                                AbortTxMeta = dict:find(Id, Dict),  
                                UpdatedParts = AbortTxMeta#tx_metadata.updated_parts, 
                                ?CLOCKSI_VNODE:abort(UpdatedParts, Id),
                                dict:erase(Id, Dict)
                end, 
    lists:foldl(AbortFun, SpeculaMeta, AbortTxList),
    
    %Abort current transaction
    CurrentUpdatedParts = CurrentTxMeta#tx_metadata.updated_parts, 
    ?CLOCKSI_VNODE:abort(CurrentUpdatedParts, TxId),

    PendingTxnOps = list:nthtail(ToAbortIndex, AllTxn), 
    %%Tag
    gen_fsm:send_event(self(), process_tx),
    {next_state, receive_reply, S0#state{pending_txn_ops=PendingTxnOps, 
                tx_id_list=lists:sublist(ToAbortIndex, TxIdList)}}.


cascading_commit_tx(TxId, Index, SpeculaMeta, TxIdList) ->
    TxMeta = dict:find(TxId, SpeculaMeta),
    TxMeta1 = commit_tx(TxId, TxMeta),
    SpeculaMeta1 = dict:store(TxId, TxMeta1, SpeculaMeta),
    try_commit_successors(lists:nth(Index,TxIdList), SpeculaMeta1).

commit_tx(TxId, TxMeta) ->
    UpdatedParts = TxMeta#tx_metadata.updated_parts,
    ?CLOCKSI_VNODE:commit(UpdatedParts, TxId, TxMeta#tx_metadata.prepare_time),
    TxMeta#tx_metadata{final_committed=true}. 

try_commit_successors([], SpeculaMetadata) ->
    SpeculaMetadata;
try_commit_successors([TxId|Rest], SpeculaMetadata) ->
    TxMeta = dict:find(TxId, SpeculaMetadata),
    ReadDep = TxMeta#tx_metadata.read_dep,
    case ReadDep of
        [] ->
            NumUpdated = TxMeta#tx_metadata.read_dep,
            case TxMeta#tx_metadata.num_prepared of
                NumUpdated ->
                    UpdatedParts = TxMeta#tx_metadata.updated_parts,
                    ?CLOCKSI_VNODE:commit(UpdatedParts, TxId, TxMeta#tx_metadata.prepare_time),
                    SpeculaMetadata1 = dict:store(TxId, 
                        TxMeta#tx_metadata{final_committed=true}, SpeculaMetadata),
                    try_commit_successors(Rest, SpeculaMetadata1);
                _ ->
                    SpeculaMetadata
            end;
        _ ->
            SpeculaMetadata
    end.

can_commit(TxMeta, SpeculaMeta) ->
    NumberUpdated = TxMeta#tx_metadata.num_updated,
    case TxMeta#tx_metadata.read_dep of
        [] -> %%No read dependency
            case TxMeta#tx_metadata.num_prepared of
                NumberUpdated ->
                    Index = TxMeta#tx_metadata.index,
                    case Index of 
                        1 ->
                            true; 
                        _ ->
                            DependentTx = dict:find(Index-1, SpeculaMeta),
                            DependentTx#tx_metadata.final_committed
                    end;
                _ ->
                    false
            end;
        _ ->
            false
    end.

get_readset([], _SpeculaMeta, Acc) ->
    Acc;
get_readset([H|T], SpeculaMeta, Acc) ->
    Tx = dict:find(H, SpeculaMeta),
    get_readset(T, SpeculaMeta, [Tx#tx_metadata.read_set|Acc]).
