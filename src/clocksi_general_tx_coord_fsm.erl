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

-define(SPECULA_TIMEOUT, 0).

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
-export([execute_batch_ops/2,
         finish_op/3,
         receive_prepared/2,
         single_committing/2,
         committing/2,
         receive_committed/2,
         abort/2,
         reply_to_client/1]).

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
-record(txn_metadata, {
        read_dep :: non_neg_integer(),
        update_dep :: non_neg_integer(),
        prepare_time :: non_neg_integer(),
        committed :: boolean(),
        updated_parts
        }).


-record(state, {
      %% Metadata for all transactions
	  from :: {pid(), term()},
      tx_id_list :: [txid()],
      all_txn :: [],
      remaining_txn :: [],
      all_updated_parts :: [],
      read_depdict :: dict(),
      prepare_depdict :: dict(),
      read_set :: [],
      %% Metadata for a single txn
	  tx_id :: txid(),
	  num_to_ack :: non_neg_integer(),
	  prepare_time :: non_neg_integer(),
	  commit_time :: non_neg_integer(),
      updated_partitions :: dict(),
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
init([From, ClientClock, Operations]) ->
    SD = #state{
            causal_clock = ClientClock,
            all_txn = Operations,
            remaining_txn = Operations,
            updated_partitions = dict:new(),
            read_set = [],
            from = From,
            read_depdict = dict:new(),
            prepare_depdict = dict:new(),
            prepare_time=0
           },
    {ok, execute_batch_ops, SD, 0}.

%% @doc Contact the leader computed in the prepare state for it to execute the
%%      operation, wait for it to finish (synchronous) and go to the prepareOP
%%       to execute the next operation.
execute_batch_ops(timeout, 
           SD=#state{causal_clock=CausalClock,
                    read_set=ReadSet,
                    read_depdict=ReadDepDict,
                    prepare_depdict=PrepareDepDict,
                    all_updated_parts=AllUpdatedParts,
                    tx_id_list=TxIdList,
                    remaining_txn=RemainingTxn
		      }) ->
    TxId = tx_utilities:create_transaction_record(CausalClock),
    [CurrentTxn|_RestTxn] = RemainingTxn,
    {WriteSet1, ReadSet1, _, MyReadDep} = process_operations(TxId, CurrentTxn,
                {dict:new(), ReadSet, dict:new(), []}),
    ReadDepDict1 = dict:store(TxId, MyReadDep, ReadDepDict),
    PrepareDepDict1 = dict:store(TxId, {0, dict:size(WriteSet1)}, PrepareDepDict),
    AllUpdatedParts1 = AllUpdatedParts ++ [WriteSet1], 
    TxIdList1 = TxIdList ++ [TxId],
    case dict:size(WriteSet1) of
        0->
            reply_to_client(SD#state{state=committed, read_depdict=ReadDepDict1, 
                prepare_depdict=PrepareDepDict1, tx_id_list=TxIdList1, 
                commit_time=clocksi_vnode:now_microsec(now())});
        1->
            UpdatedPart = dict:to_list(WriteSet1),
            ?CLOCKSI_VNODE:single_commit(UpdatedPart, TxId),
            {next_state, single_committing,
            SD#state{state=committing, num_to_ack=1, read_depdict=ReadDepDict1, all_updated_parts=AllUpdatedParts1,
                prepare_depdict=PrepareDepDict1, tx_id_list=TxIdList1, read_set=ReadSet1}};
        N->
            ?CLOCKSI_VNODE:prepare(WriteSet1, TxId),
            {next_state, receive_prepared, SD#state{num_to_ack=N, state=prepared, all_updated_parts=AllUpdatedParts1,
                read_depdict=ReadDepDict1,  prepare_depdict=PrepareDepDict1, tx_id_list=TxIdList1, updated_partitions=WriteSet1, 
                read_set=ReadSet1}, ?SPECULA_TIMEOUT}
    end.

%% @doc in this state, the fsm waits for prepare_time from each updated
%%      partitions in order to compute the final tx timestamp (the maximum
%%      of the received prepare_time).
receive_prepared(timeout,
                 S0=#state{num_to_ack=NumToAck, remaining_txn=RemainingTxn,
                            dependency=Dependency, tx_id=TxId}) ->
    {ok, MyDependency} = dict:find(TxId, Dependency),  
    case specula_utilities:coord_should_specula(RemainingTxn, 
                NumToAck, MyDependency) of
        true ->
            specula_utilities:record_specula_meta();
        false ->
            {next_state, receive_prepared, S0}
    end;

receive_prepared({specula_prepared, TxId, Partition, ReceivedPrepareTime},
                 S0=#state{num_to_ack=NumToAck,
                            dependency=Dependency,
                            tx_id=CurrentTxId,
                            prepare_time=PrepareTime}) ->
    case TxId of
        CurrentTxId -> %% Got reply to current Tx
            MaxPrepareTime = max(PrepareTime, ReceivedPrepareTime),
            {ReadDep, UpdateDep} = dict:find(TxId, Dependency),
            NewDependency = dict:store(TxId, {ReadDep, [Partition|UpdateDep]}, Dependency),
            case NumToAck of 
                1 ->
                    {next_state, committing,
                        S0#state{prepare_time=MaxPrepareTime, commit_time=MaxPrepareTime, 
                            dependency=NewDependency, state=committing}, 0};
                _ ->
                    {next_state, receive_prepared,
                     S0#state{num_to_ack= NumToAck-1, dependency=NewDependency, prepare_time=MaxPrepareTime}}
            end;
        _ ->
            ok
    end;

receive_prepared({prepared, TxId, ReceivedPrepareTime},
                 S0=#state{num_to_ack=NumToAck,
                            tx_id=CurrentTxId,
                            prepare_dep=PrepareDep,
                            read_dep=ReadDep,
                            prepare_time=PrepareTime}) ->
    case TxId of
        CurrentTxId ->
            MaxPrepareTime = max(PrepareTime, ReceivedPrepareTime),
            case NumToAck of 
                1 ->
                    {next_state, committing,
                        S0#state{prepare_time=MaxPrepareTime, commit_time=MaxPrepareTime, state=committing}, 0};
                _ ->
                    {next_state, receive_prepared,
                     S0#state{num_to_ack= NumToAck-1, prepare_time=MaxPrepareTime}}
            end;
        _ ->
           {PrepareTime, MyPrepareDep} = dict:find(TxId, PrepareDep), 
           PrepareDep1 = dict:store(TxId, {max(ReceivedPrepareTime, PrepareTime), MyPrepareDep-1}, PrepareDep),
            case MyPrepareDep of
                1 -> %No pending prepare or specula_prepare
                    case dict:find(TxId, ReadDep) of
                        [] -> %%Send the commit message of this transaction to its partitions
                            commitTx();
                        _ ->
                           {next_state, receive_prepared, S0#state{prepare_dep=PrepareDep1}} 
                    end;
                _ -> %%Still have prepare to wait for
                   {next_state, receive_prepared, S0#state{prepare_dep=PrepareDep1}} 
    end;

receive_prepared({read_valid, TxId, Key},
                 S0=#state{num_to_ack=NumToAck,
                            tx_id=CurrentTxId,
                            prepare_depdict=PrepareDepDict,
                            read_depdict=ReadDepDict}) ->
    MyReadDep = dict:find(TxId, ReadDepDict),     
    MyReadDep1 = lists:delete(Key, MyReadDep),
    case length(MyReadDep1) of
        0 ->
            case dict:find(TxId, PrepareDep) of
                {CommitTime, 0} ->
                    commitTx();
                {PreparedTime, _} ->
                   {next_state, receive_prepared, S0#state{read_dep=dict:store(TxId, [], ReadDepDict)}} 
            end;
        _ ->
            {next_state, receive_prepared, S0#state{read_dep=dict:store(TxId, MyReadDep1, ReadDepDict)}} 
    end;

receive_prepared({abort, TxId}, S0) ->
    cascading_abort(TxId, S0);

receive_prepared(abort, S0) ->
    {next_state, abort, S0, 0}.

single_committing({committed, CommitTime}, S0=#state{from=_From}) ->
    reply_to_client(S0#state{prepare_time=CommitTime, commit_time=CommitTime, state=committed});
    
single_committing(abort, S0=#state{from=_From}) ->
    reply_to_client(S0#state{state=aborted}).

%% @doc after receiving all prepare_times, send the commit message to all
%%      updated partitions, and go to the "receive_committed" state.
%%      This state is used when no commit message from the client is
%%      expected 
committing(timeout, SD0=#state{tx_id = TxId,
                              updated_partitions=UpdatedPartitions,
                              commit_time=Commit_time}) ->
    case dict:size(UpdatedPartitions) of
        0 ->
            reply_to_client(SD0#state{state=committed});
        N ->
            ?CLOCKSI_VNODE:commit(UpdatedPartitions, TxId, Commit_time),
            {next_state, receive_committed,
             SD0#state{num_to_ack=N, state=committing}}
    end.


%% @doc the fsm waits for acks indicating that each partition has successfully
%%	committed the tx and finishes operation.
%%      Should we retry sending the committed message if we don't receive a
%%      reply from every partition?
%%      What delivery guarantees does sending messages provide?
receive_committed(committed, S0=#state{num_to_ack= NumToAck}) ->
    case NumToAck of
        1 ->
            reply_to_client(S0#state{state=committed});
        _ ->
           {next_state, receive_committed, S0#state{num_to_ack= NumToAck-1}}
    end.

%% @doc when an error occurs or an updated partition 
%% does not pass the certification check, the transaction aborts.
abort(timeout, SD0=#state{tx_id = TxId,
                          updated_partitions=UpdatedPartitions}) ->
    ?CLOCKSI_VNODE:abort(UpdatedPartitions, TxId),
    reply_to_client(SD0#state{state=aborted});

abort(abort, SD0=#state{tx_id = TxId,
                        updated_partitions=UpdatedPartitions}) ->
    ?CLOCKSI_VNODE:abort(UpdatedPartitions, TxId),
    reply_to_client(SD0#state{state=aborted});

abort({prepared, _}, SD0=#state{tx_id=TxId,
                        updated_partitions=UpdatedPartitions}) ->
    ?CLOCKSI_VNODE:abort(UpdatedPartitions, TxId),
    reply_to_client(SD0#state{state=aborted}).

%% @doc when the transaction has committed or aborted,
%%       a reply is sent to the client that started the tx_id.
reply_to_client(SD=#state{from=From, tx_id=TxId, state=TxState, read_set=ReadSet,
                commit_time=CommitTime, remaining_txn=RemainingTxn}) ->
    case TxState of
        committed ->
            case length(RemainingTxn) of 
                1 ->
                    %lager:info("Finishing txn"),
                    From ! {ok, {TxId, lists:reverse(ReadSet), CommitTime}},
                    {stop, normal, SD};
                _ ->
                    %lager:info("Continuing executing txn"),
                    [_ExecutedTxn|RestTxns] = RemainingTxn,
                    {next_state, execute_batch_ops, SD#state{remaining_txn=RestTxns,
                        causal_clock=CommitTime}, 0}
            end;
        aborted ->
            %lager:info("Retrying txn"),
            {next_state, execute_batch_ops, SD, 0}
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

cascading_abort(TxId, #state{all_updated_parts=AllUpdatedParts, prepare_depdict=PrepareDepDict,
                        read_depdict=ReadDepDict, all_txn=AllTxn, tx_id_list=TxIdList}=S0) ->
    SuccessorTxs = get_successors(TxId, TxIdList), 
    AbortFun = fun(Tx) -> MyUpdatedParts = dict:find(Tx, AllUpdatedParts),
                            ?CLOCKSI_VNODE:abort(MyUpdatedParts, Tx)
                end, 
    lists:map(AbortFun, SuccessorTxs),
    prune_states(SuccessorTxs, ReadDepDict, PrepareDepDict),

    RemainingTx = list:nthtail(length(TxIdList) - length(SuccessorTxs), AllTxn), 
    {next_state, execute_batch_ops, S0#state{remaining_txn=RemainingTx}, 0}.
        
get_successors(Elem, [Elem|Tail]) ->
    Tail;
get_successors(Elem, [_|Tail]) ->
    get_successors(Elem, Tail);
get_successors(_Elem, []) ->
    [].
    
prune_states(TxList, ReadDepDict, PrepareDepDict) ->
    lists:foldl(fun(Tx, {Dict1, Dict2}) -> {dict:erase(Tx, Dict1), dict:erase(Tx,Dict2)} end, 
            {ReadDepDict, PrepareDepDict}, TxList). 


