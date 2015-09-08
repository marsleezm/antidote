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
-export([start_execute_txns/2,
         finish_op/3,
         receive_prepared/2,
         single_committing/2,
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
-record(state, {
	    from :: {pid(), term()},
	    tx_id :: txid(),
        operations :: [],
	    num_to_ack :: non_neg_integer(),
	    prepare_time :: non_neg_integer(),
	    commit_time :: non_neg_integer(),
        updated_partitions :: dict(),
        final_read_set :: [],
        read_set :: [],
        causal_clock :: non_neg_integer(),
	    state :: active | prepared | committing | committed | undefined | aborted,
        prepare_begin=0 :: non_neg_integer(),
        prepare_stat=[] :: [],
        read_stat=[] :: []}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(From, Clientclock, Operations) ->
    gen_fsm:start_link(?MODULE, [From, Clientclock, Operations], []).

start_link(From, Operations) ->
    gen_fsm:start_link(?MODULE, [From, 0, Operations], []).

finish_op(From, Key,Result) ->
    gen_fsm:send_event(From, {Key, Result}).

stop(Pid) -> gen_fsm:sync_send_all_state_event(Pid,stop).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state.
init([From, ClientClock, Operations]) ->
    %lager:info("Initing"),
    random:seed(now()),
    SD = #state{
            causal_clock = ClientClock,
            operations = Operations,
            updated_partitions = dict:new(),
            final_read_set=[],
            read_set = [],
            from = From,
            prepare_time=0
           },
    {ok, start_execute_txns, SD, 0}.


%% @doc Contact the leader computed in the prepare state for it to execute the
%%      operation, wait for it to finish (synchronous) and go to the prepareOP
%%       to execute the next operation.
start_execute_txns(timeout, SD) ->
    execute_batch_ops(SD).

execute_batch_ops(SD=#state{causal_clock=CausalClock,
                    operations=Operations
		      }) ->
    %lager:info("In execute batch"),
    TxId = tx_utilities:create_transaction_record(CausalClock),
    %lager:info("My tx id is ~w", [TxId]),
    [CurrentOps|_RestOps] = Operations, 
    ProcessOp = fun(Operation, {UpdatedParts, RSet, Buffer, ReadStat, OldPrepareBegin}) ->
                    case Operation of
                        {read, Key, Type} ->
                            ReadBegin = clocksi_vnode:now_microsec(now()),
                            {ok, {Type, Snapshot}} = case dict:find(Key, Buffer) of
                                                    error ->
                                                        Preflist = ?LOG_UTIL:get_preflist_from_key(Key),
                                                        IndexNode = hd(Preflist),
                                                        ?CLOCKSI_VNODE:read_data_item(IndexNode, Key, Type, TxId);
                                                    {ok, SnapshotState} ->
                                                        {ok, {Type,SnapshotState}}
                                                    end,
                            ReadLength = clocksi_vnode:now_microsec(now()) - ReadBegin,
                            Buffer1 = dict:store(Key, Snapshot, Buffer),
                            %%lager:info("New read set is ~w",[RSet]),
                            {UpdatedParts, [Type:value(Snapshot)|RSet], 
                                Buffer1, [ReadLength|ReadStat], OldPrepareBegin};
                        {update, Key, Type, Op} ->
                            Preflist = ?LOG_UTIL:get_preflist_from_key(Key),
                            IndexNode = hd(Preflist),
                            PrepareBegin = clocksi_vnode:now_microsec(now()),
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
                            {UpdatedParts1, RSet, Buffer1, ReadStat, PrepareBegin}
                    end
                end,
    {WriteSet1, ReadSet1, _, ReadStat, PrepareBegin} = 
                lists:foldl(ProcessOp, {dict:new(), [], dict:new(), [], 0}, CurrentOps),
    %%lager:info("Operations are ~w, WriteSet is ~w, ReadSet is ~w",[CurrentOps, WriteSet1, ReadSet1]),
    case dict:size(WriteSet1) of
        0->
            reply_to_client(SD#state{state=committed, tx_id=TxId, read_set=ReadSet1, 
                commit_time=clocksi_vnode:now_microsec(now()), 
                read_stat=lists:reverse(ReadStat), prepare_begin=PrepareBegin});
        1->
            UpdatedPart = dict:to_list(WriteSet1),
            ?CLOCKSI_VNODE:single_commit(UpdatedPart, TxId),
            {next_state, single_committing,
            SD#state{state=committing, num_to_ack=1, read_set=ReadSet1, tx_id=TxId,
                read_stat=lists:reverse(ReadStat), prepare_begin=PrepareBegin}};
        N->
            %lager:info("Txn ~w , write set size ~w",[TxId, N]),
            ?CLOCKSI_VNODE:prepare(WriteSet1, TxId),
            {next_state, receive_prepared, SD#state{num_to_ack=N, state=prepared,
                 updated_partitions=WriteSet1, read_set=ReadSet1, tx_id=TxId,
                read_stat=lists:reverse(ReadStat), prepare_begin=PrepareBegin}}
    end.

%% @doc in this state, the fsm waits for prepare_time from each updated
%%      partitions in order to compute the final tx timestamp (the maximum
%%      of the received prepare_time).
receive_prepared({prepared, TxId, ReceivedPrepareTime},
                 S0=#state{num_to_ack=NumToAck, tx_id=TxId, prepare_time=PrepareTime,
                            prepare_begin=PrepareBegin, prepare_stat=PrepareStat}) ->
    %lager:info("Got prepared of ~w", [TxId]),
    MaxPrepareTime = max(PrepareTime, ReceivedPrepareTime),
    case NumToAck of 
        1 ->
            send_commit(S0#state{prepare_time=MaxPrepareTime, commit_time=MaxPrepareTime, 
                    state=committing, 
                prepare_stat=[(clocksi_vnode:now_microsec(now())-PrepareBegin)|PrepareStat]});
        _ ->
            %%lager:info("Txn ~w , Got reply ~w, NumtoAck ~w",[TxId, ReceivedPrepareTime, NumToAck]),
            {next_state, receive_prepared,
             S0#state{num_to_ack= NumToAck-1, prepare_time=MaxPrepareTime,
                prepare_stat=[(clocksi_vnode:now_microsec(now())-PrepareBegin)|PrepareStat]}}
    end;

receive_prepared({prepared, _, _}, S0) ->
    {next_state, receive_prepared, S0};

receive_prepared({abort, TxId}, S0=#state{tx_id=TxId}) ->
    abort(S0);

receive_prepared({abort, _}, S0) ->
    {next_state, receive_prepared, S0};

receive_prepared(timeout, S0) ->
    abort(S0).

single_committing({committed, CommitTime}, S0=#state{from=_From}) ->
    reply_to_client(S0#state{prepare_time=CommitTime, commit_time=CommitTime, state=committed});
    
single_committing({abort, TxId}, S0=#state{from=_From, tx_id=TxId}) ->
    abort(S0);

single_committing(_, S0=#state{from=_From}) ->
    reply_to_client(S0#state{state=aborted}).

%% @doc after receiving all prepare_times, send the commit message to all
%%      updated partitions, and go to the "receive_committed" state.
%%      This state is used when no commit message from the client is
%%      expected 
send_commit(SD0=#state{tx_id = TxId,
                              updated_partitions=UpdatedPartitions,
                              commit_time=Commit_time}) ->
    %lager:info("Trying to commit ~w",[TxId]),
    case dict:size(UpdatedPartitions) of
        0 ->
            reply_to_client(SD0#state{state=committed});
        _ ->
            ?CLOCKSI_VNODE:commit(UpdatedPartitions, TxId, Commit_time),
            reply_to_client(SD0#state{state=committed})
    end.



%% @doc when an error occurs or an updated partition 
%% does not pass the certification check, the transaction aborts.
abort(SD0=#state{tx_id = TxId, updated_partitions=UpdatedPartitions}) ->
    ?CLOCKSI_VNODE:abort(UpdatedPartitions, TxId),
    reply_to_client(SD0#state{state=aborted}).


%% @doc when the transaction has committed or aborted,
%%       a reply is sent to the client that started the tx_id.
reply_to_client(SD=#state{from=From, tx_id=TxId, state=TxState, read_set=ReadSet,
                final_read_set=FinalReadSet, read_stat=ReadStat, prepare_stat=PrepareStat,
                commit_time=CommitTime, operations=Operations}) ->
    case TxState of
        committed ->
            case length(Operations) of 
                1 ->
                    RRSet = lists:reverse(lists:flatten([ReadSet|FinalReadSet])),
                    %lager:info("Replying to clinet"),
                    From ! {ok, {TxId, RRSet, CommitTime}},
                    stat_server:send_stat(ReadStat, lists:reverse(PrepareStat)),
                    {stop, normal, SD};
                _ ->
                    %lager:info("Continuing"),
                    [_ExecutedOps|RestOps] = Operations,
                    execute_batch_ops(SD#state{operations=RestOps,
                        final_read_set=[ReadSet|FinalReadSet], causal_clock=CommitTime})
            end;
        aborted ->
            timer:sleep(random:uniform(10)),
            execute_batch_ops(SD)
            %From ! {error, commit_fail},
            %{stop, normal, SD}
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

