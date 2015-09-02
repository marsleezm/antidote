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
%% @doc The coordinator for a given Clock SI static transaction.
%%      It handles the state of the tx and executes the operations sequentially
%%      by sending each operation to the responsible clockSI_vnode of the
%%      involved key. When a tx is finalized (committed or aborted), the fsm
%%      also finishes.

-module(clocksi_static_tx_coord_fsm).

-behavior(gen_fsm).

-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(DC_UTIL, mock_partition_fsm).
-define(LOG_UTIL, mock_partition_fsm).
-define(CLOCKSI_VNODE, mock_partition_fsm).
-define(CLOCKSI_DOWNSTREAM, mock_partition_fsm).
-else.
-define(DC_UTIL, dc_utilities).
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
         execute_batch_ops/2,
         receive_prepared/2,
         single_committing/2,
         committing/2,
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
-record(state, {
	  from :: pid(),
	  tx_id :: txid(),
      operations ::  [{read, key(), type()}| {update, key(), type(), term()}],
	  num_to_ack :: non_neg_integer(),
	  num_to_read :: non_neg_integer(),
	  prepare_time :: non_neg_integer(),
	  commit_time :: non_neg_integer(),
      updated_partitions :: dict(),
      read_set :: [term()],
	  state :: active | prepared | committing | committed | undefined | aborted}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(From, Clientclock, Operations) ->
    gen_fsm:start_link(?MODULE, [From, Clientclock, Operations], []).

start_link(From, Operations) ->
    gen_fsm:start_link(?MODULE, [From, 0, Operations], []).

stop(Pid) -> gen_fsm:sync_send_all_state_event(Pid,stop).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state.
init([From, ClientClock, Operations]) ->
    TxId = tx_utilities:create_transaction_record(ClientClock),
    SD = #state{
            tx_id = TxId,
            read_set = [],
            prepare_time = 0,
            num_to_read=0,
            num_to_ack=0,
            operations = Operations,
            updated_partitions = dict:new(),
            from = From
           },
    {ok, execute_batch_ops, SD, 0}.


%% @doc Contact the leader computed in the prepare state for it to execute the
%%      operation, wait for it to finish (synchronous) and go to the prepareOP
%%      to execute the next operation.
execute_batch_ops(timeout, SD=#state{
                                     tx_id = TxId,
                                     operations = Operations}) ->
    ProcessOp = fun(Operation, {UpdatedPartitions, NumToRead}) ->
                    case Operation of
                        {read, Key, Type} ->
                            Preflist = ?LOG_UTIL:get_preflist_from_key(Key),
                            IndexNode = hd(Preflist),
                            %%lager:info("NumToRead count: ~w",[NumToRead+1]),
                            ok = ?CLOCKSI_VNODE:async_read_data_item(IndexNode, Key, Type, TxId),
                            {UpdatedPartitions, NumToRead+1};
                        {update, Key, Type, Op} ->
                            Preflist = ?LOG_UTIL:get_preflist_from_key(Key),
                            IndexNode = hd(Preflist),
                            UpdatedPartitions1 =  dict:append(IndexNode, {Key, Type, Op}, 
                                                            UpdatedPartitions),
                            {UpdatedPartitions1, NumToRead}
                    end
                end,
    {WriteSet, NumOfReads} = lists:foldl(ProcessOp, {dict:new(),0}, Operations),
    %lager:info("Operations are ~w, WriteSet is ~w, NumOfReads ~w",[Operations, WriteSet, NumOfReads]),
    case dict:size(WriteSet) of
        0->
            case NumOfReads of
                0 ->
                    reply_to_client(SD#state{state=committed, 
                            commit_time=clocksi_vnode:now_microsec(erlang:now())});
                _ ->
                    %%lager:info("Waiting for ~w reads to reply", [NumOfReads]),
                    Snapshot_time=TxId#tx_id.snapshot_time,
                    {next_state, single_committing, SD#state{state=committing, num_to_ack=0, 
                        commit_time=Snapshot_time, num_to_read=NumOfReads}}
            end;
        1->
            UpdatedPart = dict:to_list(WriteSet),
            %lists:foldl(fun(X, Acc) -> {Part, K:eys}= X,
            %                            Acc++[{Part, [Key || {Key, _Type, _Op} <- Keys]} ]
            %            end, [], UpdatedPart),
            ?CLOCKSI_VNODE:single_commit(UpdatedPart, TxId),
            {next_state, single_committing,
            SD#state{state=committing, num_to_ack=1, num_to_read=NumOfReads}};
        N->
            ?CLOCKSI_VNODE:prepare(WriteSet, TxId),
            {next_state, receive_prepared, SD#state{num_to_ack=N, state=prepared, 
                num_to_read=NumOfReads, updated_partitions=WriteSet}}
    end.
    %{next_state, prepare, SD#state{read_set=ReadSet, updated_partitions=UpdatedPartitions}, 0}

%% @doc in this state, the fsm waits for prepare_time from each updated
%%      partitions in order to compute the final tx timestamp (the maximum
%%      of the received prepare_time).
receive_prepared({prepared, _, ReceivedPrepareTime},
                 S0=#state{num_to_ack=NumToAck,
                           num_to_read=NumToRead,
                           prepare_time=PrepareTime}) ->
    MaxPrepareTime = max(PrepareTime, ReceivedPrepareTime),
    case NumToAck of 
        1 ->
            case NumToRead of
                0 ->
                    {next_state, committing, S0#state{prepare_time=MaxPrepareTime, 
                        commit_time=MaxPrepareTime, state=committing}, 0};
                _ ->
                    {next_state, receive_prepared, S0#state{num_to_ack= NumToAck-1, 
                        commit_time=MaxPrepareTime, prepare_time=MaxPrepareTime}}
            end;
        _ ->
            {next_state, receive_prepared,
             S0#state{num_to_ack= NumToAck-1, prepare_time=MaxPrepareTime}}
    end;

receive_prepared({ok, {Type,Snapshot}},
                 S0=#state{num_to_read=NumToRead,
                            read_set=ReadSet,
                            num_to_ack=NumToAck}) ->
    ReadSet1 = ReadSet ++ [Type:value(Snapshot)],
    case NumToRead of 
        1 ->
            case NumToAck of
                0 ->
                    {next_state, committing, S0#state{state=committing, read_set=ReadSet1}, 0};
                _ ->
                    {next_state, receive_prepared, S0#state{num_to_read= NumToRead-1,
                            read_set=ReadSet1}}
            end;
        _ ->
            {next_state, receive_prepared,
             S0#state{read_set=ReadSet1, num_to_read = NumToRead-1}}
    end;

receive_prepared({abort, _}, S0) ->
    {next_state, abort, S0, 0};

receive_prepared(timeout, S0) ->
    {next_state, abort, S0, 0}.

single_committing({ok, {Type, Snapshot}},
                 S0=#state{num_to_read=NumToRead,
                            read_set=ReadSet,
                            num_to_ack=NumToAck}) ->
    %%lager:info("Got some replies ~w", [Type:value(Snapshot)]),
    ReadSet1 = ReadSet ++ [Type:value(Snapshot)],
    case NumToRead of 
        1 ->
            case NumToAck of
                0 -> 
                    reply_to_client(S0#state{read_set=ReadSet1, state=committed});
                1 ->
                    {next_state, single_committing, S0#state{num_to_read=NumToRead-1, 
                            read_set=ReadSet1}}
            end;
        _ ->
            {next_state, single_committing, S0#state{num_to_read=NumToRead-1, read_set=ReadSet1}}
    end;

single_committing({committed, CommitTime}, S0=#state{from=_From, num_to_read=NumToRead}) ->
    case NumToRead of 
        0 ->
            reply_to_client(S0#state{prepare_time=CommitTime, commit_time=CommitTime, state=committed});
        _ ->
            {next_state, single_committing, S0#state{num_to_ack=0, prepare_time=CommitTime,
                        commit_time=CommitTime}}
    end;
    
single_committing({abort, _}, S0=#state{from=_From}) ->
    reply_to_client(S0#state{state=aborted}).

%% @doc after receiving all prepare_times, send the commit message to all
%%      updated partitions, and go to the "receive_committed" state.
%%      This state is used when no commit message from the client is
%%      expected 
committing(timeout, S0=#state{tx_id = TxId,
                              updated_partitions=UpdatedPartitions,
                              commit_time=Commit_time}) ->
    case dict:size(UpdatedPartitions) of
        0 ->
            %%lager:info("Replying directly"),
            reply_to_client(S0#state{state=committed});
        _N ->
            %%lager:info("Committing"),
            ?CLOCKSI_VNODE:commit(UpdatedPartitions, TxId, Commit_time),
            reply_to_client(S0#state{state=committed})
    end.


%% @doc when an error occurs or an updated partition 
%% does not pass the certification check, the transaction aborts.
abort(timeout, SD0=#state{tx_id = TxId,
                          updated_partitions=UpdatedPartitions}) ->
    ?CLOCKSI_VNODE:abort(UpdatedPartitions, TxId),
    reply_to_client(SD0#state{state=aborted});

abort({abort, _}, SD0=#state{tx_id = TxId,
                        updated_partitions=UpdatedPartitions}) ->
    ?CLOCKSI_VNODE:abort(UpdatedPartitions, TxId),
    reply_to_client(SD0#state{state=aborted});

abort({prepared, _, _}, SD0=#state{tx_id=TxId,
                        updated_partitions=UpdatedPartitions}) ->
    ?CLOCKSI_VNODE:abort(UpdatedPartitions, TxId),
    reply_to_client(SD0#state{state=aborted});

abort({ok, _}, SD0=#state{tx_id=TxId,
                        updated_partitions=UpdatedPartitions}) ->
    ?CLOCKSI_VNODE:abort(UpdatedPartitions, TxId),
    reply_to_client(SD0#state{state=aborted}).

%% @doc when the transaction has committed or aborted,
%%       a reply is sent to the client that started the transaction.
reply_to_client(SD=#state{from=From, tx_id=TxId, read_set=ReadSet,
                                   state=TxState, commit_time=CommitTime}) ->
    
     _ = if undefined =/= From ->
            Reply = case TxState of
                        committed ->
                            CausalClock = CommitTime,
                            {ok, {TxId, ReadSet, CausalClock}};
                        aborted->
                            {error, commit_fail};
                        Reason->
                            {TxId, Reason}
                    end,
            From ! Reply;
        true ->
            ok
    end,
    {stop, normal, SD}.

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
