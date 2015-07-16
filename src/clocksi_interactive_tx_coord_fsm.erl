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
%% @doc The coordinator for a given Clock SI interactive tx_id.
%%      It handles the state of the tx and executes the operations sequentially
%%      by sending each operation to the responsible clockSI_vnode of the
%%      involved key. when a tx is finalized (committed or aborted, the fsm
%%      also finishes.

-module(clocksi_interactive_tx_coord_fsm).

-behavior(gen_fsm).

-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(DC_UTIL, mock_partition_fsm).
-define(VECTORCLOCK, mock_partition_fsm).
-define(LOG_UTIL, mock_partition_fsm).
-define(CLOCKSI_VNODE, mock_partition_fsm).
-define(CLOCKSI_DOWNSTREAM, mock_partition_fsm).
-else.
-define(DC_UTIL, dc_utilities).
-define(VECTORCLOCK, vectorclock).
-define(LOG_UTIL, log_utilities).
-define(CLOCKSI_VNODE, clocksi_vnode).
-define(CLOCKSI_DOWNSTREAM, clocksi_downstream).
-endif.


%% API
-export([start_link/2, start_link/1]).

%% Callbacks
-export([init/1,
         stop/1,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).

%% States
-export([execute_op/3,
         finish_op/3,
         prepare/2,
         prepare_2pc/2,
         receive_prepared/2,
         single_committing/2,
         committing_2pc/3,
         committing/2,
         receive_committed/2,
         abort/2,
	     perform_singleitem_read/2,
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
	  %%updated_partitions :: list(),
	  num_to_ack :: non_neg_integer(),
	  prepare_time :: non_neg_integer(),
	  commit_time :: non_neg_integer(),
	  commit_protocol :: term(),
      updated_partitions :: dict(),
      read_set :: dict(),
	  state :: active | prepared | committing | committed | undefined | aborted}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(From, Clientclock) ->
    gen_fsm:start_link(?MODULE, [From, Clientclock], []).

start_link(From) ->
    gen_fsm:start_link(?MODULE, [From, ignore], []).

finish_op(From, Key,Result) ->
    gen_fsm:send_event(From, {Key, Result}).

stop(Pid) -> gen_fsm:sync_send_all_state_event(Pid,stop).

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state.
init([From, ClientClock]) ->
    TxId = tx_utilities:create_transaction_record(ClientClock),
    SD = #state{
            tx_id = TxId,
            updated_partitions = dict:new(),
            read_set = dict:new(),
            prepare_time=0
           },
    From ! {ok, TxId},
    {ok, execute_op, SD}.

-spec perform_singleitem_read(key(),type()) -> {ok,val()} | {error,reason()}.
perform_singleitem_read(Key,Type) ->
    TxId = tx_utilities:create_transaction_record(ignore),
    Preflist = ?LOG_UTIL:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    case ?CLOCKSI_VNODE:read_data_item(IndexNode, Key, Type, TxId) of
	error ->
	    {error, unknown};
	{error, Reason} ->
	    {error, Reason};
	{ok, {Type, Snapshot}} ->
	    ReadResult = Type:value(Snapshot),
	    {ok, ReadResult}
    end.


%% @doc Contact the leader computed in the prepare state for it to execute the
%%      operation, wait for it to finish (synchronous) and go to the prepareOP
%%       to execute the next operation.
execute_op({Op_type, Args}, Sender,
           SD0=#state{tx_id=TxId,
                    updated_partitions=UpdatedPartitions,
                    read_set=ReadSet
		      }) ->
    case Op_type of
        prepare ->
            case Args of
            two_phase ->
                %lager:info("Received prepare.. Two-phase prepare"),
                {next_state, prepare_2pc, SD0#state{from=Sender, commit_protocol=Args}, 0};
            _ ->
                %lager:info("Received prepare.. Normal prepare"),
                {next_state, prepare, SD0#state{from=Sender, commit_protocol=Args}, 0}
            end;
        read ->
            {Key, Type}=Args,
            Reply = case dict:find(Key, ReadSet) of
                        error ->
                            Preflist = ?LOG_UTIL:get_preflist_from_key(Key),
                            IndexNode = hd(Preflist),
                            %lager:info("Read from node for ~w", [Key]),
                            ?CLOCKSI_VNODE:read_data_item(IndexNode, Key, Type, TxId); 
                        {ok, SnapshotState} ->
                            {ok, {Type,SnapshotState}}
                    end,
            case Reply of
                error ->
                    {reply, {error, unknown}, abort, SD0, 0};
                {error, Reason} ->
                    {reply, {error, Reason}, abort, SD0, 0};
                {ok, {Type, Snapshot}} ->
                    ReadSet1 = dict:store(Key, Snapshot, ReadSet),
                    %lager:info("Output ~w", [Snapshot]), 
                    {reply, {ok, Type:value(Snapshot)}, execute_op, SD0#state{read_set=ReadSet1}}
            end;
        update ->
            {Key, Type, Op} = Args,
            Preflist = ?LOG_UTIL:get_preflist_from_key(Key),
            IndexNode = hd(Preflist),
            UpdatedPartitions1 = case dict:is_key(IndexNode, UpdatedPartitions) of
                        false ->
                            dict:store(IndexNode, [{Key, Type, Op}], UpdatedPartitions);
                        true ->
                            dict:append(IndexNode, {Key, Type, Op}, UpdatedPartitions)
                        end,
            ReadSet1 = case dict:find(Key, ReadSet) of
                        error ->
                            Init = Type:new(),
                            {Param, Actor} = Op,
                            {ok, NewSnapshot} = Type:update(Param, Actor, Init),
                            dict:store(Key, NewSnapshot, ReadSet);
                        {ok, Snapshot} ->
                            {Param, Actor} = Op,
                            {ok, NewSnapshot} = Type:update(Param, Actor, Snapshot),
                            dict:store(Key, NewSnapshot, ReadSet)
                        end,
            {reply, ok, execute_op, SD0#state
                 {updated_partitions = UpdatedPartitions1, read_set = ReadSet1}}
		%{error, Reason} ->
		%    {reply, {error, Reason}, abort, SD0, 0}
	    %end
    end.


%% @doc this state sends a prepare message to all updated partitions and goes
%%      to the "receive_prepared"state.
prepare(timeout, SD0=#state{
                        tx_id = TxId,
                        updated_partitions=UpdatedPartitions, from=_From}) ->
    case dict:size(UpdatedPartitions) of
        0->
            Snapshot_time=TxId#tx_id.snapshot_time,
            {next_state, committing,
            SD0#state{state=committing, commit_time=Snapshot_time}, 0};
        1->
            UpdatedPart = dict:to_list(UpdatedPartitions),
            %lists:foldl(fun(X, Acc) -> {Part, Keys}= X,
            %                            Acc++[{Part, [Key || {Key, _Type, _Op} <- Keys]} ]
            %            end, [], UpdatedPart),
            ?CLOCKSI_VNODE:single_commit(UpdatedPart, TxId),
            {next_state, single_committing,
            SD0#state{state=committing, num_to_ack=1}};
        N->
            ?CLOCKSI_VNODE:prepare(UpdatedPartitions, TxId),
            {next_state, receive_prepared,
            SD0#state{num_to_ack=N, state=prepared}}
    end.
%% @doc state called when 2pc is forced independently of the number of partitions
%%      involved in the txs.
prepare_2pc(timeout, SD0=#state{
                        tx_id = TxId,
                        updated_partitions=UpdatedPartitions, from=From}) ->
    case dict:size(UpdatedPartitions) of
        0->
            Snapshot_time=TxId#tx_id.snapshot_time,
            _Res = gen_fsm:reply(From, {ok, Snapshot_time}),
            {next_state, committing_2pc,
            SD0#state{state=committing, commit_time=Snapshot_time}};
        N->
            ?CLOCKSI_VNODE:prepare(UpdatedPartitions, TxId),
            {next_state, receive_prepared,
            SD0#state{num_to_ack=N, state=prepared}}
    end.

%% @doc in this state, the fsm waits for prepare_time from each updated
%%      partitions in order to compute the final tx timestamp (the maximum
%%      of the received prepare_time).
receive_prepared({prepared, ReceivedPrepareTime},
                 S0=#state{num_to_ack=NumToAck,
                           commit_protocol=CommitProtocol,
                           from=From, prepare_time=PrepareTime}) ->
    io:format("Received prepared"),
    MaxPrepareTime = max(PrepareTime, ReceivedPrepareTime),
    case NumToAck of 1 ->
            case CommitProtocol of
            two_phase ->
                _Res = gen_fsm:reply(From, {ok, MaxPrepareTime}),
                {next_state, committing_2pc,
                S0#state{prepare_time=MaxPrepareTime, commit_time=MaxPrepareTime, state=committing}};
            _ ->
                {next_state, committing,
                S0#state{prepare_time=MaxPrepareTime, commit_time=MaxPrepareTime, state=committing}, 0}
            end;
        _ ->
            {next_state, receive_prepared,
             S0#state{num_to_ack= NumToAck-1, prepare_time=MaxPrepareTime}}
    end;

receive_prepared(abort, S0) ->
    {next_state, abort, S0, 0};

receive_prepared(timeout, S0) ->
    {next_state, abort, S0, 0}.

single_committing({committed, CommitTime}, S0=#state{from=_From}) ->
    reply_to_client(S0#state{prepare_time=CommitTime, commit_time=CommitTime, state=committed});
    
single_committing(abort, S0=#state{from=_From}) ->
    reply_to_client(S0#state{state=aborted}).

%% @doc after receiving all prepare_times, send the commit message to all
%%      updated partitions, and go to the "receive_committed" state.
%%      This state expects other process to sen the commit message to 
%%      start the commit phase.
committing_2pc(commit, Sender, SD0=#state{tx_id = TxId,
                              updated_partitions=UpdatedPartitions,
                              commit_time=Commit_time}) ->
    case dict:size(UpdatedPartitions) of
        0 ->
            reply_to_client(SD0#state{state=committed, from=Sender});
        N ->
            ?CLOCKSI_VNODE:commit(UpdatedPartitions, TxId, Commit_time),
            {next_state, receive_committed,
             SD0#state{num_to_ack=N, from=Sender, state=committing}}
    end.

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
reply_to_client(SD=#state{from=From, tx_id=TxId,
                                   state=TxState, commit_time=CommitTime}) ->
    if undefined =/= From ->
        Reply = case TxState of
            committed ->
                {ok, {TxId, CommitTime}};
            aborted->
                {aborted, TxId}
        end,
        gen_fsm:reply(From,Reply);
      true -> ok
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

-ifdef(TEST).

main_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun empty_prepare_test/1,
      fun timeout_test/1,

      fun update_single_abort_test/1,
      fun update_single_success_test/1,
      fun update_multi_abort_test1/1,
      fun update_multi_abort_test2/1,
      fun update_multi_success_test/1,

      fun read_single_fail_test/1,
      fun read_success_test/1

     ]}.

% Setup and Cleanup
setup()      -> {ok,Pid} = clocksi_interactive_tx_coord_fsm:start_link(self(), ignore), Pid. 
cleanup(Pid) -> case process_info(Pid) of undefined -> io:format("Already cleaned");
                                           _ -> clocksi_interactive_tx_coord_fsm:stop(Pid) end.

empty_prepare_test(Pid) ->
    fun() ->
            ?assertMatch({ok, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
    end.

timeout_test(Pid) ->
    fun() ->
            ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {timeout, riak_dt_pncounter, 
                    {increment, nothing}}}, infinity)),
            ?assertMatch({aborted, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
    end.

update_single_abort_test(Pid) ->
    fun() ->
            ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {fail, riak_dt_pncounter, {increment, no}}}, infinity)),
            ?assertMatch({aborted, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
    end.

update_single_success_test(Pid) ->
    fun() ->
            ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {success, riak_dt_pncounter, {increment, no}}}, infinity)),
            ?assertMatch({ok, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
    end.

update_multi_abort_test1(Pid) ->
    fun() ->
            ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {success, riak_dt_pncounter, {increment, no}}}, infinity)),
            ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {success, riak_dt_pncounter, {increment, no}}}, infinity)),
            ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {fail, riak_dt_pncounter, {increment, no}}}, infinity)),
            ?assertMatch({aborted, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
    end.

update_multi_abort_test2(Pid) ->
    fun() ->
            ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {success, riak_dt_pncounter, {increment, no}}}, infinity)),
            ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {fail, riak_dt_pncounter, {increment, no}}}, infinity)),
            ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {fail, riak_dt_pncounter, {increment, no}}}, infinity)),
            ?assertMatch({aborted, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
    end.

update_multi_success_test(Pid) ->
    fun() ->
            ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {success, riak_dt_pncounter, {increment, no}}}, infinity)),
            ?assertEqual(ok, gen_fsm:sync_send_event(Pid, {update, {success, riak_dt_pncounter, {increment, no}}}, infinity)),
            ?assertMatch({ok, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
    end.

read_single_fail_test(Pid) ->
    fun() ->
            ?assertEqual({error, mock_read_fail}, 
                    gen_fsm:sync_send_event(Pid, {read, {read_fail, riak_dt_pncounter}}, infinity))
    end.

read_success_test(Pid) ->
    fun() ->
            ?assertEqual({ok, 2}, 
                    gen_fsm:sync_send_event(Pid, {read, {counter, riak_dt_gcounter}}, infinity)),
            ?assertEqual({ok, [a]}, 
                    gen_fsm:sync_send_event(Pid, {read, {set, riak_dt_gset}}, infinity)),
            ?assertMatch({ok, _}, gen_fsm:sync_send_event(Pid, {prepare, empty}, infinity))
    end.

-endif.
