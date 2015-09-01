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
-module(repl_fsm).

-behavior(gen_server).

-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/1]).

%% Callbacks
-export([init/1,
	    handle_call/3,
	    handle_cast/2,
         code_change/3,
         handle_event/3,
         handle_info/2,
         handle_sync_event/4,
         terminate/2]).

%% States
-export([replicate/2,
        retrieve_log/2,
        quorum_replicate/3,
        chain_replicate/6,
        send_after/3,
        repl_ack/3]).

%% Spawn

-record(state, {partition :: non_neg_integer(),
		id :: non_neg_integer(),
        log_size :: non_neg_integer(),
        mode :: atom(),
        quorum :: non_neg_integer(),
        successors :: [atom()],
        replicated_log :: cache_id(),
        pending_log :: cache_id(),
        delay :: non_neg_integer(),
		self :: atom()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Partition) ->
    gen_server:start_link({global, get_replfsm_name(Partition)},
             ?MODULE, [Partition], []).

replicate(Partition, LogContent) ->
    gen_server:cast({global, get_replfsm_name(Partition)}, {replicate, LogContent}).

quorum_replicate(Fsms, MyPartition, Log) ->
    lists:foreach(fun(ReplFsm) -> 
                    gen_server:cast({global, ReplFsm}, {quorum_replicate, MyPartition, Log}) 
                    end, Fsms).

chain_replicate(0, ReplFsm, LogPartition, Log, MsgToReply, RepNeeded) ->
    gen_server:cast({global, ReplFsm}, {chain_replicate, LogPartition, Log, MsgToReply, RepNeeded});
chain_replicate(Delay, ReplFsm, LogPartition, Log, MsgToReply, RepNeeded) ->
    spawn(repl_fsm, send_after, [Delay, {global, ReplFsm}, 
        {chain_replicate, LogPartition, Log, MsgToReply, RepNeeded}]).

repl_ack(0, Fsm, Reply) ->
    gen_server:cast({global, Fsm}, {repl_ack, Reply});
repl_ack(Delay, Fsm, Reply) ->
    spawn(repl_fsm, send_after, [Delay, {global, Fsm}, {repl_ack, Reply}]).

retrieve_log(Partition, LogName) ->
    gen_server:call({global, get_replfsm_name(Partition)}, {retrieve_log, LogName}).
%%%===================================================================
%%% Internal
%%%===================================================================


init([Partition]) ->
    ReplFactor = antidote_config:get(repl_factor),
    Quorum = antidote_config:get(quorum),
    LogSize = antidote_config:get(log_size),
    Mode = antidote_config:get(mode),
    Delay = antidote_config:get(delay),
    Successors = [get_replfsm_name(Index) || {Index, _Node} <- log_utilities:get_my_next(Partition, ReplFactor-1)],
    ReplicatedLog = clocksi_vnode:open_table(Partition, repl_log),
    PendingLog = clocksi_vnode:open_table(Partition, pending_log),
    {ok, #state{partition=Partition,
                log_size = LogSize,
                quorum = Quorum,
                mode = Mode,
                successors = Successors,
                pending_log = PendingLog,
                delay = Delay,
                replicated_log = ReplicatedLog}}.

handle_call({retrieve_log, LogName},  _Sender,
	    SD0=#state{replicated_log=ReplicatedLog}) ->
    case ets:lookup(ReplicatedLog, LogName) of
        [{LogName, Log}] ->
            {reply, Log, SD0};
        [] ->
            {reply, [], SD0}
    end;

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

handle_cast({replicate, LogContent}, 
	    SD0=#state{partition=Partition, replicated_log=ReplicatedLog, quorum=Quorum,
            log_size=LogSize, successors=Successors, mode=Mode, pending_log=PendingLog}) ->
    {TxId, PendingRecord} = LogContent,        
    {RecordType, Sender, MsgToReply, Record} = PendingRecord,
    case Mode of 
        quorum ->
            ets:insert(PendingLog, {TxId, PendingRecord, Quorum-1}),
            quorum_replicate(Successors, Partition, {RecordType, {TxId, Record}});
        chain ->
            DurableLog = case ets:lookup(ReplicatedLog, Partition) of
                                    [] ->
                                        [];
                                    [{Partition, Result}] ->
                                        lists:sublist(Result, LogSize)
                        end,
            ets:insert(ReplicatedLog, {Partition, [{TxId, Record}|DurableLog]}),
            chain_replicate(0, hd(Successors), Partition, {TxId, Record}, {Sender, MsgToReply}, Quorum-1)
    end,
    {noreply, SD0};


handle_cast({repl_ack, {Type, TxId}}, SD0=#state{replicated_log=ReplicatedLog,
            partition=Partition, pending_log=PendingLog, log_size=LogSize}) ->
    case ets:lookup(PendingLog, TxId) of
        [{TxId, {RecordType, Sender, MsgToReply, Record}, AckNeeded}] ->
            case Type of
                RecordType ->
                    case AckNeeded of
                        1 -> %%Has got all neede ack, can log message already
                            DurableLog = case ets:lookup(ReplicatedLog, Partition) of
                                            [] ->
                                                [];
                                            [{Partition, Result}] ->
                                                lists:sublist(Result, LogSize)
                                        end,
                            ets:insert(ReplicatedLog, {Partition, [{TxId, Record}|DurableLog]}),
                            ets:delete(PendingLog, TxId),
                            {fsm, undefined, FSMSender} = Sender,
                            case MsgToReply of
                                false ->
                                    ok;
                                _ ->
                                    gen_fsm:send_event(FSMSender, MsgToReply)
                            end;
                        _ -> %%Wait for more replies
                            %lager:info("Log ~w waiting for more reply, Ackneeded is ~w", [Record, AckNeeded]),
                            ets:insert(PendingLog, {TxId, {RecordType, 
                                    Sender, MsgToReply, Record}, AckNeeded-1})
                    end;
                _ ->
                    ok
            end;
        [] -> %%The record is appended already, do nothing
            ok
    end,
    {noreply, SD0};

handle_cast({quorum_replicate, PrimaryPart, Log}, 
	    SD0=#state{replicated_log=ReplicatedLog, log_size=LogSize, partition=_Partition, delay=Delay}) ->
    %lager:info("Quorum got msg"),
    {Type, {TxId, Record}} = Log,
    DurableLog = case ets:lookup(ReplicatedLog, PrimaryPart) of
                                            [] ->
                                                [];
                                            [{PrimaryPart, Result}] ->
                                                lists:sublist(Result, LogSize)
                                        end,
    ets:insert(ReplicatedLog, {PrimaryPart, [{TxId, Record}|DurableLog]}),
    repl_ack(Delay, get_replfsm_name(PrimaryPart), {Type,TxId}),
    {noreply, SD0};

handle_cast({chain_replicate, Partition, Log, MsgToReply, RepNeeded},
        SD0=#state{replicated_log=ReplicatedLog, successors=Successors, log_size=LogSize, delay=Delay}) ->
    DurableLog = case ets:lookup(ReplicatedLog, Partition) of
                              [] ->
                                  [];
                              [{Partition, Result}] ->
                                  lists:sublist(Result, LogSize)
                  end,
    ets:insert(ReplicatedLog, {Partition, [Log|DurableLog]}),
    case RepNeeded of
        1 ->
            {{fsm, undefined, FSMSender}, Msg} = MsgToReply,
            case Msg of
                false ->
                    ok;
                _ ->
                    gen_fsm:send_event(FSMSender, Msg)
            end;
        _ ->
            chain_replicate(Delay, hd(Successors), Partition, Log, MsgToReply, RepNeeded-1)
      end,
     {noreply, SD0};

handle_cast(_Info, StateData) ->
    {noreply,StateData}.

handle_info(_Info, StateData) ->
    {noreply,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

terminate(_Reason, _SD) ->
    ok.

get_replfsm_name(Partition) ->
    list_to_atom(atom_to_list(repl_fsm)++integer_to_list(Partition)).

send_after(Delay, To, Msg) ->
    timer:sleep(Delay),
    %lager:info("Sending info after ~w", [Delay]),
    gen_server:cast(To, Msg).
