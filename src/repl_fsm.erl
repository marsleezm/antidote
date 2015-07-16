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
        replicate_log/3,
        repl_ack/2]).

%% Spawn

-record(state, {partition :: non_neg_integer(),
		id :: non_neg_integer(),
        log_size :: non_neg_integer(),
        quorum :: non_neg_integer(),
        successors :: [atom()],
        replicated_log :: cache_id(),
		self :: atom()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Partition) ->
    gen_server:start_link({global, get_replfsm_name(Partition)},
             ?MODULE, [Partition], []).

replicate(Partition, PendingLog) ->
    gen_server:cast({global, get_replfsm_name(Partition)}, {replicate, PendingLog}).

replicate_log(Partitions, MyPartition, Log) ->
    lists:foreach(fun(Partition) -> 
                    gen_server:cast({global, Partition}, 
                    {replicate_log, MyPartition, Log}) end, Partitions).

repl_ack(Partition, Reply) ->
    gen_server:cast({global, get_replfsm_name(Partition)}, {repl_ack, Reply}).

retrieve_log(Partition, LogName) ->
    gen_server:call({global, get_replfsm_name(Partition)}, {retrieve_log, LogName}).
%%%===================================================================
%%% Internal
%%%===================================================================


init([Partition]) ->
    ReplFactor = antidote_config:get(repl_factor),
    Quorum = antidote_config:get(quorum),
    LogSize = antidote_config:get(log_size),
    Successors = [get_replfsm_name(Index) || {Index, _Node} <- log_utilities:get_my_next(Partition, ReplFactor-1)],
    ReplicatedLog = clocksi_vnode:open_table(Partition, repl_log),
    {ok, #state{partition=Partition,
                log_size = LogSize,
                quorum = Quorum,
                successors = Successors,
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

handle_cast({replicate, PendingLog}, 
	    SD0=#state{partition=Partition, replicated_log=ReplicatedLog, successors=Successors}) ->
    {TxId, PendingRecord} = PendingLog,        
    {Type, _, _, _, TxInfo} = PendingRecord,
    ets:insert(ReplicatedLog, {TxId, PendingRecord}),
    replicate_log(Successors, Partition, {Type, {TxId, TxInfo}}),
    {noreply, SD0};

handle_cast({replicate_log, PrimaryPart, Log}, 
	    SD0=#state{replicated_log=ReplicatedLog, log_size=LogSize}) ->
    {Type, {TxId, Record}} = Log,
    DurableLog = case ets:lookup(ReplicatedLog, PrimaryPart) of
                                            [] ->
                                                [];
                                            [{PrimaryPart, Result}] ->
                                                lists:sublist(Result, LogSize)
                                        end,
    ets:insert(ReplicatedLog, {PrimaryPart, [{TxId, Record}|DurableLog]}),
    repl_ack(PrimaryPart, {Type,TxId}),
    {noreply, SD0};

handle_cast({repl_ack, {Type, TxId}}, SD0=#state{replicated_log=ReplicatedLog,
            partition=Partition,
            log_size=LogSize}) ->
    case ets:lookup(ReplicatedLog, TxId) of
        [{TxId, {RecordType, AckNeeded, Sender, MsgToReply, Record}}] ->
            case Type of
                RecordType ->
                    case AckNeeded of
                        1 -> %%Has got all neede ack, can log message already
                            %lager:info("Returned to client"),
                            DurableLog = case ets:lookup(ReplicatedLog, Partition) of
                                            [] ->
                                                [];
                                            [{Partition, Result}] ->
                                                lists:sublist(Result, LogSize)
                                        end,
                            ets:insert(ReplicatedLog, {Partition, [{TxId, Record}|DurableLog]}),
                            ets:delete(ReplicatedLog, TxId),
                            %lager:info("#####DONE#####Sending ~p to ~p", [Sender, MsgToReply]),
                            {fsm, undefined, FSMSender} = Sender,
                            gen_fsm:send_event(FSMSender, MsgToReply);
                        _ -> %%Wait for more replies
                            ets:insert(ReplicatedLog, {TxId, {RecordType, AckNeeded-1,
                                    Sender, MsgToReply, Record}})
                    end;
                _ ->
                    ok
            end;
        [] -> %%The record is appended already, do nothing
            %lager:info("No need to do anything ~p", [_SomeRecord]),
            ok
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

%open_local_tables([], Dict) ->
%    Dict;
%open_local_tables([{Part, Node}|Rest], Dict) ->
%    try
%    Tab = ets:new(int_to_atom(Part),
%            [set,?TABLE_CONCURRENCY]),
%    NewDict = dict:store(Part, Tab, Dict),
%    open_local_tables(Rest, NewDict)
%    catch
%    _:_Reason ->
%        lager:info("Error opening table..."),
%        %% Someone hasn't finished cleaning up yet
%        open_local_tables([{Part, Node}|Rest], Dict)
%    end.

%int_to_atom(Int) ->
%    list_to_atom(integer_to_list(Int)).

get_replfsm_name(Partition) ->
    list_to_atom(atom_to_list(repl_fsm)++integer_to_list(Partition)).
