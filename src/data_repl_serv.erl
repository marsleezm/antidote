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
-module(data_repl_serv).

-behavior(gen_server).

-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(NUM_VERSIONS, 20).
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
-export([repl_prepare/4,
        repl_commit/3,
        read/3]).

%% Spawn

-record(state, {
        successors :: [atom()],
        replicated_log :: cache_id(),
        pending_log :: cache_id(),
        delay :: non_neg_integer(),
		self :: atom()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name) ->
    gen_server:start_link({global, Name},
             ?MODULE, [Name], []).

repl_prepare(Name, TxId, WriteSet, Sender) ->
    gen_server:cast({global, Name}, {repl_prepare, TxId, WriteSet, Sender}).

repl_commit(Name, TxId, CommitTime) ->
    gen_server:cast({global, Name}, {repl_commit, TxId, CommitTime}).

read(Name, TxId, Key) ->
    gen_server:call({global, Name}, {read, TxId, Key}).

%%%===================================================================
%%% Internal
%%%===================================================================


init([Name]) ->
    lager:info("Data repl inited with name ~w", [Name]),
    ReplicatedLog = tx_utilities:open_private_table(repl_log),
    PendingLog = tx_utilities:open_private_table(pending_log),
    {ok, #state{
                pending_log = PendingLog,
                replicated_log = ReplicatedLog}}.

handle_call({retrieve_log, LogName},  _Sender,
	    SD0=#state{replicated_log=ReplicatedLog}) ->
    case ets:lookup(ReplicatedLog, LogName) of
        [{LogName, Log}] ->
            {reply, Log, SD0};
        [] ->
            {reply, [], SD0}
    end;

handle_call({read, TxId, Key}, _Sender, 
	    SD0=#state{replicated_log=ReplicatedLog}) ->
    case ets:lookup(ReplicatedLog, Key) of
        [] ->
            lager:info("Nothing!"),
            {reply, {ok, []}, SD0};
        [{Key, ValueList}] ->
            lager:info("Value list is ~w", [ValueList]),
            MyClock = TxId#tx_id.snapshot_time,
            Value = find_version(ValueList, MyClock),
            {reply, Value, SD0}
    end;

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

handle_cast({repl_prepare, Type, TxId, Partition, WriteSet, TimeStamp, Sender}, 
	    SD0=#state{pending_log=PendingLog, replicated_log=ReplicatedLog}) ->
    %lager:info("Got repl prepare for {~w, ~w}, write set is ~w", [TxId, Partition, WriteSet]),
    case Type of
        prepare ->
            ets:insert(PendingLog, {{TxId, Partition}, {WriteSet, TimeStamp}}),
            %lager:info("Got serv.. Replying to ~w", [Sender]),
            gen_server:cast({global, Sender}, {ack, Partition, TxId}), 
            {noreply, SD0};
        single_commit ->
            AppendFun = fun({Key, Value}) ->
                case ets:lookup(ReplicatedLog, Key) of
                    [] ->
                        true = ets:insert(ReplicatedLog, {Key, [{TimeStamp, Value}]});
                    [{Key, ValueList}] ->
                        {RemainList, _} = lists:split(min(?NUM_VERSIONS,length(ValueList)), ValueList),
                        true = ets:insert(ReplicatedLog, {Key, [{TimeStamp, Value}|RemainList]})
                end end,
            lists:foreach(AppendFun, WriteSet),
            gen_server:cast({global, Sender}, {ack, Partition, TxId}), 
            {noreply, SD0}
    end;

handle_cast({repl_commit, TxId, CommitTime, Partitions}, 
	    SD0=#state{replicated_log=ReplicatedLog,
            pending_log=PendingLog}) ->
    %lager:info("Got repl commit for ~w", [TxId]),
    append_by_parts(PendingLog, ReplicatedLog, TxId, CommitTime, Partitions),
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

find_version([],  _SnapshotTime) ->
    {ok, []};
find_version([{TS, Value}|Rest], SnapshotTime) ->
    case SnapshotTime >= TS of
        true ->
            {ok, Value};
        false ->
            find_version(Rest, SnapshotTime)
    end.

append_by_parts(_, _, _, _, []) ->
    ok;
append_by_parts(PendingLog, ReplicatedLog, TxId, CommitTime, [Part|Rest]) ->
    case ets:lookup(PendingLog, {TxId, Part}) of
        [{{TxId, Part}, {WriteSet, _}}] ->
            AppendFun = fun({Key, Value}) ->
                            %lager:info("Adding ~w, ~w into log", [Key, Value]),
                            case ets:lookup(ReplicatedLog, Key) of
                                [] ->
                                    true = ets:insert(ReplicatedLog, {Key, [{CommitTime, Value}]});
                                [{Key, ValueList}] ->
                                    {RemainList, _} = lists:split(min(?NUM_VERSIONS,length(ValueList)), ValueList),
                                    true = ets:insert(ReplicatedLog, {Key, [{CommitTime, Value}|RemainList]})
                            end end,
            lists:foreach(AppendFun, WriteSet),
            ets:delete(PendingLog, {TxId, Part});
        [] ->
            lager:warning("Something is wrong!!! Remove log for ~w, ~w", [TxId, Part])
    end,
    append_by_parts(PendingLog, ReplicatedLog, TxId, CommitTime, Rest). 
