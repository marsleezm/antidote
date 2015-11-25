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
%% TODO: should implement heart-beat for timestamp.
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
-export([relay_read/4,
        prepare_specula/5,
        commit_specula/4,
        abort_specula/3,
        if_prepared/3,
        if_bulk_prepared/3,
        read/3]).

%% Spawn

-record(state, {
        successors :: [atom()],
        replicated_log :: cache_id(),
        pending_log :: cache_id(),
        delay :: non_neg_integer(),
        name :: atom(),
		self :: atom()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name) ->
    gen_server:start_link({global, Name},
             ?MODULE, [Name], []).

read(Name, Key, TxId) ->
    gen_server:call({global, Name}, {read, Key, TxId}).

if_prepared(Name, TxId, Keys) ->
    gen_server:call({global, Name}, {if_prepared, TxId, Keys}).

if_bulk_prepared(Name, TxId, Partition) ->
    gen_server:call({global, Name}, {if_bulk_prepared, TxId, Partition}).

relay_read(Name, Key, TxId, Reader) ->
    gen_server:cast({global, Name}, {relay_read, Key, TxId, Reader}).

prepare_specula(Name, TxId, Partition, WriteSet, PrepareTime) ->
    gen_server:cast({global, Name}, {prepare_specula, TxId, Partition, WriteSet, PrepareTime}).

commit_specula(Name, TxId, Partition, CommitTime) ->
    gen_server:cast({global, Name}, {commit_specula, TxId, Partition, CommitTime}).

abort_specula(Name, TxId, Partition) ->
    gen_server:cast({global, Name}, {abort_specula, TxId, Partition}).

%%%===================================================================
%%% Internal
%%%===================================================================


init([Name]) ->
    lager:info("Data repl inited with name ~w", [Name]),
    ReplicatedLog = tx_utilities:open_private_table(repl_log),
    PendingLog = tx_utilities:open_private_table(pending_log),
    {ok, #state{name=Name,
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

handle_call({read, Key, TxId}, _Sender, 
	    SD0=#state{replicated_log=ReplicatedLog}) ->
    lager:info("Reading for ~w , key ~w", [TxId, Key]),
    case ets:lookup(ReplicatedLog, Key) of
        [] ->
            lager:info("Nothing!"),
            {reply, {ok, []}, SD0};
        [{Key, ValueList}] ->
            lager:info("Value list is ~w", [ValueList]),
            MyClock = TxId#tx_id.snapshot_time,
            case find_version(ValueList, MyClock) of
                {specula, SpeculaTxId, Value} ->
                    lager:info("Found specula value ~w from ~w", [ValueList, SpeculaTxId]),
                    ets:insert(dependency, {SpeculaTxId, TxId}),         
                    ets:insert(anti_dep, {TxId, SpeculaTxId}),        
                    {reply, {ok, Value}, SD0};
                    %{reply, {{specula, SpeculaTxId}, Value}, SD0};
                Value ->
                    {reply, {ok, Value}, SD0}
            end
    end;

handle_call({if_prepared, TxId, Keys}, _Sender, SD0=#state{replicated_log=ReplicatedLog, name=Name}) ->
    Result = lists:all(fun(Key) ->
                    lager:info("~w: Check ~w for ~w", [Name, Key, TxId]),
                    case ets:lookup(ReplicatedLog, Key) of
                        [{Key, [{_, _, TxId}|_]}] -> lager:info("Check ok"), true;
                        R -> lager:info("Check false, Record is ~w", [R]), false
                    end end, Keys),
    {reply, Result, SD0};

handle_call({if_bulk_prepared, TxId, Partition}, _Sender, SD0=#state{
            pending_log=PendingLog, name=Name}) ->
    lager:info("~w: checking if bulk_prepared for ~w ~w", [Name, TxId, Partition]),
    case ets:lookup(PendingLog, {TxId, Partition}) of
        [{{TxId, Partition}, {_, _}}] ->
            lager:info("It's inserted"),
            {reply, true, SD0};
        Record ->
            lager:info("~w: something else", [Record]),
            {reply, false, SD0}
    end;

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

handle_cast({relay_read, Key, TxId, Reader}, 
	    SD0=#state{replicated_log=ReplicatedLog}) ->
    case ets:lookup(ReplicatedLog, Key) of
        [] ->
            %lager:info("Nothing!"),
            gen_server:reply(Reader, {ok, []}),
            {noreply, SD0};
        [{Key, ValueList}] ->
            %lager:info("Value list is ~w", [ValueList]),
            MyClock = TxId#tx_id.snapshot_time,
            Value = find_version(ValueList, MyClock),
            gen_server:reply(Reader, Value),
            {noreply, SD0}
    end;

handle_cast({prepare_specula, TxId, Partition, WriteSet, TimeStamp}, 
	    SD0=#state{replicated_log=ReplicatedLog, pending_log=PendingLog, name=Name}) ->
    lager:info("~w: Specula prepare for [~w, ~w] of writeset ~w", [Name, TxId, Partition, WriteSet]),
    KeySet = lists:foldl(fun({Key, Value}, KS) ->
                    case ets:lookup(ReplicatedLog, Key) of
                        [] ->
                            %% Putting TxId in the record to mark the transaction as speculative 
                            %% and for dependency tracking that will happen later
                            true = ets:insert(ReplicatedLog, {Key, [{TimeStamp, Value, TxId}]}),
                            lager:info("Inserted for ~w, empty", [Key]),
                            [Key|KS];
                        [{Key, ValueList}] ->
                            {RemainList, _} = lists:split(min(?NUM_VERSIONS,length(ValueList)), ValueList),
                            true = ets:insert(ReplicatedLog, {Key, [{TimeStamp, Value, TxId}|RemainList]}),
                            lager:info("Inserted for ~w", [Key]),
                            [Key|KS]
                    end end, [], WriteSet),
    ets:insert(PendingLog, {{TxId, Partition}, KeySet}),
    {noreply, SD0};

%% Where shall I put the speculative version?
%% In ets, faster for read.
handle_cast({abort_specula, TxId, Partition}, 
	    SD0=#state{replicated_log=ReplicatedLog, pending_log=PendingLog}) ->
    [{{TxId, Partition}, KeySet}] = ets:lookup(PendingLog, {TxId, Partition}),
    lager:info("Aborting specula for ~w ~w", [TxId, Partition]),
    lists:foreach(fun(Key) ->
                    case ets:lookup(ReplicatedLog, Key) of
                        [] -> %% TODO: this can not happen 
                            ok;
                        [{Key, ValueList}] ->
                            NewValueList = delete_version(ValueList, TxId), 
                            ets:insert(ReplicatedLog, {Key, NewValueList})
                    end 
                  end, KeySet),
    case ets:lookup(dependency, TxId) of
        [] ->
            ok; %% No read dependency was created!
        List ->
            lists:foreach(fun({TId, DependTxId}) ->
                            ets:delete_object(dependency, {TId, DependTxId}),
                            specula_tx_cert_server:read_invalid(DependTxId#tx_id.server_pid, DependTxId)
                          end, List)
    end,
    {noreply, SD0};
    
handle_cast({commit_specula, TxId, Partition, CommitTime}, 
	    SD0=#state{replicated_log=ReplicatedLog, pending_log=PendingLog}) ->
    lager:info("Committing specula for ~w ~w", [TxId, Partition]),
    [{{TxId, Partition}, KeySet}] = ets:lookup(PendingLog, {TxId, Partition}),
    lists:foreach(fun(Key) ->
                    case ets:lookup(ReplicatedLog, Key) of
                        [] -> %% TODO: this can not happen 
                            lager:info("Found nothing for ~w, not possible", [Key]),
                            ok;
                        [{Key, ValueList}] ->
                            lager:info("Key ~w, ValueList is ~w", [Key, ValueList]),
                            NewValueList = replace_version(ValueList, TxId, CommitTime), 
                            lager:info("NewValueList is ~w", [NewValueList]),
                            ets:insert(ReplicatedLog, {Key, NewValueList})
                    end 
                  end, KeySet),
    case ets:lookup(dependency, TxId) of
        [] ->
            ok; %% No read dependency was created!
        List ->
            lists:foreach(fun({TId, DependTxId}) ->
                            ets:delete_object(dependency, {TId, DependTxId}),
                            case DependTxId#tx_id.snapshot_time >= CommitTime of
                                true ->
                                    specula_tx_cert_server:read_valid(DependTxId#tx_id.server_pid, DependTxId);
                                false ->
                                    specula_tx_cert_server:read_invalid(DependTxId#tx_id.server_pid, DependTxId)
                          end end, List)
    end,
    {noreply, SD0};

handle_cast({repl_prepare, Type, TxId, Partition, WriteSet, TimeStamp, Sender}, 
	    SD0=#state{pending_log=PendingLog, replicated_log=ReplicatedLog}) ->
    lager:info("Got repl prepare for {~w, ~w}, write set is ~w", [TxId, Partition, WriteSet]),
    case Type of
        prepared ->
            ets:insert(PendingLog, {{TxId, Partition}, {WriteSet, TimeStamp}}),
            lager:info("Got prepare.. Replying to ~w", [Sender]),
            gen_server:cast({global, Sender}, {ack, Partition, TxId}), 
            {noreply, SD0};
        single_commit ->
            lager:info("Got single commit.. Replying to ~w", [Sender]),
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
    lager:info("Got repl commit for ~w of partitions ~w", [TxId, Partitions]),
    append_by_parts(PendingLog, ReplicatedLog, TxId, CommitTime, Partitions),
    {noreply, SD0};

handle_cast({repl_abort, TxId, Partitions}, 
	    SD0=#state{
            pending_log=PendingLog}) ->
    %lager:info("Got repl commit for ~w", [TxId]),
    abort_by_parts(PendingLog, TxId, Partitions),
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
            Value;
        false ->
            find_version(Rest, SnapshotTime)
    end;
find_version([{TS, Value, TxId}|Rest], SnapshotTime) ->
    case SnapshotTime >= TS of
        true ->
            {specula, TxId, Value};
        false ->
            find_version(Rest, SnapshotTime)
    end.

append_by_parts(_, _, _, _, []) ->
    ok;
append_by_parts(PendingLog, ReplicatedLog, TxId, CommitTime, [Part|Rest]) ->
    case ets:lookup(PendingLog, {TxId, Part}) of
        [{{TxId, Part}, {WriteSet, _}}] ->
            lager:info("For ~w ~w found writeset", [TxId, Part, WriteSet]),
            AppendFun = fun({Key, Value}) ->
                            lager:info("Adding ~w, ~w into log", [Key, Value]),
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

abort_by_parts(_, _, []) ->
    ok;
abort_by_parts(PendingLog, TxId, [Part|Rest]) ->
    ets:delete(PendingLog, {TxId, Part}),
    abort_by_parts(PendingLog, TxId, Rest). 

delete_version([{_, _, TxId}|Rest], TxId) -> 
    Rest;
delete_version([{TS, V, TxId}|Rest], TxId) -> 
    [{TS, V, TxId}|delete_version(Rest, TxId)].

replace_version([{_, Value, TxId}|Rest], TxId, CommitTime) -> 
    [{CommitTime, Value}|Rest];
replace_version([{TS, V, TxId}|Rest], TxId, CommitTime) -> 
    [{TS, V, TxId}|replace_version(Rest, TxId, CommitTime)].

