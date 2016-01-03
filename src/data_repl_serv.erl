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
%%
%% TODO: Two problems for read
%%      1. A replica is only allowed to serve a request if it's timestamp
%%       is higher than the snapshot time. The replica should update its 
%%       timestamp after it receives a commit from master.
%%      2. A replica needs to handles commit/abort sequentially. Basically
%%       if the timestamp of abort/commit it received is higher than
%%       the pending ones, it has to wait (can not update it's timestamp).
%%

-module(data_repl_serv).

-behavior(gen_server).

-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(NUM_VERSIONS, 10).
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
	check_key/2,
	check_table/1,
        debug_read/3,
        prepare_specula/5,
        commit_specula/4,
        abort_specula/3,
        if_prepared/3,
        if_bulk_prepared/3,
        num_specula_read/1,
        read/3]).

%% Spawn

-record(state, {
        successors :: [atom()],
        replicated_log :: cache_id(),
        pending_log :: cache_id(),
        delay :: non_neg_integer(),
        num_specula_read=0 :: non_neg_integer(),
        num_read=0 :: non_neg_integer(),
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

check_table(Name) ->
    gen_server:call({global, Name}, {check_table}).

check_key(Name, Key) ->
    gen_server:call({global, Name}, {check_key, Key}).

num_specula_read(Node) ->
    gen_server:call({global, Node}, {num_specula_read}).

debug_read(Name, Key, TxId) ->
    gen_server:call({global, Name}, {debug_read, Key, TxId}).

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

handle_call({num_specula_read}, _Sender, SD0=#state{num_specula_read=NumSpeculaRead, num_read=NumRead}) ->
    {reply, {NumSpeculaRead, NumRead}, SD0};

handle_call({check_table}, _Sender, SD0=#state{pending_log=PendingLog}) ->
    lager:info("Log info: ~w", [ets:tab2list(PendingLog)]),
    {reply, ok, SD0};

handle_call({check_key, Key}, _Sender, SD0=#state{replicated_log=ReplicatedLog}) ->
    Result = ets:lookup(ReplicatedLog, Key),
    {reply, Result, SD0};

handle_call({debug_read, Key, TxId}, _Sender, 
	    SD0=#state{replicated_log=ReplicatedLog}) ->
    lager:info("Got debug read for ~w", [Key]),
    case ets:lookup(ReplicatedLog, Key) of
        [] ->
            lager:info("Debug reading ~w, there is nothing", [Key]),
            {reply, {ok, []}, SD0};
        [{Key, ValueList}] ->
            lager:info("Debug reading ~w, Value list is ~w", [Key, ValueList]),
            MyClock = TxId#tx_id.snapshot_time,
            case find_nonspec_version(ValueList, MyClock) of
                Value ->
                    lager:info("Found value is ~w", [Value]),
                    {reply, {ok, Value}, SD0}
            end
    end;

handle_call({read, Key, TxId}, _Sender, 
	    SD0=#state{replicated_log=ReplicatedLog, num_read=NumRead,
                num_specula_read=NumSpeculaRead}) ->
    %lager:info("DataRepl Reading for ~w , key ~p", [TxId, Key]),
    case ets:lookup(ReplicatedLog, Key) of
        [] ->
            lager:warning("Nothing for ~p, ~w", [Key, TxId]),
            {reply, {ok, []}, SD0#state{num_read=NumRead+1}};
        [{Key, ValueList}] ->
            %lager:info("Value list is ~p", [ValueList]),
            MyClock = TxId#tx_id.snapshot_time,
            case find_version(ValueList, MyClock) of
                {specula, SpeculaTxId, Value} ->
                    %lager:info("Found specula value ~p from ~w", [ValueList, SpeculaTxId]),
                    ets:insert(dependency, {SpeculaTxId, TxId}),         
                    ets:insert(anti_dep, {TxId, SpeculaTxId}),        
                    lager:warning("Inserting antidep from ~w to ~w for key ~w", [TxId, SpeculaTxId, Key]),
                    {reply, {ok, Value}, SD0#state{num_specula_read=NumSpeculaRead+1, num_read=NumRead+1}};
                    %{reply, {{specula, SpeculaTxId}, Value}, SD0};
                Value ->
                    case Value of [] -> lager:warning("No value for ~p, ~p, value list is ~p", [Key, TxId, ValueList]);
                                _ -> ok
                    end,
                    %lager:info("Found value ~p", [Value]),
                    {reply, {ok, Value}, SD0#state{num_read=NumRead+1}}
            end
    end;

handle_call({if_prepared, TxId, Keys}, _Sender, SD0=#state{replicated_log=ReplicatedLog}) ->
    Result = lists:all(fun(Key) ->
                    lager:info("Check ~w for ~w", [Key, TxId]),
                    case ets:lookup(ReplicatedLog, Key) of
                        [{Key, [{_, _, TxId}|_]}] -> lager:info("Check ok"),
                                true;
                        R -> lager:info("Check false, Record is ~w", [R]), 
                                false
                    end end, Keys),
    {reply, Result, SD0};

handle_call({if_bulk_prepared, TxId, Partition}, _Sender, SD0=#state{
            pending_log=PendingLog}) ->
    lager:info("checking if bulk_prepared for ~w ~w", [TxId, Partition]),
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
    lager:warning("~w, ~p data repl read", [TxId, Key]),
    case ets:lookup(ReplicatedLog, Key) of
        [] ->
            lager:info("Nothing for ~p!", [Key]),
            gen_server:reply(Reader, {ok, []}),
            {noreply, SD0};
        [{Key, ValueList}] ->
            MyClock = TxId#tx_id.snapshot_time,
            Value = find_version(ValueList, MyClock),
            lager:info("Got value for ~p", [ValueList, Key]),
            gen_server:reply(Reader, Value),
            {noreply, SD0}
    end;

handle_cast({prepare_specula, TxId, Partition, WriteSet, TimeStamp}, 
	    SD0=#state{replicated_log=ReplicatedLog, pending_log=PendingLog}) ->
    %lager:warning("Specula prepare for [~w, ~w]", [TxId, Partition]),
    KeySet = lists:foldl(fun({Key, Value}, KS) ->
                    case ets:lookup(ReplicatedLog, Key) of
                        [] ->
                            %% Putting TxId in the record to mark the transaction as speculative 
                            %% and for dependency tracking that will happen later
                            true = ets:insert(ReplicatedLog, {Key, [{TimeStamp, Value, TxId}]}),
                            %lager:info("Inserted for ~w, empty", [Key]),
                            [Key|KS];
                        [{Key, ValueList}] ->
                            {RemainList, _} = lists:split(min(?NUM_VERSIONS,length(ValueList)), ValueList),
                            true = ets:insert(ReplicatedLog, {Key, [{TimeStamp, Value, TxId}|RemainList]}),
                            %lager:info("Inserted for ~w", [Key]),
                            [Key|KS]
                    end end, [], WriteSet),
    ets:insert(PendingLog, {{TxId, Partition}, KeySet}),
    {noreply, SD0};

%% Where shall I put the speculative version?
%% In ets, faster for read.
handle_cast({abort_specula, TxId, Partition}, 
	    SD0=#state{replicated_log=ReplicatedLog, pending_log=PendingLog}) ->
    [{{TxId, Partition}, KeySet}] = ets:lookup(PendingLog, {TxId, Partition}),
    %lager:warning("Aborting specula for ~w ~w", [TxId, Partition]),
    lists:foreach(fun(Key) ->
                    case ets:lookup(ReplicatedLog, Key) of
                        [] -> %% TODO: this can not happen 
                            ok;
                        [{Key, ValueList}] ->
                            %lager:info("Deleting ~p of ~w, Value list is ~p", [Key, TxId, ValueList]),
                            NewValueList = delete_version(ValueList, TxId), 
                            ets:insert(ReplicatedLog, {Key, NewValueList})
                    end 
                  end, KeySet),
    specula_utilities:deal_abort_deps(TxId),
    {noreply, SD0};
    
handle_cast({commit_specula, TxId, Partition, CommitTime}, 
	    SD0=#state{replicated_log=ReplicatedLog, pending_log=PendingLog}) ->
    %lager:warning("Committing specula for ~w ~w", [TxId, Partition]),
    [{{TxId, Partition}, KeySet}] = ets:lookup(PendingLog, {TxId, Partition}),
    lists:foreach(fun(Key) ->
                    case ets:lookup(ReplicatedLog, Key) of
                        [] -> %% TODO: this can not happen 
                            lager:error("Found nothing for ~p, not possible", [Key]),
                            ok;
                        [{Key, ValueList}] ->
                            %lager:info("Commit specula for key ~p, TxId ~w with ~w, ValueList is ~p", [Key, TxId, CommitTime, ValueList]),
                            NewValueList = replace_version(ValueList, TxId, CommitTime), 
                            %lager:info("NewValueList is ~w", [NewValueList]),
                            ets:insert(ReplicatedLog, {Key, NewValueList})
                    end 
                  end, KeySet),
    specula_utilities:deal_commit_deps(TxId, CommitTime),
    {noreply, SD0};

handle_cast({repl_prepare, Type, TxId, Partition, WriteSet, TimeStamp, Sender}, 
	    SD0=#state{pending_log=PendingLog, replicated_log=ReplicatedLog}) ->
    case Type of
        prepared ->
	    case ets:lookup(PendingLog, {TxId, Partition}) of
		[] ->
            	    ets:insert(PendingLog, {{TxId, Partition}, {WriteSet, TimeStamp}});
		[{{TxId, Partition}, to_abort}] ->
		    %lager:warning("Prep ~w ~w arrived late! Aborted", [TxId, Partition]),
		    ets:delete(PendingLog, {TxId, Partition});
		[{{TxId, Partition}, CommitTime}] ->
		    %lager:warning("Prep ~w ~w arrived late! Committed", [TxId, Partition]),
		    ets:delete(PendingLog, {TxId, Partition}),
		    AppendFun = fun({Key, Value}) ->
                            %lager:info("DataRepl Adding ~p, ~p wth ~w of ~w into log", [Key, Value, CommitTime, TxId]),
                            case ets:lookup(ReplicatedLog, Key) of
                                [] ->
                                    true = ets:insert(ReplicatedLog, {Key, [{CommitTime, Value}]});
                                [{Key, ValueList}] ->
                                    {RemainList, _} = lists:split(min(?NUM_VERSIONS,length(ValueList)), ValueList),
                                    true = ets:insert(ReplicatedLog, {Key, [{CommitTime, Value}|RemainList]})
                            end end,
            	    lists:foreach(AppendFun, WriteSet)
	    end,
            %lager:info("Got repl prepare for {~w, ~w}, replying to ~w", [TxId, Partition, Sender]),
            gen_server:cast({global, Sender}, {ack, Partition, TxId}), 
            {noreply, SD0};
        single_commit ->
            AppendFun = fun({Key, Value}) ->
                case ets:lookup(ReplicatedLog, Key) of
                    [] ->
                        %lager:info("Data repl inserting ~p, ~p of ~w to table", [Key, Value, TimeStamp]),
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
    %lager:info("Got repl commit for ~w of partitions ~w", [TxId, Partitions]),
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

find_nonspec_version([],  _SnapshotTime) ->
    [];
find_nonspec_version([{TS, Value}|Rest], SnapshotTime) ->
    case SnapshotTime >= TS of
        true ->
            Value;
        false ->
            find_version(Rest, SnapshotTime)
    end;
find_nonspec_version([{_, _, _}|Rest], SnapshotTime) ->
    find_version(Rest, SnapshotTime).


find_version([],  _SnapshotTime) ->
    [];
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
            %lager:info("For ~w ~w found writeset", [TxId, Part, WriteSet]),
            AppendFun = fun({Key, Value}) ->
                            %lager:info("Adding ~p, ~p wth ~w of ~w into log", [Key, Value, CommitTime, TxId]),
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
	    %lager:warning("Commit ~w ~w arrived early! Committing with ~w", [TxId, Part, CommitTime]),
	        ets:insert(PendingLog, {{TxId, Part}, CommitTime})
    end,
    append_by_parts(PendingLog, ReplicatedLog, TxId, CommitTime, Rest). 

abort_by_parts(_, _, []) ->
    ok;
abort_by_parts(PendingLog, TxId, [Part|Rest]) ->
    case ets:lookup(PendingLog, {TxId, Part}) of
	    [] ->
	        %lager:warning("Abort ~w ~w arrived early!", [TxId, Part]),
	        ets:insert(PendingLog, {{TxId, Part}, to_abort});
	    _ ->
    	    ets:delete(PendingLog, {TxId, Part})
    end,
    abort_by_parts(PendingLog, TxId, Rest). 

delete_version([{_, _, TxId}|Rest], TxId) -> 
    Rest;
delete_version([{C, V}|Rest], TxId) -> 
    [{C, V}|delete_version(Rest, TxId)];
delete_version([{TS, V, TId}|Rest], TxId) -> 
    [{TS, V, TId}|delete_version(Rest, TxId)].

replace_version([{_, Value, TxId}|Rest], TxId, CommitTime) -> 
    [{CommitTime, Value}|Rest];
replace_version([{C, V}|Rest], TxId, CommitTime) -> 
    [{C, V}|replace_version(Rest, TxId, CommitTime)];
replace_version([{TS, V, PrepTxId}|Rest], TxId, CommitTime) -> 
    [{TS, V, PrepTxId}|replace_version(Rest, TxId, CommitTime)].

