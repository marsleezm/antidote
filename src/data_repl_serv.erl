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
-define(READ_TIMEOUT, 15000).
%% API
-export([start_link/2]).

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
        append_values/3,
        get_table/1,
	    check_key/2,
        get_ts/2,
        clean_data/2,
	    check_table/1,
        verify_table/2,
        debug_read/3,
        prepare_specula/5,
        %commit_specula/4,
        %abort_specula/3,
        if_prepared/3,
        if_bulk_prepared/3,
        num_specula_read/1,
        single_read/2,
        read/4]).

%% Spawn

-record(state, {
        successors :: [atom()],
        replicated_log :: cache_id(),
        pending_log :: cache_id(),
        delay :: non_neg_integer(),
        init_ts_dict=false :: boolean(),
        num_specula_read=0 :: non_neg_integer(),
        num_read=0 :: non_neg_integer(),
        set_size :: non_neg_integer(),
        current_dict :: dict(),
        backup_dict :: dict(),
        ts_dict :: dict(),
        do_specula :: boolean(),
        name :: atom(),
		self :: atom()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name, Parts) ->
    gen_server:start_link({global, Name},
             ?MODULE, [Name, Parts], []).

read(Name, Key, TxId, Part) ->
    gen_server:call({global, Name}, {read, Key, TxId, Part}, ?READ_TIMEOUT).

get_table(Name) ->
    gen_server:call({global, Name}, {get_table}, ?READ_TIMEOUT).

get_ts(Name, WriteSet) ->
    gen_server:call({global, Name}, {get_ts, WriteSet}, ?READ_TIMEOUT).

single_read(Name, Key) ->
    TxId = tx_utilities:create_tx_id(0),
    gen_server:call({global, Name}, {read, Key, TxId}, ?READ_TIMEOUT).

append_values(Name, KeyValues, CommitTime) ->
    gen_server:call({global, Name}, {append_values, KeyValues, CommitTime}).

verify_table(Name, List) ->
    gen_server:call({global, Name}, {verify_table, List}, infinity).

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

prepare_specula(Name, TxId, Partition, WriteSet, PrepareTime) ->
    gen_server:cast({global, Name}, {prepare_specula, TxId, Partition, WriteSet, PrepareTime}).

relay_read(Name, Key, TxId, Reader) ->
    gen_server:cast({global, Name}, {relay_read, Key, TxId, Reader}).

clean_data(Name, Sender) ->
    gen_server:cast({global, Name}, {clean_data, Sender}).


%commit_specula(Name, TxId, Partition, CommitTime) ->
%    gen_server:cast({global, Name}, {commit_specula, TxId, Partition, CommitTime}).

%abort_specula(Name, TxId, Partition) ->
%    gen_server:cast({global, Name}, {abort_specula, TxId, Partition}).

%%%===================================================================
%%% Internal
%%%===================================================================


init([Name, _Parts]) ->
    lager:info("Data repl inited with name ~w", [Name]),
    ReplicatedLog = tx_utilities:open_public_table(repl_log),
    PendingLog = tx_utilities:open_private_table(pending_log),
    NumPartitions = length(hash_fun:get_partitions()),
    DoSpecula = antidote_config:get(do_specula),
    Concurrent = antidote_config:get(concurrent),
    %TsDict = lists:foldl(fun(Part, Acc) ->
    %            dict:store(Part, 0, Acc) end, dict:new(), Parts),
    %lager:info("Parts are ~w, TsDict is ~w", [Parts, dict:to_list(TsDict)]),
    %lager:info("Concurrent is ~w, num partitions are ~w", [Concurrent, NumPartitions]),
    {ok, #state{name=Name, set_size= max(NumPartitions*Concurrent, 40),
                pending_log = PendingLog, current_dict = dict:new(), ts_dict=dict:new(), do_specula=DoSpecula,
                backup_dict = dict:new(), replicated_log = ReplicatedLog}}.

handle_call({get_table}, _Sender, SD0=#state{replicated_log=ReplicatedLog}) ->
    {reply, ReplicatedLog, SD0};

handle_call({get_pid}, _Sender, SD0) ->
        {reply, self(), SD0};

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

handle_call({append_values, KeyValues, CommitTime}, _Sender, SD0=#state{replicated_log=ReplicatedLog}) ->
    lists:foreach(fun({Key, Value}) ->
                    true = ets:insert(ReplicatedLog, {Key, [{CommitTime, Value}]})
                end, KeyValues),
    {reply, ok, SD0};

handle_call({read, Key, TxId, {_Part, _}}, Sender, 
	    SD0=#state{replicated_log=ReplicatedLog, num_read=NumRead, pending_log=PendingLog,
                num_specula_read=NumSpeculaRead, do_specula=DoSpecula}) ->
    case DoSpecula of
        false ->
            case ready_or_block(TxId, Key, PendingLog, Sender) of
                not_ready->
                    {noreply, SD0};
                ready ->
                    Result = read_value(Key, TxId, ReplicatedLog),
                    {reply, Result, SD0#state{num_read=NumRead+1}}%i, relay_read={NumRR+1, AccRR+get_time_diff(T1, T2)}}}
            end;
        true ->
            case specula_read(TxId, Key, PendingLog, Sender) of
                not_ready->
                    {noreply, SD0};
                {specula, Value} ->
                    {reply, {ok, Value}, SD0#state{num_specula_read=NumSpeculaRead+1}};
                ready ->
                    Result = read_value(Key, TxId, ReplicatedLog),
                    %MyClock = TxId#tx_id.snapshot_time,
                    %TsDict1 = dict:update(Part, fun(OldTs) -> max(MyClock, OldTs) end, TsDict),
                    {reply, Result, SD0#state{num_read=NumRead+1}}
            end
    end;

handle_call({update_ts, Partitions}, _Sender, SD0=#state{ts_dict=TsDict, init_ts_dict=InitTs}) ->
    case InitTs of true ->  {reply, ok, SD0};
                   false ->  
                            Parts = find_parts_for_name(Partitions),
                            TsDict1 = lists:foldl(fun(Part, D) ->
                                dict:store(Part, 0, D)
                                end, TsDict, Parts),
                            {reply, ok, SD0#state{ts_dict=TsDict1, init_ts_dict=true}}
    end;

handle_call({get_ts, WriteSet}, _Sender, SD0=#state{pending_log=PendingLog}) ->
    MaxTs = lists:foldl(fun({Key, _Value}, ToPrepTS) ->
                case ets:lookup(PendingLog, Key) of
                    [] ->
                        ToPrepTS;
                    [{Key, [{_PrepTxId, _PrepTS, LastReaderTS, _PrepValue, _Reader}|_RemainList]}] ->
                        max(ToPrepTS, LastReaderTS+1);
                    [{Key, LastReaderTS}] ->
                        %lager:info("LastReader ts is ~p", [LastReaderTS]),
                        max(ToPrepTS, LastReaderTS+1)
                end end, 0, WriteSet),
    {reply, MaxTs, SD0};

handle_call({if_prepared, TxId, Keys}, _Sender, SD0=#state{replicated_log=ReplicatedLog}) ->
    Result = lists:all(fun(Key) ->
                    %lager:warning("Check ~w for ~w", [Key, TxId]),
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

handle_call({verify_table, List}, _Sender, SD0=#state{name=Name, replicated_log=ReplicatedLog}) ->
   lager:info("Start verifying on ~w", [Name]),
   lists:foreach(fun({K, V}=Elem) -> 
                    case ets:lookup(ReplicatedLog, K) of [Elem] -> ok;
                        Other -> 
                            lager:error("Doesn't match! Origin is [{~p, ~p}], Rep is ~p", [K, V, Other])
                    end end, List),
   {reply, ok, SD0};

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

handle_cast({relay_read, Key, TxId, Reader}, 
	    SD0=#state{replicated_log=ReplicatedLog}) ->
    %lager:warning("~w, ~p data repl read", [TxId, Key]),
    case ets:lookup(ReplicatedLog, Key) of
        [] ->
            %lager:warning("Nothing for ~p!", [Key]),
            gen_server:reply(Reader, {ok, []}),
            {noreply, SD0};
        [{Key, ValueList}] ->
            MyClock = TxId#tx_id.snapshot_time,
            Value = find_version(ValueList, MyClock),
            %lager:warning("Got value for ~p", [ValueList, Key]),
            gen_server:reply(Reader, Value),
            {noreply, SD0}
    end;

handle_cast({prepare_specula, TxId, Partition, WriteSet, ToPrepTS}, 
	    SD0=#state{pending_log=PendingLog}) ->
    KeySet = lists:foldl(fun({Key, Value}, KS) ->
                      case ets:lookup(PendingLog, Key) of
                          [] ->
                              true = ets:insert(PendingLog, {Key, [{TxId, ToPrepTS, ToPrepTS, Value, []}]}),
                              [Key|KS];
                          [{Key, [{PrepTxId, PrepTS, _LastReaderTS, PrepValue, Reader}|RemainList]}] ->
                              true = ets:insert(PendingLog, {Key, [{TxId, ToPrepTS, ToPrepTS, Value, []}|[{PrepTxId, PrepTS, PrepValue, Reader}|RemainList]]}),
                              [Key|KS];
                          [{Key, _}] ->
                              true = ets:insert(PendingLog, {Key, [{TxId, ToPrepTS, ToPrepTS, Value, []}]}),
                              [Key|KS]
                      end end, [], WriteSet),
    ets:insert(PendingLog, {{TxId, Partition}, KeySet}),
   %lager:warning("Specula prepare for [~w, ~w, KeySet is ~p]", [TxId, Partition, KeySet]),
    {noreply, SD0};

handle_cast({clean_data, Sender}, SD0=#state{replicated_log=OldReplicatedLog, pending_log=OldPendingLog}) ->
    %lager:info("Got request!"),
    ets:delete(OldPendingLog),
    ets:delete(OldReplicatedLog),
    ReplicatedLog = tx_utilities:open_public_table(repl_log),
    PendingLog = tx_utilities:open_private_table(pending_log), 
    lager:info("Data repl replying!"),
    Sender ! cleaned,
    {noreply, SD0#state{pending_log = PendingLog, current_dict = dict:new(), backup_dict = dict:new(), 
                num_specula_read=0, num_read=0, replicated_log = ReplicatedLog, init_ts_dict=false}};

%% Where shall I put the speculative version?
%% In ets, faster for read.
%handle_cast({abort_specula, TxId, Partition}, 
%	    SD0=#state{replicated_log=ReplicatedLog, pending_log=PendingLog, ts_dict=TsDict}) ->
%    lager:info("Abort specula for ~w, ~w", [TxId, Partition]),
    %TsDict1 = lists:foldl(fun(Partition, D) ->
%                [{{TxId, Partition}, KeySet}] = ets:lookup(PendingLog, {TxId, Partition}),
%                ets:delete(PendingLog, {TxId, Partition}),
%                MaxTs = clean_abort_prepared(PendingLog, KeySet, TxId, ReplicatedLog, 0),
%                TsDict1= dict:update(Partition, fun(OldTs) -> max(MaxTs, OldTs) end, TsDict),
     %     end, TsDict, Partitions),
%    specula_utilities:deal_abort_deps(TxId),
%    {noreply, SD0#state{ts_dict=TsDict1}};
    
%handle_cast({commit_specula, TxId, Partition, CommitTime}, 
%	    SD0=#state{replicated_log=ReplicatedLog, pending_log=PendingLog, ts_dict=TsDict}) ->
%   %lager:warning("Committing specula for ~w ~w", [TxId, Partition]),
    %TsDict1 = lists:foldl(fun(Partition, D) ->
%              [{{TxId, Partition}, KeySet}] = ets:lookup(PendingLog, {TxId, Partition}),
%              ets:delete(PendingLog, {TxId, Partition}),
%              MaxTs = update_store(KeySet, TxId, CommitTime, ReplicatedLog, PendingLog, 0),
%    TsDict1 = dict:update(Partition, fun(OldTs) -> max(MaxTs, OldTs) end, TsDict),
    %      end, TsDict, Partitions),
%    specula_utilities:deal_commit_deps(TxId, CommitTime),
%    {noreply, SD0#state{ts_dict=TsDict1}};

handle_cast({repl_prepare, Type, TxId, Partition, WriteSet, TimeStamp, Sender}, 
	    SD0=#state{pending_log=PendingLog, replicated_log=ReplicatedLog, current_dict=CurrentDict, backup_dict=BackupDict}) ->
    case Type of
        prepared ->
             %lager:warning("Got repl prepare for ~w, ~w", [TxId, Partition]),
            case dict:find(TxId, CurrentDict) of 
                {ok, aborted} ->
                   %lager:warning("~w, ~w aborted already", [TxId, Partition]),
                    {noreply, SD0};
                {ok, {committed, CommitTS}} ->
                   %lager:warning("~w, ~w committed already", [TxId, Partition]),
                    add_to_commit_tab(WriteSet, CommitTS, ReplicatedLog),
                    {noreply, SD0};
                error ->
                    case dict:find(TxId, BackupDict) of 
                        {ok, aborted} ->
                           %lager:warning("~w, ~w aborted already", [TxId, Partition]),
                            {noreply, SD0};
                        {ok, {committed, CommitTS}} ->
                           %lager:warning("~w, ~w committed already", [TxId, Partition]),
                            add_to_commit_tab(WriteSet, CommitTS, ReplicatedLog),
                            {noreply, SD0};
                        error ->
                            {KeySet, ToPrepTS} =   lists:foldl(fun({Key, _Value}, {KS, Ts}) ->
                                                        case ets:lookup(PendingLog, Key) of
                                                            [] -> {[Key|KS], Ts};
                                                            [{Key, [{_PrepTxId, _, LastReaderTS, _, _}|_Rest]}] ->
                                                                {[Key|KS], max(Ts, LastReaderTS+1)};
                                                            [{Key, LastReaderTS}] ->%lager:warning("LastReaderTS is ~w, Ts is ~w", [LastReaderTS, Ts]), 
                                                                {[Key|KS], max(Ts, LastReaderTS+1)}
                                                        end end, {[], TimeStamp}, WriteSet),
                            lists:foreach(fun({Key, Value}) ->
                                        case ets:lookup(PendingLog, Key) of
                                            [] ->
                                                true = ets:insert(PendingLog, {Key, [{TxId, ToPrepTS, ToPrepTS, Value, []}]});
                                            [{Key, [{PrepTxId, PrepTS, _LastReaderTS, PrepValue, Reader}|RemainList]}] ->
                                                true = ets:insert(PendingLog, {Key, [{TxId, ToPrepTS, ToPrepTS, Value, []}|[{PrepTxId, PrepTS, PrepValue, Reader}|RemainList]]});
                                            [{Key, _}] ->
                                                true = ets:insert(PendingLog, {Key, [{TxId, ToPrepTS, ToPrepTS, Value, []}]})
                            end end,  WriteSet),

                             %lager:warning("Got repl prepare for ~w, ~p", [TxId, KeySet]),
                            ets:insert(PendingLog, {{TxId, Partition}, KeySet}),
                            gen_server:cast({global, Sender}, {ack, Partition, TxId, ToPrepTS}), 
                            {noreply, SD0}
                    end
            end;
        single_commit ->
            AppendFun = fun({Key, Value}) ->
                case ets:lookup(ReplicatedLog, Key) of
                    [] ->
                        %lager:warning("Data repl inserting ~p, ~p of ~w to table", [Key, Value, TimeStamp]),
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
	    SD0=#state{replicated_log=ReplicatedLog, set_size=SetSize, pending_log=PendingLog, do_specula=DoSpecula,  current_dict=CurrentDict}) ->
    %lager:warning("repl commit for ~w ~w", [TxId, Partitions]),
    CurrentD1 = lists:foldl(fun(Partition, S) ->
            case ets:lookup(PendingLog, {TxId, Partition}) of
                [{{TxId, Partition}, KeySet}] ->
                    ets:delete(PendingLog, {TxId, Partition}),
                    update_store(KeySet, TxId, CommitTime, ReplicatedLog, PendingLog),
                    S;
                [] ->
                   %lager:warning("Repl commit arrived early! ~w", [TxId]),
                    dict:store(TxId, {committed, CommitTime}, S)
        end end, CurrentDict, Partitions),
    case DoSpecula of
        true -> specula_utilities:deal_commit_deps(TxId, CommitTime); 
        _ -> ok
    end,
    case dict:size(CurrentD1) > SetSize of
          true ->
            {noreply, SD0#state{current_dict=dict:new(), backup_dict=CurrentD1}};
          false ->
            {noreply, SD0#state{current_dict=CurrentD1}}
      end;

handle_cast({repl_abort, TxId, Partitions}, 
	    SD0=#state{pending_log=PendingLog, set_size=SetSize, replicated_log=ReplicatedLog, do_specula=DoSpecula, current_dict=CurrentDict}) ->
   %lager:warning("repl abort for ~w ~w", [TxId, Partitions]),
    CurrentDict1 = lists:foldl(fun(Partition, S) ->
               case ets:lookup(PendingLog, {TxId, Partition}) of
                    [{{TxId, Partition}, KeySet}] ->
                       %lager:warning("Found ~p for ~w, ~w", [KeySet, TxId, Partition]),
                        ets:delete(PendingLog, {TxId, Partition}),
                        _MaxTs = clean_abort_prepared(PendingLog, KeySet, TxId, ReplicatedLog, 0),
                        S;
                    [] -> 
                       %lager:warning("Repl abort arrived early! ~w", [TxId]),
                        dict:store(TxId, aborted, S)
                end
        end, CurrentDict, Partitions),
    case DoSpecula of
        true -> specula_utilities:deal_abort_deps(TxId);
        _ -> ok
    end,
    case dict:size(CurrentDict1) > SetSize of
        true ->
           %lager:warning("Current set is too large!"),
            {noreply, SD0#state{current_dict=dict:new(), backup_dict=CurrentDict1}};
        false ->
            {noreply, SD0#state{current_dict=CurrentDict1}}
    end;

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

read_value(Key, TxId, InMemoryStore) ->
    case ets:lookup(InMemoryStore, Key) of
        [] ->
            {ok, []};
        [{Key, ValueList}] ->
            MyClock = TxId#tx_id.snapshot_time,
            find_version(ValueList, MyClock)
    end.

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
    {ok, []};
find_version([{TS, Value}|Rest], SnapshotTime) ->
    case SnapshotTime >= TS of
        true ->
            {ok, Value};
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

update_store([], _TxId, _TxCommitTime, _InMemoryStore, _PreparedTxs) ->
    ok;
update_store([Key|Rest], TxId, TxCommitTime, InMemoryStore, PreparedTxs) ->
    {PendingReaders, Value} = case ets:lookup(PreparedTxs, Key) of
        [{Key, [{TxId, _PT, LastReaderTS, PrepValue, Readers}|PendingOthers]}] ->
            case PendingOthers of
                [] -> ets:insert(PreparedTxs, {Key, max(TxCommitTime, LastReaderTS)});
                [{NTxId, NPT, NV, NPendingReader}|Others] -> ets:insert(PreparedTxs, {Key, [{NTxId, NPT, max(TxCommitTime, LastReaderTS), NV, NPendingReader}|Others]})
            end,
            lager:warning("For ~w, My Readers are ~w", [TxId, Readers]),
            {Readers, PrepValue};
        [{Key, [{OtherTxId, OtherPT, LastReaderTS, OtherValue, OtherPendingReaders}|PendingOthers]}] ->
            {{TxId, _PrepTime, PrepValue, Readers}, RemainPending} = delete_item(PendingOthers, TxId, []),
            ets:insert(PreparedTxs, {Key, [{OtherTxId, OtherPT, max(LastReaderTS, TxCommitTime), OtherValue, OtherPendingReaders}|RemainPending]}),
            lager:warning("For ~w, Readers are ~w, previous list is ~w", [TxId, Readers, PendingOthers]),
            {Readers, PrepValue} 
    end,
    Values = case ets:lookup(InMemoryStore, Key) of
                [] ->
                    true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, Value}]}),
                    [[], Value];
                [{Key, ValueList}] ->
                    {RemainList, _} = lists:split(min(?NUM_VERSIONS,length(ValueList)), ValueList),
                    [{_CommitTime, First}|_] = RemainList,
                    true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, Value}|RemainList]}),
                    [First, Value]
            end,
    lists:foreach(fun({SnapshotTime, Sender}) ->
            case SnapshotTime >= TxCommitTime of
                true ->
                    gen_server:reply(Sender, {ok, lists:nth(2,Values)});
                false ->
                    gen_server:reply(Sender, {ok, hd(Values)})
            end end, PendingReaders),
    update_store(Rest, TxId, TxCommitTime, InMemoryStore, PreparedTxs).

clean_abort_prepared(_PreparedTxs, [], _TxId, _InMemoryStore, TS) ->
    TS;
clean_abort_prepared(PendingLog, [Key | Rest], TxId, ReplicatedLog, TS) ->
    %[{Key, List}] = ets:lookup(PendingLog, Key),
    {PendingReaders, Value} = case ets:lookup(PendingLog, Key) of
        [{Key, [{TxId, _PT, LastReaderTS, PrepValue, Readers}|PendingOthers]}] ->
            case PendingOthers of
                [] -> ets:delete(PendingLog, Key);
                [{NTxId, NPT, NV, NPendingReader}|Others] -> ets:insert(PendingLog, {Key, [{NTxId, NPT, LastReaderTS, NV, NPendingReader}|Others]})
            end,
            {Readers, PrepValue};
        [{Key, [{OtherTxId, OtherPT, LastReaderTS, OtherValue, OtherPendingReaders}|PendingOthers]}] ->
            {{TxId, _PrepTime, PrepValue, Readers}, RemainPending} = delete_item(PendingOthers, TxId, []),
            ets:insert(PendingLog, {Key, [{OtherTxId, OtherPT, LastReaderTS, OtherValue, OtherPendingReaders}|RemainPending]}),
            {Readers, PrepValue}
    end,
    %lager:warning("Clean abort: for key ~p, readers are ~p, prep deps are ~w", [Key, Readers, RemainList]),
    case PendingReaders of
        [] ->
            clean_abort_prepared(PendingLog, Rest, TxId, ReplicatedLog, TS);
        _ ->
            Value = case ets:lookup(ReplicatedLog, Key) of
                        [{Key, ValueList}] ->
                            {_, V} = hd(ValueList),
                            V;
                        [] ->
                            []
                    end,
            MaxRTS = lists:foldl(fun({SnapshotTime, Sender}, ReaderTS) ->
                          gen_server:reply(Sender, {ok, Value}), max(SnapshotTime, ReaderTS)
                      end,
                    0, PendingReaders),
            clean_abort_prepared(PendingLog, Rest, TxId, ReplicatedLog, max(TS, MaxRTS))
    end.

%append_by_parts(_, _, _, _, []) ->
%    ok;
%append_by_parts(PendingLog, ReplicatedLog, TxId, CommitTime, [Part|Rest]) ->
%    case ets:lookup(PendingLog, {TxId, Part}) of
%        [{{TxId, Part}, {WriteSet, _}}] ->
%            %lager:warning("For ~w ~w found writeset", [TxId, Part, WriteSet]),
%            AppendFun = fun({Key, Value}) ->
%                            %lager:warning("Adding ~p, ~p wth ~w of ~w into log", [Key, Value, CommitTime, TxId]),
%                            case ets:lookup(ReplicatedLog, Key) of
%                                [] ->
%                                    true = ets:insert(ReplicatedLog, {Key, [{CommitTime, Value}]});
%                                [{Key, ValueList}] ->
%                                    {RemainList, _} = lists:split(min(?NUM_VERSIONS,length(ValueList)), ValueList),
%                                    true = ets:insert(ReplicatedLog, {Key, [{CommitTime, Value}|RemainList]})
%                            end end,
%            lists:foreach(AppendFun, WriteSet),
%            ets:delete(PendingLog, {TxId, Part});
%        [] ->
%	     %%lager:warning("Commit ~w ~w arrived early! Committing with ~w", [TxId, Part, CommitTime]),
%	        ets:insert(PendingLog, {{TxId, Part}, CommitTime})
%    end,
%    append_by_parts(PendingLog, ReplicatedLog, TxId, CommitTime, Rest). 

%abort_by_parts(_, _, [], Set) ->
%    Set;
%abort_by_parts(PendingLog, TxId, [Part|Rest], Set) ->
%    case ets:lookup(PendingLog, {TxId, Part}) %of
%	    [] ->
 %           Set1 = sets:add_element({TxId, Part}, Set),
 %           abort_by_parts(PendingLog, TxId, Rest, Set1); 
%	    _ ->
 %   	    ets:delete(PendingLog, {TxId, Part}),
 %           abort_by_parts(PendingLog, TxId, Rest, Set) 
 %   end.

%% The first case should never happen
delete_item([], _TxId, Prev) ->
    {{}, Prev};
delete_item([{TxId, T, V, R}|Rest], TxId, Prev) ->
    {{TxId, T, V, R}, Prev++lists:reverse(Rest)};
delete_item([{WhateverId, T, V, R}|Rest], TxId, Prev) ->
    delete_item(Rest, TxId, [{WhateverId, T, V, R}|Prev]).

%delete_version([{_, _, TxId}|Rest], TxId) -> 
%    Rest;
%delete_version([{C, V}|Rest], TxId) -> 
%    [{C, V}|delete_version(Rest, TxId)];
%delete_version([{TS, V, TId}|Rest], TxId) -> 
%    [{TS, V, TId}|delete_version(Rest, TxId)].

%replace_version([{_, Value, TxId}|Rest], TxId, CommitTime) -> 
%    [{CommitTime, Value}|Rest];
%replace_version([{C, V}|Rest], TxId, CommitTime) -> 
%    [{C, V}|replace_version(Rest, TxId, CommitTime)];
%replace_version([{TS, V, PrepTxId}|Rest], TxId, CommitTime) -> 
%    [{TS, V, PrepTxId}|replace_version(Rest, TxId, CommitTime)].

%%% A list that has timestamp in descending order
%insert_version([], MyTxId, MyPrepTime, MyValue) -> 
%    [{MyTxId, MyPrepTime, MyValue, []}];
%insert_version([{_TxId, Timestamp, _Value, _Reader}|_Rest]=VList, MyTxId, MyPrepTime, MyValue) when MyPrepTime >= Timestamp -> 
%    [{MyTxId, MyPrepTime, MyValue, []}| VList];
%insert_version([{TxId, Timemstamp, Value, Reader}|Rest], MyTxId, MyPrepTime, MyValue) -> 
%    [{TxId, Timemstamp, Value, Reader}|insert_version(Rest, MyTxId, MyPrepTime, MyValue)].

ready_or_block(TxId, Key, PreparedTxs, Sender) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(PreparedTxs, Key) of
        [] ->
            ets:insert(PreparedTxs, {Key, SnapshotTime}),
            ready;
        [{Key, [{PreparedTxId, PrepareTime, LastReaderTime, Value, PendingReader}|Others]}] ->
            case PrepareTime =< SnapshotTime of
                true ->
                    ets:insert(PreparedTxs, {Key, [{PreparedTxId, PrepareTime, max(LastReaderTime, SnapshotTime), Value,
                        [{TxId#tx_id.snapshot_time, Sender}|PendingReader]}|Others]}),
                    not_ready;
                false ->
                    ready
            end;
        [{Key, LastReaderTime}] ->
            ets:insert(PreparedTxs, {Key, max(SnapshotTime, LastReaderTime)}),
            ready
    end.

specula_read(TxId, Key, PreparedTxs, Sender) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(PreparedTxs, Key) of
        [] ->
            ets:insert(PreparedTxs, {Key, SnapshotTime}),
            ready;
        [{Key, [{PreparedTxId, PrepareTime, LastReaderTime, Value, PendingReaders}| PendingPrepare]}] ->
        %[{Key, VersionList}] ->
            case SnapshotTime >= PrepareTime of
                true ->
                    case prepared_by_local(PreparedTxId) of
                        true ->
                            case SnapshotTime > LastReaderTime of
                                true -> ets:insert(PreparedTxs, {Key, [{PreparedTxId, PrepareTime, SnapshotTime, Value, PendingReaders}| PendingPrepare]});
                                false -> ok
                            end,
                            add_read_dep(TxId, PreparedTxId, Key), 
                            {specula, Value};
                        false ->
                            NewReaderTime = max(SnapshotTime, LastReaderTime),
                            ets:insert(PreparedTxs, {Key, [{PreparedTxId, PrepareTime, NewReaderTime, Value, PendingReaders}| PendingPrepare]}),
                            ready
                    end;
                false ->
                    case read_or_block(PendingPrepare, [], SnapshotTime, Sender) of
                        ready -> ready;
                        {specula, PTxId, Value} ->%lager:warning("Specula reading ~p, from ~w to ~w", [Key, TxId, PTxId]), 
                            case SnapshotTime > LastReaderTime of
                                true -> ets:insert(PreparedTxs, {Key, [{PreparedTxId, PrepareTime, SnapshotTime, Value, [{SnapshotTime, Sender}|PendingReaders]}| PendingPrepare]});
                                false -> ok
                            end,
                            add_read_dep(TxId, PTxId, Key), 
                            {specula, Value};
                        {not_ready, NewList, _PTxId} ->
                            ets:insert(PreparedTxs, {Key, NewList}), not_ready
                    end
            end;
        [{Key, LastReaderTime}] ->
            ets:insert(PreparedTxs, {Key, max(SnapshotTime, LastReaderTime)}),
            ready
    end.

read_or_block([], _, _SnapshotTime, _) ->
    ready;
read_or_block([{PTxId, PrepTime, Value, Reader}|Rest], Prev, SnapshotTime, Sender) when SnapshotTime >= PrepTime ->
    case prepared_by_local(PTxId) of
        true ->
            %lager:warning("Prepare by local"),
            {specula, PTxId, Value};
        false ->
            case Prev of [] -> {not_ready, [{PTxId, PrepTime, Value, [{SnapshotTime, Sender}|Reader]}|Rest], PTxId};
                          _ -> {not_ready, lists:reverse(Prev)++[{PTxId, PrepTime, Value, [{SnapshotTime, Sender}|Reader]}|Rest], PTxId}
            end 
    end;
read_or_block([{PTxId, PrepTime, Value, Reader}|Rest], Prev, SnapshotTime, Sender) ->
    read_or_block(Rest, [{PTxId, PrepTime, Value, Reader}|Prev], SnapshotTime, Sender).

add_read_dep(ReaderTx, WriterTx, _Key) ->
   %lager:warning("Add read dep from ~w to ~w", [ReaderTx, WriterTx]),
    ets:insert(dependency, {WriterTx, ReaderTx}),
    ets:insert(anti_dep, {ReaderTx, WriterTx}).

prepared_by_local(TxId) ->
    node(TxId#tx_id.server_pid) == node().

add_to_commit_tab(WriteSet, TxCommitTime, Tab) ->
    lists:foreach(fun({Key, Value}) ->
            case ets:lookup(Tab, Key) of
                [] ->
                    true = ets:insert(Tab, {Key, [{TxCommitTime, Value}]});
                [{Key, ValueList}] ->
                    {RemainList, _} = lists:split(min(?NUM_VERSIONS,length(ValueList)), ValueList),
                    true = ets:insert(Tab, {Key, [{TxCommitTime, Value}|RemainList]})
            end
    end, WriteSet).

find_parts_for_name(Partitions) ->
    Repls = antidote_config:get(to_repl),
    [ReplNodes] = [L || {N, L} <- Repls, N == node()],
    lists:foldl(fun(Node, List) ->
                PartList = [P || {P, N} <- Partitions, N == Node],
                PartList++List
                end, [], ReplNodes).

