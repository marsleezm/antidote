%% -------------------------------------------------------------------
%
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
-module(specula_utilities).
-define(SPECULA_TIMEOUT, 2000).

-include("include/speculation.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(SEND_MSG(PID, MSG), PID ! MSG).
-else.
-define(SEND_MSG(PID, MSG), riak_core_vnode:reply(PID, MSG)).
-endif.

-export([should_specula/2, make_prepared_specula/9, find_specula_version/6,
            clean_specula_committed/4, coord_should_specula/1, make_specula_final/7]).

coord_should_specula(_TxnMetadata) ->
    %case TxnMetadata#txn_metadata.final_committed of
    %    true ->
    %        true;
    %    false ->
    %        false
    %end.
    true.

%% If this txn corresponds to any specula-committed version,
%% 1. Make the specula_committed version final committed
%% 2. Check txns that depend on this txn
%%      If depending txn should be aborted, remove them from state and notify corresponding coord
%%      If depending txn works fine, either return to coord 'prepared' or 'read-valid'. 
%% Return true if found any specula version and made specula; false otherwise
make_specula_final(TxId, Key, TxCommitTime, SpeculaStore, InMemoryStore, PreparedStore, SpeculaDep) ->
    %% Firstly, make this specula version finally committed.
    case ets:lookup(SpeculaStore, Key) of
        [{Key, [{TxId, _, SpeculaValue}|T]}] ->    
            case ets:lookup(InMemoryStore, Key) of
                  [] ->
                      true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, SpeculaValue}]});
                  [{Key, ValueList}] ->
                      {RemainList, _} = lists:split(min(20,length(ValueList)), ValueList),
                      true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, SpeculaValue}|RemainList]})
            end,
            %%Remove this version from specula_store
            true = ets:insert(SpeculaStore, {Key, T}),
            %% Check if any txn depends on this version
            case ets:lookup(SpeculaDep, Key) of
                [] -> %% No dependency, do nothing!
                    ok;
                [{Key, DepList}] -> %% Do something for each of the dependency...
                    handle_dependency(DepList, TxCommitTime, PreparedStore, SpeculaStore, Key),
                    true = ets:delete(SpeculaDep, Key)
            end,
            true;
        [{Key, []}] -> %% The version should still be in prepared table. It's not made specula yet.
            false;    
        [] -> %% The version should still be in prepared table. It's not made specula yet.
            false;    
        Record ->
            lager:warning("Something is wrong!!!! ~w", [Record]),
            error
    end.

clean_specula_committed(TxId, Key, SpeculaStore, SpeculaDep) ->
    case ets:lookup(SpeculaStore, Key) of
        [{Key, [{TxId, _, _SpeculaValue}|T]}] ->
            ets:insert(SpeculaStore, {Key, T}),
            case ets:lookup(SpeculaDep, Key) of
                [] ->
                    ok;
                [{Key, List}] ->
                    handle_abort_dependency(List),
                    ets:delete(SpeculaDep, Key)
            end,
            true;
        [{Key, []}] -> %% Just prepared
            false;    
        [] -> %% Just prepared
            false;    
        Record ->
            lager:warning("Something is wrong!!!! ~w", [Record]),
            error
    end.


should_specula(PreparedTime, SnapshotTime) ->
    case SnapshotTime - ?SPECULA_TIMEOUT > PreparedTime of
        false ->
            NowTime = clocksi_vnode:now_microsec(now()),
            NowTime - ?SPECULA_TIMEOUT > PreparedTime;
        true ->
            true
    end.

make_prepared_specula(Key, Record, SnapshotTime, PreparedStore, InMemoryStore,
                             SpeculaStore, SpeculaDep, OpType, CoordPId) ->
    {TxId, PrepareTime, Type, {Param, Actor}} = Record,
    SpeculaValue =  case ets:lookup(SpeculaStore, Key) of 
                        [] ->
                            %% Fetch from committed store
                            case ets:lookup(InMemoryStore, Key) of
                                [] ->
                                    NewSpeculaValue = generate_snapshot([], Type, Param, Actor),
                                    true = ets:insert(SpeculaStore, {Key, [{TxId, PrepareTime, NewSpeculaValue}]}),
                                    NewSpeculaValue;
                                [{Key, CommittedVersions}] ->
                                    [{_CommitTime, Snapshot}|_] = CommittedVersions,
                                    NewSpeculaValue = generate_snapshot(Snapshot, Type, Param, Actor),
                                    true = ets:insert(SpeculaStore, {Key, [{TxId, PrepareTime, NewSpeculaValue}]}),
                                    NewSpeculaValue
                            end;
                        [{Key, SpeculaVersions}] ->
                            [{SpeculaTxId, _PrepareTime, Snapshot}|_] = SpeculaVersions,
                            NewSpeculaValue = generate_snapshot(Snapshot, Type, Param, Actor),
                            true = ets:insert(SpeculaStore, {Key, 
                                    [{TxId, PrepareTime, NewSpeculaValue}|SpeculaVersions]}),
                            add_specula_meta(SpeculaDep, SpeculaTxId, TxId, SnapshotTime, OpType, CoordPId),
                            NewSpeculaValue
                    end,
    true = ets:delete(PreparedStore, Key),
    {specula, {Type, SpeculaValue}}.

find_specula_version(TxId, Key, SnapshotTime, SpeculaStore, SpeculaDep, SenderPId) ->
    case ets:lookup(SpeculaStore, Key) of
        [] -> %% No specula version
            false;
        [{Key, ValueList}] ->
            case find_version(ValueList, SnapshotTime) of
                false -> %% No corresponding specula version TODO: will this ever happen?
                    false;
                {DependingTxId, Value} -> 
                    add_specula_meta(SpeculaDep, DependingTxId, TxId, SnapshotTime, read, SenderPId),
                    Value
            end
    end.
            

find_version([], _SnapshotTime) ->
    false;
find_version([{TxId, TS, Value}|Rest], SnapshotTime) ->
    case SnapshotTime >= TS of
        true ->
            {TxId,Value};
        false ->
            find_version(Rest, SnapshotTime)
    end.


%%%%%%%%%%%%%%%%  Private function %%%%%%%%%%%%%%%%%
generate_snapshot(Snapshot, Type, Param, Actor) ->
    case Snapshot of
        [] ->
            Init = Type:new(),
            {ok, NewSnapshot} = Type:update(Param, Actor, Init),
            NewSnapshot;
        _ ->
            {ok, NewSnapshot} = Type:update(Param, Actor, Snapshot),
            NewSnapshot
    end.          

add_specula_meta(SpeculaDep, DependingTxId, TxId, SnapshotTime, OpType, CoordPId) ->
    case ets:lookup(SpeculaDep, DependingTxId) of
        [] ->
            true = ets:insert(SpeculaDep, {DependingTxId, [{TxId, OpType, CoordPId, SnapshotTime}]});
        [{DependingTxId, DepList}] ->
            true = ets:insert(SpeculaDep, {DependingTxId, [{TxId, OpType, CoordPId, SnapshotTime}|DepList]})
    end.

    
handle_dependency([], _TxCommitTime, _PreparedStore, _SpeculaStore, _Key) ->
    ok;
handle_dependency([{DepTxId, OpType, CoordPId, DepSnapshotTime}|T], TxCommitTime, 
                PreparedStore, SpeculaStore, Key) ->
    case DepSnapshotTime < TxCommitTime of
        true -> %% The transaction committed with larger timestamp which will invalide depending txns..
                %% Has to abort...
            %% Notify coordinator
            ?SEND_MSG(CoordPId, {abort, DepTxId}),
            case OpType of
                update ->
                    case ets:lookup(PreparedStore, Key) of                
                        [] ->
                            case ets:lookup(SpeculaStore, Key) of
                                [] ->
                                    lager:warning("It's impossible!!");
                                [{Key, [{DepTxId, _DepPrepareTime, _Value}|T]}] ->
                                    %% TODO: should I do cascading abort in this scenario?
                                    %% Seems not, because aborting txns are announced by coordinators..
                                    ets:insert(SpeculaStore, {Key, T});
                                _ ->
                                    lager:warning("It's impossible!!")
                            end;
                        _ ->
                            ets:delete(PreparedStore, Key)
                    end;
                read ->
                    ok
            end; 
        false ->
            case OpType of
                update ->
                    ?SEND_MSG(CoordPId, {prepare, DepTxId});
                read ->
                    ?SEND_MSG(CoordPId, {read_valid, DepTxId, Key})
            end
    end,
    handle_dependency(T, TxCommitTime, PreparedStore, SpeculaStore, Key).


handle_abort_dependency([]) ->
    ok;
handle_abort_dependency([{DepTxId, OpType, CoordPId, _DepSnapshotTime}|T]) ->
    case OpType of
        update ->
            ?SEND_MSG(CoordPId, {prepare, DepTxId});
        read ->  
            ?SEND_MSG(CoordPId, {abort, DepTxId})
    end,
    handle_abort_dependency(T).

-ifdef(TEST).
generate_snapshot_test() ->
    Type = riak_dt_pncounter,
    Snapshot1 = generate_snapshot([], Type, increment, haha),
    ?assertEqual(1, Type:value(Snapshot1)),
    Snapshot2 = generate_snapshot(Snapshot1, Type, increment, haha),
    ?assertEqual(2, Type:value(Snapshot2)).

make_specula_final_test() ->
    TxId1 = tx_utilities:create_transaction_record(clocksi_vnode:now_microsec(now())),
    TxId2 = tx_utilities:create_transaction_record(clocksi_vnode:now_microsec(now())),
    TxId3 = tx_utilities:create_transaction_record(clocksi_vnode:now_microsec(now())),
    Key = 1,
    TxCommitTime = 1000, 
    SpeculaStore = ets:new(specula_store, [set,named_table,protected]),
    InMemoryStore = ets:new(inmemory_store, [set,named_table,protected]),
    PreparedStore = ets:new(prepared_store, [set,named_table,protected]),
    SpeculaDep = ets:new(specula_dep, [set,named_table,protected]),
    ets:insert(SpeculaStore, {Key, [{TxId1, 900, whatever}]}),

    %% Will succeed
    Result1 = make_specula_final(TxId1, Key, TxCommitTime, SpeculaStore, InMemoryStore, 
                PreparedStore, SpeculaDep),
    ?assertEqual(Result1, true),
    ?assertEqual([{Key, []}], ets:lookup(SpeculaStore, Key)),
    ?assertEqual([{Key, [{TxCommitTime, whatever}]}], ets:lookup(InMemoryStore, Key)),

    Result2 = make_specula_final(TxId1, Key, TxCommitTime, SpeculaStore, InMemoryStore, 
                PreparedStore, SpeculaDep),
    ?assertEqual(Result2, false),
    
    %% Wrong key
    TxCommitTime2 = 2000,
    ets:insert(SpeculaStore, {Key, [{TxId2, 990, whereever}]}),
    Result3 = make_specula_final(TxId1, Key, TxCommitTime2, SpeculaStore, InMemoryStore, 
                PreparedStore, SpeculaDep),
    ?assertEqual(Result3, error),

    %% Multiple values in inmemory_store will not lost.
    Result4 = make_specula_final(TxId2, Key, TxCommitTime2, SpeculaStore, InMemoryStore, 
                PreparedStore, SpeculaDep),
    ?assertEqual(Result4, true),
    ?assertEqual([{Key, []}], ets:lookup(SpeculaStore, Key)),
    ?assertEqual([{Key, [{TxCommitTime2, whereever}, {TxCommitTime, whatever}]}], 
            ets:lookup(InMemoryStore, Key)),

    %% Deps will be handled correctly
    CoordPId = self(),
    Dependency = [{TxId3, update, CoordPId, 1500}, {TxId3, update, CoordPId, 2500}, 
            {TxId3, read, CoordPId, 1500}, {TxId3, read, CoordPId, 2500}],
    ets:insert(SpeculaStore, {Key, [{TxId2, 999, lotsofdep}]}),
    ets:insert(SpeculaDep, {Key, Dependency}),
    Result5 = make_specula_final(TxId2, Key, TxCommitTime2, 
                SpeculaStore, InMemoryStore, PreparedStore, SpeculaDep),
    receive Msg1 ->
        ?assertEqual(Msg1, {abort, TxId3})
    end,
    receive Msg2 ->
        ?assertEqual(Msg2, {prepare, TxId3})
    end,
    receive Msg3 ->
        ?assertEqual(Msg3, {abort, TxId3})
    end,
    receive Msg4 ->
        ?assertEqual(Msg4, {read_valid, TxId3, Key})
    end,
    ?assertEqual(Result5, true),
    ?assertEqual(ets:lookup(SpeculaDep, Key), []),

    ets:delete(SpeculaStore),
    ets:delete(InMemoryStore),
    ets:delete(PreparedStore),
    ets:delete(SpeculaDep),
    pass.
    
clean_specula_committed_test() ->
    TxId1 = tx_utilities:create_transaction_record(clocksi_vnode:now_microsec(now())),
    TxId2 = tx_utilities:create_transaction_record(clocksi_vnode:now_microsec(now())),
    CoordPId = self(),
    Key = 1,
    SpeculaStore = ets:new(specula_store, [set,named_table,protected]),
    SpeculaDep = ets:new(specula_dep, [set,named_table,protected]),
    
    Result1 = clean_specula_committed(TxId1, Key, SpeculaStore, SpeculaDep),
    ?assertEqual(Result1, false),

    ets:insert(SpeculaStore, {Key, [{TxId1, 900, value1}]}), 
    Result2 = clean_specula_committed(TxId2, Key, SpeculaStore, SpeculaDep),
    ?assertEqual(Result2, error),

    Result3 = clean_specula_committed(TxId1, Key, SpeculaStore, SpeculaDep),
    ?assertEqual(Result3, true),

    Result4 = clean_specula_committed(TxId1, Key, SpeculaStore, SpeculaDep),
    ?assertEqual(Result4, false),

    ets:insert(SpeculaStore, {Key, [{TxId1, 950, value1}]}), 
    ets:insert(SpeculaDep, {Key, [{TxId2, update, CoordPId, 100}, {TxId2, read, CoordPId, 200}]}), 
    Result5 = clean_specula_committed(TxId1, Key, SpeculaStore, SpeculaDep),
    receive Msg1 ->
        ?assertEqual(Msg1, {prepare, TxId2})
    end,
    receive Msg2 ->
        ?assertEqual(Msg2, {abort, TxId2})
    end,
    ?assertEqual(Result5, true),
    ?assertEqual(ets:lookup(SpeculaDep, Key), []),

    ets:delete(SpeculaStore),
    ets:delete(SpeculaDep),
    pass.

make_prepared_specula_test() ->
    TxId1 = tx_utilities:create_transaction_record(clocksi_vnode:now_microsec(now())),
    TxId2 = tx_utilities:create_transaction_record(clocksi_vnode:now_microsec(now())),
    CoordPId = self(),
    Type = riak_dt_pncounter,
    Key = 1,
    Record1 = {TxId1, 500, Type, {increment, haha}},
    Record2 = {TxId2, 500, Type, {increment, haha}},
    SnapshotTime = 300,
    {ok, Counter1} = Type:update(increment, haha, Type:new()),
    {ok, Counter2} = Type:update(increment, haha, Counter1),
    {ok, Counter3} = Type:update(increment, haha, Counter2),
    SpeculaStore = ets:new(specula_store, [set,named_table,protected]),
    SpeculaDep = ets:new(specula_dep, [set,named_table,protected]),
    InMemoryStore = ets:new(inmemory_store, [set,named_table,protected]),
    PreparedStore = ets:new(prepared_cache, [set,named_table,protected]),

    ets:insert(PreparedStore, {Key, whatever}),   
    Result1 = make_prepared_specula(Key, Record1, SnapshotTime, PreparedStore, InMemoryStore, 
                SpeculaStore, SpeculaDep, update, CoordPId),
    ?assertEqual(Result1, {specula, {Type, Counter1}}),
    ?assertEqual([], ets:lookup(PreparedStore, Key)),

    ets:insert(InMemoryStore, {Key, [{200, Counter1}]}),   
    ets:delete(SpeculaStore, Key),   
    Result2 = make_prepared_specula(Key, Record1, SnapshotTime, PreparedStore, InMemoryStore, 
                SpeculaStore, SpeculaDep, update, CoordPId),
    ?assertEqual(Result2, {specula, {Type, Counter2}}),

    Result3 = make_prepared_specula(Key, Record2, SnapshotTime, PreparedStore, InMemoryStore, 
                SpeculaStore, SpeculaDep, update, CoordPId),
    ?assertEqual(Result3, {specula, {Type, Counter3}}),
    ?assertEqual(ets:lookup(SpeculaDep, TxId1), [{TxId1, [{TxId2, update, CoordPId, SnapshotTime}]}]),
    ets:delete(SpeculaStore),
    ets:delete(InMemoryStore),
    ets:delete(PreparedStore),
    ets:delete(SpeculaDep),
    pass.

find_specula_version_test() ->
    TxId1 = tx_utilities:create_transaction_record(clocksi_vnode:now_microsec(now())),
    TxId2 = tx_utilities:create_transaction_record(clocksi_vnode:now_microsec(now())),
    TxId3 = tx_utilities:create_transaction_record(clocksi_vnode:now_microsec(now())),
    TxId4 = tx_utilities:create_transaction_record(clocksi_vnode:now_microsec(now())),
    CoordPId = self(),
    Type = riak_dt_pncounter,
    Key = 1,
    {ok, Counter1} = Type:update(increment, haha, Type:new()),
    {ok, Counter2} = Type:update(increment, haha, Counter1),
    Record1 = {TxId1, 400, Type, {increment, haha}},
    Record2 = {TxId2, 600, Type, {increment, haha}},
    InMemoryStore = ets:new(inmemory_store, [set,named_table,protected]),
    PreparedStore = ets:new(prepared_cache, [set,named_table,protected]),
    SpeculaStore = ets:new(specula_store, [set,named_table,protected]),
    SpeculaDep = ets:new(specula_dep, [set,named_table,protected]),

    SnapshotTime1 = 100,
    SnapshotTime2 = 500,
    SnapshotTime3 = 700,
    _ = make_prepared_specula(Key, Record1, SnapshotTime1, PreparedStore, InMemoryStore, 
                SpeculaStore, SpeculaDep, update, CoordPId),
    ?assertEqual(ets:lookup(SpeculaDep, TxId1), []),
    _ = make_prepared_specula(Key, Record2, SnapshotTime2, PreparedStore, InMemoryStore, 
                SpeculaStore, SpeculaDep, update, CoordPId),
    ?assertEqual(ets:lookup(SpeculaDep, TxId1), [{TxId1, [{TxId2, update, CoordPId, SnapshotTime2}]}]),

    Result1 = find_specula_version(TxId2, Key, SnapshotTime1, SpeculaStore, SpeculaDep, CoordPId),
    ?assertEqual(Result1, false),

    Result2 = find_specula_version(TxId3, Key, SnapshotTime2, SpeculaStore, SpeculaDep, CoordPId),
    ?assertEqual(Result2, Counter1),
    ?assertEqual(ets:lookup(SpeculaDep, TxId1), [{TxId1, [{TxId3, read,  CoordPId, SnapshotTime2},
                    {TxId2, update, CoordPId, SnapshotTime2}]}]),

    Result3 = find_specula_version(TxId4, Key, SnapshotTime3, SpeculaStore, SpeculaDep, CoordPId),
    ?assertEqual(Result3, Counter2),
    ?assertEqual(ets:lookup(SpeculaDep, TxId2), [{TxId2, [{TxId4, read,  CoordPId, SnapshotTime3}]}]),

    ets:delete(SpeculaStore),
    ets:delete(SpeculaDep),
    ets:delete(InMemoryStore),
    ets:delete(PreparedStore),
    pass.

    

-endif.
