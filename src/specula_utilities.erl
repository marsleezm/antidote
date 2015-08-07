% -------------------------------------------------------------------
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
-include("include/antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(SEND_MSG(PID, MSG), PID ! MSG).
-else.
-define(SEND_MSG(PID, MSG),  gen_fsm:send_event(PID, MSG)).
-endif.

-export([should_specula/2, make_prepared_specula/6, speculate_and_read/4, find_specula_version/4,
            abort_specula_committed/3, coord_should_specula/1, make_specula_version_final/5,
            finalize_dependency/5]).

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
%% TODO: This function should checks all keys of a transaction. Current one is incorrect 
make_specula_version_final(TxId, Key, TxCommitTime, SpeculaStore, InMemoryStore) ->
    %% Firstly, make this specula version finally committed.
    %lager:info("In making specula final for tx ~w",[TxId]),
    case ets:lookup(SpeculaStore, Key) of
        [{Key, [{TxId, _, SpeculaValue}|T]}] ->    
            %lager:info("Found ~w of TxId ~w in speculstore",[Key, TxId]),
            case ets:lookup(InMemoryStore, Key) of
                  [] ->
                      true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, SpeculaValue}]});
                  [{Key, ValueList}] ->
                      {RemainList, _} = lists:split(min(20,length(ValueList)), ValueList),
                      true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, SpeculaValue}|RemainList]})
            end,
            %%Remove this version from specula_store
            case T of 
                [] ->
                    true = ets:delete(SpeculaStore, Key);
                _ ->
                    true = ets:insert(SpeculaStore, {Key, T})
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

abort_specula_committed(TxId, Key, SpeculaStore) ->
    case ets:lookup(SpeculaStore, Key) of
        [{Key, [{TxId, _, _SpeculaValue}|T]}] ->
            ets:insert(SpeculaStore, {Key, T}),
            true;
        _ -> %% Just prepared
            false   
        %[] -> %% Just prepared
        %    false;    
        %Record ->
        %    error
    end.


should_specula(PreparedTime, SnapshotTime) ->
    case SnapshotTime - ?SPECULA_TIMEOUT > PreparedTime of
        false ->
            NowTime = clocksi_vnode:now_microsec(now()),
            NowTime - ?SPECULA_TIMEOUT > PreparedTime;
        true ->
            true
    end.

speculate_and_read(Key, MyTxId, PreparedRecord, Tables) ->
    {PreparedTxs, InMemoryStore, SpeculaStore, SpeculaDep} = Tables,
    {SpeculatedTxId, Value} = make_prepared_specula(Key, PreparedRecord, PreparedTxs, InMemoryStore,
                               SpeculaStore, SpeculaDep),
    add_specula_meta(SpeculaDep, SpeculatedTxId, MyTxId, read, Key),
    {specula, Value}.


make_prepared_specula(Key, PreparedRecord, PreparedTxs, InMemoryStore,
                             SpeculaStore, SpeculaDep) ->
    {TxId, PrepareTime, Type, {Param, Actor}} = PreparedRecord,
    lager:info("Trying to make prepared specula ~w for ~w",[Key, TxId]),
    SpeculaValue =  case ets:lookup(SpeculaStore, Key) of 
                        [] ->
                            %% Fetch from committed store
                            case ets:lookup(InMemoryStore, Key) of
                                [] ->
                                    NewSpeculaValue = generate_snapshot([], Type, Param, Actor),
                                    true = ets:insert(SpeculaStore, {Key, [{TxId, PrepareTime, NewSpeculaValue}]}),
                                    %lager:info("Appending the first specula value of Key ~w, Value ~w",[Key, NewSpeculaValue]),
                                    NewSpeculaValue;
                                [{Key, CommittedVersions}] ->
                                    [{_CommitTime, Snapshot}|_] = CommittedVersions,
                                    NewSpeculaValue = generate_snapshot(Snapshot, Type, Param, Actor),
                                    %lager:info("Appending the first specula value of Key ~w, Value ~w",[Key, NewSpeculaValue]),
                                    true = ets:insert(SpeculaStore, {Key, [{TxId, PrepareTime, NewSpeculaValue}]}),
                                    NewSpeculaValue
                            end;
                        [{Key, SpeculaVersions}] ->
                            [{SpeculaTxId, _PrepareTime, Snapshot}|_] = SpeculaVersions,
                            NewSpeculaValue = generate_snapshot(Snapshot, Type, Param, Actor),
                            true = ets:insert(SpeculaStore, {Key, 
                                    [{TxId, PrepareTime, NewSpeculaValue}|SpeculaVersions]}),
                            %lager:info("Make prepared specula..adding dep ~w",[Sender]),
                            add_specula_meta(SpeculaDep, SpeculaTxId, TxId, update, Key),
                            NewSpeculaValue
                    end,
    true = ets:delete(PreparedTxs, Key),
    {TxId, {Type, SpeculaValue}}.

find_specula_version(TxId=#tx_id{snapshot_time=SnapshotTime}, Key, SpeculaStore, SpeculaDep) ->
    case ets:lookup(SpeculaStore, Key) of
        [] -> %% No specula version
            false;
        [{Key, ValueList}] ->
            case find_version(ValueList, SnapshotTime) of
                false -> %% No corresponding specula version TODO: will this ever happen?
                    false;
                {DependingTxId, Value} -> 
                    %lager:info("Find specula version..adding dep ~w",[SenderPId]),
                    add_specula_meta(SpeculaDep, DependingTxId, TxId, read, Key),
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

%%TODO: to optimize: no need to store the snapshottime of txn.. TxId already includs it.
add_specula_meta(SpeculaDep, DependingTxId, TxId, OpType, Key) ->
    %lager:info("Adding specula meta: deping tx ~w, depent tx ~w",[DependingTxId, TxId]),    
    case ets:lookup(SpeculaDep, DependingTxId) of
        [] ->
            true = ets:insert(SpeculaDep, {DependingTxId, [{TxId, OpType, Key}]});
        [{DependingTxId, DepList}] ->
            true = ets:insert(SpeculaDep, {DependingTxId, [{TxId, OpType, Key}|DepList]})
    end.


finalize_dependency(NumToCount, TxId, TxCommitTime, SpeculaDep, Type) ->
    %% Check if any txn depends on this version
    case ets:lookup(SpeculaDep, TxId) of
        [] -> %% No dependency, do nothing!
            NumToCount;
        [{TxId, DepList}] -> %% Do something for each of the dependency...
            lager:info("Found dependency ~w for key ~w",[DepList, TxId]),
            true = ets:delete(SpeculaDep, TxId),
            case Type of
                commit ->
                    handle_dependency(NumToCount, DepList, TxCommitTime);
                abort ->
                    handle_abort_dependency(NumToCount, DepList)
            end
    end.
    
handle_dependency(NumInvalidRead, [], _TxCommitTime) ->
    NumInvalidRead;
handle_dependency(NumInvalidRead, [{DepTxId=#tx_id{snapshot_time=DepSnapshotTime, 
                    server_pid=CoordPid}, OpType, Key}|T], TxCommitTime) ->
    case DepSnapshotTime < TxCommitTime of
        true -> %% The transaction committed with larger timestamp which will invalide depending txns..
                %% Has to abort...
            %% Notify coordinator
            %% TODO: not totally correct
            %lager:info("DepsnapshotTime ~w, CommitTime is ~w, Sending abort to coordinator ~w of tx ~w",[DepSnapshotTime, TxCommitTime, CoordPid, DepTxId]),
            ?SEND_MSG(CoordPid, {abort, DepTxId}),
            %%TODO: treat differently when there is read
            handle_dependency(NumInvalidRead+1, T, TxCommitTime);
            %case OpType of
            %    update ->
            %        case ets:lookup(PreparedTxs, Key) of                
            %            [] ->
            %                case ets:lookup(SpeculaStore, Key) of
            %                    [] ->
            %                        lager:warning("It's impossible!!");
            %                    [{Key, [{DepTxId, _DepPrepareTime, _Value}|T]}] ->
            %                        %% TODO: should I do cascading abort in this scenario?
            %                        %% Seems not, because aborting txns are announced by coordinators..
            %                        ets:insert(SpeculaStore, {Key, T});
            %                    _ ->
            %                        lager:warning("It's impossible!!")
            %                end;
            %            _ ->
            %                ets:delete(PreparedTxs, Key)
            %        end;
            %    read ->
            %        ok
            %end; 
        false ->
            case OpType of
                update ->
                    %% TODO: not totally correct; any way to get a smaller timestamp?
                    %lager:info("Sending prepare to coordinator ~w of tx ~w",[CoordPid, DepTxId]),
                    ?SEND_MSG(CoordPid, {prepared, DepTxId, clocksi_vnode:now_microsec(now())}),
                    handle_dependency(NumInvalidRead, T, TxCommitTime);
                read ->
                    %lager:info("Sending read_valid to coordinator ~w of tx ~w, key ~w",[CoordPid, DepTxId, Key]),
                    ?SEND_MSG(CoordPid, {read_valid, DepTxId, Key}),
                    handle_dependency(NumInvalidRead, T, TxCommitTime)
            end
    end.


handle_abort_dependency(NumAbortRead, []) ->
    NumAbortRead;
handle_abort_dependency(NumAbortRead, [{DepTxId, OpType, _Key}|T]) ->
    CoordPid = DepTxId#tx_id.server_pid,
    case OpType of
        update ->
            %%TODO: it's not correct here. Should wait until all updates on the same partition to
            %% decide if should send prepare or abort
            %lager:info("Sending prepare of Deptx ~w",[DepTxId]),
            ?SEND_MSG(CoordPid, {prepared, DepTxId, clocksi_vnode:now_microsec(now())}),
            handle_abort_dependency(NumAbortRead, T);
        read ->  
            %lager:info("Sending read of Deptx ~w",[DepTxId]),
            ?SEND_MSG(CoordPid, {abort, DepTxId}),
            handle_abort_dependency(NumAbortRead, T)
    end.

-ifdef(TEST).
generate_snapshot_test() ->
    Type = riak_dt_pncounter,
    Snapshot1 = generate_snapshot([], Type, increment, haha),
    ?assertEqual(1, Type:value(Snapshot1)),
    Snapshot2 = generate_snapshot(Snapshot1, Type, increment, haha),
    ?assertEqual(2, Type:value(Snapshot2)).

make_specula_version_final_test() ->
    TxId1 = tx_utilities:create_transaction_record(clocksi_vnode:now_microsec(now())),
    Tx1PrepareTime = clocksi_vnode:now_microsec(now()),
    Tx1CommitTime = clocksi_vnode:now_microsec(now()),
    TxId2 = tx_utilities:create_transaction_record(clocksi_vnode:now_microsec(now())),
    Tx2PrepareTime = clocksi_vnode:now_microsec(now()),
    Tx2CommitTime = clocksi_vnode:now_microsec(now()),
    TxId3 = tx_utilities:create_transaction_record(clocksi_vnode:now_microsec(now())),
    Tx3PrepareTime = clocksi_vnode:now_microsec(now()),
    Tx3CommitTime = clocksi_vnode:now_microsec(now()),
    TxId4 = tx_utilities:create_transaction_record(clocksi_vnode:now_microsec(now())),
    Key = 1,
    SpeculaStore = ets:new(specula_store, [set,named_table,protected]),
    InMemoryStore = ets:new(inmemory_store, [set,named_table,protected]),
    PreparedTxs = ets:new(prepared_store, [set,named_table,protected]),
    SpeculaDep = ets:new(specula_dep, [set,named_table,protected]),
    ets:insert(SpeculaStore, {Key, [{TxId1, Tx1PrepareTime, whatever}]}),

    %% Will succeed
    Result1 = make_specula_version_final(TxId1, Key, Tx1CommitTime, SpeculaStore, InMemoryStore),
    finalize_dependency(0 ,TxId1, Tx1CommitTime, SpeculaDep, commit),
    ?assertEqual(Result1, true),
    ?assertEqual([], ets:lookup(SpeculaStore, Key)),
    ?assertEqual([{Key, [{Tx1CommitTime, whatever}]}], ets:lookup(InMemoryStore, Key)),

    Result2 = make_specula_version_final(TxId1, Key, Tx1CommitTime, SpeculaStore, InMemoryStore),
    ?assertEqual(Result2, false),
    
    %% Wrong key
    ets:insert(SpeculaStore, {Key, [{TxId2, Tx2PrepareTime, whereever}]}),
    Result3 = make_specula_version_final(TxId1, Key, Tx2CommitTime, SpeculaStore, InMemoryStore),
    ?assertEqual(Result3, error),

    %% Multiple values in inmemory_store will not lost.
    Result4 = make_specula_version_final(TxId2, Key, Tx2CommitTime, SpeculaStore, InMemoryStore),
    finalize_dependency(0, TxId2, Tx2CommitTime, SpeculaDep, commit),
    ?assertEqual(Result4, true),
    ?assertEqual([], ets:lookup(SpeculaStore, Key)),
    ?assertEqual([{Key, [{Tx2CommitTime, whereever}, {Tx1CommitTime, whatever}]}], 
            ets:lookup(InMemoryStore, Key)),

    %% Deps will be handled correctly
    %% Tx2, smaller timestamp than Tx3: both aborted
    %% Tx3, larger timestamp than Tx3: read valid, write prepared
    Dependency = [{TxId2, update, Key}, {TxId2, read, Key}, {TxId4, update, Key}, {TxId4, read, Key}],
    ets:insert(SpeculaStore, {Key, [{TxId3, Tx3PrepareTime, lotsofdep}]}),
    ets:insert(SpeculaDep, {TxId3, Dependency}),
    Result5 = make_specula_version_final(TxId3, Key, Tx3CommitTime, 
                SpeculaStore, InMemoryStore),
    finalize_dependency(0, TxId3, Tx3CommitTime, SpeculaDep, commit),
    receive Msg1 ->
        ?assertEqual({abort, TxId2}, Msg1)
    end,
    receive Msg2 ->
        ?assertEqual({abort, TxId2}, Msg2)
    end,
    receive Msg3 ->
        ?assertMatch({prepared, TxId4, _}, Msg3)
    end,
    receive Msg4 ->
        ?assertEqual({read_valid, TxId4, Key}, Msg4)
    end,
    ?assertEqual(Result5, true),
    ?assertEqual(ets:lookup(SpeculaDep, TxId2), []),

    ets:delete(SpeculaStore),
    ets:delete(InMemoryStore),
    ets:delete(PreparedTxs),
    ets:delete(SpeculaDep),
    pass.
    
clean_specula_committed_test() ->
    TxId1 = tx_utilities:create_transaction_record(clocksi_vnode:now_microsec(now())),
    Tx1PrepareTime = clocksi_vnode:now_microsec(now()),
    TxId2 = tx_utilities:create_transaction_record(clocksi_vnode:now_microsec(now())),
    Key = 1,
    SpeculaStore = ets:new(specula_store, [set,named_table,protected]),
    SpeculaDep = ets:new(specula_dep, [set,named_table,protected]),
    
    Result1 = abort_specula_committed(TxId1, Key, SpeculaStore),
    ?assertEqual(Result1, false),

    ets:insert(SpeculaStore, {Key, [{TxId1, Tx1PrepareTime, value1}]}), 
    Result2 = abort_specula_committed(TxId2, Key, SpeculaStore),
    ?assertEqual(Result2, false),

    Result3 = abort_specula_committed(TxId1, Key, SpeculaStore),
    ?assertEqual(Result3, true),

    Result4 = abort_specula_committed(TxId1, Key, SpeculaStore),
    ?assertEqual(Result4, false),

    ets:insert(SpeculaStore, {Key, [{TxId1, Tx1PrepareTime, value1}]}), 
    ets:insert(SpeculaDep, {TxId1, [{TxId2, update, Key}, {TxId2, read, Key}]}), 
    Result5 = abort_specula_committed(TxId1, Key, SpeculaStore),
    finalize_dependency(0, TxId1, ignore, SpeculaDep, abort),
    receive Msg1 ->
        ?assertMatch({prepared, TxId2, _}, Msg1)
    end,
    receive Msg2 ->
        ?assertEqual({abort, TxId2}, Msg2)
    end,
    ?assertEqual(Result5, true),
    ?assertEqual(ets:lookup(SpeculaDep, TxId1), []),

    ets:delete(SpeculaStore),
    ets:delete(SpeculaDep),
    pass.
    

make_prepared_specula_test() ->
    TxId1 = tx_utilities:create_transaction_record(clocksi_vnode:now_microsec(now())),
    TxId2 = tx_utilities:create_transaction_record(clocksi_vnode:now_microsec(now())),
    Type = riak_dt_pncounter,
    Key = 1,
    Record1 = {TxId1, 500, Type, {increment, haha}},
    Record2 = {TxId2, 500, Type, {increment, haha}},
    {ok, Counter1} = Type:update(increment, haha, Type:new()),
    {ok, Counter2} = Type:update(increment, haha, Counter1),
    {ok, Counter3} = Type:update(increment, haha, Counter2),
    SpeculaStore = ets:new(specula_store, [set,named_table,protected]),
    SpeculaDep = ets:new(specula_dep, [set,named_table,protected]),
    InMemoryStore = ets:new(inmemory_store, [set,named_table,protected]),
    PreparedTxs = ets:new(prepared_txs, [set,named_table,protected]),

    ets:insert(PreparedTxs, {Key, whatever}),   
    Result1 = make_prepared_specula(Key, Record1, PreparedTxs, InMemoryStore, 
                SpeculaStore, SpeculaDep),
    ?assertEqual(Result1, {TxId1, {Type, Counter1}}),
    ?assertEqual([], ets:lookup(PreparedTxs, Key)),

    ets:insert(InMemoryStore, {Key, [{200, Counter1}]}),   
    ets:delete(SpeculaStore, Key),   
    Result2 = make_prepared_specula(Key, Record1, PreparedTxs, InMemoryStore, 
                SpeculaStore, SpeculaDep),
    ?assertEqual(Result2, {TxId1, {Type, Counter2}}),

    Result3 = make_prepared_specula(Key, Record2, PreparedTxs, InMemoryStore, 
                SpeculaStore, SpeculaDep),
    ?assertEqual(Result3, {TxId2, {Type, Counter3}}),
    ?assertEqual(ets:lookup(SpeculaDep, TxId1), [{TxId1, [{TxId2, update, Key}]}]),
    ets:delete(SpeculaStore),
    ets:delete(InMemoryStore),
    ets:delete(PreparedTxs),
    ets:delete(SpeculaDep),
    pass.

find_specula_version_test() ->
    TxId1 = tx_utilities:create_transaction_record(clocksi_vnode:now_microsec(now())),
    Tx1PrepareTime = clocksi_vnode:now_microsec(now()),
    TxId2 = tx_utilities:create_transaction_record(clocksi_vnode:now_microsec(now())),
    Tx2PrepareTime = clocksi_vnode:now_microsec(now()),
    TxId3 = tx_utilities:create_transaction_record(clocksi_vnode:now_microsec(now())),
    Type = riak_dt_pncounter,
    Key = 1,
    {ok, Counter1} = Type:update(increment, haha, Type:new()),
    {ok, Counter2} = Type:update(increment, haha, Counter1),
    Record1 = {TxId1, Tx1PrepareTime, Type, {increment, haha}},
    Record2 = {TxId2, Tx2PrepareTime, Type, {increment, haha}},
    InMemoryStore = ets:new(inmemory_store, [set,named_table,protected]),
    PreparedTxs = ets:new(prepared_txs, [set,named_table,protected]),
    SpeculaStore = ets:new(specula_store, [set,named_table,protected]),
    SpeculaDep = ets:new(specula_dep, [set,named_table,protected]),

    _ = make_prepared_specula(Key, Record1, PreparedTxs, InMemoryStore, 
                SpeculaStore, SpeculaDep),
    ?assertEqual(ets:lookup(SpeculaDep, TxId1), []),
    _ = make_prepared_specula(Key, Record2, PreparedTxs, InMemoryStore, 
                SpeculaStore, SpeculaDep),
    ?assertEqual(ets:lookup(SpeculaDep, TxId1), [{TxId1, [{TxId2, update, Key}]}]),

    Result1 = find_specula_version(TxId1, Key, SpeculaStore, SpeculaDep),
    ?assertEqual(Result1, false),

    Result2 = find_specula_version(TxId2, Key, SpeculaStore, SpeculaDep),
    ?assertEqual(Result2, Counter1),
    ?assertEqual(ets:lookup(SpeculaDep, TxId1), [{TxId1, [{TxId2, read, Key}, {TxId2, update, Key}]}]),

    Result3 = find_specula_version(TxId3, Key, SpeculaStore, SpeculaDep),
    ?assertEqual(Result3, Counter2),
    ?assertEqual(ets:lookup(SpeculaDep, TxId2), [{TxId2, [{TxId3, read, Key}]}]),

    ets:delete(SpeculaStore),
    ets:delete(SpeculaDep),
    ets:delete(InMemoryStore),
    ets:delete(PreparedTxs),
    pass.


-endif.
