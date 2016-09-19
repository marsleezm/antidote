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

-include("include/speculation.hrl").
-include("include/antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(SEND_MSG(PID, MSG), PID ! MSG).
-else.
-define(SEND_MSG(PID, MSG),  gen_fsm:send_event(PID, MSG)).
-endif.

-export([should_specula/3, make_prepared_specula/4, speculate_and_read/4, generate_snapshot/4, 
            coord_should_specula/1, finalize_version_and_reply/5, add_specula_meta/4,
            finalize_dependency/5, deal_commit_deps/3, deal_abort_deps/2]).

deal_commit_deps(PreparedTxs, TxId, CommitTime) ->
    case ets:lookup(PreparedTxs, {dep, TxId}) of
        [] ->
            lager:warning("Dealing commit deps for ~w, no deps", [TxId]),
            ok; %% No read dependency was created!
        [{_, List}] ->
            lager:warning("Dealing commit deps for ~w, deps are ~w", [TxId, List]),
            SortList = lists:keysort(2, List),
            {DepNum, LastTxId} = lists:foldl(fun(DependTxId, {Num, PrevTxId}) ->
                            case PrevTxId of
                                DependTxId ->
                                    {Num+1, PrevTxId}; 
                                _ ->
                                    case PrevTxId of 
                                        nil -> ok; 
                                        _ -> 
                                            case PrevTxId#tx_id.snapshot_time >= CommitTime of
                                                true ->
                                                    gen_server:cast(PrevTxId#tx_id.server_pid, {read_valid, PrevTxId, Num});
                                                false ->
                                                    gen_server:cast(PrevTxId#tx_id.server_pid, {read_invalid, CommitTime, PrevTxId})
                                            end
                                    end,
                                    {1, DependTxId}
                          end end, {0, nil}, SortList),
            case LastTxId#tx_id.snapshot_time >= CommitTime of
                true ->
                    gen_server:cast(LastTxId#tx_id.server_pid, {read_valid, LastTxId, DepNum});
                false ->
                    gen_server:cast(LastTxId#tx_id.server_pid, {read_invalid, CommitTime, LastTxId})
            end,
            ets:delete(PreparedTxs, {dep, TxId})
    end.

deal_abort_deps(PreparedTxs, TxId) ->
    case ets:lookup(PreparedTxs, {dep, TxId}) of
        [] ->
            ok; %% No read dependency was created!
        [{_, List}] ->
            lager:warning("Dealing abort deps for ~w, deps are ~w", [TxId, List]),
            SortList = lists:keysort(2, List),
            _ = lists:foldl(fun(DependTxId, PrevTxId) ->
                            case PrevTxId of
                                DependTxId ->
                                    DependTxId; 
                                _ ->
                                    gen_server:cast(DependTxId#tx_id.server_pid, {read_aborted, -1, DependTxId}),
                                    DependTxId
                          end end, nil, SortList),
            ets:delete(PreparedTxs, {dep, TxId})
    end.


coord_should_specula(Aborted) ->
    %case TxnMetadata#txn_metadata.final_committed of
    %    true ->
    %        true;
    %    false ->
    %        false
    %end.
    Aborted<3.

%% If this txn corresponds to any specula-committed version,
%% 1. Make the specula_committed version final committed
%% 2. Check txns that depend on this txn
%%      If depending txn should be aborted, remove them from state and notify corresponding coord
%%      If depending txn works fine, either return to coord 'prepared' or 'read-valid'. 
%% Return true if found any specula version and made specula; false otherwise
%% TODO: This function should checks all keys of a transaction. Current one is incorrect 
finalize_version_and_reply(Key, TxCommitTime, SpeculaValue, InMemoryStore, PendingReaders) ->
    %% Firstly, make this specula version finally committed.
    Values= case ets:lookup(InMemoryStore, Key) of
                [] ->
                    lager:info("Inserting specula version ~w", [SpeculaValue]),
                    true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, SpeculaValue}]}),
                    [[], SpeculaValue];
                [{Key, ValueList}] ->
                    lager:info("Inserting specula list ~w", [ValueList]),
                    {RemainList, _} = lists:split(min(20,length(ValueList)), ValueList),
                    true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, SpeculaValue}|RemainList]}),
                    {_, FirstValue} = hd(ValueList),
                    [FirstValue, SpeculaValue]
            end,
    lists:foreach(fun({SnapshotTime, Sender}) ->
                            case SnapshotTime >= TxCommitTime of
                                true ->
                                    riak_core_vnode:reply(Sender, {ok, lists:nth(Values,2)});
                                false ->
                                    riak_core_vnode:reply(Sender, {ok, hd(Values)})
                            end end,
                        PendingReaders).


should_specula(PreparedTime, SnapshotTime, SpeculaTimeout) ->
    SnapshotTime - PreparedTime > SpeculaTimeout.
    %case SnapshotTime - ?SPECULA_TIMEOUT > PreparedTime of
    %    false ->
    %        NowTime = tx_utilities:get_ts(),
    %        NowTime - ?SPECULA_TIMEOUT > PreparedTime;
    %    true ->
    %        true
    %end.

speculate_and_read(Key, MyTxId, PreparedRecord, Tables) ->
    {PreparedTxs, InMemoryStore, SpeculaDep} = Tables,
    {SpeculatedTxId, Value} = make_prepared_specula(Key, PreparedRecord, PreparedTxs, InMemoryStore),
    %lager:info("Specula reading ~w of ~w", [Key, MyTxId]),
    add_specula_meta(SpeculaDep, SpeculatedTxId, MyTxId, Key),
    {specula, Value}.

make_prepared_specula(Key, PreparedRecord, PreparedTxs, _InMemoryStore) ->
    {TxId, PrepareTime, Value, PendingReaders} = PreparedRecord,
    %lager:info("Trying to make prepared specula ~w for ~w",[Key, TxId]),
    true = ets:insert(PreparedTxs, {Key, {specula, TxId, PrepareTime, Value, PendingReaders}}),
    {TxId, Value}.


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
add_specula_meta(SpeculaDep, DependingTxId, TxId, Key) ->
    case ets:lookup(SpeculaDep, DependingTxId) of
        [] ->
            true = ets:insert(SpeculaDep, {DependingTxId, [{TxId, Key}]});
        [{DependingTxId, DepList}] ->
            true = ets:insert(SpeculaDep, {DependingTxId, [{TxId, Key}|DepList]})
    end.

finalize_dependency(NumToCount, TxId, TxCommitTime, SpeculaDep, Type) ->
    %% Check if any txn depends on this version
    case ets:lookup(SpeculaDep, TxId) of
        [] -> %% No dependency, do nothing!
            NumToCount;
        [{TxId, DepList}] -> %% Do something for each of the dependency...
            %lager:info("Found dependency ~w for key ~w",[DepList, TxId]),
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
                    server_pid=CoordPid, client_pid=_, txn_seq=_, dc_id=_}, _Key}|T], TxCommitTime) ->
    case DepSnapshotTime < TxCommitTime of
        true -> %% The transaction committed with larger timestamp which will invalide depending txns..
                %% Has to abort...
            %lager:info("Invalid read! Snapshot time is ~w, commit time is ~w", [DepSnapshotTime, TxCommitTime]),
            ?SEND_MSG(CoordPid, {abort, DepTxId}),
            handle_dependency(NumInvalidRead+1, T, TxCommitTime);
        false ->
            %lager:info("Sending read valid ~w for key ~w",[DepTxId, Key]),
            ?SEND_MSG(CoordPid, {read_valid, DepTxId, 0}),
            handle_dependency(NumInvalidRead, T, TxCommitTime)
    end.

handle_abort_dependency(NumAbortRead, []) ->
    NumAbortRead;
handle_abort_dependency(NumAbortRead, [{DepTxId, _Key}|T]) ->
    CoordPid = DepTxId#tx_id.server_pid,
    ?SEND_MSG(CoordPid, {abort, DepTxId}),
    handle_abort_dependency(NumAbortRead, T).

-ifdef(TEST).
generate_snapshot_test() ->
    Type = riak_dt_pncounter,
    Snapshot1 = generate_snapshot([], Type, increment, haha),
    ?assertEqual(1, Type:value(Snapshot1)),
    Snapshot2 = generate_snapshot(Snapshot1, Type, increment, haha),
    ?assertEqual(2, Type:value(Snapshot2)).

finalize_specula_and_reply_test() ->
    TxId1 = tx_utilities:create_tx_id(tx_utilities:now_microsec()),
    Tx1PrepareTime = tx_utilities:now_microsec(),
    Tx1CommitTime = tx_utilities:now_microsec(),
    TxId2 = tx_utilities:create_tx_id(tx_utilities:now_microsec()),
    Tx2CommitTime = tx_utilities:now_microsec(),
    TxId3 = tx_utilities:create_tx_id(tx_utilities:now_microsec()),
    Tx3PrepareTime = tx_utilities:now_microsec(),
    Tx3CommitTime = tx_utilities:now_microsec(),
    TxId4 = tx_utilities:create_tx_id(tx_utilities:now_microsec()),
    Key = 1,
    InMemoryStore = ets:new(inmemory_store, [set,named_table,protected]),
    PreparedTxs = ets:new(prepared_store, [set,named_table,protected]),
    SpeculaDep = ets:new(specula_dep, [set,named_table,protected]),
    ets:insert(PreparedTxs, {Key, {TxId1, Tx1PrepareTime, whatever}}),

    %% Will succeed
    ets:delete(PreparedTxs, Key),
    _ = finalize_version_and_reply(Key, Tx1CommitTime, whatever, InMemoryStore, []),
    finalize_dependency(0 ,TxId1, Tx1CommitTime, SpeculaDep, commit),
    ?assertEqual([{Key, [{Tx1CommitTime, whatever}]}], ets:lookup(InMemoryStore, Key)),

    %% Multiple values in inmemory_store will not lost.
    _ = finalize_version_and_reply(Key, Tx2CommitTime, whatever, InMemoryStore, []),
    finalize_dependency(0, TxId2, Tx2CommitTime, SpeculaDep, commit),
    ?assertEqual([{Key, [{Tx2CommitTime, whatever}, {Tx1CommitTime, whatever}]}], 
            ets:lookup(InMemoryStore, Key)),

    %% Deps will be handled correctly
    %% Tx2, smaller timestamp than Tx3: both aborted
    %% Tx3, larger timestamp than Tx3: read valid, write prepared
    Dependency = [{TxId2, Key}, {TxId4, Key}],
    ets:insert(PreparedTxs, {Key, {TxId3, Tx3PrepareTime, lotsofdep}}),
    ets:insert(SpeculaDep, {TxId3, Dependency}),
    _ = finalize_version_and_reply(Key, Tx3CommitTime, 
                whatever, InMemoryStore, []),
    finalize_dependency(0, TxId3, Tx3CommitTime, SpeculaDep, commit),
    %receive Msg2 ->
    %    ?assertEqual({abort, TxId2}, Msg2)
    %end,
    %receive Msg4 ->
    %    ?assertEqual({read_valid, TxId4, 0}, Msg4)
    %end,
    ?assertEqual(ets:lookup(SpeculaDep, TxId2), []),

    ets:delete(InMemoryStore),
    ets:delete(PreparedTxs),
    ets:delete(SpeculaDep),
    pass.
    
clean_specula_committed_test() ->
    TxId1 = tx_utilities:create_tx_id(tx_utilities:now_microsec()),
    TxId2 = tx_utilities:create_tx_id(tx_utilities:now_microsec()),
    Key = 1,
    SpeculaDep = ets:new(specula_dep, [set,named_table,protected]),
    PreparedTxs = ets:new(prepared_txs, [set,named_table,protected]),

    ets:insert(SpeculaDep, {TxId1, [{TxId2, Key}]}), 
    finalize_dependency(0, TxId1, ignore, SpeculaDep, abort),
    %receive Msg2 ->
    %    ?assertEqual({abort, TxId2}, Msg2)
    %end,
    ?assertEqual(ets:lookup(SpeculaDep, TxId1), []),

    ets:delete(PreparedTxs),
    ets:delete(SpeculaDep),
    pass.
    

make_prepared_specula_test() ->
    TxId1 = tx_utilities:create_tx_id(tx_utilities:now_microsec()),
    Key = 1,
    Record1 = {TxId1, 500, haha, []},
    SpeculaDep = ets:new(specula_dep, [set,named_table,protected]),
    InMemoryStore = ets:new(inmemory_store, [set,named_table,protected]),
    PreparedTxs = ets:new(prepared_txs, [set,named_table,protected]),

    ets:insert(PreparedTxs, {Key, whatever}),   
    Result1 = make_prepared_specula(Key, Record1, PreparedTxs, InMemoryStore),
    ?assertEqual(Result1, {TxId1, haha}),

    ets:insert(InMemoryStore, {Key, [{200, gogo}]}),   
    ets:delete(PreparedTxs, Key),   
    Result2 = make_prepared_specula(Key, Record1, PreparedTxs, InMemoryStore), 
    ?assertEqual(Result2, {TxId1, haha}),

    ets:delete(PreparedTxs),
    ets:delete(InMemoryStore),
    ets:delete(SpeculaDep),
    pass.

%find_specula_version_test() ->
%    TxId1 = tx_utilities:create_tx_id(tx_utilities:now_microsec()),
%    Tx1PrepareTime = tx_utilities:now_microsec(),
%    TxId2 = tx_utilities:create_tx_id(tx_utilities:now_microsec()),
%    Type = riak_dt_pncounter,
%    Key = 1,
%    {ok, Counter1} = Type:update(increment, haha, Type:new()),
%    Record1 = {TxId1, Tx1PrepareTime, Type, {increment, haha}},
%    InMemoryStore = ets:new(inmemory_store, [set,named_table,protected]),
%    PreparedTxs = ets:new(prepared_txs, [set,named_table,protected]),
%    SpeculaDep = ets:new(specula_dep, [set,named_table,protected]),

%    _ = make_prepared_specula(Key, Record1, PreparedTxs, InMemoryStore), 
%    ?assertEqual(ets:lookup(SpeculaDep, TxId1), []),

%    Result1 = find_specula_version(TxId1, Key, PreparedTxs, SpeculaDep),
%    ?assertEqual(Result1, false),

%    Result2 = find_specula_version(TxId2, Key, PreparedTxs, SpeculaDep),
%    ?assertEqual(Result2, Counter1),
%    ?assertEqual(ets:lookup(SpeculaDep, TxId1), [{TxId1, [{TxId2, Key}]}]),

%    ets:delete(SpeculaDep),
%    ets:delete(InMemoryStore),
%    ets:delete(PreparedTxs),
%    pass.


-endif.
