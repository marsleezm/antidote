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
-define(SPECULA_TIMEOUT, 10000).

-export([should_specula/2, make_prepared_specula/9, find_specula_version/6,
            clean_specula_committed/4, coord_should_specula/1, make_specula_final/7]).

coord_should_specula(_) ->
    true.

%% If this txn corresponds to any specula-committed version,
%% 1. Make the specula_committed version final committed
%% 2. Check txns that depend on this txn
%%      If depending txn should be aborted, remove them from state and notify corresponding coord
%%      If depending txn works fine, either return to coord 'prepared' or 'read-valid'. 
make_specula_final(TxId, Key, TxCommitTime, SpeculaStore, InMemoryStore, PreparedStore, SpeculaDep) ->
    %% Firstly, make this specula version finally committed.
    case ets:lookup(SpeculaStore, Key) of
        [{Key, [{TxId, SpeculaValue}|T]}] ->    
            case ets:lookup(InMemoryStore, Key) of
                  [] ->
                      true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, SpeculaValue}]});
                  [{Key, ValueList}] ->
                      {RemainList, _} = lists:split(min(20,length(ValueList)), ValueList),
                      true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, SpeculaValue}|RemainList]})
            end,
            %%Remove this version from specula_store
            true = ets:insert(SpeculaStore, {Key, T}),
            %% Check if this version has any dependency
            case ets:lookup(SpeculaDep, Key) of
                [] -> %% No dependency, do nothing!
                    false;
                [{Key, List}] -> %% Do something for each of the dependency...
                    handle_dependency(List, TxCommitTime, PreparedStore, SpeculaStore, Key),
                    ets:delete(SpeculaDep, Key)
            end;
        [] -> %% Just prepared
            false;    
        Record ->
            lager:warning("Something is wrong!!!! ~w", [Record])
    end.

clean_specula_committed(TxId, Key, SpeculaStore, SpeculaDep) ->
    case ets:lookup(SpeculaStore, Key) of
        [{Key, [{TxId, _SpeculaValue}|T]}] ->
            ets:insert(SpeculaStore, {Key, T}),
            case ets:lookup(SpeculaDep, Key) of
                [] ->
                    ok;
                [{Key, List}] ->
                    handle_abort_dependency(List),
                    ets:delete(SpeculaDep, Key)
            end;
        [] -> %% Just prepared
            false;    
        Record ->
            lager:warning("Something is wrong!!!! ~w", [Record])
    end.


should_specula(PreparedTime, SnapshotTime) ->
    case SnapshotTime - ?SPECULA_TIMEOUT > PreparedTime of
        false ->
            NowTime = clocksi_vnode:now_microsec(now()),
            NowTime - ?SPECULA_TIMEOUT > PreparedTime;
        true ->
            true
    end.

make_prepared_specula(Key, Record, SnapshotTime, PreparedCache, SnapshotCache,
                             SpeculaCache, SpeculaDep, OpType, CoordPId) ->
    {TxId, PrepareTime, Type, {Param, Actor}} = Record,
    SpeculaValue =  case ets:lookup(SpeculaCache, Key) of 
                        [] ->
                            %% Fetch from committed store
                            case ets:lookup(SnapshotCache, Key) of
                                [] ->
                                    NewSpeculaValue = generate_snapshot([], Type, Param, Actor),
                                    true = ets:insert(SpeculaCache, [{PrepareTime, TxId, NewSpeculaValue}]),
                                    NewSpeculaValue;
                                [{Key, CommittedVersions}] ->
                                    [{_CommitTime, Snapshot}|_] = CommittedVersions,
                                    NewSpeculaValue = generate_snapshot(Snapshot, Type, Param, Actor),
                                    true = ets:insert(SpeculaCache, [{TxId, PrepareTime, NewSpeculaValue}]),
                                    NewSpeculaValue
                            end;
                        [{Key, SpeculaVersions}] ->
                            [{_CommitTime, SpeculaTxId, Snapshot}|_] = SpeculaVersions,
                            NewSpeculaValue = generate_snapshot(Snapshot, Type, Param, Actor),
                            true = ets:insert(SpeculaCache, [{TxId, PrepareTime, NewSpeculaValue}|SpeculaVersions]),
                            add_specula_meta(SpeculaDep, SpeculaTxId, TxId, SnapshotTime, OpType, CoordPId),
                            NewSpeculaValue
                    end,
    true = ets:delete(PreparedCache, Key),
    {specula, {Type, SpeculaValue}}.

find_specula_version(TxId, Key, SnapshotTime, SpeculaCache, SpeculaDep, SenderPId) ->
    case ets:find(SpeculaCache, Key) of
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

    
handle_dependency([], _TxCommitTime, _PreparedCache, _SpeculaCache, _Key) ->
    ok;
handle_dependency([{DepTxId, OpType, CoordPId, DepSnapshotTime}|T], TxCommitTime, 
                PreparedCache, SpeculaCache, Key) ->
    case DepSnapshotTime < TxCommitTime of
        false -> %% Has to abort...
            %% Notify coordinator
            gen_fsm:send_event(CoordPId, {abort, DepTxId}),
            case OpType of
                update ->
                    case ets:lookup(PreparedCache, Key) of                
                        [] ->
                            case ets:lookup(SpeculaCache, Key) of
                                [] ->
                                    lager:warning("It's impossible!!");
                                [{Key, [{DepTxId, _DepPrepareTime, _Value}|T]}] ->
                                    %% TODO: should I do cascading abort in this scenario?
                                    %% Seems not, because aborting txns are announced by coordinators..
                                    ets:insert(SpeculaCache, {Key, T});
                                _ ->
                                    lager:warning("It's impossible!!")
                            end;
                        _ ->
                            ets:delete(PreparedCache, Key)
                    end;
                read ->
                    ok
            end; 
        true ->
            case OpType of
                update ->
                    gen_fsm:send_event(CoordPId, {prepare, DepTxId});
                read ->
                    gen_fsm:send_event(CoordPId, {read_valid, DepTxId, Key})
            end
    end,
    handle_dependency(T, TxCommitTime, PreparedCache, SpeculaCache, Key).


handle_abort_dependency([]) ->
    ok;
handle_abort_dependency([{DepTxId, OpType, CoordPId, _DepSnapshotTime}|T]) ->
    case OpType of
        update ->
            gen_fsm:send_event(CoordPId, {prepare, DepTxId});
        read ->  
            gen_fsm:send_event(CoordPId, {abort, DepTxId})
    end,
    handle_abort_dependency(T).

