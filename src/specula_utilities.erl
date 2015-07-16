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
-module(specula_utilities).
-define(SPECULA_TIMEOUT, 10000).

-export([should_specula/2, make_prepared_specula/8]).

should_specula(PreparedTime, SnapshotTime) ->
    case SnapshotTime - ?SPECULA_TIMEOUT > PreparedTime of
        false ->
            NowTime = clocksi_vnode:now_microsec(now()),
            NowTime - ?SPECULA_TIMEOUT > PreparedTime;
        true ->
            true
    end.


%%!!TODO get TXID of txn that generated the previous version
make_prepared_specula(Key, Record, SnapshotTime, PreparedCache, SnapshotCache,
                             SpeculaCache, SpeculaDep, PreparedList) ->
    PList0 = lists:delete(Record, PreparedList),
    {TxId, Time, Type, {Param, Actor}} = Record,
    case ets:lookup(SpeculaCache, Key) of 
        [] ->
            %fetch from committed store
            case ets:lookup(SnapshotCache, Key) of
                [] ->
                    NewSpeculaValue = generate_snapshot([], Type, Param, Actor),
                    true = ets:insert(SpeculaCache, [{Time, TxId, NewSpeculaValue}]);
                [{Key, CommittedVersions}] ->
                    [{_CommitTime, Snapshot}|_] = CommittedVersions,
                    NewSpeculaValue = generate_snapshot(Snapshot, Type, Param, Actor),
                    true = ets:insert(SpeculaCache, [{Time, TxId, NewSpeculaValue}])
            end;
        [{Key, SpeculaVersions}] ->
            [{_CommitTime, SpeculaTxId, Snapshot}|_] = SpeculaVersions,
            NewSpeculaValue = generate_snapshot(Snapshot, Type, Param, Actor),
            true = ets:insert(SpeculaCache, [{Time, TxId, NewSpeculaValue}|SpeculaVersions]),
            add_specula_meta(SpeculaDep, SpeculaTxId, TxId, SnapshotTime)
    end,
    true = ets:insert(PreparedCache, {Key, PList0}).


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

add_specula_meta(SpeculaDep, DependingTxId, TxId, SnapshotTime) ->
    case ets:lookup(SpeculaDep, DependingTxId) of
        [] ->
            true = ets:insert(SpeculaDep, {DependingTxId, [{TxId, SnapshotTime}]});
        [{DependingTxId, DepList}] ->
            true = ets:insert(SpeculaDep, {DependingTxId, [{TxId, SnapshotTime}|DepList]})
    end.
    
