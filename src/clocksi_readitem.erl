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
-module(clocksi_readitem).


-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


%% States
-export([check_prepared/4, return/4]).

check_prepared(Key, MyTxId, Tables, SpeculaTimeout) ->
    SnapshotTime = MyTxId#tx_id.snapshot_time,
    {PreparedTx, _, SpeculaDep} = Tables,
    case ets:lookup(PreparedTx, Key) of
        [] ->
            ready;
        [{Key, {PrepareTxId, PrepareTime, Type, Op}}] ->
            case PrepareTime =< SnapshotTime of
                true ->
                    case specula_utilities:should_specula(PrepareTime, SnapshotTime, SpeculaTimeout) of
                        true ->
                            specula_utilities:speculate_and_read(Key, MyTxId, {PrepareTxId, PrepareTime, Type, Op}, Tables);
                        false ->
                            {not_ready, 2}
                    end;
                false ->
                    ready
            end;
        [{Key, {SpeculaTxId, PrepareTime, SpeculaValue}}] ->
            case specula_utilities:should_specula(PrepareTime, SnapshotTime, SpeculaTimeout) of
                true ->
                    specula_utilities:add_specula_meta(SpeculaDep, SpeculaTxId, MyTxId, Key),
                    {specula, SpeculaValue};
                false ->
                    {not_ready, 2}
            end
    end.

%% @doc return:
%%  - Reads and returns the log of specified Key using replication layer.
return(Key, Type, TxId, InMemoryStore) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(InMemoryStore, Key) of
        [] ->
            {ok, Type:new()};
        [{Key, ValueList}] ->
            {ok, find_version(ValueList, SnapshotTime, Type)}
    end.

%%%%%%%%%Intenal%%%%%%%%%%%%%%%%%%
find_version([], _SnapshotTime, Type) ->
    Type:new();
find_version([{TS, Value}|Rest], SnapshotTime, Type) ->
    case SnapshotTime >= TS of
        true ->
            Value;
        false ->
            find_version(Rest, SnapshotTime, Type)
    end.
