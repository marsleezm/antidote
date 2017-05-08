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

%% API
-export([check_prepared/4, return/4]).

check_prepared(Key, TxId, Sender, PreparedCache) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(PreparedCache, Key) of
        [] ->
            ready;
        [{Key, {PrepTxId, Time}, Readers}] ->
            case Time =< SnapshotTime of
                true ->
                    %lager:info("~w of ~w is blocked due to ~w", [TxId, Key, PrepTxId]),
                    ets:insert(PreparedCache, {Key, {PrepTxId, Time}, [{Sender, TxId}|Readers]}),
                    not_ready;
                false ->
                    ready
            end
    end.

%% @doc return:
%%  - Reads and returns the log of specified Key using replication layer.
return(Key, Type,TxId, SnapshotCache) ->
    case ets:lookup(SnapshotCache, Key) of
        [] ->
           %lager:warning("~w: found nothing for ~w", [TxId, Key]),
            {ok, {Type, Type:new()}};
        [{Key, ValueList}] ->
            MyClock = TxId#tx_id.snapshot_time,
            {ok, find_version(ValueList, MyClock, Type)}
    end.


%%%%%%%%%Intenal%%%%%%%%%%%%%%%%%%
find_version([], _SnapshotTime, Type) ->
    %{error, not_found};
   %lager:warning("found nothing ~w", [Key]),
    {Type, Type:new()};
find_version([{TS, Value}|Rest], SnapshotTime, Type) ->
    case SnapshotTime >= TS of
        true ->
           %lager:warning("found something for ~w", [Key]),
            {Type, Value};
        false ->
            find_version(Rest, SnapshotTime, Type)
    end.
