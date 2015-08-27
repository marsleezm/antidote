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
-export([check_prepared/3, return/4, check_clock/3]).


%% Spawn


%%%===================================================================
%%% API
%%%===================================================================

%% @doc check_clock: Compares its local clock with the tx timestamp.
%%      if local clock is behind, it sleeps the fms until the clock
%%      catches up. CLOCK-SI: clock skew.
%%
check_clock(Key,TxId, Tables) ->
    T_TS = TxId#tx_id.snapshot_time,
    Time = clocksi_vnode:now_microsec(erlang:now()),
    case T_TS > Time of
        true ->
        %% dont sleep in case there is another read waiting
            %% timer:sleep((T_TS - Time) div 1000 +1 );
        %%lager:info("Clock not ready"),
            not_ready;
        false ->
            check_prepared(Key,TxId, Tables)
    end.


check_prepared(Key,TxId,PreparedCache) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(PreparedCache, Key) of
        [] ->
            ready;
        [{Key, {_TxId, Time}}] ->
            case Time =< SnapshotTime of
                true ->
                    {not_ready, 2};
                false ->
                    ready
            end
    end.

%% @doc return:
%%  - Reads and returns the log of specified Key using replication layer.
return(Key, Type,TxId, SnapshotCache) ->
    %lager:info("Returning for key ~w",[Key]),
    case ets:lookup(SnapshotCache, Key) of
        [] ->
            {ok, {Type,Type:new()}};
        [{Key, ValueList}] ->
            MyClock = TxId#tx_id.snapshot_time,
            find_version(ValueList, MyClock, Type)
    end.


%%%%%%%%%Intenal%%%%%%%%%%%%%%%%%%
find_version([], _SnapshotTime, Type) ->
    %{error, not_found};
    {ok, {Type,Type:new()}};
find_version([{TS, Value}|Rest], SnapshotTime, Type) ->
    case SnapshotTime >= TS of
        true ->
            {ok, {Type,Value}};
        false ->
            find_version(Rest, SnapshotTime, Type)
    end.
