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
-module(tx_utilities).

-include("antidote.hrl").

-ifdef(TEST).
-define(GET_MAX_TS(APP, KEY), {ok, clocksi_vnode:now_microsec(now())}).
-else.
-define(GET_MAX_TS(APP, KEY), application:get_env(APP, KEY)).
-endif.

-export([create_transaction_record/1, get_ts/0, update_ts/1, increment_ts/1, get_snapshot_time/1]).


-spec create_transaction_record(snapshot_time() | ignore) -> txid().
create_transaction_record(ClientClock) ->
    %% Seed the random because you pick a random read server, this is stored in the process state
    {A1,A2,A3} = now(),
    _ = random:seed(A1, A2, A3),
    TransactionId = #tx_id{snapshot_time=update_ts(ClientClock), server_pid=self()},
    TransactionId.

-spec get_ts() -> non_neg_integer().
get_ts() ->
    {ok, TS} = ?GET_MAX_TS(antidote, max_ts),
    max(clocksi_vnode:now_microsec(now()), TS).

-spec update_ts(non_neg_integer()) -> non_neg_integer().
update_ts(SnapshotTS) ->
    {ok, TS} = ?GET_MAX_TS(antidote, max_ts),
    case TS > SnapshotTS of
        true ->
            TS;
        _ ->
            application:set_env(antidote, max_ts, SnapshotTS),
            SnapshotTS 
    end.

-spec increment_ts(non_neg_integer()) -> non_neg_integer().
increment_ts(SnapshotTS) ->
    {ok, TS} = ?GET_MAX_TS(antidote, max_ts),
    lager:info("TS is ~w, SnapshotTS is ~w", [TS, SnapshotTS]),
    MaxTS = max(SnapshotTS, TS),
    application:set_env(antidote, max_ts, MaxTS+1),
    MaxTS+1.

%%@doc Set the transaction Snapshot Time to the maximum value of:
%%     1.ClientClock, which is the last clock of the system the client
%%       starting this transaction has seen, and
%%     2.machine's local time, as returned by erlang:now().
-spec get_snapshot_time(snapshot_time())
                         -> {ok, snapshot_time()}.
get_snapshot_time(ClientClock) ->
      wait_for_clock(ClientClock).
  
-spec get_snapshot_time() -> {ok, snapshot_time()}.
get_snapshot_time() ->
      Now = clocksi_vnode:now_microsec(erlang:now()) - ?OLD_SS_MICROSEC,
      {ok, Now}.
  
-spec wait_for_clock(snapshot_time()) ->
                             {ok, snapshot_time()}.
wait_for_clock(Clock) ->
     case get_snapshot_time() of
         {ok, SnapshotTime} ->
             case SnapshotTime > Clock of
                 true ->
                     %% No need to wait
                     {ok, SnapshotTime};
                 false ->
                     %% wait for snapshot time to catch up with Client Clock
                     timer:sleep(10),
                     wait_for_clock(Clock)
             end
    end.


