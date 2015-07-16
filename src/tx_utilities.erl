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

-export([create_transaction_record/1]).

-spec create_transaction_record(snapshot_time() | ignore) -> txid().
create_transaction_record(ClientClock) ->
    %% Seed the random because you pick a random read server, this is stored in the process state
    {A1,A2,A3} = now(),
    _ = random:seed(A1, A2, A3),
    {ok, SnapshotTime} = case ClientClock of
        ignore ->
            get_snapshot_time();
        _ ->
            get_snapshot_time(ClientClock)
    end,
    TransactionId = #tx_id{snapshot_time=SnapshotTime, server_pid=self()},
    TransactionId.

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
