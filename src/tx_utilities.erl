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

-export([create_transaction_record/1, update_ts/1, increment_ts/1]).

-spec create_transaction_record(snapshot_time() | ignore) -> txid().
create_transaction_record(ClientClock) ->
    %% Seed the random because you pick a random read server, this is stored in the process state
    {A1,A2,A3} = now(),
    _ = random:seed(A1, A2, A3),
    TransactionId = #tx_id{snapshot_time=get_and_update_ts(ClientClock), server_pid=self()},
    TransactionId.

-spec get_and_update_ts(non_neg_integer()) -> non_neg_integer().
get_and_update_ts(CausalTS) ->
    {ok, TS} = ?GET_MAX_TS(antidote, max_ts),
    Now = clocksi_vnode:now_microsec(now()),
    Max2 = max(CausalTS, max(Now, TS)),
    application:set_env(antidote, max_ts, Max2+1),
    Max2+1.

-spec update_ts(non_neg_integer()) -> non_neg_integer().
update_ts(SnapshotTS) ->
    {ok, TS} = ?GET_MAX_TS(antidote, max_ts),
    case TS >= SnapshotTS of
        true ->
            TS;
        false ->
            application:set_env(antidote, max_ts, SnapshotTS),
            SnapshotTS
    end.

-spec increment_ts(non_neg_integer()) -> non_neg_integer().
increment_ts(SnapshotTS) ->
    {ok, TS} = ?GET_MAX_TS(antidote, max_ts),
    MaxTS = max(SnapshotTS, TS),
    application:set_env(antidote, max_ts, MaxTS+1),
    MaxTS+1.

