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
-define(GET_AND_UPDATE_TS(_), now_microsec()).
-else.
-define(GET_AND_UPDATE_TS(Clock), clock_service:get_and_update_ts(Clock)).
-endif.

-export([create_tx_id/1, now_microsec/0, open_table/2, open_private_table/1, get_table_name/2]).

-spec create_tx_id(snapshot_time() | ignore) -> txid().
create_tx_id(ClientClock) ->
    %% Seed the random because you pick a random read server, this is stored in the process state
    %{A1,A2,A3} = now(),
    _A = ClientClock,
    %_ = random:seed(A1, A2, A3),
    TransactionId = #tx_id{snapshot_time=max(ClientClock, now_microsec()), server_pid=self()},
    TransactionId.

%% @doc converts a tuple {MegaSecs,Secs,MicroSecs} into microseconds
now_microsec() ->
    {MegaSecs, Secs, MicroSecs} = now(),
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.

open_table(Partition, Name) ->
    try
    ets:new(get_table_name(Partition,Name),
        [set,protected,named_table,?TABLE_CONCURRENCY])
    catch
    _:_Reason ->
        %% Someone hasn't finished cleaning up yet
        open_table(Partition, Name)
    end.

open_private_table(Name) ->
    try
    ets:new(Name,
        [set,private])
    catch
    Ex ->
        lager:warn("Error when opening private table ~w", [Ex]),
        %% Someone hasn't finished cleaning up yet
        open_private_table(Name)
    end.

get_table_name(Partition,Base) ->
      list_to_atom(atom_to_list(Base) ++ "-" ++ integer_to_list(Partition)).




