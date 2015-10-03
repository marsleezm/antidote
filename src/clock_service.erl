%% -------------------------------------------------------------------
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
-module(clock_service).

-behavior(gen_server).

-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/0]).

%% Callbacks
-export([init/1,
        handle_call/3,
        handle_cast/2,
        code_change/3,
        handle_event/3,
        handle_info/2,
        handle_sync_event/4,
        terminate/2]).

%% States
-export([get_and_update_ts/1, update_ts/1, increment_ts/1, get_ts/0]).

%% Spawn
-record(state, {max_ts :: non_neg_integer()}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc This starts a gen_server responsible for servicing reads to key
%%      handled by this Partition.  To allow for read concurrency there
%%      can be multiple copies of these servers per parition, the Id is
%%      used to distinguish between them.  Since these servers will be
%%      reading from ets tables shared by the clock_si and materializer
%%      vnodes, they should be started on the same physical nodes as
%%      the vnodes with the same partition.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_and_update_ts(CausalTS) ->
    gen_server:call(?MODULE,
            {get_and_update_ts, CausalTS},infinity).

update_ts(SnapshotTime) ->
    gen_server:call(?MODULE,
            {update_ts, SnapshotTime},infinity).

increment_ts(SnapshotTime) ->
    gen_server:call(?MODULE,
            {increment_ts, SnapshotTime},infinity).

get_ts()->
    gen_server:call(?MODULE,
            {get_ts},infinity).

init(_) ->
    {ok, #state{max_ts=tx_utilities:now_microsec()}}.

handle_call({get_and_update_ts, CausalTS}, _,
        SD0=#state{max_ts=TS}) ->
    Now = tx_utilities:now_microsec(),
    Max2 = max(CausalTS, max(Now, TS)) + 1,
    {reply, Max2, SD0#state{max_ts=Max2}};

handle_call({update_ts, SnapshotTS}, _,
        SD0=#state{max_ts=TS}) ->
    case TS >= SnapshotTS of
        true ->
            {reply, TS, SD0};
        false ->
            {reply, SnapshotTS, SD0#state{max_ts=SnapshotTS}}
    end;

handle_call({increment_ts, SnapshotTS}, _,
        SD0=#state{max_ts=TS}) ->
    NewTS = max(SnapshotTS, TS) + 1,
    {reply, NewTS, SD0#state{max_ts=SnapshotTS}};

handle_call({get_ts}, _, SD0=#state{max_ts=TS}) ->
    {reply, max(tx_utilities:now_microsec(), TS), SD0}.


handle_cast(_, State) ->
    {noreply, State}.

handle_info(_Info, StateData) ->
    {noreply,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

terminate(_Reason, _SD) ->
    ok.
