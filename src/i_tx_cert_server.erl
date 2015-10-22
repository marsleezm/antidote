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
-module(i_tx_cert_server).

-behavior(gen_server).

-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/1]).

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
-export([certify/4]).

%% Spawn

-record(state, {partition :: non_neg_integer(),
        tx_id :: txid(),
        prepare_time = 0 :: non_neg_integer(),
        local_updates :: [],
        remote_updates :: [],
        sender :: term(),
        to_ack :: non_neg_integer(),
        pending_metadata :: cache_id()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name) ->
    gen_server:start_link({local, Name},
             ?MODULE, [], []).

certify(ICertServer, TxId, LocalUpdates, RemoteUpdates) ->
    gen_server:call(ICertServer, {certify, TxId, LocalUpdates, RemoteUpdates}).
%%%===================================================================
%%% Internal
%%%===================================================================

init([]) ->
    %PendingMetadata = tx_utilities:open_private_table(pending_metadata),
    {ok, #state{}}.

handle_call({certify, TxId, LocalUpdates, RemoteUpdates},  Sender, SD0) ->
    case length(LocalUpdates) of
        0 ->
            clocksi_vnode:prepare(RemoteUpdates, TxId, remote),
            {noreply, SD0#state{tx_id=TxId, to_ack=length(RemoteUpdates), local_updates=LocalUpdates, remote_updates=
                RemoteUpdates, sender=Sender}};
        _ ->
            clocksi_vnode:prepare(LocalUpdates, TxId, local),
            {noreply, SD0#state{tx_id=TxId, to_ack=length(LocalUpdates), local_updates=LocalUpdates, remote_updates=
                RemoteUpdates, sender=Sender}}
    end;

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

handle_cast({prepared, TxId, PrepareTime, local}, 
	    SD0=#state{to_ack=N, tx_id=TxId, local_updates=LocalUpdates,
            remote_updates=RemoteUpdates, sender=Sender, prepare_time=OldPrepTime}) ->
    case N of
        1 -> 
            case length(RemoteUpdates) of
                0 ->
                    CommitTime = max(PrepareTime, OldPrepTime),
                    clocksi_vnode:commit(LocalUpdates, TxId, CommitTime),
                    gen_server:reply(Sender, {ok, {committed, CommitTime}}),
                    {noreply, SD0#state{prepare_time=CommitTime, tx_id={}}};
                _ ->
                    MaxPrepTime = max(PrepareTime, OldPrepTime),
                    clocksi_vnode:prepare(RemoteUpdates, TxId, remote),
                    {noreply, SD0#state{prepare_time=MaxPrepTime, to_ack=length(RemoteUpdates)}}
                end;
        _ ->
            %lager:info("~w, needs ~w", [TxId, N-1]),
            %lager:info("Not done for ~w", [TxId]),
            MaxPrepTime = max(OldPrepTime, PrepareTime),
            {noreply, SD0#state{to_ack=N-1, prepare_time=MaxPrepTime}}
    end;
handle_cast({prepared, _, _, local}, 
	    SD0) ->
    %lager:info("Received prepare of previous prepared txn! ~w", [OtherTxId]),
    {noreply, SD0};

handle_cast({abort, TxId, local}, SD0=#state{tx_id=TxId, local_updates=LocalUpdates, sender=Sender}) ->
    clocksi_vnode:abort(LocalUpdates, TxId),
    gen_server:reply(Sender, {aborted, TxId}),
    {noreply, SD0#state{tx_id={}}};
handle_cast({abort, _, local}, SD0) ->
    %lager:info("Received abort for previous txn ~w", [OtherTxId]),
    {noreply, SD0};

handle_cast({prepared, TxId, PrepareTime, remote}, 
	    SD0=#state{pending_metadata=PendingMetadata}) ->
    lager:info("Received remote prepare"),
    case ets:lookup(PendingMetadata, TxId) of
        [{TxId, {1, MaxPrepTime, LocalUpdates, RemoteUpdates, Sender}}] ->
            CommitTime = max(MaxPrepTime, PrepareTime),
            ets:delete(PendingMetadata, TxId),
            clocksi_vnode:commmit(LocalUpdates, TxId, CommitTime),
            clocksi_vnode:commmit(RemoteUpdates, TxId, CommitTime),
            gen_server:reply(Sender, {ok, {committed, CommitTime}});
        [{TxId, {N, MaxPrepTime, LocalUpdates, RemoteUpdates, Sender}}] ->
            ets:insert(PendingMetadata, {TxId, {N-1, max(MaxPrepTime, PrepareTime), LocalUpdates, RemoteUpdates, Sender}});
        Record ->
            lager:info("Something is right: ~w", [Record])
    end,
    {noreply, SD0};

handle_cast({abort, TxId, remote}, 
	    SD0=#state{pending_metadata=PendingMetadata}) ->
    case ets:lookup(PendingMetadata, TxId) of
        [] ->
            {noreply, SD0};
        [{TxId, {_, _, LocalUpdates, RemoteUpdates, Sender}}] ->
            ets:delete(PendingMetadata, TxId),
            clocksi_vnode:abort(LocalUpdates, TxId),
            clocksi_vnode:abort(RemoteUpdates, TxId),
            gen_server:reply(Sender, {aborted, TxId}),
            {noreply, SD0}
    end;

handle_cast(_Info, StateData) ->
    {noreply,StateData}.

handle_info(_Info, StateData) ->
    {noreply,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

terminate(Reason, _SD) ->
    lager:info("Cert server terminated with ~w", [Reason]),
    ok.

