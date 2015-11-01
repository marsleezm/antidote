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
-module(specula_tx_cert_server).

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

%% Spawn

-record(state, {partition :: non_neg_integer(),
        tx_id :: txid(),
        prepare_time = 0 :: non_neg_integer(),
        local_parts :: [],
        remote_parts :: [],
        pending_txs :: cache_id(),
        is_specula = false :: boolean(),
        sender :: term(),
        to_ack :: non_neg_integer()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name) ->
    gen_server:start_link({local, Name},
             ?MODULE, [], []).

%%%===================================================================
%%% Internal
%%%===================================================================

init([]) ->
    %PendingMetadata = tx_utilities:open_private_table(pending_metadata),
    PendingTxs = tx_utilities:open_private_table(pending_txs), 
    {ok, #state{pending_txs=PendingTxs}}.

handle_call({certify, TxId, LocalUpdates, RemoteUpdates},  Sender, SD0) ->
    LocalParts = [Part || {Part, _} <- LocalUpdates],
    %LocalKeys = lists:map(fun({Node, Ups}) -> {Node, [Key || {Key, _} <- Ups]} end, LocalUpdates),
    %lager:info("Got req: localKeys ~p", [LocalKeys]),
    case length(LocalUpdates) of
        0 ->
            specula_vnode:prepare(RemoteUpdates, TxId, remote),
            {noreply, SD0#state{tx_id=TxId, to_ack=length(RemoteUpdates), local_parts=LocalParts, remote_parts=
                RemoteUpdates, sender=Sender}};
        _ ->
            %lager:info("Local updates are ~w", [LocalUpdates]),
            specula_vnode:prepare(LocalUpdates, TxId, local),
            {noreply, SD0#state{tx_id=TxId, to_ack=length(LocalUpdates), local_parts=LocalParts, remote_parts=
                RemoteUpdates, sender=Sender}}
    end;

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

handle_cast({prepared, TxId, PrepareTime, local}, 
	    SD0=#state{to_ack=N, tx_id=TxId, local_parts=LocalParts,
            remote_parts=RemoteUpdates, sender=Sender, prepare_time=OldPrepTime}) ->
    case N of
        1 -> 
            RemoteParts = [Part || {Part, _} <- RemoteUpdates],
            case length(RemoteParts) of
                0 ->
                    %lager:info("Trying to prepare!!"),
                    CommitTime = max(PrepareTime, OldPrepTime),
                    specula_vnode:commit(LocalParts, TxId, CommitTime),
                    gen_server:reply(Sender, {ok, {committed, CommitTime}}),
                    {noreply, SD0#state{prepare_time=CommitTime, tx_id={}}};
                _ ->
                    MaxPrepTime = max(PrepareTime, OldPrepTime),
                    specula_vnode:prepare(RemoteUpdates, TxId, remote),
                    {noreply, SD0#state{prepare_time=MaxPrepTime, to_ack=length(RemoteParts),
                        remote_parts=RemoteParts}}
                end;
        _ ->
            %lager:info("Not done for ~w", [TxId]),
            MaxPrepTime = max(OldPrepTime, PrepareTime),
            {noreply, SD0#state{to_ack=N-1, prepare_time=MaxPrepTime}}
    end;

handle_cast({prepared, _, _, local}, 
	    SD0) ->
    %lager:info("Received prepare of previous prepared txn! ~w", [OtherTxId]),
    {noreply, SD0};

handle_cast({abort, TxId, local}, SD0=#state{tx_id=TxId, local_parts=LocalParts, sender=Sender}) ->
    specula_vnode:abort(LocalParts, TxId),
    gen_server:reply(Sender, {aborted, TxId}),
    {noreply, SD0#state{tx_id={}}};
handle_cast({abort, _, local}, SD0) ->
    %lager:info("Received abort for previous txn ~w", [OtherTxId]),
    {noreply, SD0};

handle_cast({prepared, TxId, PrepareTime, remote}, 
	    SD0=#state{remote_parts=RemoteParts, sender=Sender, 
            prepare_time=MaxPrepTime, to_ack=N, local_parts=LocalParts}) ->
    %lager:info("Received remote prepare"),
    case N of
        1 ->
            CommitTime = max(MaxPrepTime, PrepareTime),
            specula_vnode:commit(LocalParts, TxId, CommitTime),
            specula_vnode:commit(RemoteParts, TxId, CommitTime),
            gen_server:reply(Sender, {ok, {committed, CommitTime}}),
            {noreply, SD0#state{prepare_time=CommitTime, tx_id={}}};
        N ->
            {noreply, SD0#state{to_ack=N-1, prepare_time=max(MaxPrepTime, PrepareTime)}}
    end;
handle_cast({prepared, _, _, remote}, 
	    SD0) ->
    {noreply, SD0};

handle_cast({abort, TxId, remote}, 
	    SD0=#state{remote_parts=RemoteParts, sender=Sender, tx_id=TxId, local_parts=LocalParts}) ->
    specula_vnode:abort(LocalParts, TxId),
    specula_vnode:abort(RemoteParts, TxId),
    gen_server:reply(Sender, {aborted, TxId}),
    {noreply, SD0#state{tx_id={}}};

handle_cast({abort, _, remote}, 
	    SD0) ->
    {noreply, SD0};

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

