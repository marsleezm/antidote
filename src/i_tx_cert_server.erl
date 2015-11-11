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

-record(state, {partition :: non_neg_integer(),
        tx_id :: txid(),
        prepare_time = 0 :: non_neg_integer(),
        local_parts :: [],
        remote_parts :: [],
        do_repl :: boolean(),
        sender :: term(),
        to_ack :: non_neg_integer()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name) ->
    lager:info("Normal tx cert started wit name ~w", [Name]),
    gen_server:start_link({global, Name},
             ?MODULE, [], []).

%%%===================================================================
%%% Internal
%%%===================================================================

init([]) ->
    %PendingMetadata = tx_utilities:open_private_table(pending_metadata),
    {ok, #state{do_repl=antidote_config:get(do_repl)}}.

handle_call({get_stat}, _Sender, SD0) ->
    {reply, {0, 0, 0, 0, 0}, SD0};

handle_call({get_hash_fun}, _Sender, SD0) ->
    L = hash_fun:get_hash_fun(),
    {reply, L, SD0};

handle_call({start_tx}, _Sender, SD0) ->
    TxId = tx_utilities:create_transaction_record(0),
    {reply, TxId, SD0};

handle_call({read, Key, TxId, Node0}, Sender, SD0) ->
    Node = case Node0 of
                {raw,  N, P} ->
                    hash_fun:get_vnode_by_id(P, N);
                _ ->
                    Node0
              end,
            %lager:info("No Value"),
    case Node of
        {_,_} ->
            clocksi_vnode:relay_read(Node, Key, TxId, Sender),
            %lager:info("Well, from clocksi_vnode"),
            {noreply, SD0};
        _ ->
            data_repl_serv:relay_read(Node, TxId, Key, Sender),
            {noreply, SD0}
    end;

handle_call({certify, TxId, LocalUpdates0, RemoteUpdates0},  Sender, SD0) ->
    {LocalUpdates, RemoteUpdates} = case LocalUpdates0 of   
                                        {raw, LList} ->
                                            {raw, RList} = RemoteUpdates0,
                                            {[{hash_fun:get_vnode_by_id(P, N), Ups}  || {N, P, Ups} <- LList],
                                             [{hash_fun:get_vnode_by_id(P, N), Ups}  || {N, P, Ups} <- RList]};
                                        _ ->
                                            {LocalUpdates0, RemoteUpdates0}
                                    end,
    LocalParts = [Part || {Part, _} <- LocalUpdates],
    %LocalKeys = lists:map(fun({Node, Ups}) -> {Node, [Key || {Key, _} <- Ups]} end, LocalUpdates),
    %lager:info("TxId ~w: localUps ~p, remoteUps ~p", [TxId, LocalUpdates, RemoteUpdates]),
    %lager:info("TxId ~w", [TxId]),
    case length(LocalUpdates) of
        0 ->
            clocksi_vnode:prepare(RemoteUpdates, TxId, remote),
            {noreply, SD0#state{tx_id=TxId, to_ack=length(RemoteUpdates), local_parts=LocalParts, remote_parts=
                RemoteUpdates, sender=Sender}};
        _ ->
            %lager:info("Local updates are ~w", [LocalUpdates]),
            clocksi_vnode:prepare(LocalUpdates, TxId, local),
            {noreply, SD0#state{tx_id=TxId, to_ack=length(LocalUpdates), local_parts=LocalParts, remote_parts=
                RemoteUpdates, sender=Sender}}
    end;

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

handle_cast({prepared, TxId, PrepareTime, local}, 
	    SD0=#state{to_ack=N, tx_id=TxId, local_parts=LocalParts, do_repl=DoRepl,
            remote_parts=RemoteUpdates, sender=Sender, prepare_time=OldPrepTime}) ->
    case N of
        1 -> 
            RemoteParts = [Part || {Part, _} <- RemoteUpdates],
            case length(RemoteParts) of
                0 ->
                    %lager:info("Trying to prepare!!"),
                    CommitTime = max(PrepareTime, OldPrepTime),
                    clocksi_vnode:commit(LocalParts, TxId, CommitTime),
                    repl_fsm:repl_commit(LocalParts, TxId, CommitTime, DoRepl),
                    gen_server:reply(Sender, {ok, {committed, CommitTime}}),
                    {noreply, SD0#state{prepare_time=CommitTime, tx_id={}}};
                _ ->
                    MaxPrepTime = max(PrepareTime, OldPrepTime),
                    clocksi_vnode:prepare(RemoteUpdates, TxId, remote),
                    %lager:info("~w: to ack is ~w, parts are ~w", [TxId, length(RemoteParts), RemoteParts]),
                    {noreply, SD0#state{prepare_time=MaxPrepTime, to_ack=length(RemoteParts),
                        remote_parts=RemoteParts}}
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

handle_cast({abort, TxId, local}, SD0=#state{tx_id=TxId, local_parts=LocalParts, 
            do_repl=DoRepl, sender=Sender}) ->
    clocksi_vnode:abort(LocalParts, TxId),
    repl_fsm:repl_abort(LocalParts, TxId, DoRepl),
    gen_server:reply(Sender, {aborted, TxId}),
    {noreply, SD0#state{tx_id={}}};
handle_cast({abort, _, local}, SD0) ->
    %lager:info("Received abort for previous txn ~w", [OtherTxId]),
    {noreply, SD0};

handle_cast({prepared, TxId, PrepareTime, remote}, 
	    SD0=#state{remote_parts=RemoteParts, sender=Sender, tx_id=TxId, do_repl=DoRepl, 
            prepare_time=MaxPrepTime, to_ack=N, local_parts=LocalParts}) ->
    %lager:info("Received remote prepare ~w, ~w, N is ~w", [TxId, PrepareTime, N]),
    case N of
        1 ->
            %lager:info("Decided to commit a txn ~w, prepare time is ~w", [TxId, PrepareTime]),
            CommitTime = max(MaxPrepTime, PrepareTime),
            clocksi_vnode:commit(LocalParts, TxId, CommitTime),
            clocksi_vnode:commit(RemoteParts, TxId, CommitTime),
            repl_fsm:repl_commit(LocalParts, TxId, CommitTime, DoRepl),
            repl_fsm:repl_commit(RemoteParts, TxId, CommitTime, DoRepl),
            gen_server:reply(Sender, {ok, {committed, CommitTime}}),
            {noreply, SD0#state{prepare_time=CommitTime, tx_id={}}};
        N ->
            {noreply, SD0#state{to_ack=N-1, prepare_time=max(MaxPrepTime, PrepareTime)}}
    end;
handle_cast({prepared, _, _, remote}, 
	    SD0) ->
    {noreply, SD0};

handle_cast({abort, TxId, remote}, 
	    SD0=#state{remote_parts=RemoteParts, sender=Sender, tx_id=TxId, 
        local_parts=LocalParts, do_repl=DoRepl}) ->
    clocksi_vnode:abort(LocalParts, TxId),
    clocksi_vnode:abort(RemoteParts, TxId),
    repl_fsm:repl_abort(LocalParts, TxId, DoRepl),
    repl_fsm:repl_abort(RemoteParts, TxId, DoRepl),
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

