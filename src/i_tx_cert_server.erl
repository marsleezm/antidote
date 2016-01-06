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
        last_commit_ts :: non_neg_integer(),
        rep_dict :: dict(),
        to_ack :: non_neg_integer()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name) ->
    %lager:info("Normal tx cert started wit name ~w", [Name]),
    gen_server:start_link({global, Name},
             ?MODULE, [], []).

%%%===================================================================
%%% Internal
%%%===================================================================

init([]) ->
    %PendingMetadata = tx_utilities:open_private_table(pending_metadata),
    DoRepl = antidote_config:get(do_repl),
    RepDict = hash_fun:build_rep_dict(DoRepl),
    {ok, #state{do_repl=DoRepl, last_commit_ts=0, rep_dict=RepDict}}.

handle_call({get_stat}, _Sender, SD0) ->
    {reply, {0, 0, 0, 0, 0}, SD0};

handle_call({single_commit, Node, Key, Value}, Sender, SD0) ->
    clocksi_vnode:single_commit(Node,[{Key, Value}], Sender),
    {noreply, SD0};

handle_call({get_hash_fun}, _Sender, SD0) ->
    L = hash_fun:get_hash_fun(),
    {reply, L, SD0};

handle_call({start_tx}, _Sender, SD0=#state{last_commit_ts=LastCommitTS}) ->
    TxId = tx_utilities:create_tx_id(LastCommitTS+1),
    {reply, TxId, SD0#state{last_commit_ts=LastCommitTS+1}};

handle_call({start_read_tx}, _Sender, SD0) ->
    TxId = tx_utilities:create_tx_id(0),
    {reply, TxId, SD0};

handle_call({read, Key, TxId, Node}, Sender, SD0) ->
    clocksi_vnode:relay_read(Node, Key, TxId, Sender, no_specula),
    {noreply, SD0};

handle_call({certify, TxId, LocalUpdates, RemoteUpdates},  Sender, SD0) ->
    %{LocalUpdates, RemoteUpdates} = case LocalUpdates0 of   
    %                                    {raw, LList} ->
    %                                        {raw, RList} = RemoteUpdates0,
    %                                        {[{hash_fun:get_vnode_by_id(P, N), Ups}  || {N, P, Ups} <- LList],
    %                                         [{hash_fun:get_vnode_by_id(P, N), Ups}  || {N, P, Ups} <- RList]};
    %                                    _ ->
    %                                        {LocalUpdates0, RemoteUpdates0}
    %                                end,
    %LocalKeys = lists:map(fun({Node, Ups}) -> {Node, [Key || {Key, _} <- Ups]} end, LocalUpdates),
    %lager:info("TxId ~w: localUps ~p, remoteUps ~p", [TxId, LocalUpdates, RemoteUpdates]),
    %lager:info("TxId ~w", [TxId]),
    case length(LocalUpdates) of
        0 ->
            RemoteParts = [P || {P, _} <- RemoteUpdates],
            clocksi_vnode:prepare(RemoteUpdates, TxId, {remote,ignore}),
            {noreply, SD0#state{tx_id=TxId, to_ack=length(RemoteUpdates), local_parts=[], remote_parts=
                RemoteParts, sender=Sender}};
        N ->
            LocalParts = [Part || {Part, _} <- LocalUpdates],
            %lager:info("Local updates are ~w", [LocalUpdates]),
            clocksi_vnode:prepare(LocalUpdates, TxId, local),
            {noreply, SD0#state{tx_id=TxId, to_ack=N, local_parts=LocalParts, remote_parts=
                RemoteUpdates, sender=Sender}}
    end;

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

handle_cast({prepared, TxId, PrepareTime, local}, 
	    SD0=#state{to_ack=N, tx_id=TxId, local_parts=LocalParts, do_repl=DoRepl, rep_dict=RepDict,
            remote_parts=RemoteUpdates, sender=Sender, prepare_time=OldPrepTime}) ->
    %lager:info("Got prepared local"),
    case N of
        1 -> 
            RemoteParts = [Part || {Part, _} <- RemoteUpdates],
            case length(RemoteParts) of
                0 ->
                    %lager:info("Trying to prepare!!"),
                    CommitTime = max(PrepareTime, OldPrepTime),
                    clocksi_vnode:commit(LocalParts, TxId, CommitTime),
                    repl_fsm:repl_commit(LocalParts, TxId, CommitTime, DoRepl, false, RepDict),
                    gen_server:reply(Sender, {ok, {committed, CommitTime}}),
                    {noreply, SD0#state{prepare_time=CommitTime, tx_id={}, last_commit_ts=CommitTime}};
                L ->
                    MaxPrepTime = max(PrepareTime, OldPrepTime),
                    clocksi_vnode:prepare(RemoteUpdates, TxId, {remote,ignore}),
                    %lager:info("~w: Updates are ~p, to ack is ~w, parts are ~w", [TxId, RemoteUpdates, length(RemoteParts), RemoteParts]),
                    {noreply, SD0#state{prepare_time=MaxPrepTime, to_ack=L,
                        remote_parts=RemoteParts}}
                end;
        _ ->
            %lager:info("~w, needs ~w", [TxId, N-1]),
            %lager:info("Not done for ~w", [TxId]),
            MaxPrepTime = max(OldPrepTime, PrepareTime),
            {noreply, SD0#state{to_ack=N-1, prepare_time=MaxPrepTime}}
    end;
handle_cast({prepared, _OtherTxId, _, local}, 
	    SD0) ->
    %lager:info("Received prepare of previous prepared txn! ~w", [OtherTxId]),
    {noreply, SD0};

handle_cast({abort, TxId, local, FromNode}, SD0=#state{tx_id=TxId, local_parts=LocalParts, 
            do_repl=DoRepl, sender=Sender, rep_dict=RepDict}) ->
    %lager:info("Local abort ~w", [TxId]),
    LocalParts1 = lists:delete(FromNode, LocalParts), 
    clocksi_vnode:abort(LocalParts1, TxId),
    repl_fsm:repl_abort(LocalParts1, TxId, DoRepl, false, RepDict),
    gen_server:reply(Sender, {aborted, TxId}),
    {noreply, SD0#state{tx_id={}}};
handle_cast({abort, _, local, _}, SD0) ->
    %lager:info("Received abort for previous txn ~w", [OtherTxId]),
    {noreply, SD0};

handle_cast({prepared, TxId, PrepareTime, remote}, 
	    SD0=#state{remote_parts=RemoteParts, sender=Sender, tx_id=TxId, do_repl=DoRepl, 
            prepare_time=MaxPrepTime, to_ack=N, local_parts=LocalParts, rep_dict=RepDict}) ->
    %lager:info("Received remote prepare ~w, ~w, N is ~w", [TxId, PrepareTime, N]),
    case N of
        1 ->
            %lager:info("Decided to commit a txn ~w, prepare time is ~w", [TxId, PrepareTime]),
            CommitTime = max(MaxPrepTime, PrepareTime),
            clocksi_vnode:commit(LocalParts, TxId, CommitTime),
            clocksi_vnode:commit(RemoteParts, TxId, CommitTime),
            repl_fsm:repl_commit(LocalParts, TxId, CommitTime, DoRepl, false, RepDict),
            repl_fsm:repl_commit(RemoteParts, TxId, CommitTime, DoRepl, false, RepDict),
            gen_server:reply(Sender, {ok, {committed, CommitTime}}),
            {noreply, SD0#state{prepare_time=CommitTime, tx_id={}, last_commit_ts=CommitTime}};
        N ->
            %lager:info("Need ~w more replies for ~w", [N-1, TxId]),
            {noreply, SD0#state{to_ack=N-1, prepare_time=max(MaxPrepTime, PrepareTime)}}
    end;
handle_cast({prepared, _, _, remote}, 
	    SD0) ->
    {noreply, SD0};

handle_cast({abort, TxId, {remote,_}, FromNode}, 
	    SD0=#state{remote_parts=RemoteParts, sender=Sender, tx_id=TxId, 
        local_parts=LocalParts, do_repl=DoRepl, rep_dict=RepDict}) ->
    %lager:info("Remote abort ~w", [TxId]),
    RemoteParts1 = lists:delete(FromNode, RemoteParts),
    clocksi_vnode:abort(LocalParts, TxId),
    clocksi_vnode:abort(RemoteParts1, TxId),
    repl_fsm:repl_abort(LocalParts, TxId, DoRepl, false, RepDict),
    repl_fsm:repl_abort(RemoteParts1, TxId, DoRepl, false, RepDict),
    gen_server:reply(Sender, {aborted, TxId}),
    {noreply, SD0#state{tx_id={}}};

handle_cast({abort, _, {remote,_}, _}, 
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

