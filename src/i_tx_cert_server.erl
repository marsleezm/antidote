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
        rep_dict :: dict(),
        prepare_time = 0 :: non_neg_integer(),
        stage :: read | local_cert | remote_cert,
        local_parts :: [],
        remote_parts :: [],
        do_repl :: boolean(),
        pending_to_ack :: non_neg_integer(),
        sender :: term(),
        total_repl_factor :: non_neg_integer(),
        last_commit_ts :: non_neg_integer(),
        to_ack :: non_neg_integer()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name) ->
   %lager:warning("Specula tx cert started wit name ~w, id is ~p", [Name, self()]),
    gen_server:start_link({local, Name},
             ?MODULE, [], []).

%%%===================================================================
%%% Internal
%%%===================================================================

init([]) ->
    DoRepl = antidote_config:get(do_repl),
    RepDict = hash_fun:build_rep_dict(DoRepl),
    %PendingMetadata = tx_utilities:open_private_table(pending_metadata),
    [{_, Replicas}] = ets:lookup(meta_info, node()),
    TotalReplFactor = length(Replicas)+1,
    {ok, #state{do_repl=antidote_config:get(do_repl), total_repl_factor=TotalReplFactor, last_commit_ts=0, rep_dict=RepDict}}.

handle_call({get_stat}, _Sender, SD0) ->
    {reply, {0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, SD0};

handle_call({append_values, Node, KeyValues, CommitTime}, Sender, SD0) ->
    clocksi_vnode:append_values(Node, KeyValues, CommitTime, Sender),
    {noreply, SD0};

handle_call({get_pid}, _Sender, SD0) ->
      {reply, self(), SD0};

handle_call({get_hash_fun}, _Sender, SD0) ->
    L = hash_fun:get_hash_fun(),
    {reply, L, SD0};

handle_call({start_tx}, _Sender, SD0=#state{last_commit_ts=LastCommitTS}) ->
    TxId = tx_utilities:create_tx_id(LastCommitTS+1),
   %lager:warning("Starting tx ~w", [TxId]),
    {reply, TxId, SD0#state{last_commit_ts=LastCommitTS+1, stage=read, pending_to_ack=0}};

handle_call({start_read_tx}, _Sender, SD0) ->
    TxId = tx_utilities:create_tx_id(0),
    {reply, TxId, SD0};

handle_call({read, Key, TxId, Node}, Sender, SD0) ->
    clocksi_vnode:relay_read(Node, Key, TxId, Sender, no_specula),
    {noreply, SD0};

handle_call({certify, TxId, LocalUpdates, RemoteUpdates},  Sender, SD0=#state{last_commit_ts=LastCommitTs, 
            total_repl_factor=TotalReplFactor}) ->
   %lager:warning("Certifying tx ~w", [TxId]),
    case length(LocalUpdates) of
        0 ->
            case length(RemoteUpdates) of
                0 -> gen_server:reply(Sender, {ok, {committed, LastCommitTs}}),
                    {noreply, SD0};
                _ -> 
                    RemoteParts = [P || {P, _} <- RemoteUpdates],
                    clocksi_vnode:prepare(RemoteUpdates, TxId, {remote,ignore}),
                    {noreply, SD0#state{tx_id=TxId, to_ack=length(RemoteUpdates)*TotalReplFactor, local_parts=[], remote_parts=
                        RemoteParts, sender=Sender, stage=remote_cert}}
            end;
        N ->
            LocalParts = [Part || {Part, _} <- LocalUpdates],
            %lager:warning("Sending certify to ~w", [LocalParts]),
            case RemoteUpdates of
                [] ->
                    clocksi_vnode:prepare(LocalUpdates, TxId, local_only);
                _ ->
                    clocksi_vnode:prepare(LocalUpdates, TxId, local)
            end,
            {noreply, SD0#state{tx_id=TxId, to_ack=N, pending_to_ack=N*(TotalReplFactor-1), local_parts=LocalParts, remote_parts=
                RemoteUpdates, sender=Sender, stage=local_cert}}
    end;

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

handle_cast({load, Sup, Type, Param}, SD0) ->
    case Type of tpcc -> tpcc_load:load(Param);
                 micro -> micro_load:load(Param)
    end,
    Sup ! done,
    {noreply, SD0};

handle_cast({clean_data, Sender}, SD0) ->
    Sender ! cleaned,
    {noreply, SD0};

handle_cast({prepared, TxId, PrepareTime}, 
	    SD0=#state{to_ack=N, tx_id=TxId, pending_to_ack=PN, local_parts=LocalParts, do_repl=DoRepl, stage=local_cert,
            remote_parts=RemoteUpdates, sender=Sender, prepare_time=OldPrepTime, total_repl_factor=TotalReplFactor,rep_dict=RepDict}) ->
   %lager:warning("Got prepared local for ~w, to ack is ~w, pn is ~w", [TxId, N, PN]),
    case N of
        1 -> 
            RemoteParts = [Part || {Part, _} <- RemoteUpdates],
            case length(RemoteParts) of
                0 ->
                    case PN of
                        0 ->
                            CommitTime = max(PrepareTime, OldPrepTime),
                            clocksi_vnode:commit(LocalParts, TxId, CommitTime),
                            repl_fsm:repl_commit(LocalParts, TxId, CommitTime, DoRepl, false, RepDict),
                            gen_server:reply(Sender, {ok, {committed, CommitTime}}),
                            {noreply, SD0#state{prepare_time=CommitTime, tx_id={}, last_commit_ts=CommitTime}};
                        _ ->
                            {noreply, SD0#state{to_ack=PN, pending_to_ack=0, prepare_time=max(PrepareTime, OldPrepTime), stage=remote_cert}}
                    end;
                L ->
                    MaxPrepTime = max(PrepareTime, OldPrepTime),
                    clocksi_vnode:prepare(RemoteUpdates, TxId, {remote,ignore}),
                    %lager:warning("~w: Updates are ~p, to ack is ~w, parts are ~w", [TxId, RemoteUpdates, length(RemoteParts), RemoteParts]),
                    {noreply, SD0#state{prepare_time=MaxPrepTime, to_ack=L*TotalReplFactor+PN, pending_to_ack=0,
                        remote_parts=RemoteParts, stage=remote_cert}}
                end;
        _ ->
            %lager:warning("~w, needs ~w", [TxId, N-1]),
            %lager:warning("Not done for ~w", [TxId]),
            MaxPrepTime = max(OldPrepTime, PrepareTime),
            {noreply, SD0#state{to_ack=N-1, prepare_time=MaxPrepTime}}
    end;
handle_cast({prepared, _OtherTxId, _}, 
	    SD0=#state{stage=local_cert}) ->
      %lager:info("Received prepare of previous prepared txn! ~w", [OtherTxId]),
    {noreply, SD0};

handle_cast({aborted, TxId, FromNode}, SD0=#state{tx_id=TxId, local_parts=LocalParts, 
            do_repl=DoRepl, sender=Sender, stage=local_cert, rep_dict=RepDict}) ->
    %lager:warning("Local abort ~w", [TxId]),
    LocalParts1 = lists:delete(FromNode, LocalParts), 
    clocksi_vnode:abort(LocalParts1, TxId),
    repl_fsm:repl_abort(LocalParts1, TxId, DoRepl, false, RepDict),
    gen_server:reply(Sender, {aborted, local}),
    {noreply, SD0#state{tx_id={}}};
handle_cast({aborted, _, _}, SD0=#state{stage=local_cert}) ->
    {noreply, SD0};

%handle_cast({real_prepared, TxId, PrepareTime}, SD0=#state{stage=remote_cert, tx_id=TxId}) ->
%    handle_cast({prepared, TxId, PrepareTime}, SD0);

%handle_cast({real_prepared, _OtherTxId, _PrepareTime}, SD0=#state{stage=remote_cert}) ->
%    {noreply, SD0};

handle_cast({prepared, TxId, PrepareTime}, 
	    SD0=#state{remote_parts=RemoteParts, sender=Sender, tx_id=TxId, do_repl=DoRepl, rep_dict=RepDict, 
            prepare_time=MaxPrepTime, to_ack=N, local_parts=LocalParts, stage=remote_cert}) ->
    %lager:warning("Received remote prepare ~w, ~w, N is ~w", [TxId, PrepareTime, N]),
    case N of
        1 ->
            %lager:warning("Decided to commit a txn ~w, prepare time is ~w", [TxId, PrepareTime]),
            CommitTime = max(MaxPrepTime, PrepareTime),
            clocksi_vnode:commit(LocalParts, TxId, CommitTime),
            clocksi_vnode:commit(RemoteParts, TxId, CommitTime),
            repl_fsm:repl_commit(LocalParts, TxId, CommitTime, DoRepl, false, RepDict),
            repl_fsm:repl_commit(RemoteParts, TxId, CommitTime, DoRepl, false, RepDict),
            gen_server:reply(Sender, {ok, {committed, CommitTime}}),
            {noreply, SD0#state{prepare_time=CommitTime, tx_id={}, last_commit_ts=CommitTime}};
        N ->
            %lager:warning("Need ~w more replies for ~w", [N-1, TxId]),
            {noreply, SD0#state{to_ack=N-1, prepare_time=max(MaxPrepTime, PrepareTime)}}
    end;
handle_cast({prepared, _, _}, 
	    SD0) ->
    {noreply, SD0};

handle_cast({aborted, TxId, FromNode}, 
	    SD0=#state{remote_parts=RemoteParts, sender=Sender, tx_id=TxId, 
        local_parts=LocalParts, do_repl=DoRepl, stage=remote_cert, rep_dict=RepDict}) ->
     %lager:warning("Remote abort ~w", [TxId]),
    RemoteParts1 = lists:delete(FromNode, RemoteParts),
    clocksi_vnode:abort(LocalParts, TxId),
    clocksi_vnode:abort(RemoteParts1, TxId),
    repl_fsm:repl_abort(LocalParts, TxId, DoRepl, false, RepDict),
    repl_fsm:repl_abort(RemoteParts1, TxId, DoRepl, false, RepDict),
    gen_server:reply(Sender, {aborted, remote}),
    {noreply, SD0#state{tx_id={}}};

handle_cast({aborted, _, _}, 
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

