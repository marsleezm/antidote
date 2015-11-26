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
-define(CLOCKSI_VNODE, mock_partition_fsm).
-define(REPL_FSM, mock_partition_fsm).
-define(SPECULA_TX_CERT_SERVER, mock_partition_fsm).
-define(CACHE_SERV, mock_partition_fsm).
-define(DATA_REPL_SERV, mock_partition_fsm).
-define(READ_VALID(SEND, TXID), mock_partition_fsm:read_valid(SEND, TXID)).
-define(READ_INVALID(SEND, CT, TXID), mock_partition_fsm:read_invalid(SEND, CT, TXID)).
-else.
-define(CLOCKSI_VNODE, clocksi_vnode).
-define(REPL_FSM, repl_fsm).
-define(SPECULA_TX_CERT_SERVER, specula_tx_cert_server).
-define(CACHE_SERV, cache_serv).
-define(DATA_REPL_SERV, data_repl_serv).
-define(READ_VALID(SEND, TXID), gen_server:cast(SEND, {read_valid, TXID})).
-define(READ_INVALID(SEND, CT, TXID), gen_server:cast(SEND, {read_invalid, CT, TXID})).
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
        last_commit_ts = 0 :: non_neg_integer(),
        to_ack :: non_neg_integer(),
        read_deps :: integer(),
        local_updates :: [],
        remote_updates :: [],
        rep_dict :: dict(),
        read_dep_dict :: dict(),
        pending_list=[] :: [txid()],
        stage :: atom(),
        invalid_ts :: non_neg_integer(),

        %specula_data :: cache_id(),
        pending_txs :: cache_id(),
        do_repl :: boolean(),
        is_specula = false :: boolean(),
        read_aborted=0 :: non_neg_integer(),
        hit_cache=0 :: non_neg_integer(),
        committed=0 :: non_neg_integer(),
        aborted=0 :: non_neg_integer(),
        speculated=0 :: non_neg_integer(),
        sender :: term()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name) ->
    lager:info("Specula tx cert started wit name ~w", [Name]),
    gen_server:start_link({global, Name},
             ?MODULE, [], []).

%%%===================================================================
%%% Internal
%%%===================================================================

init([]) ->
    %PendingMetadata = tx_utilities:open_private_table(pending_metadata),
    Lists = antidote_config:get(to_repl),
    [RepNodes] = [Reps || {Node, Reps} <- Lists, Node == node()],
    RepDict = lists:foldl(fun(N, D) -> dict:store(N, 
        list_to_atom(atom_to_list(node())++"repl"++atom_to_list(N)), D) end, dict:new(), RepNodes), 
    PendingTxs = tx_utilities:open_private_table(pending_txs), 
    %SpeculaData = tx_utilities:open_private_table(specula_data),
    {ok, #state{pending_txs=PendingTxs, rep_dict=RepDict, read_deps=0, read_dep_dict=dict:new(),
            do_repl=antidote_config:get(do_repl)}}.

handle_call({single_commit, Node, Key, Value}, Sender, SD0) ->
    clocksi_vnode:single_commit(Node,[{Key, Value}], Sender),
    {noreply, SD0};

handle_call({start_tx}, _Sender, SD0) ->
    TxId = tx_utilities:create_tx_id(0),
    lager:info("Starting txid ~w", [TxId]),
    {reply, TxId, SD0#state{tx_id=TxId, stage=reading, invalid_ts=0, read_deps=0}};

handle_call({get_stat}, _Sender, SD0=#state{aborted=Aborted, committed=Committed, read_aborted=ReadAborted, hit_cache=HitCache,
        speculated=Speculated}) ->
    lager:info("Hit cache is ~w, Num of read aborted ~w, Num of aborted is ~w, Num of committed is ~w, Speculated ~w", 
                [HitCache, ReadAborted, Aborted, Committed, Speculated]),
    {reply, {HitCache, ReadAborted, Aborted, Committed, Speculated}, SD0};

handle_call({set_internal_data, Type, Param}, _Sender, SD0)->
    case Type of
        last_commit_time ->
            lager:info("Set lct to ~w", [Param]),
            {noreply, SD0#state{last_commit_ts=Param}}
    end;

handle_call({get_internal_data, Type, Param}, _Sender, SD0=#state{pending_list=PendingList, 
        pending_txs=PendingTxs, read_deps=ReadDeps, read_dep_dict=ReadDepDict, last_commit_ts=LastCommitTs,
        committed=Committed, aborted=Aborted, read_aborted=ReadAborted})->
    case Type of
        pending_list ->
            {reply, PendingList, SD0};
        pending_txs ->
            case ets:lookup(PendingTxs, Param) of
                [] ->
                    {reply, [], SD0};
                [Content] ->
                    {reply, Content, SD0}
            end;
        dependency ->
            case ets:lookup(dependency, Param) of
                [] ->
                    {reply, [], SD0};
                Content ->
                    {reply, Content, SD0}
            end;
        anti_dep ->
            case ets:lookup(anti_dep, Param) of
                [] ->
                    {reply, [], SD0};
                Content ->
                    {reply, Content, SD0}
            end;
        my_dep ->
            {reply, ReadDeps, SD0};
        read_dep ->
            {reply, dict:find(Param, ReadDepDict), SD0};
        last_commit_time ->
            lager:info("Get lct is ~w", [LastCommitTs]),
            {reply, LastCommitTs, SD0};
        committed ->
            {reply, Committed, SD0};
        aborted ->
            {reply, Aborted, SD0};
        read_aborted ->
            {reply, ReadAborted, SD0}
    end;

handle_call({certify, TxId, LocalUpdates, RemoteUpdates},  Sender, SD0=#state{rep_dict=RepDict, 
            pending_txs=PendingTxs, last_commit_ts=LastCommitTs, read_deps=OldReadDeps,
            pending_list=PendingList, speculated=Speculated, read_dep_dict=ReadDepDict}) ->
    %LocalKeys = lists:map(fun({Node, Ups}) -> {Node, [Key || {Key, _} <- Ups]} end, LocalUpdates),
    %% If there was a legacy ongoing transaction.
    %lager:info("Certifying, txId is ~w, local num is ~w, remote num is ~w", 
    %        [TxId, length(LocalUpdates), length(RemoteUpdates)]),
    %lager:info("Got req: txid ~w, localUpdates ~p, remote updates ~p", [TxId, LocalUpdates, RemoteUpdates]),
    ReadDeps = length(ets:lookup(anti_dep, TxId)),
    true = ets:delete(anti_dep, TxId),
    lager:info("Start certifying ~w, old read deps is ~w, new read deps is ~w", [TxId, OldReadDeps, ReadDeps]),
    case length(LocalUpdates) of
        0 ->
            RemotePartitions = [P || {P, _} <- RemoteUpdates],
            ReadDepDict1 = case ReadDeps+OldReadDeps of
                                0 -> ReadDepDict;
                                N -> dict:store(TxId, N, ReadDepDict)
                            end,
            ets:insert(PendingTxs, {TxId, {length(RemotePartitions), LastCommitTs+1, [], RemotePartitions}}),
            ?CLOCKSI_VNODE:prepare(RemoteUpdates, TxId, {remote, node()}),
            gen_server:reply(Sender, {ok, {specula_commit, LastCommitTs}}),
            add_to_table(RemoteUpdates, TxId, LastCommitTs+1, RepDict),
            {noreply, SD0#state{tx_id=[], speculated=Speculated+1, stage=specula, read_dep_dict=ReadDepDict1,
                    pending_list=PendingList++[TxId], prepare_time=LastCommitTs+1}};
        _ ->
            %lager:info("Local updates are ~w", [LocalUpdates]),
            LocalPartitions = [P || {P, _} <- LocalUpdates],
            ?CLOCKSI_VNODE:prepare(LocalUpdates, TxId, local),
            {noreply, SD0#state{tx_id=TxId, to_ack=length(LocalUpdates), read_deps=ReadDeps+OldReadDeps, 
                    local_updates=LocalPartitions, 
                remote_updates=RemoteUpdates, sender=Sender, prepare_time=LastCommitTs+1, stage=local_cert}}
    end;

handle_call({read, Key, TxId, Node}, Sender, SD0) ->
    ?CLOCKSI_VNODE:relay_read(Node, Key, TxId, Sender, local),
    {noreply, SD0};

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

handle_cast({prepared, ReceivedTxId, PrepareTime, local}, 
	    SD0=#state{to_ack=N, tx_id=TxId, local_updates=LocalParts, do_repl=DoRepl, read_deps=ReadDeps,
            remote_updates=RemoteUpdates, sender=Sender, read_dep_dict=ReadDepDict, pending_list=PendingList,
             speculated=Speculated, last_commit_ts=LastCommitTs,
            prepare_time=OldPrepTime, pending_txs=PendingTxs, committed=Committed, rep_dict=RepDict}) ->
    lager:info("Got local prepare for ~w", [TxId]),
    case ReceivedTxId of
        TxId ->
            case N of
                1 -> 
                    lager:info("Got all local prepare!"),
                    NewMaxPrep = max(PrepareTime, OldPrepTime),
                    RemoteParts = [P || {P, _} <- RemoteUpdates],
                    ToAck = length(RemoteParts),
                    case ( ToAck ==0 ) and (ReadDeps == 0) of
                        true ->
                            case PendingList of
                                [] -> 
                                    CommitTime = max(NewMaxPrep, LastCommitTs+1),
                                    lager:info("Committing ~w with ~w", [TxId, CommitTime]),
                                    commit_tx(TxId, NewMaxPrep, LocalParts, DoRepl,
                                        PendingTxs),
                                    gen_server:reply(Sender, {ok, {committed, NewMaxPrep}}),
                                    {noreply, SD0#state{last_commit_ts=CommitTime, tx_id={}, committed=Committed+1,
                                        speculated=Speculated+1}};
                                _ ->
                                    %lager:info("Specula committing "),
                                    ets:insert(PendingTxs, {TxId, {0, NewMaxPrep, LocalParts, RemoteParts}}),
                                    gen_server:reply(Sender, {ok, {specula_commit, NewMaxPrep}}),
                                    add_to_table(RemoteUpdates, TxId, NewMaxPrep, RepDict),
                                    %lager:info("Pending list is ~w, TxId is ~w", [PendingList, TxId]),
                                    {noreply, SD0#state{speculated=Speculated+1, tx_id=[], 
                                        pending_list=PendingList++[TxId]}}
                            end;
                        _ ->
                            ?CLOCKSI_VNODE:prepare(RemoteUpdates, TxId, {remote, node()}),
                            %% Add dependent data into the table
                            gen_server:reply(Sender, {ok, {specula_commit, NewMaxPrep}}),
                            ets:insert(PendingTxs, {TxId, {ToAck, NewMaxPrep, LocalParts, RemoteParts}}),
                            add_to_table(RemoteUpdates, TxId, NewMaxPrep, RepDict),
                            case ReadDeps of
                                0 ->
                                    {noreply, SD0#state{tx_id=[], speculated=Speculated+1, 
                                    pending_list=PendingList++[TxId]}};
                                _ ->
                                    ReadDepDict1 = dict:store(TxId, ReadDeps, ReadDepDict),
                                    {noreply, SD0#state{tx_id=[], speculated=Speculated+1, read_dep_dict=ReadDepDict1, 
                                    pending_list=PendingList++[TxId]}}
                            end
                        end;
                        %%TODO: Maybe it does not speculatively-commit.
                _ ->
                    lager:info("~w needs ~w local prep replies", [TxId, N-1]),
                    MaxPrepTime = max(OldPrepTime, PrepareTime),
                    {noreply, SD0#state{to_ack=N-1, prepare_time=MaxPrepTime}}
            end;
        _ ->
            %lager:info("Got bad prepare for ~w, PT is ~w", [TxId, PrepareTime]),
            {noreply, SD0}
    end;

%% TODO: if we don't direclty speculate after getting all local prepared, maybe we can wait a littler more
%%       and here we should check if the transaction can be directly committed or not. 
handle_cast({read_valid, TxId}, SD0=#state{tx_id=TxId, read_deps=ReadDeps}) ->
    lager:info("~w got read valid before certify!", [TxId]),
    {noreply, SD0#state{read_deps=ReadDeps-1}};
handle_cast({read_valid, TxId}, SD0=#state{pending_txs=PendingTxs, rep_dict=RepDict, read_dep_dict=ReadDepDict, 
            pending_list=PendingList, do_repl=DoRepl, read_aborted=ReadAborted, tx_id=CurrentTxId, committed=Committed,
            local_updates=LocalParts, last_commit_ts=LastCommitTS, sender=Sender}) ->
    lager:info("Got read valid for ~w", [TxId]),
    case dict:find(TxId, ReadDepDict) of
        {ok, 1} -> 
            ReadDepDict1 = dict:erase(TxId, ReadDepDict),
            case hd(PendingList) of
                 TxId ->
                    {NewPendingList, NewMaxPT, NewReadDep, NewReadAborted, NewCommitted} = try_to_commit(
                            LastCommitTS, PendingList, RepDict, ReadDepDict1, DoRepl, PendingTxs, 
                        ReadAborted, Committed, {true, false}),
                    case NewReadAborted of
                        ReadAborted ->
                            %% No transaction is aborted
                            lager:info("Seting last commit ts to ~w", [NewMaxPT]),
                            {noreply, SD0#state{
                                pending_list=NewPendingList, read_dep_dict=NewReadDep,
                                  last_commit_ts=NewMaxPT, read_aborted=NewReadAborted, committed=NewCommitted}};
                        _ ->
                              %% The current transaction must have been aborted due to flow dependency
                            lager:info("Seting last commit ts to ~w", [NewMaxPT]),
                            ReadDepDict2 = abort_tx(CurrentTxId, LocalParts, DoRepl, 
                                PendingTxs, ReadDepDict1),
                            gen_server:reply(Sender, {aborted, CurrentTxId}),
                            {noreply, SD0#state{
                              pending_list=NewPendingList, tx_id=[], read_dep_dict=ReadDepDict2,
                                last_commit_ts=NewMaxPT, read_aborted=NewReadAborted, committed=NewCommitted}}
                    end;
                _ ->
                    {noreply, SD0#state{read_dep_dict=ReadDepDict1}}
            end;
        {ok, N} -> 
            ReadDepDict1 = dict:store(TxId, N-1, ReadDepDict),
            {noreply, SD0#state{read_dep_dict=ReadDepDict1}};
        error ->
            {noreply, SD0}
    end;

handle_cast({read_invalid, MyCommitTime, TxId}, SD0=#state{tx_id=TxId, sender=Sender, 
        stage=Stage, invalid_ts=InvalidTS, do_repl=DoRepl, read_dep_dict=ReadDepDict, 
        local_updates=LocalParts, pending_txs=PendingTxs}) ->
    lager:info("Got read invalid for ~w", [TxId]),
    case Stage of
        reading ->
            {noreply, SD0#state{invalid_ts=max(MyCommitTime, InvalidTS)}};
        local_cert ->
            %% TODO: Replace to read verification
            RD1 = abort_tx(TxId, LocalParts, DoRepl, PendingTxs, ReadDepDict),
            gen_server:reply(Sender, {aborted, TxId}),
            {noreply, SD0#state{read_dep_dict=RD1, tx_id=[]}}
    end;

handle_cast({read_invalid, _MyCommitTime, PendingTxId}, SD0=#state{tx_id=CurrentTxId, sender=Sender, 
        do_repl=DoRepl, read_dep_dict=ReadDepDict, pending_list=PendingList, aborted=Aborted, 
        rep_dict=RepDict, local_updates=LocalParts, pending_txs=PendingTxs}) ->
    lager:info("Got read invalid for ~w", [PendingTxId]),
    case start_from(PendingTxId, PendingList) of
        [] ->
            %lager:info("Not aborting anything"),
            {noreply, SD0};
        L ->
            lager:info("Abort due to read invalid, Pending list is ~w, PendingTxId is ~w, List is ~w", [PendingList, PendingTxId, L]),
            RD = abort_specula_list(L, RepDict, ReadDepDict, DoRepl, PendingTxs),
            %% The current transaction is aborted! So replying to client.
            case CurrentTxId of
                [] ->
                    {noreply, SD0#state{read_dep_dict=RD, tx_id=[], 
                        pending_list=lists:sublist(PendingList, length(PendingList)-length(L)),
                        aborted=Aborted+length(L)}};
                _ ->
                    RD1 = abort_tx(CurrentTxId, LocalParts, DoRepl, PendingTxs, RD),
                    gen_server:reply(Sender, {aborted, CurrentTxId}),
                    {noreply, SD0#state{read_dep_dict=RD1, tx_id=[], 
                        pending_list=lists:sublist(PendingList, length(PendingList)-length(L)),
                        aborted=Aborted+length(L)}}
            end
    end;

handle_cast({abort, ReceivedTxId, local}, SD0=#state{tx_id=TxId, local_updates=LocalUpdates, 
        do_repl=DoRepl, sender=Sender, pending_txs=PendingTxs, read_dep_dict=ReadDepDict}) ->
    case ReceivedTxId of
        TxId ->
            %lager:info("Received abort for ~w, setting tx_id to []", [TxId]),
            ReadDepDict1 = abort_tx(TxId, LocalUpdates, DoRepl, PendingTxs, ReadDepDict),
            gen_server:reply(Sender, {aborted, TxId}),
            {noreply, SD0#state{tx_id=[], read_dep_dict=ReadDepDict1}};
        _ ->
            {noreply, SD0}
    end;
handle_cast({prepared, PendingTxId, PendingPT, remote}, 
	    SD0=#state{pending_list=PendingList, do_repl=DoRepl, pending_txs=PendingTxs,
            sender=Sender, tx_id=CurrentTxId, rep_dict=RepDict, last_commit_ts=LastCommitTS,
             committed=Committed, read_aborted=ReadAborted, 
            local_updates=LocalParts, read_dep_dict=ReadDepDict}) ->
    lager:info("Got remote prepare for ~w", [PendingTxId]),
    case ets:lookup(PendingTxs, PendingTxId) of
        [{PendingTxId, {1, OldMaxPrepTime, LPs, RPs}}] ->
            NewMaxPrepTime = max(PendingPT, OldMaxPrepTime),
            lager:info("got all replies, Can try to commit"),
            ets:insert(PendingTxs, {PendingTxId, {0, NewMaxPrepTime, LPs, RPs}}),
            case hd(PendingList) of
                 PendingTxId ->
                    {NewPendingList, NewMaxPT, NewReadDep, NewReadAborted, NewCommitted} = try_to_commit(
                            LastCommitTS, PendingList, RepDict, ReadDepDict, DoRepl, PendingTxs, 
                        ReadAborted, Committed, {false, {NewMaxPrepTime, LPs, RPs}}),
                    case NewReadAborted of
                        ReadAborted ->
                            %% No transaction is aborted
                            lager:info("Seting last commit ts to ~w", [NewMaxPT]),
                            {noreply, SD0#state{
                                pending_list=NewPendingList, read_dep_dict=NewReadDep,
                                  last_commit_ts=NewMaxPT, read_aborted=NewReadAborted, committed=NewCommitted}};
                        _ ->
                              %% The current transaction must have been aborted due to flow dependency
                            lager:info("Seting last commit ts to ~w", [NewMaxPT]),
                            ReadDep1 = abort_tx(CurrentTxId, LocalParts, DoRepl, PendingTxs, NewReadDep),
                            gen_server:reply(Sender, {aborted, CurrentTxId}),
                            {noreply, SD0#state{
                              pending_list=NewPendingList, tx_id=[], read_dep_dict=ReadDep1, 
                                last_commit_ts=NewMaxPT, read_aborted=NewReadAborted, committed=NewCommitted}}
                    end;
                _ ->
                    lager:info("Can not commit due to pending ~w", [PendingList]),
                    {noreply, SD0}
            end;
        [{PendingTxId, {N, OldMaxPrepTime, LPs, RPs}}] ->
            lager:info("Needs ~w more replies", [N-1]),
            ets:insert(PendingTxs, {PendingTxId, {N-1, max(PendingPT, OldMaxPrepTime), LPs, RPs}}),
            {noreply, SD0};
        [] ->
            {noreply, SD0}
    end;
            

%handle_cast({abort, TxId, remote}, 
%	    SD0=#state{remote_updates=RemoteUpdates, pending_txs=PendingTxs, pending_list=PendingList, 
%            do_repl=DoRepl, tx_id=TxId, local_updates=LocalUpdates, aborted=Aborted}) ->
%    abort_tx(TxId, LocalUpdates, RemoteUpdates, DoRepl, PendingTxs),
%    %gen_server:reply(Sender, {aborted, TxId}),
%    {L, _} = lists:split(length(PendingList) - 1, PendingList),
%    %lager:info("Abort remote"),
%    {noreply, SD0#state{tx_id=[], aborted=Aborted+1, 
%                pending_list=L}};

handle_cast({abort, PendingTxId, remote}, 
	    SD0=#state{pending_list=PendingList, tx_id=TxId, sender=Sender, rep_dict=RepDict,
            do_repl=DoRepl, read_dep_dict=ReadDepDict, pending_txs=PendingTxs, local_updates=LocalParts,
            aborted=Aborted}) ->
    case start_from(PendingTxId, PendingList) of
        [] ->
            %lager:info("Not aborting anything"),
            {noreply, SD0};
        L ->
            %lager:info("Abort remote again, Pending list is ~w, PendingTxId is ~w, List is ~w", [PendingList, PendingTxId, L]),
            RD = abort_specula_list(L, RepDict, ReadDepDict, DoRepl, PendingTxs),
            %% The current transaction is aborted! So replying to client.
            case TxId of
                [] ->
                    {noreply, SD0#state{read_dep_dict=RD, tx_id=[], 
                        pending_list=lists:sublist(PendingList, length(PendingList)-length(L)),
                        aborted=Aborted+length(L)}};
                _ ->
                    RD1 = abort_tx(TxId, LocalParts, DoRepl, PendingTxs, RD),
                    gen_server:reply(Sender, {aborted, TxId}),
                    {noreply, SD0#state{read_dep_dict=RD1, tx_id=[], 
                        pending_list=lists:sublist(PendingList, length(PendingList)-length(L)),
                        aborted=Aborted+length(L)}}
            end
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

%%%%%%%%%%%%%%%%%%%%%
try_to_abort(PendingList, ToAbortTxs, ReadDepDict, RepDict, PendingTxs, DoRepl, ReadAborted) ->
    lager:info("Trying to abort ~w ~n", [ToAbortTxs]),
    case ToAbortTxs of
        [] ->
            %lager:info("Nothing to abort"),
            {ReadDepDict, PendingList, ReadAborted};
        _ ->
            MinIndex = find_min_index(ToAbortTxs, PendingList),
            FullLength = length(PendingList),
            lager:info("Min index is ~w, FulLength is ~w, ReadAborted is ~w ~n", [MinIndex, FullLength, ReadAborted]),
            L = lists:sublist(PendingList, MinIndex, FullLength),
            RD = abort_specula_list(L, RepDict, ReadDepDict, DoRepl, PendingTxs),
            {RD, lists:sublist(PendingList, MinIndex-1), ReadAborted+FullLength-MinIndex+1}
    end.

%% Same reason, no need for RemoteParts
commit_tx(TxId, CommitTime, LocalParts, DoRepl, PendingTxs) ->
    true = ets:delete(PendingTxs, TxId),
    lager:info("Sending commit of ~w to ~w", [TxId, LocalParts]),
    ?CLOCKSI_VNODE:commit(LocalParts, TxId, CommitTime),
    ?REPL_FSM:repl_commit(LocalParts, TxId, CommitTime, DoRepl).

commit_specula_tx(TxId, CommitTime, LocalParts, RemoteParts, DoRepl, PendingTxs, RepDict) ->
    commit_tx(TxId, CommitTime, LocalParts, DoRepl, PendingTxs),
    lager:info("Sending specula commit to ~w", [RemoteParts]),
    ?CLOCKSI_VNODE:commit(RemoteParts, TxId, CommitTime),
    ?REPL_FSM:repl_commit(RemoteParts, TxId, CommitTime, DoRepl, true),
    %io:format(user, "Calling commit to table with ~w, ~w, ~w ~n", [RemoteParts, TxId, CommitTime]),
    commit_to_table(RemoteParts, TxId, CommitTime, RepDict).

abort_specula_tx(TxId, LocalParts, RemoteParts, DoRepl, PendingTxs, RepDict, ReadDepDict) ->
    NewReadDepDict = abort_tx(TxId, LocalParts, DoRepl, PendingTxs, ReadDepDict),
    abort_to_table(RemoteParts, TxId, RepDict),
    ?CLOCKSI_VNODE:abort(RemoteParts, TxId),
    ?REPL_FSM:repl_abort(RemoteParts, TxId, DoRepl, true),
    NewReadDepDict.

%% Do not need remote partitions, because abort_tx can only be called before a transaction sends out its
%% remote prepare. If a txn has already sent its prepare request, it would be in specula state already,
%% which will be aborted with specula_abrot 
abort_tx(TxId, LocalParts, DoRepl, PendingTxs, ReadDepDict) ->
    true = ets:delete(PendingTxs, TxId),
    %lager:info("Deleting ~w", [TxId]),
    ?CLOCKSI_VNODE:abort(LocalParts, TxId),
    ?REPL_FSM:repl_abort(LocalParts, TxId, DoRepl),
    dict:erase(TxId, ReadDepDict).

add_to_table([], _, _, _) ->
    ok;
add_to_table([{{Partition, Node}, Updates}|Rest], TxId, PrepareTime, RepDict) ->
    case dict:find(Node, RepDict) of
        {ok, DataReplServ} ->
            ?DATA_REPL_SERV:prepare_specula(DataReplServ, TxId, Partition, Updates, PrepareTime);
        _ ->
            %% Send to local cache.
            ?CACHE_SERV:prepare_specula(TxId, Partition, Updates, PrepareTime)
    end,
    add_to_table(Rest, TxId, PrepareTime, RepDict).

commit_to_table([], _, _, _) ->
    ok;
commit_to_table([{Partition, Node}|Rest], TxId, CommitTime, RepDict) ->
    %io:format(user, "Part is ~w, Node is ~w, Rest is ~w ~n", [Partition, Node, Rest]),
    lager:info("Committing to table for ~w", [Partition]),
    case dict:find(Node, RepDict) of
        {ok, DataReplServ} ->
            lager:info("Sending to ~w", [DataReplServ]),
            ?DATA_REPL_SERV:commit_specula(DataReplServ, TxId, Partition, CommitTime);
        _ ->
            %% Send to local cache.
            ?CACHE_SERV:commit_specula(TxId, Partition, CommitTime)
    end,
    commit_to_table(Rest, TxId, CommitTime, RepDict).

abort_to_table([], _, _) ->
    ok;
abort_to_table([{Partition, Node}|Rest], TxId, RepDict) ->
    case dict:find(Node, RepDict) of
        {ok, DataReplServ} ->
            ?DATA_REPL_SERV:abort_specula(DataReplServ, TxId, Partition);
        _ ->
            %% Send to local cache.
            ?CACHE_SERV:abort_specula(TxId, Partition)
    end,
    abort_to_table(Rest, TxId, RepDict).

%% Deal dependencies and check if any following transactions can be committed.
try_to_commit(LastCommitTime, [], _RepDict, ReadDepDict, _DoRepl, _PendingTxs, ReadAborted, Committed, _) ->
    {[], LastCommitTime, ReadDepDict, ReadAborted, Committed};
try_to_commit(LastCommitTime, [H|Rest]=PendingList, RepDict, ReadDepDict, 
                        DoRepl, PendingTxs, ReadAborted, Committed, {ReadOk, PrepareOk}) ->
    lager:info("Checking if can commit tx ~w", [H]),
    Result =
            case (ReadOk == true) or (dict:find(H, ReadDepDict) == error) of
                true ->
                    lager:info("Txn has no read dependency"),
                    %% The next txn has no dependency
                    case PrepareOk of
                        false ->
                            case ets:lookup(PendingTxs, H) of
                                %% The next txn has got all prepare request
                                [{H, {0, PendingMaxPT, LPs, RPs}}] ->
                                    {true, max(PendingMaxPT, LastCommitTime+1), LPs, RPs};
                                R ->
                                    lager:warning("Not fully prepared.. ~w", [R]),
                                    false
                            end;
                        {PrepareTime, LPs, RPs} ->
                                {true, max(PrepareTime, LastCommitTime+1), LPs, RPs}
                    end;
                false ->
                    lager:info("Txn has read dependency!! WTF? ~w", [dict:find(H, ReadDepDict)]),
                    false
            end,
    lager:info("If can commit decision for ~w is ~w", [H, Result]),
    case Result of
        false ->
            {PendingList, LastCommitTime, ReadDepDict, ReadAborted, Committed};
        {true, CommitTime, LocalParts, RemoteParts} ->
            DepList = ets:lookup(dependency, H),
            lager:info("~w: My read dependncy are ~w", [H, DepList]),
            {ReadDepDict1, ToAbortTxs} = solve_read_dependency(CommitTime, ReadDepDict, DepList),
            lager:info("Before commit specula tx, ToAbortTs are ~w", [ToAbortTxs]),
            commit_specula_tx(H, CommitTime, LocalParts, RemoteParts, DoRepl,
               PendingTxs, RepDict),
            {ReadDepDict2, PendingList1, ReadAborted1}
                = try_to_abort(Rest, ToAbortTxs, ReadDepDict1, RepDict, PendingTxs, DoRepl, ReadAborted),
            try_to_commit(CommitTime, PendingList1, RepDict, ReadDepDict2, DoRepl, PendingTxs, ReadAborted1, Committed+1,
                {false, false})
    end.

abort_specula_list([], _RepDict, ReadDep, _, _) ->
    ReadDep;
abort_specula_list([H|T], RepDict, ReadDep, DoRepl, PendingTxs) ->
    lager:info("Trying to abort ~w", [H]),
    [{H, {_, _, LocalParts, RemoteParts}}] = ets:lookup(PendingTxs, H), 
    ReadDep1 = abort_specula_tx(H, LocalParts, RemoteParts, DoRepl, PendingTxs, RepDict, ReadDep),
    abort_specula_list(T, RepDict, ReadDep1, DoRepl, PendingTxs). 

%% Deal with reads that depend on this txn
solve_read_dependency(CommitTime, ReadDep, DepList) ->
    %io:format(user, "My commit time is ~w, DepList is ~w ~n", [CommitTime, DepList]),
    Self = self(),
    lists:foldl(fun({TxId, DepTxId}, {RD, ToAbort}) ->
                    TxServer = DepTxId#tx_id.server_pid,
                    ets:delete_object(dependency, {TxId, DepTxId}), 
                    case DepTxId#tx_id.snapshot_time >= CommitTime of
                        %% This read is still valid
                        true ->
                            lager:info("Read still valid for ~w", [DepTxId]),
                            case TxServer of
                                Self ->
                                    lager:info("~w is my own, read valid", [DepTxId]),
                                    case dict:find(DepTxId, RD) of
                                        {ok, 1} ->
                                            {dict:erase(DepTxId, RD), ToAbort};
                                        {ok, N} ->
                                            {dict:store(DepTxId, N-1, RD), ToAbort};
                                        error -> %% This txn hasn't even started certifying...
                                            {RD, ToAbort}
                                    end;
                                _ ->
                                    lager:info("~w is not my own, read valid", [DepTxId]),
                                    ?READ_VALID(TxServer, DepTxId),
                                    {RD, ToAbort}
                            end;
                        false ->
                            %% Read is not valid
                            case TxServer of
                                Self ->
                                    lager:info("~w is my own, read invalid", [DepTxId]),
                                    {RD, [DepTxId|ToAbort]};
                                _ ->
                                    lager:info("~w is not my own, read invalid", [DepTxId]),
                                    ?READ_INVALID(TxServer, CommitTime, DepTxId),
                                    {RD, ToAbort}
                            end
                    end
                end, {ReadDep, []}, DepList).
    
start_from(_, []) ->
    [];
start_from(H, [H|T])->
    [H|T];
start_from(H, [_|T]) ->
    start_from(H, T).

find_min_index(FirstList, FullList) ->
    Set = sets:from_list(FirstList),
    {_, MinIndex} = lists:foldl(fun(E, {Index, Num}) ->
                case sets:is_element(E, Set) of
                    true ->  {Index+1, min(Index, Num)};
                    false -> {Index+1, Num}
                end end, {1, 1}, FullList),
    MinIndex.

-ifdef(TEST).

%add_to_table_test() ->
%    MyTable = ets:new(whatever, [private, set]),
%    Updates= [{node1, [{key1, 1}, {key2, 2}]}, {node2, [{key3, 2}]}],
%    TxId1 = tx_utilities:create_tx_id(0),
%    add_to_table(Updates, TxId1, MyTable),
%    ?assertEqual([{key1, 1, TxId1}], ets:lookup(MyTable, key1)),
%    ?assertEqual([{key2, 2, TxId1}], ets:lookup(MyTable, key2)),
%    ?assertEqual([{key3, 2, TxId1}], ets:lookup(MyTable, key3)),
%    TxId2 = tx_utilities:create_tx_id(0),
%    Updates2 =  [{node1, [{key1, 2}]}, {node3, [{key4, 4}]}],
%    add_to_table(Updates2, TxId2, MyTable),
%    ?assertEqual([{key1, 2, TxId2}], ets:lookup(MyTable, key1)),
%    ?assertEqual([{key2, 2, TxId1}], ets:lookup(MyTable, key2)),
%    ?assertEqual([{key3, 2, TxId1}], ets:lookup(MyTable, key3)),
%    ?assertEqual([{key4, 4, TxId2}], ets:lookup(MyTable, key4)),
%    true = ets:delete(MyTable).

%remove_from_table_test() ->
%    MyTable = ets:new(whatever, [private, set]),
%    Updates= [{node1, [{key1, 1}, {key2, 2}]}, {node2, [{key3, 2}]}],
%    TxId1 = tx_utilities:create_tx_id(0),
%    add_to_table(Updates, TxId1, MyTable),
%    TxId2 = tx_utilities:create_tx_id(0),
%    remove_from_table(Updates, TxId2, MyTable),
%    ?assertEqual([{key1, 1, TxId1}], ets:lookup(MyTable, key1)),
%    ?assertEqual([{key2, 2, TxId1}], ets:lookup(MyTable, key2)),
%    ?assertEqual([{key3, 2, TxId1}], ets:lookup(MyTable, key3)),
%    remove_from_table(Updates, TxId1, MyTable),
%    ?assertEqual([], ets:lookup(MyTable, key1)),
%    ?assertEqual([], ets:lookup(MyTable, key2)),
%    ?assertEqual([], ets:lookup(MyTable, key3)),
%    true = ets:delete(MyTable).

start_from_test() ->
    L = [1,2,3,4,5,6],
    ?assertEqual([1,2,3,4,5,6], start_from(1, L)),
    ?assertEqual([3,4,5,6], start_from(3, L)),
    ?assertEqual([6], start_from(6, L)),
    ?assertEqual([], start_from(7, L)).

abort_tx_test() ->
    MyTable = ets:new(whatever, [private, set]),
    ReadDepDict = dict:new(),
    mock_partition_fsm:start_link(),
    LocalParts = [lp1, lp2],
    TxId1 = tx_utilities:create_tx_id(0),
    ets:insert(MyTable, {TxId1, whatever}),
    ReadDepDict1 = dict:store(TxId1, 5, ReadDepDict),
    ReadDepDict2 = abort_tx(TxId1, LocalParts, true, MyTable, ReadDepDict1),
    ?assertEqual([], ets:lookup(MyTable, TxId1)),
    ?assertEqual(dict:find(TxId1, ReadDepDict2), error), %{abort, T1, [p1]}, nothing
    ?assertEqual(true, mock_partition_fsm:if_applied({abort, TxId1, LocalParts}, nothing)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_abort, TxId1, LocalParts}, nothing)),
    true = ets:delete(MyTable).

commit_tx_test() ->
    MyTable = ets:new(whatever, [private, set]),
    LocalParts = [lp1, lp2],
    TxId1 = tx_utilities:create_tx_id(0),
    ets:insert(MyTable, {TxId1, whatever}),
    CT = 10000,
    commit_tx(TxId1, CT, LocalParts, true, MyTable),
    ?assertEqual([], ets:lookup(MyTable, TxId1)),
    ?assertEqual(true, mock_partition_fsm:if_applied({commit, TxId1, LocalParts}, CT)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_commit, TxId1, LocalParts}, CT)),
    ets:delete(MyTable).

abort_specula_list_test() ->
    MyTable = ets:new(whatever, [private, set]),
    ets:new(dependency, [public, bag, named_table]),
    ReadDepDict = dict:new(),
    RepDict0 = dict:store(n1, n1rep, dict:new()),
    RepDict = dict:store(n2, n2rep, RepDict0),
    T1 = tx_utilities:create_tx_id(0),
    T2 = tx_utilities:create_tx_id(0),
    T3 = tx_utilities:create_tx_id(0),
    ReadDepDict1 = dict:store(T1, 3, ReadDepDict),
    ReadDepDict2 = dict:store(T2, 1, ReadDepDict1),
    ReadDepDict3 = dict:store(T3, 0, ReadDepDict2),
    ets:insert(MyTable, {T1, {1, 1, [{p1, n1}], [{p2, n2}]}}),
    ets:insert(MyTable, {T2, {1, 1, [{p1, n1}], [{p2, n2}]}}),
    ets:insert(MyTable, {T3, {1, 1, [{p1, n1}], [{p2, n2}]}}),
    ReadDepDict4 = abort_specula_list([T1, T2, T3], RepDict, ReadDepDict3, true, MyTable),
    ?assertEqual(error, dict:find(T1, ReadDepDict4)),
    ?assertEqual(error, dict:find(T2, ReadDepDict4)),
    ?assertEqual(error, dict:find(T3, ReadDepDict4)),
    ?assertEqual([], ets:lookup(MyTable, T1)),
    ?assertEqual([], ets:lookup(MyTable, T2)),
    ?assertEqual([], ets:lookup(MyTable, T3)),
    ?assertEqual(true, mock_partition_fsm:if_applied({abort, T1, [{p1, n1}]}, nothing)),
    ?assertEqual(true, mock_partition_fsm:if_applied({abort, T1, [{p2, n2}]}, nothing)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_abort, T1, [{p1, n1}]}, nothing)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_abort, T1, [{p2, n2}]}, nothing)),
    ?assertEqual(true, mock_partition_fsm:if_applied({abort, T2, [{p1, n1}]}, nothing)),
    ?assertEqual(true, mock_partition_fsm:if_applied({abort, T2, [{p2, n2}]}, nothing)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_abort, T2, [{p1, n1}]}, nothing)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_abort, T2, [{p2, n2}]}, nothing)),
    ets:delete(MyTable).

solve_read_dependency_test() ->
    T1 = tx_utilities:create_tx_id(0), 
    T2 = T1#tx_id{server_pid=test_fsm}, 
    CommitTime = tx_utilities:now_microsec(),
    T3 = tx_utilities:create_tx_id(0),
    T4 = T3#tx_id{server_pid=test_fsm}, 
    %% T1, T2 should be aborted, T3, T4 should get read_valid.
    ReadDepDict = dict:new(),
    ReadDepDict1 = dict:store(T3, 2, ReadDepDict),
    {RD2, T} = solve_read_dependency(CommitTime, ReadDepDict1, [{nil, T1}, {nil, T2}, {nil, T3}, {nil, T4}]),
    ?assertEqual({[{T3,1}], [T1]}, {dict:to_list(RD2), T}),
    ?assertEqual(true, mock_partition_fsm:if_applied({read_valid, T4}, nothing)),
    ?assertEqual(true, mock_partition_fsm:if_applied({read_invalid, T2}, nothing)).

try_to_commit_test() ->
    MyTable = ets:new(whatever, [private, set]),
    T1 = tx_utilities:create_tx_id(0),
    %CommitTime1 = tx_utilities:now_microsec(),
    T2 = tx_utilities:create_tx_id(0),
    MaxPT1 = T2#tx_id.snapshot_time+1,
    %CommitTime2 = tx_utilities:now_microsec(),
    T3 = tx_utilities:create_tx_id(0),
    T4 = tx_utilities:create_tx_id(0),
    T5 = tx_utilities:create_tx_id(0),
    %MaxPT2 = tx_utilities:now_microsec(),
    RepDict00 = dict:store(n1, n1rep, dict:new()),
    RepDict01 = dict:store(n2, n2rep, RepDict00),
    RepDict = dict:store(n3, n3rep, RepDict01),
    ReadDepDict1 = dict:store(T1, 1, dict:new()),
    ReadDepDict2 = dict:store(T3, 2, ReadDepDict1),
    %% T1 can not commit due to having read dependency 
    io:format(user, "Abort ~w ~n", [MaxPT1]),
    {PD1, MPT1, RD1, Aborted, Committed} =  try_to_commit(MaxPT1, [T1, T2], RepDict, ReadDepDict2, 
                                    true, MyTable, 0, 0, {false, false}),
    ?assertEqual(Aborted, 0),
    ?assertEqual(Committed, 0),
    ?assertEqual(RD1, ReadDepDict2),
    ?assertEqual(PD1, [T1, T2]),
    ?assertEqual(MPT1, MaxPT1),

    ets:insert(MyTable, {T1, {0, 2, [{lp1, n1}], [{rp1, n3}]}}), 
    ets:insert(MyTable, {T2, {1, 2, [{lp1, n1}], [{rp1, n3}]}}), 
    ets:insert(MyTable, {T3, {0, 2, [{lp2, n2}], [{rp2, n4}]}}), 
    ReadDepDict3 = dict:erase(T1, ReadDepDict2),
    %% T1 can commit because of read_dep ok. 
    {PD2, MPT2, RD2, Aborted1, Committed1} =  try_to_commit(MaxPT1, [T1, T2], RepDict, ReadDepDict3, 
                                    true, MyTable, 0, 0, {true, false}),
    ?assertEqual(Aborted1, 0),
    ?assertEqual(Committed1, 1),
    ?assertEqual(true, mock_partition_fsm:if_applied({commit_specula, n3rep, T1, rp1}, MPT2)),
    ?assertEqual(dict:to_list(RD2), dict:to_list(ReadDepDict3)),
    ?assertEqual(PD2, [T2]),
    ?assertEqual(MPT2, MaxPT1+1),

    %% T2 can commit becuase of prepare OK
    {_PD3, MPT3, _RD3, Aborted2, Committed2} =  try_to_commit(MPT2, [T2, T3], RepDict, ReadDepDict3, 
                                    true, MyTable, Aborted1, Committed1, {false, {MPT2+10, [{lp1, n1}], [{rp1, n3}]}}),
    ?assertEqual([], ets:lookup(MyTable, T2)),
    ?assertEqual(Aborted2, 0),
    ?assertEqual(Committed2, 2),
    %?assertEqual(true, mock_partition_fsm:if_applied({repl_commit, TxId1, LocalParts}, CT)),
    ?assertEqual(true, mock_partition_fsm:if_applied({commit, T2, [{lp1, n1}]}, MPT3)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_commit, T2, [{rp1, n3}]}, MPT3)),
    ?assertEqual(true, mock_partition_fsm:if_applied({commit_specula, n3rep, T2, rp1}, MPT3)),

    %% T3, T4 get committed and T5 gets aborted.
    ets:insert(MyTable, {T3, {0, T3#tx_id.snapshot_time+1, [{lp2, n2}], [{rp2, n4}]}}), 
    ets:insert(MyTable, {T4, {0, T5#tx_id.snapshot_time+1, [{lp2, n2}], [{rp2, n4}]}}), 
    ets:insert(MyTable, {T5, {0, 6, [{lp2, n2}], [{rp2, n4}]}}), 
    ets:insert(dependency, {T3, T4}),
    ets:insert(dependency, {T3, T5}),
    ets:insert(dependency, {T4, T5}),
    ReadDepDict4 = dict:store(T4, 1, ReadDepDict3),
    ReadDepDict5 = dict:store(T5, 2, ReadDepDict4),
    ReadDepDict6 = dict:erase(T3, ReadDepDict5),
    io:format(user, "My commit time is ~w, Txns are ~w ~w ~w", [0, T3, T4, T5]),
    {PD4, MPT4, RD4, Aborted3, Committed3} =  try_to_commit(0, [T3, T4, T5], RepDict, ReadDepDict6, 
                                    true, MyTable, Aborted2, Committed2, {false, false}),
    ?assertEqual(Aborted3, 1),
    ?assertEqual(Committed3, 4),
    ?assertEqual(RD4, dict:new()),
    ?assertEqual(PD4, []),
    ?assertEqual(MPT4, T5#tx_id.snapshot_time+1),
    ?assertEqual(ets:lookup(MyTable, T4),  []), 
    ?assertEqual(ets:lookup(MyTable, T5),  []),

    ?assertEqual(true, mock_partition_fsm:if_applied({commit_specula, cache_serv, T3, rp2}, T3#tx_id.snapshot_time+1)),
    ?assertEqual(true, mock_partition_fsm:if_applied({commit_specula, cache_serv, T4, rp2}, MPT4)),
    ?assertEqual(true, mock_partition_fsm:if_applied({abort_specula, cache_serv, T5, rp2}, nothing)),
    ?assertEqual(true, mock_partition_fsm:if_applied({commit, T4, [{lp2, n2}]}, MPT4)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_commit, T4, [{rp2, n4}]}, MPT4)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_abort, T5, [{rp2, n4}]}, nothing )).
    
-endif.
