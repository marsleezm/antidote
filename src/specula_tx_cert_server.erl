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

-define(NO_TXN, nil).
-define(FLOW_ABORT, -1).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(CLOCKSI_VNODE, mock_partition_fsm).
-define(REPL_FSM, mock_partition_fsm).
-define(SPECULA_TX_CERT_SERVER, mock_partition_fsm).
-define(CACHE_SERV, mock_partition_fsm).
-define(DATA_REPL_SERV, mock_partition_fsm).
-define(READ_VALID(SEND, RTXID, WTXID), mock_partition_fsm:read_valid(SEND, RTXID, WTXID)).
-define(READ_INVALID(SEND, CT, TXID), mock_partition_fsm:read_invalid(SEND, CT, TXID)).
-else.
-define(CLOCKSI_VNODE, clocksi_vnode).
-define(REPL_FSM, repl_fsm).
-define(SPECULA_TX_CERT_SERVER, specula_tx_cert_server).
-define(CACHE_SERV, cache_serv).
-define(DATA_REPL_SERV, data_repl_serv).
-define(READ_VALID(SEND, RTXID, WTXID), gen_server:cast(SEND, {read_valid, RTXID, WTXID})).
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

-record(state, {
        tx_id :: txid(),
        %prepare_time = 0 :: non_neg_integer(),
        min_commit_ts = 0 :: non_neg_integer(),
        min_snapshot_ts = 0 :: non_neg_integer(),
        local_updates :: [],
        remote_updates :: [],
        rep_dict :: dict(),
        dep_dict :: dict(),
        pending_list=[] :: [txid()],
        invalid_ts :: non_neg_integer(),

        stage :: read|local_cert|remote_cert,
        %specula_data :: cache_id(),
        pending_txs :: cache_id(),
        do_repl :: boolean(),
        read_aborted=0 :: non_neg_integer(),
        committed=0 :: non_neg_integer(),
        num_specula_read=0 :: non_neg_integer(),
        aborted=0 :: non_neg_integer(),
        speculated=0 :: non_neg_integer(),
        specula_length :: non_neg_integer(),
        sender :: term()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name) ->
   lager:warning("Specula tx cert started wit name ~w", [Name]),
    gen_server:start_link({global, Name},
             ?MODULE, [], []).

%%%===================================================================
%%% Internal
%%%===================================================================

init([]) ->
    %PendingMetadata = tx_utilities:open_private_table(pending_metadata),
    lager:info("Specula tx starts with ~p", [self()]),
    DoRepl = antidote_config:get(do_repl),
    SpeculaLength = antidote_config:get(specula_length),
    RepDict = case DoRepl of
                true ->
                    Lists = antidote_config:get(to_repl),
                    [RepNodes] = [Reps || {Node, Reps} <- Lists, Node == node()],
                    lists:foldl(fun(N, D) -> dict:store(N, 
                        list_to_atom(atom_to_list(node())++"repl"++atom_to_list(N)), D) end, dict:new(), RepNodes);
                false ->
                    dict:new()
    end,
    PendingTxs = tx_utilities:open_private_table(pending_txs), 
    %SpeculaData = tx_utilities:open_private_table(specula_data),
    {ok, #state{pending_txs=PendingTxs, rep_dict=RepDict, dep_dict=dict:new(), invalid_ts=0,
            do_repl=DoRepl, specula_length=SpeculaLength}}.

handle_call({single_commit, Node, Key, Value}, Sender, SD0) ->
    clocksi_vnode:single_commit(Node,[{Key, Value}], Sender),
    {noreply, SD0};

handle_call({start_read_tx}, _Sender, SD0) ->
    TxId = tx_utilities:create_tx_id(0),
    {reply, TxId, SD0};

handle_call({start_tx}, _Sender, SD0=#state{dep_dict=D, min_snapshot_ts=MinSnapshotTS}) ->
    TxId = tx_utilities:create_tx_id(MinSnapshotTS+1),
   lager:warning("Starting txid ~w", [TxId]),
    D1 = dict:store(TxId, {0, [], 0}, D),
    {reply, TxId, SD0#state{tx_id=TxId, invalid_ts=0, dep_dict=D1, stage=read, min_snapshot_ts=MinSnapshotTS+1}};

handle_call({get_stat}, _Sender, SD0=#state{aborted=Aborted, committed=Committed, read_aborted=ReadAborted, 
        speculated=Speculated, num_specula_read=NumSpeculaRead}) ->
   lager:warning("Num of read aborted ~w, Num of aborted is ~w, Num of committed is ~w, Speculated ~w, NumSpeculaRead is ~w", [ReadAborted, Aborted, Committed, Speculated, NumSpeculaRead]),
    {reply, {ReadAborted, Aborted, Committed, Speculated, NumSpeculaRead}, SD0};

handle_call({set_internal_data, Type, Param}, _Sender, SD0)->
    case Type of
        last_commit_time ->
           lager:warning("Set lct to ~w", [Param]),
            {noreply, SD0#state{min_commit_ts=Param}}
    end;

handle_call({get_internal_data, Type, Param}, _Sender, SD0=#state{pending_list=PendingList, 
        pending_txs=PendingTxs, dep_dict=DepDict, min_commit_ts=LastCommitTs,
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
        read_dep ->
            case dict:find(Param, DepDict) of
                {ok, {Prep, ReadDep, _}} ->
                    lager:info("Prep ~w, Read dep ~w for ~w", [Prep,ReadDep, Param]),
                    {reply, ReadDep, SD0};
                error ->
                    {reply, error, SD0}
            end;
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

handle_call({certify, TxId, LocalUpdates, RemoteUpdates},  Sender, SD0=#state{rep_dict=RepDict, aborted=Aborted, 
            pending_txs=PendingTxs, min_commit_ts=LastCommitTs, tx_id=TxId, invalid_ts=InvalidTs, specula_length=SpeculaLength, 
            pending_list=PendingList, speculated=Speculated, dep_dict=DepDict, num_specula_read=NumSpeculaRead}) ->
    %RemoteKeys = lists:map(fun({Node, Ups}) -> {Node, [Key || {Key, _} <- Ups]} end, RemoteUpdates),
    %% If there was a legacy ongoing transaction.
   lager:warning("Got req: txid ~w, localUpdates ~p, remote updates ~p", [TxId, LocalUpdates, RemoteUpdates]),
    ReadDepTxs = [T2  || {_, T2} <- ets:lookup(anti_dep, TxId)],
    true = ets:delete(anti_dep, TxId),
    %OldReadDeps = dict:find(TxId, DepDict),
   lager:warning("Start certifying ~w, readDepTxs is ~w", [TxId, ReadDepTxs]),
    case InvalidTs == 0 of
        false -> 
            %% Some read is invalid even before the txn starts.. If invalid_ts is larger than 0, it can possibly be saved.
           lager:warning("Aborting txn due to invalidTS ~w!", [TxId]),
            {reply, {aborted, TxId}, SD0#state{tx_id=?NO_TXN, dep_dict=dict:erase(TxId, DepDict), aborted=Aborted+1}};
        true ->
            case length(LocalUpdates) of
                0 ->
                    RemotePartitions = [P || {P, _} <- RemoteUpdates],
                    DepDict1 = dict:update(TxId, 
                        fun({_, B, _}) -> lager:warning("Previous readdep is ~w", [B]),
                            {length(RemotePartitions), ReadDepTxs--B, LastCommitTs+1} end, DepDict),
                    NumSpeculaRead1 = case ReadDepTxs of [] -> NumSpeculaRead;
                                                        _ -> NumSpeculaRead+1
                                      end, 
                   lager:warning("Trying to prepare ~w", [TxId]),
                    ?CLOCKSI_VNODE:prepare(RemoteUpdates, TxId, {remote, node()}),
                    case length(PendingList) >= SpeculaLength of
                        true -> %% Wait instead of speculate.
                            {noreply, SD0#state{tx_id=TxId, dep_dict=DepDict1, local_updates=[], num_specula_read=NumSpeculaRead1, 
                                        remote_updates=RemoteUpdates, sender=Sender, stage=remote_cert}};
                        false -> %% Can speculate. After replying, removing TxId
                           lager:warning("Speculating directly for ~w", [TxId]),
                            gen_server:reply(Sender, {ok, {specula_commit, LastCommitTs}}),
                            %% Update specula data structure, and clean the txid so we know current txn is already replied
                            add_to_table(RemoteUpdates, TxId, LastCommitTs+1, RepDict),
                            PendingList1 = PendingList ++ [TxId],
                            ets:insert(PendingTxs, {TxId, {[], RemotePartitions}}),
                            {noreply, SD0#state{tx_id=?NO_TXN, speculated=Speculated+1, dep_dict=DepDict1, min_snapshot_ts=LastCommitTs+1,
                                    pending_list=PendingList1, num_specula_read=NumSpeculaRead1}}
                    end;
                _ ->
                   lager:warning("Local updates are ~w", [LocalUpdates]),
                    LocalPartitions = [P || {P, _} <- LocalUpdates],
                    DepDict1 = dict:update(TxId, 
                            fun({_, B, _}) ->  lager:warning("Previous readdep is ~w", [B]),
                             {length(LocalUpdates), ReadDepTxs--B, LastCommitTs+1} end, DepDict),
                   lager:warning("Prepare ~w", [TxId]),
                    ?CLOCKSI_VNODE:prepare(LocalUpdates, TxId, local),
                    NumSpeculaRead1 = case ReadDepTxs of [] -> NumSpeculaRead;
                                                        _ -> NumSpeculaRead+1
                                      end,
                    {noreply, SD0#state{tx_id=TxId, dep_dict=DepDict1, sender=Sender, local_updates=LocalPartitions,
                        remote_updates=RemoteUpdates, stage=local_cert, num_specula_read=NumSpeculaRead1}}
            end
    end;

handle_call({certify, TxId, _, _},  _Sender, SD0=#state{tx_id=?NO_TXN, dep_dict=DepDict}) ->
    lager:error("Current tx is aborted"),
    {reply, {aborted, TxId}, SD0#state{tx_id=?NO_TXN, dep_dict=dict:erase(TxId, DepDict)}};

handle_call({read, Key, TxId, Node}, Sender, SD0) ->
    ?CLOCKSI_VNODE:relay_read(Node, Key, TxId, Sender, specula),
    {noreply, SD0};

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

%% Receiving local prepare. Can only receive local prepare for two kinds of transaction
%%  Current transaction that has not finished local cert phase
%%  Transaction that has already aborted.
handle_cast({prepared, TxId, PrepareTime, local}, 
	    SD0=#state{tx_id=TxId, local_updates=LocalParts, do_repl=DoRepl, 
            remote_updates=RemoteUpdates, sender=Sender, dep_dict=DepDict, pending_list=PendingList,
             speculated=Speculated, min_commit_ts=LastCommitTs, specula_length=SpeculaLength,
            pending_txs=PendingTxs, rep_dict=RepDict}) ->
   lager:warning("Got local prepare for ~w", [TxId]),
    case dict:find(TxId, DepDict) of
        %% Maybe can commit already.
        {ok, {1, ReadDepTxs, OldPrepTime}} ->
            NewMaxPrep = max(PrepareTime, OldPrepTime),
            RemoteParts = [P || {P, _} <- RemoteUpdates],
            RemoteToAck = length(RemoteParts),
            case (RemoteToAck == 0) and (ReadDepTxs == [])
                  and (PendingList == []) of
                true ->
                    CommitTime = max(NewMaxPrep, LastCommitTs+1),
                   lager:warning("Committing ~w with ~w", [TxId, CommitTime]),
                    %% TODO: remember to remove entry of tx in dict and ets
                    true = ets:delete(PendingTxs, TxId),
                    {DepDict1, _} = commit_tx(TxId, NewMaxPrep, LocalParts, [], DoRepl,
                                    dict:erase(TxId, DepDict)),
                    gen_server:reply(Sender, {ok, {committed, NewMaxPrep}}),
                    {noreply, SD0#state{min_commit_ts=CommitTime, tx_id=?NO_TXN, pending_list=[], min_snapshot_ts=CommitTime, 
                        dep_dict=DepDict1}};
                false ->
                    case length(PendingList) >= SpeculaLength of
                        true -> 
                            %%In wait stage, only prepare and doesn't add data to table
                           lager:warning("Decided to wait and prepare ~w!!", [TxId]),
                            ?CLOCKSI_VNODE:prepare(RemoteUpdates, TxId, {remote, node()}),
                            DepDict1 = dict:store(TxId, {RemoteToAck, ReadDepTxs, NewMaxPrep}, DepDict),
                            {noreply, SD0#state{dep_dict=DepDict1, stage=remote_cert}};
                        false ->
                           lager:warning("Speculate current tx with ~w, remote parts are ~w, Num is ~w", [TxId, RemoteParts, length(RemoteParts)]),
                            ?CLOCKSI_VNODE:prepare(RemoteUpdates, TxId, {remote, node()}),
                            %% Add dependent data into the table
                            gen_server:reply(Sender, {ok, {specula_commit, NewMaxPrep}}),
                            ets:insert(PendingTxs, {TxId, {LocalParts, RemoteParts}}),
                            add_to_table(RemoteUpdates, TxId, NewMaxPrep, RepDict),
                            DepDict1 = dict:store(TxId, {RemoteToAck, ReadDepTxs, NewMaxPrep}, DepDict),
                            {noreply, SD0#state{tx_id=?NO_TXN, speculated=Speculated+1, dep_dict=DepDict1, 
                                pending_list=PendingList++[TxId], min_snapshot_ts=NewMaxPrep}}
                    end
                end;
        {ok, {N, ReadDeps, OldPrepTime}} ->
           lager:warning("~w needs ~w local prep replies", [TxId, N-1]),
            DepDict1 = dict:store(TxId, {N-1, ReadDeps, max(PrepareTime, OldPrepTime)}, DepDict),
            {noreply, SD0#state{dep_dict=DepDict1}};
        error ->
            {noreply, SD0}
    end;

handle_cast({prepared, OtherTxId, _, local}, SD0) ->
   lager:warning("Received prepared for ~w that has aborted!", [OtherTxId]),
    {noreply, SD0}; 

%% TODO: if we don't direclty speculate after getting all local prepared, maybe we can wait a littler more
%%       and here we should check if the transaction can be directly committed or not. 
handle_cast({read_valid, PendingTxId, PendedTxId}, SD0=#state{pending_txs=PendingTxs, rep_dict=RepDict, dep_dict=DepDict, 
            pending_list=PendingList, do_repl=DoRepl, read_aborted=ReadAborted, tx_id=CurrentTxId, committed=Committed,
            min_commit_ts=MinCommitTS, sender=Sender, stage=Stage, speculated=Speculated, 
            local_updates=LocalParts, remote_updates=RemoteUpdates}) ->
   lager:warning("Got read valid for ~w of ~w", [PendingTxId, PendedTxId]),
    case dict:find(PendingTxId, DepDict) of
        {ok, {0, SolvedReadDeps, 0}} -> 
            %% Txn is still reading!!
           lager:warning("Inserting read valid for ~w, old deps are ~w",[PendingTxId, SolvedReadDeps]),
            {noreply, SD0#state{dep_dict=dict:store(PendingTxId, {0, [PendedTxId|SolvedReadDeps], 0}, DepDict)}};
        {ok, {0, [PendedTxId], OldPrepTime}} -> %% Maybe the transaction can commit 
           lager:warning("Trying to see if I am the first"),
            case PendingList of
                 [] ->
                   lager:warning("Committing current txn... Current txn id ~w should be the same as  pending id~w", [CurrentTxId, PendingTxId]),
                    CurCommitTime = max(MinCommitTS+1, OldPrepTime),
                    RemoteParts = [P||{P, _} <-RemoteUpdates],
                    DepDict2 = dict:erase(PendingTxId, DepDict),
                    true = ets:delete(PendingTxs, PendingTxId),
                    {DepDict3, _} = commit_specula_tx(PendingTxId, CurCommitTime, LocalParts, RemoteParts, DoRepl, 
                        DepDict2, RepDict),
                    gen_server:reply(Sender, {ok, {committed, CurCommitTime}}),
                    {noreply, SD0#state{min_commit_ts=CurCommitTime, tx_id=?NO_TXN, min_snapshot_ts=CurCommitTime,
                         pending_list=[], committed=Committed+1, dep_dict=DepDict3}};
                 [PendingTxId|_] ->
                    DepDict1 = dict:store(PendingTxId, {0,[], OldPrepTime}, DepDict),
                    {NewPendingList, NewMaxPT, DepDict2, NewReadAborted, NewCommitted} = try_to_commit(
                            MinCommitTS, PendingList, RepDict, DepDict1, DoRepl, PendingTxs, 
                            ReadAborted, Committed),
                    %% Only needs local parts and remote parts when need to commit/abort/specula_commit,
                    %% so identify when needs to abort.
                    case decide_after_cascade(NewPendingList, DepDict2, NewReadAborted-ReadAborted, CurrentTxId, Stage) of
                        invalid -> %% Should be aborted later
                           lager:warning("Tnx invalid"),
                            {noreply, SD0#state{pending_list=NewPendingList, dep_dict=DepDict2, invalid_ts=?FLOW_ABORT,
                                  min_commit_ts=NewMaxPT, read_aborted=NewReadAborted, min_snapshot_ts=NewMaxPT,
                                    committed=NewCommitted}};
                        abort_local ->
                           lager:warning("Abort local Txn"),
                            abort_tx(CurrentTxId, LocalParts, [], DoRepl, PendingTxs),
                            DepDict3 = dict:erase(CurrentTxId, DepDict2),
                            gen_server:reply(Sender, {aborted, CurrentTxId}),
                            {noreply, SD0#state{
                              pending_list=NewPendingList, tx_id=?NO_TXN, dep_dict=DepDict3, min_snapshot_ts=NewMaxPT,
                                min_commit_ts=NewMaxPT, read_aborted=NewReadAborted, committed=NewCommitted}};
                        abort_remote ->
                           lager:warning("Abort remote Txn"),
                            RemoteParts = [P||{P, _} <-RemoteUpdates],
                            abort_tx(CurrentTxId, LocalParts, RemoteParts, DoRepl, PendingTxs),
                            DepDict3 = dict:erase(CurrentTxId, DepDict2),
                            gen_server:reply(Sender, {aborted, CurrentTxId}),
                            {noreply, SD0#state{
                              pending_list=NewPendingList, tx_id=?NO_TXN, dep_dict=DepDict3, min_snapshot_ts=NewMaxPT,
                                min_commit_ts=NewMaxPT, read_aborted=NewReadAborted, committed=NewCommitted}};
                        wait ->
                           lager:warning("Wait "),
                            {noreply, SD0#state{pending_list=NewPendingList, dep_dict=DepDict2, min_snapshot_ts=NewMaxPT,
                                  min_commit_ts=NewMaxPT, read_aborted=NewReadAborted, committed=NewCommitted}};
                        {commit, OldCurPrepTime} ->
                           lager:warning("Commit local txn"),
                            CurCommitTime = max(NewMaxPT+1, OldCurPrepTime),
                            RemoteParts = [P||{P, _} <-RemoteUpdates],
                            true = ets:delete(PendingTxs, CurrentTxId),
                            {DepDict3, _} = commit_tx(CurrentTxId, CurCommitTime, LocalParts, RemoteParts, DoRepl,
                                dict:erase(CurrentTxId, DepDict2)),
                            gen_server:reply(Sender, {ok, {committed, CurCommitTime}}),
                            {noreply, SD0#state{min_commit_ts=CurCommitTime, tx_id=?NO_TXN, read_aborted=NewReadAborted, 
                                pending_list=[], committed=NewCommitted, min_snapshot_ts=CurCommitTime, dep_dict=DepDict3}};
                        specula -> %% Can not commit, but can specualte (due to the number of specula txns decreased)
                           lager:warning("Specula current txn ~w", [CurrentTxId]),
                            {ok, {Prep, Read, OldCurPrepTime}} = dict:find(CurrentTxId, DepDict2), 
                            SpeculaPrepTime = max(NewMaxPT+1, OldCurPrepTime),
                            DepDict3=dict:store(CurrentTxId, {Prep, Read, SpeculaPrepTime}, DepDict2),
                            gen_server:reply(Sender, {ok, {specula_commit, SpeculaPrepTime}}),
                            add_to_table(RemoteUpdates, CurrentTxId, SpeculaPrepTime, RepDict),
                            PendingList1 = NewPendingList ++ [CurrentTxId],
                            RemoteParts = [P||{P, _} <-RemoteUpdates],
                            ets:insert(PendingTxs, {CurrentTxId, {LocalParts, RemoteParts}}),
                            {noreply, SD0#state{tx_id=?NO_TXN, speculated=Speculated+1, dep_dict=DepDict3,
                                    pending_list=PendingList1, min_commit_ts=NewMaxPT, min_snapshot_ts=SpeculaPrepTime,
                                    read_aborted=NewReadAborted, committed=NewCommitted}}
                    end;
                _ ->
                    DepDict1 = dict:store(PendingTxId, {0, [], OldPrepTime}, DepDict),
                   lager:warning("got all replies, but I am not the first!"),
                    {noreply, SD0#state{dep_dict=DepDict1}}
            end;
        {ok, {PrepDeps, ReadDepTxs, OldPrepTime}} -> 
           lager:warning("Can not commit... Remaining prepdep is ~w, read dep is ~w", [PrepDeps, lists:delete(PendedTxId, ReadDepTxs)]),
            {noreply, SD0#state{dep_dict=dict:store(PendingTxId, {PrepDeps, lists:delete(PendedTxId, ReadDepTxs), 
                OldPrepTime}, DepDict)}};
        error ->
           lager:warning("WTF, no info?"),
            {noreply, SD0}
    end;

handle_cast({read_invalid, MyCommitTime, TxId}, SD0=#state{tx_id=CurrentTxId, sender=Sender, 
        do_repl=DoRepl, dep_dict=DepDict, pending_list=PendingList, aborted=Aborted, stage=Stage, 
        rep_dict=RepDict, local_updates=LocalParts, remote_updates=RemoteParts, pending_txs=PendingTxs,
        invalid_ts=InvalidTS}) ->
   lager:warning("Got read invalid for ~w", [TxId]),
    case start_from(TxId, PendingList) of
        [] ->
            case TxId of
                CurrentTxId ->
                    case Stage of
                        local_cert ->
                            abort_tx(TxId, LocalParts, [], DoRepl, PendingTxs),
                            RD1 = dict:erase(TxId, DepDict),
                            gen_server:reply(Sender, {aborted, TxId}),
                            {noreply, SD0#state{dep_dict=RD1, tx_id=?NO_TXN}};
                        remote_cert ->
                            abort_tx(TxId, LocalParts, RemoteParts, DoRepl, PendingTxs),
                            RD1 = dict:erase(TxId, DepDict),
                            gen_server:reply(Sender, {aborted, TxId}),
                            {noreply, SD0#state{dep_dict=RD1, tx_id=?NO_TXN}};
                        read ->
                            case (MyCommitTime == -1) or (InvalidTS == -1) of
                                true ->
                                    {noreply, SD0#state{invalid_ts=-1}};
                                _ ->
                                    {noreply, SD0#state{invalid_ts=max(MyCommitTime, InvalidTS)}}
                            end
                    end;
                _ ->
                    {noreply, SD0}
            end;
        L ->
            RD = abort_specula_list(L, RepDict, DepDict, DoRepl, PendingTxs),
           lager:warning("Abort due to read invalid, Pending list is ~w, PendingTxId is ~w, List is ~w", [PendingList, TxId, L]),
            case CurrentTxId of
                ?NO_TXN ->
                    {noreply, SD0#state{dep_dict=RD, 
                        pending_list=lists:sublist(PendingList, length(PendingList)-length(L)),
                        aborted=Aborted+length(L)}};
                _ ->
                   lager:warning("Current tx is ~w, Stage is ~w", [CurrentTxId, Stage]),
                    case Stage of
                        local_cert -> 
                            abort_tx(CurrentTxId, LocalParts, [], DoRepl, PendingTxs),
                            gen_server:reply(Sender, {aborted, CurrentTxId}),
                            RD1 = dict:erase(CurrentTxId, RD),
                            {noreply, SD0#state{dep_dict=RD1, tx_id=?NO_TXN, 
                                pending_list=lists:sublist(PendingList, length(PendingList)-length(L)),
                                aborted=Aborted+length(L)}};
                        remote_cert -> 
                            abort_tx(CurrentTxId, LocalParts, RemoteParts, DoRepl, PendingTxs),
                            gen_server:reply(Sender, {aborted, CurrentTxId}),
                            RD1 = dict:erase(CurrentTxId, RD),
                            {noreply, SD0#state{dep_dict=RD1, tx_id=?NO_TXN, 
                                pending_list=lists:sublist(PendingList, length(PendingList)-length(L)),
                                aborted=Aborted+length(L)}};
                        read ->
                            case (MyCommitTime == -1) or (InvalidTS == -1) of
                                true ->
                                    {noreply, SD0#state{dep_dict=RD, invalid_ts=-1,
                                        pending_list=lists:sublist(PendingList, length(PendingList)-length(L)),
                                        aborted=Aborted+length(L)}};
                                _ -> 
                                    {noreply, SD0#state{dep_dict=RD, invalid_ts=max(MyCommitTime, InvalidTS), 
                                        pending_list=lists:sublist(PendingList, length(PendingList)-length(L)),
                                        aborted=Aborted+length(L)}}
                            end
                    end
            end
    end;

handle_cast({abort, ReceivedTxId, local}, SD0=#state{tx_id=TxId, local_updates=LocalUpdates, 
        do_repl=DoRepl, sender=Sender, pending_txs=PendingTxs, dep_dict=DepDict}) ->
    case ReceivedTxId of
        TxId ->
           lager:warning("Received local abort for ~w", [TxId]),
            abort_tx(TxId, LocalUpdates, [], DoRepl, PendingTxs),
            DepDict1 = dict:erase(TxId, DepDict),
            gen_server:reply(Sender, {aborted, TxId}),
            {noreply, SD0#state{tx_id=?NO_TXN, dep_dict=DepDict1}};
        _ ->
            {noreply, SD0}
    end;
handle_cast({prepared, PendingTxId, PendingPT, remote}, 
	    SD0=#state{pending_list=PendingList, do_repl=DoRepl, pending_txs=PendingTxs,
            sender=Sender, tx_id=CurrentTxId, rep_dict=RepDict, min_commit_ts=MinCommitTS,
            committed=Committed, read_aborted=ReadAborted, remote_updates=RemoteUpdates,
            local_updates=LocalParts, dep_dict=DepDict, stage=Stage, speculated=Speculated}) ->
   lager:warning("Got remote prepare for ~w", [PendingTxId]),
    case dict:find(PendingTxId, DepDict) of
        {ok, {1, [], OldPrepTime}} -> %% Maybe the transaction can commit 
          lager:warning("Has all remote prep"),
            case PendingList of
                [] -> %% This is the just speculated txn.. But new txn has not come yet.
                   lager:warning("Pending list is ~w", [PendingList]),
                    CurCommitTime = max(MinCommitTS+1, max(OldPrepTime, PendingPT)),
                    RemoteParts = [P||{P, _} <-RemoteUpdates],
                    DepDict2 = dict:erase(PendingTxId, DepDict),
                    true = ets:delete(PendingTxs, PendingTxId),
                    {DepDict3, _} = commit_specula_tx(PendingTxId, CurCommitTime, LocalParts, RemoteParts, DoRepl,
                        DepDict2, RepDict),
                    gen_server:reply(Sender, {ok, {committed, CurCommitTime}}),
                    {noreply, SD0#state{min_commit_ts=CurCommitTime, tx_id=?NO_TXN, min_snapshot_ts=CurCommitTime,
                         pending_list=[], dep_dict=DepDict3}};
                [PendingTxId|_] ->
                    DepDict1 = dict:store(PendingTxId, {0, [], max(PendingPT, OldPrepTime)}, DepDict),
                   lager:warning("got all replies, Can try to commit"),
                    {NewPendingList, NewMaxPT, DepDict2, NewReadAborted, NewCommitted} = try_to_commit(
                            MinCommitTS, PendingList, RepDict, DepDict1, DoRepl, PendingTxs, 
                            ReadAborted, Committed),
                     %% Only needs local parts and remote parts when need to commit/abort/specula_commit,
                    %% so identify when needs to abort.
                    case decide_after_cascade(NewPendingList, DepDict2, NewReadAborted-ReadAborted, CurrentTxId, Stage) of
                        invalid -> %% Should be aborted later
                           lager:warning("Current tx invalid"),
                            {noreply, SD0#state{pending_list=NewPendingList, dep_dict=DepDict2, invalid_ts=?FLOW_ABORT,
                                  min_commit_ts=NewMaxPT, min_snapshot_ts=NewMaxPT,
                                    read_aborted=NewReadAborted, committed=NewCommitted}};
                        abort_local ->
                           lager:warning("Abort local txn"),
                            abort_tx(CurrentTxId, LocalParts, [], DoRepl, PendingTxs),
                            DepDict3 = dict:erase(CurrentTxId, DepDict2),
                            gen_server:reply(Sender, {aborted, CurrentTxId}),
                            {noreply, SD0#state{
                              pending_list=NewPendingList, tx_id=?NO_TXN, dep_dict=DepDict3,
                                min_commit_ts=NewMaxPT, min_snapshot_ts=NewMaxPT,
                                read_aborted=NewReadAborted, committed=NewCommitted}};
                        abort_remote ->
                           lager:warning("Abort remote txn"),
                            RemoteParts = [P||{P, _} <-RemoteUpdates],
                            abort_tx(CurrentTxId, LocalParts, RemoteParts, DoRepl, PendingTxs),
                            DepDict3 = dict:erase(CurrentTxId, DepDict2),
                            gen_server:reply(Sender, {aborted, CurrentTxId}),
                            {noreply, SD0#state{
                              pending_list=NewPendingList, tx_id=?NO_TXN, dep_dict=DepDict3,
                                min_commit_ts=NewMaxPT, min_snapshot_ts=NewMaxPT,
                                read_aborted=NewReadAborted, committed=NewCommitted}};
                        wait -> 
                           lager:warning("Local txn waits"),
                            {noreply, SD0#state{pending_list=NewPendingList, dep_dict=DepDict2, min_snapshot_ts=NewMaxPT,
                                    min_commit_ts=NewMaxPT, read_aborted=NewReadAborted, committed=NewCommitted}};
                        {commit, OldCurPrepTime} ->
                           lager:warning("Commit local txn"),
                            CurCommitTime = max(NewMaxPT+1, OldCurPrepTime),
                            RemoteParts = [P||{P, _} <-RemoteUpdates],
                            true = ets:delete(PendingTxs, CurrentTxId),
                            {DepDict3, _} = commit_tx(CurrentTxId, CurCommitTime, LocalParts, RemoteParts, DoRepl,
                                dict:erase(CurrentTxId, DepDict2)),
                            gen_server:reply(Sender, {ok, {committed, CurCommitTime}}),
                            {noreply, SD0#state{min_commit_ts=CurCommitTime, tx_id=?NO_TXN, read_aborted=NewReadAborted,
                                  pending_list=[], committed=NewCommitted, min_snapshot_ts=CurCommitTime, dep_dict=DepDict3}};
                        specula -> %% Can not commit, but can specualte (due to the number of specula txns decreased)
                           lager:warning("Specula current txn ~w", [CurrentTxId]),
                            {ok, {Prep, Read, OldCurPrepTime}} = dict:find(CurrentTxId, DepDict2),
                            SpeculaPrepTime = max(NewMaxPT+1, OldCurPrepTime),
                            DepDict3=dict:store(CurrentTxId, {Prep, Read, SpeculaPrepTime}, DepDict2),
                            gen_server:reply(Sender, {ok, {specula_commit, SpeculaPrepTime}}),
                            add_to_table(RemoteUpdates, CurrentTxId, SpeculaPrepTime, RepDict),
                            PendingList1 = NewPendingList ++ [CurrentTxId],
                            RemoteParts = [P||{P, _} <-RemoteUpdates],
                            ets:insert(PendingTxs, {CurrentTxId, {LocalParts, RemoteParts}}),
                            {noreply, SD0#state{tx_id=?NO_TXN, speculated=Speculated+1, dep_dict=DepDict3,
                                    pending_list=PendingList1, min_commit_ts=NewMaxPT, min_snapshot_ts=SpeculaPrepTime,
                                    read_aborted=NewReadAborted, committed=NewCommitted}}
                    end;
                _ ->
                    DepDict1 = dict:store(PendingTxId, {0, [], max(PendingPT, OldPrepTime)}, DepDict),
                    lager:warning("got all replies, but I am not the first! PendingList is ~w", [PendingList]),
                    {noreply, SD0#state{dep_dict=DepDict1}}
            end;
        {ok, {PrepDeps, ReadDeps, OldPrepTime}} -> %% Maybe the transaction can commit 
            lager:warning("Not enough.. Prep ~w, Read ~w, PrepTime ~w", [PrepDeps, ReadDeps, OldPrepTime]),
            DepDict1=dict:store(PendingTxId, {PrepDeps-1, ReadDeps, max(PendingPT, OldPrepTime)}, DepDict),
            {noreply, SD0#state{dep_dict=DepDict1}};
        error ->
            {noreply, SD0}
    end;
            
%% Aborting the current transaction
handle_cast({abort, TxId, {remote,_}}, 
	    SD0=#state{tx_id=TxId, pending_txs=PendingTxs,  do_repl=DoRepl, local_updates=LocalParts, 
            remote_updates=RemoteUpdates, dep_dict=DepDict, sender=Sender}) ->
   lager:warning("Aborting ~w remote", [TxId]),
    RemoteParts = [P || {P, _} <- RemoteUpdates],
    abort_tx(TxId, LocalParts, RemoteParts, DoRepl, PendingTxs),
    DepDict1 = dict:erase(PendingTxs, DepDict),
    gen_server:reply(Sender, {aborted, TxId}),
   lager:warning("Abort remote"),
    {noreply, SD0#state{tx_id=?NO_TXN, dep_dict=DepDict1}};
%% Not aborting current transaction
handle_cast({abort, PendingTxId, {remote, _}}, 
	    SD0=#state{pending_list=PendingList, rep_dict=RepDict, tx_id=CurrentTxId, sender=Sender,
            do_repl=DoRepl, dep_dict=DepDict, pending_txs=PendingTxs, stage=Stage,
            aborted=Aborted, local_updates=LocalParts, remote_updates=RemoteUpdates}) ->
   lager:warning("Aborting ~w remote, not current transaction", [PendingTxId]),
    case start_from(PendingTxId, PendingList) of
        [] ->
           lager:warning("Not aborting anything"),
            {noreply, SD0};
        L ->
           lager:warning("Abort remote again, Pending list is ~w, PendingTxId is ~w, List is ~w", [PendingList, PendingTxId, L]),
            DepDict1 = abort_specula_list(L, RepDict, DepDict, DoRepl, PendingTxs),
            case CurrentTxId of
                ?NO_TXN ->
                    {noreply, SD0#state{dep_dict=DepDict1, 
                        pending_list=lists:sublist(PendingList, length(PendingList)-length(L)),
                        aborted=Aborted+length(L)}};
                _ ->
                   lager:warning("Trying to abort local txn ~w", [CurrentTxId]),
                    RemoteParts = [P || {P, _} <- RemoteUpdates],
                    case Stage of
                        read ->
                            %% The current transaction is aborted! So replying to client.
                            {noreply, SD0#state{dep_dict=DepDict1, invalid_ts=?FLOW_ABORT, 
                                pending_list=lists:sublist(PendingList, length(PendingList)-length(L))}};
                        _ ->
                            abort_tx(CurrentTxId, LocalParts, RemoteParts, DoRepl, PendingTxs),
                            DepDict2 = dict:erase(CurrentTxId, DepDict1),
                            gen_server:reply(Sender, {aborted, CurrentTxId}),
                            %% The current transaction is aborted! So replying to client.
                            {noreply, SD0#state{dep_dict=DepDict2, tx_id=?NO_TXN, 
                                pending_list=lists:sublist(PendingList, length(PendingList)-length(L)),
                                aborted=Aborted+length(L)}}
                    end
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
    lager:error("Cert server terminated with ~w", [Reason]),
    ok.

%%%%%%%%%%%%%%%%%%%%%
decide_after_cascade(PendingList, DepDict, NumAborted, TxId, Stage) ->
   lager:warning("PendingList ~w, DepDict ~w, NumAborted ~w, TxId ~w, Stage ~w", [PendingList, DepDict, NumAborted, TxId, Stage]),
    case TxId of
        ?NO_TXN -> wait;
        _ -> 
            case NumAborted > 0 of
                true -> case Stage of read -> invalid; %% Abort due to flow dependency. 
                                     local_cert -> abort_local;
                                     remote_cert -> abort_remote
                        end;
                false ->
                    case dict:find(TxId, DepDict) of %% TODO: should give a invalid_ts 
                        error -> case Stage of read -> invalid;
                                             local_cert -> abort_local;
                                             remote_cert -> abort_remote
                                 end;
                        R ->
                            case Stage of 
                                remote_cert ->
                                    case PendingList of 
                                        [] -> 
                                            case R of            
                                                {ok, {0, [], PrepTime}} -> {commit, PrepTime};
                                                 _ -> specula
                                            end;
                                        _ -> specula 
                                    end;
                                %% Can not specula commit during read or local_cert stage
                                _ -> wait
                            end
                    end
            end
    end.

try_to_abort(PendingList, ToAbortTxs, DepDict, RepDict, PendingTxs, DoRepl, ReadAborted) ->
   lager:warning("Trying to abort ~w ~n", [ToAbortTxs]),
    MinIndex = find_min_index(ToAbortTxs, PendingList),
    FullLength = length(PendingList),
   lager:warning("Min index is ~w, FulLength is ~w, ReadAborted is ~w ~n", [MinIndex, FullLength, ReadAborted]),
    L = lists:sublist(PendingList, MinIndex, FullLength),
    RD = abort_specula_list(L, RepDict, DepDict, DoRepl, PendingTxs),
    %% Remove entries so that the current txn (which is not in pending list) knows that it should be aborted. 
    RD1 = lists:foldl(fun(T, D) -> dict:erase(T, D) end, RD, ToAbortTxs),
    {RD1, lists:sublist(PendingList, MinIndex-1), ReadAborted+FullLength-MinIndex+1}.

%% Same reason, no need for RemoteParts
commit_tx(TxId, CommitTime, LocalParts, RemoteParts, DoRepl, DepDict) ->
   lager:warning("Committing tx ~w", [TxId]),
    DepList = ets:lookup(dependency, TxId),
   lager:warning("~w: My read dependncy are ~w", [TxId, DepList]),
    {DepDict1, ToAbortTxs} = solve_read_dependency(CommitTime, DepDict, DepList),
   lager:warning("Commit ~w to ~w, ~w", [TxId, LocalParts, RemoteParts]),
    ?CLOCKSI_VNODE:commit(LocalParts, TxId, CommitTime),
    ?REPL_FSM:repl_commit(LocalParts, TxId, CommitTime, DoRepl),
    ?CLOCKSI_VNODE:commit(RemoteParts, TxId, CommitTime),
    ?REPL_FSM:repl_commit(RemoteParts, TxId, CommitTime, DoRepl, true),
    {DepDict1, ToAbortTxs}.

commit_specula_tx(TxId, CommitTime, LocalParts, RemoteParts, DoRepl, DepDict, RepDict) ->
   lager:warning("Committing specula tx ~w", [TxId]),
    DepList = ets:lookup(dependency, TxId),
   lager:warning("~w: My read dependncy are ~w", [TxId, DepList]),
    {DepDict1, ToAbortTxs} = solve_read_dependency(CommitTime, DepDict, DepList),

    ?CLOCKSI_VNODE:commit(LocalParts, TxId, CommitTime),
    ?REPL_FSM:repl_commit(LocalParts, TxId, CommitTime, DoRepl),
    ?CLOCKSI_VNODE:commit(RemoteParts, TxId, CommitTime),
    ?REPL_FSM:repl_commit(RemoteParts, TxId, CommitTime, DoRepl, true),
   lager:warning("Specula commit ~w to ~w, ~w", [TxId, LocalParts, RemoteParts]),
    %io:format(user, "Calling commit to table with ~w, ~w, ~w ~n", [RemoteParts, TxId, CommitTime]),
    commit_to_table(RemoteParts, TxId, CommitTime, RepDict),
    {DepDict1, ToAbortTxs}.

abort_specula_tx(TxId, LocalParts, RemoteParts, DoRepl, PendingTxs, RepDict, DepDict) ->
   lager:warning("Aborting specula tx ~w", [TxId]),
   lager:warning("Trying to abort specula ~w to ~w, ~w", [TxId, LocalParts, RemoteParts]),
    abort_tx(TxId, LocalParts, RemoteParts, DoRepl, PendingTxs),
    abort_to_table(RemoteParts, TxId, RepDict),
    dict:erase(TxId, DepDict).

abort_tx(TxId, LocalParts, RemoteParts, DoRepl, PendingTxs) ->
   lager:warning("Trying to abort ~w to ~w, ~w", [TxId, LocalParts, RemoteParts]),
    true = ets:delete(PendingTxs, TxId),
    DepList = ets:lookup(dependency, TxId),
   lager:warning("~w: My read dependncy are ~w", [TxId, DepList]),
    Self = self(),
    lists:foreach(fun({_, DepTxId}) ->
                            TxServer = DepTxId#tx_id.server_pid,
                            ets:delete_object(dependency, {TxId, DepTxId}),
                            case TxServer of
                                Self ->
                                    ok;
                                _ ->
                                   lager:warning("~w is not my own, read invalid", [DepTxId]),
                                    ?READ_INVALID(TxServer, -1, DepTxId)
                            end
                    end, DepList),
   lager:warning("Deleting ~w", [TxId]),
    ?CLOCKSI_VNODE:abort(LocalParts, TxId),
    ?REPL_FSM:repl_abort(LocalParts, TxId, DoRepl),
    ?CLOCKSI_VNODE:abort(RemoteParts, TxId),
    ?REPL_FSM:repl_abort(RemoteParts, TxId, DoRepl, true).

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
   lager:warning("Committing to table for ~w", [Partition]),
    case dict:find(Node, RepDict) of
        {ok, DataReplServ} ->
           lager:warning("Sending to ~w", [DataReplServ]),
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
try_to_commit(LastCommitTime, [], _RepDict, DepDict, _DoRepl, _PendingTxs, ReadAborted, Committed) ->
   lager:warning("Returning ~w ~w ~w ~w ~w", [[], LastCommitTime, DepDict, ReadAborted, Committed]),
    {[], LastCommitTime, DepDict, ReadAborted, Committed};
try_to_commit(LastCommitTime, [H|Rest]=PendingList, RepDict, DepDict, 
                        DoRepl, PendingTxs, ReadAborted, Committed) ->
   lager:warning("Checking if can commit tx ~w", [H]),
    Result = case dict:find(H, DepDict) of
                {ok, {0, [], PendingMaxPT}} ->
                    {true, max(PendingMaxPT, LastCommitTime+1)};
                _ ->
                    false
            end,
   lager:warning("If can commit decision for ~w is ~w", [H, Result]),
    case Result of
        false ->
           lager:warning("Returning ~w ~w ~w ~w ~w", [PendingList, LastCommitTime, DepDict, ReadAborted, Committed]),
            {PendingList, LastCommitTime, DepDict, ReadAborted, Committed};
        {true, CommitTime} ->
            [{H, {LocalParts, RemoteParts}}] = ets:lookup(PendingTxs, H),
            true = ets:delete(PendingTxs, H),
            {DepDict1, ToAbortTxs} = commit_specula_tx(H, CommitTime, LocalParts, RemoteParts, DoRepl,
               dict:erase(H, DepDict), RepDict),
           lager:warning("Before commit specula tx, ToAbortTs are ~w, dict is ~w", [ToAbortTxs, DepDict1]),
            case ToAbortTxs of
                [] ->
                    try_to_commit(CommitTime, Rest, RepDict, DepDict1, DoRepl, PendingTxs, ReadAborted, Committed+1);
                _ ->
                    {DepDict2, PendingList1, ReadAborted1}
                            = try_to_abort(Rest, ToAbortTxs, DepDict1, RepDict, PendingTxs, DoRepl, ReadAborted),
                    try_to_commit(CommitTime, PendingList1, RepDict, DepDict2, DoRepl, PendingTxs, ReadAborted1, Committed+1)
            end
    end.

abort_specula_list([], _RepDict, DepDict, _, _) ->
    DepDict;
abort_specula_list([H|T], RepDict, DepDict, DoRepl, PendingTxs) ->
   lager:warning("Trying to abort ~w", [H]),
    [{H, {LocalParts, RemoteParts}}] = ets:lookup(PendingTxs, H), 
    DepDict1 = abort_specula_tx(H, LocalParts, RemoteParts, DoRepl, PendingTxs, RepDict, DepDict),
    abort_specula_list(T, RepDict, DepDict1, DoRepl, PendingTxs). 

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
                           lager:warning("Read still valid for ~w", [DepTxId]),
                            case TxServer of
                                Self ->
                                   lager:warning("~w is my own, read valid", [DepTxId]),
                                    case dict:find(DepTxId, RD) of
                                        {ok, {0, _SolvedReadDeps, 0}} -> %% Local transaction is still reading
                                           lager:warning("Deleting {~w, ~w} from antidep", [DepTxId, TxId]),
                                            ets:delete_object(anti_dep, {DepTxId, TxId}), 
                                            {RD, ToAbort};
                                        {ok, {PrepDeps, ReadDeps, PrepTime}} ->
                                           lager:warning("Storing ~w for ~w", [lists:delete(TxId, ReadDeps), DepTxId]),
                                            {dict:store(DepTxId, {PrepDeps, lists:delete(TxId, ReadDeps), 
                                                PrepTime}, RD), ToAbort};
                                        error -> %% This txn hasn't even started certifying 
                                                 %% or has been aborted already
                                           lager:warning("This txn has not even started"),
                                            {RD, ToAbort}
                                    end;
                                _ ->
                                   lager:warning("~w is not my own, read valid", [DepTxId]),
                                    ?READ_VALID(TxServer, DepTxId, TxId),
                                    {RD, ToAbort}
                            end;
                        false ->
                            %% Read is not valid
                            case TxServer of
                                Self ->
                                   lager:warning("~w is my own, read invalid", [DepTxId]),
                                    {RD, [DepTxId|ToAbort]};
                                _ ->
                                   lager:warning("~w is not my own, read invalid", [DepTxId]),
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
    ets:new(dependency, [private, set, named_table]),
    mock_partition_fsm:start_link(),
    LocalParts = [lp1, lp2],
    TxId1 = tx_utilities:create_tx_id(0),
    ets:insert(MyTable, {TxId1, whatever}),
    abort_tx(TxId1, LocalParts, [], true, MyTable),
    ?assertEqual([], ets:lookup(MyTable, TxId1)),
    ?assertEqual(true, mock_partition_fsm:if_applied({abort, TxId1, LocalParts}, nothing)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_abort, TxId1, LocalParts}, nothing)),
    true = ets:delete(dependency),
    true = ets:delete(MyTable).

commit_tx_test() ->
    MyTable = ets:new(whatever, [private, set]),
    ets:new(dependency, [private, set, named_table]),
    LocalParts = [lp1, lp2],
    TxId1 = tx_utilities:create_tx_id(0),
    ets:insert(MyTable, {TxId1, whatever}),
    CT = 10000,
    commit_tx(TxId1, CT, LocalParts, [], true, dict:new()),
    ?assertEqual(true, mock_partition_fsm:if_applied({commit, TxId1, LocalParts}, CT)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_commit, TxId1, LocalParts}, CT)),
    ets:delete(dependency).

abort_specula_list_test() ->
    MyTable = ets:new(whatever, [private, set]),
    ets:new(dependency, [public, bag, named_table]),
    DepDict = dict:new(),
    RepDict0 = dict:store(n1, n1rep, dict:new()),
    RepDict = dict:store(n2, n2rep, RepDict0),
    T1 = tx_utilities:create_tx_id(0),
    T2 = tx_utilities:create_tx_id(0),
    T3 = tx_utilities:create_tx_id(0),
    DepDict1 = dict:store(T1, {3, 1, 1}, DepDict),
    DepDict2 = dict:store(T2, {1, 1, 1}, DepDict1),
    DepDict3 = dict:store(T3, {0, 1, 1}, DepDict2),
    ets:insert(MyTable, {T1, {[{p1, n1}], [{p2, n2}]}}),
    ets:insert(MyTable, {T2, {[{p1, n1}], [{p2, n2}]}}),
    ets:insert(MyTable, {T3, {[{p1, n1}], [{p2, n2}]}}),
    DepDict4 = abort_specula_list([T1, T2, T3], RepDict, DepDict3, true, MyTable),
    ?assertEqual(error, dict:find(T1, DepDict4)),
    ?assertEqual(error, dict:find(T2, DepDict4)),
    ?assertEqual(error, dict:find(T3, DepDict4)),
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
    T0 = tx_utilities:create_tx_id(0), 
    T1 = tx_utilities:create_tx_id(0), 
    T2 = T1#tx_id{server_pid=test_fsm}, 
    CommitTime = tx_utilities:now_microsec(),
    T3 = tx_utilities:create_tx_id(0),
    T4 = T3#tx_id{server_pid=test_fsm}, 
    %% T1, T2 should be aborted, T3, T4 should get read_valid.
    DepDict = dict:new(),
    DepDict1 = dict:store(T3, {2, [T0, T2,T1], 4}, DepDict),
    {RD2, T} = solve_read_dependency(CommitTime, DepDict1, [{T0, T1}, {T0, T2}, {T0, T3}, {T0, T4}]),
    ?assertEqual({[{T3,{2,[T2,T1],4}}], [T1]}, {dict:to_list(RD2), T}),
    ?assertEqual(true, mock_partition_fsm:if_applied({read_valid, T4}, T0)),
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
    DepDict1 = dict:store(T1, {1, [], 2}, dict:new()),
    DepDict2 = dict:store(T2, {2, [1,2], 2}, DepDict1),
    DepDict3 = dict:store(T3, {2, [], 2}, DepDict2),
    %% T1 can not commit due to having read dependency 
    io:format(user, "Abort ~w ~n", [MaxPT1]),
    {PD1, MPT1, RD1, Aborted, Committed} =  try_to_commit(MaxPT1, [T1, T2], RepDict, DepDict2, 
                                    true, MyTable, 0, 0),
    ?assertEqual(Aborted, 0),
    ?assertEqual(Committed, 0),
    ?assertEqual(RD1, DepDict2),
    ?assertEqual(PD1, [T1, T2]),
    ?assertEqual(MPT1, MaxPT1),

    ets:insert(MyTable, {T1, {[{lp1, n1}], [{rp1, n3}]}}), 
    ets:insert(MyTable, {T2, {[{lp1, n1}], [{rp1, n3}]}}), 
    ets:insert(MyTable, {T3, {[{lp2, n2}], [{rp2, n4}]}}), 
    DepDict4 = dict:store(T1, {0, [], 2}, DepDict3),
    %% T1 can commit because of read_dep ok. 
    {PD2, MPT2, RD2, Aborted1, Committed1} =  try_to_commit(MaxPT1, [T1, T2], RepDict, DepDict4, 
                                    true, MyTable, 0, 0),
    ?assertEqual(Aborted1, 0),
    ?assertEqual(Committed1, 1),
    ?assertEqual(true, mock_partition_fsm:if_applied({commit_specula, n3rep, T1, rp1}, MPT2)),
    io:format(user, "RD2 ~w, Dict4 ~w", [dict:to_list(RD2), dict:to_list(dict:erase(T1, DepDict4))]),
    ?assertEqual(RD2, dict:erase(T1, DepDict4)),
    ?assertEqual(PD2, [T2]),
    ?assertEqual(MPT2, MaxPT1+1),

    %% T2 can commit becuase of prepare OK
    DepDict5 = dict:store(T2, {0, [], 2}, RD2),
    {_PD3, MPT3, RD3, Aborted2, Committed2} =  try_to_commit(MPT2, [T2, T3], RepDict, DepDict5, 
                                    true, MyTable, Aborted1, Committed1),
    ?assertEqual([], ets:lookup(MyTable, T2)),
    ?assertEqual(Aborted2, 0),
    ?assertEqual(Committed2, 2),
    %?assertEqual(true, mock_partition_fsm:if_applied({repl_commit, TxId1, LocalParts}, CT)),
    ?assertEqual(true, mock_partition_fsm:if_applied({commit, T2, [{lp1, n1}]}, MPT3)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_commit, T2, [{rp1, n3}]}, MPT3)),
    ?assertEqual(true, mock_partition_fsm:if_applied({commit_specula, n3rep, T2, rp1}, MPT3)),

    %% T3, T4 get committed and T5 gets aborted.
    ets:insert(MyTable, {T3, {[{lp2, n2}], [{rp2, n4}]}}), 
    ets:insert(MyTable, {T4, {[{lp2, n2}], [{rp2, n4}]}}), 
    ets:insert(MyTable, {T5, {[{lp2, n2}], [{rp2, n4}]}}), 
    ets:insert(dependency, {T3, T4}),
    ets:insert(dependency, {T3, T5}),
    ets:insert(dependency, {T4, T5}),
    DepDict6 = dict:store(T3, {0, [], T3#tx_id.snapshot_time+1}, RD3),
    DepDict7 = dict:store(T4, {0, [T3], T5#tx_id.snapshot_time+1}, DepDict6),
    DepDict8 = dict:store(T5, {0, [T4,T5], 6}, DepDict7),
    io:format(user, "My commit time is ~w, Txns are ~w ~w ~w", [0, T3, T4, T5]),
    {PD4, MPT4, RD4, Aborted3, Committed3} =  try_to_commit(0, [T3, T4, T5], RepDict, DepDict8, 
                                    true, MyTable, Aborted2, Committed2),
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
