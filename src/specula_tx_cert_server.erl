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
-define(MAX, 9999).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(CLOCKSI_VNODE, mock_partition_fsm).
-define(REPL_FSM, mock_partition_fsm).
-define(SPECULA_TX_CERT_SERVER, mock_partition_fsm).
-define(CACHE_SERV, mock_partition_fsm).
-define(DATA_REPL_SERV, mock_partition_fsm).
-define(READ_VALID(SEND, RTXID, WTXID), mock_partition_fsm:read_valid(SEND, RTXID, WTXID)).
-define(READ_INVALID(SEND, CT, TXID), mock_partition_fsm:read_invalid(SEND, CT, TXID)).
-define(READ_ABORTED(SEND, CT, TXID), mock_partition_fsm:read_aborted(SEND, CT, TXID)).
-else.
-define(CLOCKSI_VNODE, clocksi_vnode).
-define(REPL_FSM, repl_fsm).
-define(SPECULA_TX_CERT_SERVER, specula_tx_cert_server).
-define(CACHE_SERV, cache_serv).
-define(DATA_REPL_SERV, data_repl_serv).
-define(READ_VALID(SEND, RTXID, WTXID), gen_server:cast(SEND, {read_valid, RTXID, WTXID})).
-define(READ_INVALID(SEND, CT, TXID), gen_server:cast(SEND, {read_invalid, CT, TXID})).
-define(READ_ABORTED(SEND, CT, TXID), gen_server:cast(SEND, {read_aborted, CT, TXID})).
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
        local_updates :: [],
        remote_updates :: [],
        rep_dict :: dict(),
        dep_dict :: dict(),
        pending_list=[] :: [txid()],
        lp_start :: non_neg_integer(),
        rp_start :: non_neg_integer(),
        specula_length :: non_neg_integer(),

        stage :: read|local_cert|remote_cert,
        %specula_data :: cache_id(),
        pending_txs :: cache_id(),
        total_repl_factor :: non_neg_integer(),

        specula_read :: term(),
        min_commit_ts = 0 :: non_neg_integer(),
        min_snapshot_ts = 0 :: non_neg_integer(),
        invalid_ts=0 :: non_neg_integer(),
        pending_prepares=0 :: non_neg_integer(),
        num_specula_read=0 :: non_neg_integer(),
        committed=0 :: non_neg_integer(),
        start_prepare=0 :: non_neg_integer(),
        txn_type :: term(),
        %cert_stat={0,0} :: {non_neg_integer(), non_neg_integer()},

        read_aborted=0 :: non_neg_integer(),
        read_invalid=0 :: non_neg_integer(),
        cert_aborted=0 :: non_neg_integer(),
        cascade_aborted=0 :: non_neg_integer(),

        sender :: term()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name) ->
   %lager:warning("Specula tx cert started wit name ~w, id is ~p", [Name, self()]),
    gen_server:start_link({local,Name},
             ?MODULE, [], []).

%%%===================================================================
%%% Internal
%%%===================================================================

init([]) ->
    %PendingMetadata = tx_utilities:open_private_table(pending_metadata),
    %DoRepl = antidote_config:get(do_repl),
    RepDict = hash_fun:build_rep_dict(true),
    SpeculaLength = antidote_config:get(specula_length),
    PendingTxs = tx_utilities:open_private_table(pending_txs), 
    SpeculaRead = case antidote_config:get(specula_read) of
                        specula -> specula; 
                        nospecula -> no_specula
                  end,
    ets:insert(PendingTxs, {{new_order, commit}, 0, 0, 0}),
    ets:insert(PendingTxs, {{new_order, abort}, 0, 0, 0}),
    ets:insert(PendingTxs, {{payment, commit}, 0, 0, 0}),
    ets:insert(PendingTxs, {{payment, abort}, 0, 0, 0}),
    ets:insert(PendingTxs, {{general, commit}, 0, 0, 0}),
    ets:insert(PendingTxs, {{general, abort}, 0, 0, 0}),
    [{_, Replicas}] = ets:lookup(meta_info, node()),
    TotalReplFactor = length(Replicas)+1,
   %lager:warning("TotalReplFactor is ~w", [TotalReplFactor]),
    %SpeculaData = tx_utilities:open_private_table(specula_data),
    {ok, #state{pending_txs=PendingTxs, dep_dict=dict:new(), total_repl_factor=TotalReplFactor, 
            specula_length=SpeculaLength, specula_read=SpeculaRead, rep_dict=RepDict}}.

handle_call({append_values, Node, KeyValues, CommitTime}, Sender, SD0) ->
    clocksi_vnode:append_values(Node, KeyValues, CommitTime, Sender),
    {noreply, SD0};

handle_call({get_pid}, _Sender, SD0) ->
    {reply, self(), SD0};

handle_call({start_read_tx}, _Sender, SD0) ->
    TxId = tx_utilities:create_tx_id(0),
    {reply, TxId, SD0};

handle_call({start_tx}, _Sender, SD0=#state{dep_dict=D, min_snapshot_ts=MinSnapshotTS, min_commit_ts=MinCommitTS}) ->
    NewSnapshotTS = max(MinSnapshotTS, MinCommitTS) + 1, 
    TxId = tx_utilities:create_tx_id(NewSnapshotTS),
   %lager:warning("Starting txid ~w, MinSnapshotTS is ~w", [TxId, MinSnapshotTS+1]),
    D1 = dict:store(TxId, {0, [], 0}, D),
    {reply, TxId, SD0#state{tx_id=TxId, invalid_ts=0, dep_dict=D1, stage=read, min_snapshot_ts=NewSnapshotTS, pending_prepares=0}};

handle_call({get_stat}, _Sender, SD0=#state{cert_aborted=CertAborted, committed=Committed, read_aborted=ReadAborted, cascade_aborted=CascadeAborted, num_specula_read=NumSpeculaRead, pending_txs=PendingTxs, read_invalid=ReadInvalid}) ->
   %lager:warning("Num of read cert_aborted ~w, Num of cert_aborted is ~w, Num of committed is ~w, NumSpeculaRead is ~w", [ReadAborted, CascadeAborted, Committed, NumSpeculaRead]),
    [{{new_order, commit}, T11, T12, C1}] = ets:lookup(PendingTxs, {new_order, commit}),
    [{{new_order, abort}, T21, T22, C2}] = ets:lookup(PendingTxs, {new_order, abort}),
    [{{payment, commit}, T31, T32, C3}] = ets:lookup(PendingTxs, {payment, commit}),
    [{{payment, abort}, T41, T42, C4}] = ets:lookup(PendingTxs, {payment, abort}),
    [{{general, commit}, T51, T52, C5}] = ets:lookup(PendingTxs, {general, commit}),
    [{{general, abort}, T61, T62, C6}] = ets:lookup(PendingTxs, {general, abort}),
    A11 = T11 div max(1, C1), 
    A12 = T12 div max(1, C1), 
    C11 = T21 div max(1, C2),
    C12 = T22 div max(1, C2),
    A21 = T31 div max(1, C3), 
    A22 = T32 div max(1, C3), 
    C21 = T41 div max(1, C4),
    C22 = T42 div max(1, C4),
    A31 = T51 div max(1, C5), 
    A32 = T52 div max(1, C5), 
    C31 = T61 div max(1, C6),
    C32 = T62 div max(1, C6),
    AvgT = 0,
    {reply, [ReadAborted, ReadInvalid, CertAborted, CascadeAborted, Committed, 0, NumSpeculaRead, AvgT, A11, A12, C11, C12, A21, A22, C21, C22, A31, A32, C31, C32], SD0};

handle_call({set_int_data, Type, Param}, _Sender, SD0)->
    case Type of
        last_commit_time ->
           %lager:warning("Set lct to ~w", [Param]),
            {noreply, SD0#state{min_commit_ts=Param}}
    end;

handle_call({get_int_data, Type, Param}, _Sender, SD0=#state{pending_list=PendingList, 
        pending_txs=PendingTxs, dep_dict=DepDict, min_commit_ts=LastCommitTs, read_invalid=ReadInvalid, cascade_aborted=CascadeAborted,
        committed=Committed, cert_aborted=CertAborted, read_aborted=ReadAborted})->
    case Type of
        pending_list ->
            {reply, PendingList, SD0};
        pending_tab ->
            {reply, ets:tab2list(PendingTxs), SD0};
        dep_dict ->
            {reply, dict:to_list(DepDict), SD0};
        specula_time ->
            [{{general, abort}, T1, C1}] = ets:lookup(PendingTxs, {general, abort}),
            [{{general, commit}, T2, C2}] = ets:lookup(PendingTxs, {general, commit}),
            {reply, {T1 div max(1,C1), T2 div max(1, C2)}, SD0};
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
        cert_aborted ->
            {reply, CertAborted, SD0};
        read_invalid ->
            {reply, ReadInvalid, SD0};
        read_aborted ->
            {reply, ReadAborted, SD0};
        cascade_aborted ->
            {reply, CascadeAborted, SD0}
    end;

handle_call({certify, TxId, LocalUpdates, RemoteUpdates},  Sender, SD0) ->
    handle_call({certify, TxId, LocalUpdates, RemoteUpdates, general},  Sender, SD0);
handle_call({certify, TxId, LocalUpdates, RemoteUpdates, TxnType},  Sender, SD0=#state{rep_dict=RepDict, read_aborted=ReadAborted, read_invalid=ReadInvalid, total_repl_factor=TotalReplFactor, committed=Committed, pending_txs=PendingTxs, min_commit_ts=LastCommitTs, tx_id=TxId,  invalid_ts=InvalidTs, specula_length=SpeculaLength, pending_list=PendingList, dep_dict=DepDict, num_specula_read=NumSpeculaRead}) ->
    %% If there was a legacy ongoing transaction.
    ReadDepTxs = [T2  || {_, T2} <- ets:lookup(anti_dep, TxId)],
    true = ets:delete(anti_dep, TxId),
    %OldReadDeps = dict:find(TxId, DepDict),
   %lager:warning("Start certifying ~w, readDepTxs is ~w", [TxId, ReadDepTxs]),
    case InvalidTs  of
        0 ->
            case LocalUpdates of
                [] ->
                    RemotePartitions = [P || {P, _} <- RemoteUpdates],
                    case RemotePartitions of
                        [] -> %% Is read-only transaction!!
                            case ReadDepTxs of 
                                [] -> gen_server:reply(Sender, {ok, {committed, LastCommitTs}}),
                                      DepDict1 = dict:erase(TxId, DepDict),
                                      {noreply, SD0#state{dep_dict=DepDict1, committed=Committed+1, 
                                                  tx_id=?NO_TXN}}; 
                                _ ->  gen_server:reply(Sender, {ok, {specula_commit, LastCommitTs}}),
                                      DepDict1 = dict:update(TxId, fun({_, B, _}) -> 
                                        {read_only, ReadDepTxs--B, LastCommitTs} end, DepDict),
                                      {noreply, SD0#state{dep_dict=DepDict1, tx_id=?NO_TXN}}
                            end;
                        _ ->
                            NumSpeculaRead1 = case ReadDepTxs of [] -> NumSpeculaRead;
                                                                _ -> NumSpeculaRead+1
                                              end, 
                             %lager:warning("Trying to prepare ~w", [TxId]),
                            Now = os:timestamp(),
                            case length(PendingList) >= SpeculaLength of
                                true -> %% Wait instead of speculate.
                                    ?CLOCKSI_VNODE:prepare(RemoteUpdates, TxId, {remote, ignore}),
                                    DepDict1 = dict:update(TxId, 
                                         fun({_, B, _}) ->  %lager:warning("Previous readdep is ~w", [B]),
                                            {TotalReplFactor*length(RemotePartitions), ReadDepTxs--B, LastCommitTs+1} end, DepDict),
                                    {noreply, SD0#state{tx_id=TxId, dep_dict=DepDict1, local_updates=[], lp_start=Now, rp_start=Now, num_specula_read=NumSpeculaRead1, remote_updates=RemoteUpdates, sender=Sender, stage=remote_cert}};
                                false -> %% Can speculate. After replying, removing TxId
                                     %lager:warning("Speculating directly for ~w", [TxId]),
                                    %% Update specula data structure, and clean the txid so we know current txn is already replied
                                    {ProposeTS, AvoidNum} = add_to_table(RemoteUpdates, TxId, LastCommitTs, RepDict),
                                   %lager:warning("Proposets for ~w is ~w", [ProposeTS, TxId]),
                                    ?CLOCKSI_VNODE:prepare(RemoteUpdates, ProposeTS, TxId, {remote, node()}),
                                    gen_server:reply(Sender, {ok, {specula_commit, ProposeTS}}),
                                    PendingList1 = PendingList ++ [TxId],
                                    DepDict1 = dict:update(TxId, 
                                         fun({_, B, _}) ->  %lager:warning("Previous readdep is ~w", [B]),
                                            {TotalReplFactor*length(RemotePartitions)-AvoidNum, ReadDepTxs--B, LastCommitTs+1} end, DepDict),
                                    ets:insert(PendingTxs, {TxId, {[], RemotePartitions, Now, Now, TxnType}}),
                                    {noreply, SD0#state{tx_id=?NO_TXN, dep_dict=DepDict1, min_snapshot_ts=ProposeTS,
                                            pending_list=PendingList1, num_specula_read=NumSpeculaRead1}}
                            end
                        end;
                _ ->
                    LocalPartitions = [P || {P, _} <- LocalUpdates],
                    NumToAck = length(LocalUpdates),
                    DepDict1 = dict:update(TxId, 
                            fun({_, B, _}) ->
                             {NumToAck, ReadDepTxs--B, LastCommitTs+1} end, DepDict),
                   %lager:warning("Prepare ~w to ~w", [TxId, LocalPartitions]),
                    ?CLOCKSI_VNODE:prepare(LocalUpdates, TxId, local),
                    NumSpeculaRead1 = case ReadDepTxs of [] -> NumSpeculaRead;
                                                        _ -> NumSpeculaRead+1
                                      end,
                   %lager:warning("Total repl fact is ~w", [TotalReplFactor]),
                   %lager:warning("Total to ack is ~w", [(TotalReplFactor-1)*NumToAck]),
                    {noreply, SD0#state{tx_id=TxId, dep_dict=DepDict1, sender=Sender, local_updates=LocalPartitions, txn_type=TxnType,
                        remote_updates=RemoteUpdates, lp_start=os:timestamp(), pending_prepares=(TotalReplFactor-1)*NumToAck, stage=local_cert, num_specula_read=NumSpeculaRead1}}
            end;
        -1 -> 
            %% Some read is invalid even before the txn starts.. If invalid_ts is larger than 0, it can possibly be saved.
           %lager:warning("Aborting txn due to invalidTS ~w!", [TxId]),
            %lager:warning("~w Read aborted! ReadAborted is now ~w", [TxId, ReadAborted+1]),
            {reply, {aborted, TxId}, SD0#state{tx_id=?NO_TXN, dep_dict=dict:erase(TxId, DepDict), read_aborted=ReadAborted+1}};
        _ ->
            %lager:warning("~w Read invalid! ReadInvalid is now ~w", [TxId, ReadInvalid+1]),
            {reply, {aborted, TxId}, SD0#state{tx_id=?NO_TXN, dep_dict=dict:erase(TxId, DepDict), read_invalid=ReadInvalid+1}}
    end;

%handle_call({certify, TxId, _, _},  _Sender, SD0=#state{tx_id=?NO_TXN, dep_dict=DepDict}) ->
%    lager:error("Current tx is cert_aborted, received certify of ~w", [TxId]),
%    {reply, {aborted, TxId}, SD0#state{tx_id=?NO_TXN, dep_dict=dict:erase(TxId, DepDict)}};

handle_call({read, Key, TxId, Node}, Sender, SD0=#state{specula_read=SpeculaRead}) ->
    ?CLOCKSI_VNODE:relay_read(Node, Key, TxId, Sender, SpeculaRead),
    {noreply, SD0};

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

handle_cast({load, Sup, Type, Param}, SD0) ->
    lager:info("Got load req!"),
    case Type of tpcc -> tpcc_load:load(Param);
                 micro -> micro_load:load(Param)
    end,
    lager:info("Finished loading!"),
    Sup ! done,
    lager:info("Replied!"),
    {noreply, SD0};

handle_cast({clean_data, Sender}, #state{pending_txs=OldPendingTxs}) ->
    ets:delete(OldPendingTxs), 
    %DoRepl = antidote_config:get(do_repl),
    RepDict = hash_fun:build_rep_dict(true),
    SpeculaLength = antidote_config:get(specula_length),
    PendingTxs = tx_utilities:open_private_table(pending_txs),
    SpeculaRead = case antidote_config:get(specula_read) of
                        specula -> specula;
                        nospecula -> no_specula
                  end,
    ets:insert(PendingTxs, {{new_order, commit}, 0, 0, 0}),
    ets:insert(PendingTxs, {{new_order, abort}, 0, 0, 0}),
    ets:insert(PendingTxs, {{payment, commit}, 0, 0, 0}),
    ets:insert(PendingTxs, {{payment, abort}, 0, 0, 0}),
    ets:insert(PendingTxs, {{general, commit}, 0, 0, 0}),
    ets:insert(PendingTxs, {{general, abort}, 0, 0, 0}),
    [{_, Replicas}] = ets:lookup(meta_info, node()),
    TotalReplFactor = length(Replicas)+1,
    %SpeculaData = tx_utilities:open_private_table(specula_data),
    Sender ! cleaned,
    {noreply, #state{pending_txs=PendingTxs, dep_dict=dict:new(), 
            specula_length=SpeculaLength, specula_read=SpeculaRead, rep_dict=RepDict, total_repl_factor=TotalReplFactor}};

%% Receiving local prepare. Can only receive local prepare for two kinds of transaction
%%  Current transaction that has not finished local cert phase
%%  Transaction that has already cert_aborted.
handle_cast({pending_prepared, TxId, PrepareTime}, 
	    SD0=#state{tx_id=TxId, local_updates=LocalParts, remote_updates=RemoteUpdates, sender=Sender, txn_type=TxnType, 
                dep_dict=DepDict, pending_list=PendingList, specula_length=SpeculaLength, total_repl_factor=TotalReplFactor, 
                pending_prepares=PendingPrepares, pending_txs=PendingTxs, rep_dict=RepDict, lp_start=LT}) ->  
   %lager:warning("Speculative receive pending_prepared for ~w, current pp is ~w", [TxId, PendingPrepares+1]),
    case dict:find(TxId, DepDict) of
          %% Maybe can commit already.
        {ok, {1, ReadDepTxs, OldPrepTime}} ->
            NewMaxPrep = max(PrepareTime, OldPrepTime),
            RemoteParts = [P || {P, _} <- RemoteUpdates],
            RemoteToAck = length(RemoteParts),
            case length(PendingList) >= SpeculaLength of
                true ->
                     %lager:warning("Pedning prep: decided to wait and prepare ~w, pending list is ~w!!", [TxId, PendingList]),
                    ?CLOCKSI_VNODE:prepare(RemoteUpdates, TxId, {remote, ignore}),
                    DepDict1 = dict:store(TxId, {RemoteToAck*TotalReplFactor+PendingPrepares+1, ReadDepTxs, NewMaxPrep}, DepDict),
                    {noreply, SD0#state{dep_dict=DepDict1, rp_start=os:timestamp(), stage=remote_cert}};
                false ->
                    %lager:warning("Pending prep: decided to speculate ~w and prepare to ~w pending list is ~w!!", [TxId, RemoteUpdates, PendingList]),
                    %% Add dependent data into the table
                    ets:insert(PendingTxs, {TxId, {LocalParts, RemoteParts, LT, os:timestamp(), TxnType}}),
                    {ProposeTS, AvoidNum} = add_to_table(RemoteUpdates, TxId, NewMaxPrep, RepDict),
                   %lager:warning("Proposets for ~w is ~w", [ProposeTS, TxId]),
                    ?CLOCKSI_VNODE:prepare(RemoteUpdates, ProposeTS, TxId, {remote, node()}),
                    gen_server:reply(Sender, {ok, {specula_commit, NewMaxPrep}}),
                    DepDict1 = dict:store(TxId, {TotalReplFactor*RemoteToAck+PendingPrepares+1-AvoidNum, ReadDepTxs, NewMaxPrep}, DepDict),
                    {noreply, SD0#state{tx_id=?NO_TXN, dep_dict=DepDict1, pending_list=PendingList++[TxId]}}
            end;
        {ok, {N, ReadDeps, OldPrepTime}} ->
             %lager:warning("~w needs ~w local prep replies", [TxId, N-1]),
            DepDict1 = dict:store(TxId, {N-1, ReadDeps, max(PrepareTime, OldPrepTime)}, DepDict),
            {noreply, SD0#state{dep_dict=DepDict1, pending_prepares=PendingPrepares+1}};
        error ->
            {noreply, SD0}
    end;
handle_cast({pending_prepared, _OtherTxId, _}, SD0) ->
    {noreply, SD0}; 

%handle_cast({real_prepared, TxId, _}, SD0=#state{pending_prepares=PendingPrepares, tx_id=TxId, stage=local_cert}) ->
%     %lager:warning("Real prepare in local cert for ~w", [TxId]),
%    {noreply, SD0#state{pending_prepares=PendingPrepares-1}};

%handle_cast({real_prepared, TxId, PrepareTime}, SD0) ->
%      %lager:warning("Real prepare in not local cert ~w", [TxId]),
%    handle_cast({prepared, TxId, PrepareTime}, SD0); 

handle_cast({prepared, TxId, PrepareTime}, 
	    SD0=#state{tx_id=TxId, local_updates=LocalParts, stage=local_cert, total_repl_factor=TotalReplFactor, 
            remote_updates=RemoteUpdates, sender=Sender, dep_dict=DepDict, pending_list=PendingList, txn_type=TxnType,
             min_commit_ts=LastCommitTs, specula_length=SpeculaLength, pending_prepares=PendingPrepares, lp_start=LT,
            pending_txs=PendingTxs, rep_dict=RepDict, committed=Committed}) ->
    %lager:warning("Got local prepare for ~w", [TxId, PrepareTime]),
    case dict:find(TxId, DepDict) of
        %% Maybe can commit already.
        {ok, {1, ReadDepTxs, OldPrepTime}} ->
            NewMaxPrep = max(PrepareTime, OldPrepTime),
            RemoteParts = [P || {P, _} <- RemoteUpdates],
            RemoteToAck = length(RemoteParts),
            case (RemoteToAck == 0) and (ReadDepTxs == [])
                  and (PendingList == []) and (PendingPrepares == 0) of
                true ->
                    CommitTime = max(NewMaxPrep, LastCommitTs+1),
                    {DepDict1, _} = commit_tx(TxId, NewMaxPrep, LocalParts, [], 
                                    dict:erase(TxId, DepDict), RepDict),
                    gen_server:reply(Sender, {ok, {committed, NewMaxPrep}}),
                    {noreply, SD0#state{min_commit_ts=CommitTime, tx_id=?NO_TXN, pending_list=[], 
                        dep_dict=DepDict1, committed=Committed+1}};
                false ->
                    %lager:info("Pending list is ~w, pending prepares is ~w, ReadDepTxs is ~w", [PendingList, PendingPrepares, ReadDepTxs]),
                    case length(PendingList) >= SpeculaLength of
                        true -> 
                            %%In wait stage, only prepare and doesn't add data to table
                             %lager:warning("Decided to wait and prepare ~w, pending list is ~w!!", [TxId, PendingList]),
                            ?CLOCKSI_VNODE:prepare(RemoteUpdates, TxId, {remote, ignore}),
                            DepDict1 = dict:store(TxId, {TotalReplFactor*RemoteToAck+PendingPrepares, ReadDepTxs, NewMaxPrep}, DepDict),
                            {noreply, SD0#state{dep_dict=DepDict1, stage=remote_cert, rp_start=os:timestamp()}};
                        false ->
                             %lager:warning("Speculate current tx with ~w, remote parts are ~w, Num is ~w", [TxId, RemoteParts, length(RemoteParts)]),
                            %% Add dependent data into the table
                            ets:insert(PendingTxs, {TxId, {LocalParts, RemoteParts, LT, os:timestamp(), TxnType}}),
                            {ProposeTS, NumAvoid} = add_to_table(RemoteUpdates, TxId, NewMaxPrep, RepDict),
                   %lager:warning("Proposets for ~w is ~w", [ProposeTS, TxId]),
                            ?CLOCKSI_VNODE:prepare(RemoteUpdates, ProposeTS, TxId, {remote, node()}),
                            gen_server:reply(Sender, {ok, {specula_commit, NewMaxPrep}}),
                            DepDict1 = dict:store(TxId, {TotalReplFactor*RemoteToAck+PendingPrepares-NumAvoid, ReadDepTxs, NewMaxPrep}, DepDict),
                            {noreply, SD0#state{tx_id=?NO_TXN, dep_dict=DepDict1, pending_list=PendingList++[TxId], min_snapshot_ts=NewMaxPrep}}
                    end
                end;
        {ok, {N, ReadDeps, OldPrepTime}} ->
             %lager:warning("~w needs ~w local prep replies", [TxId, N-1]),
            DepDict1 = dict:store(TxId, {N-1, ReadDeps, max(PrepareTime, OldPrepTime)}, DepDict),
            {noreply, SD0#state{dep_dict=DepDict1}};
        error ->
            {noreply, SD0}
    end;

handle_cast({prepared, PendingTxId, PendingPT}, 
	    SD0=#state{dep_dict=DepDict}) ->
    %lager:warning("Got remote prepare for ~w, pt is ~w", [PendingTxId, PendingPT]),
    case dict:find(PendingTxId, DepDict) of
        {ok, {1, [], OldPrepTime}} -> %% Maybe the transaction can commit 
            NowPrepTime =  max(OldPrepTime, PendingPT),
            try_commit_pending(NowPrepTime, PendingTxId, SD0);
        {ok, {PrepDeps, ReadDeps, OldPrepTime}} -> %% Maybe the transaction can commit 
           %lager:warning("Not enough.. Prep ~w, Read ~w, PrepTime ~w", [PrepDeps, ReadDeps, OldPrepTime]),
            DepDict1=dict:store(PendingTxId, {PrepDeps-1, ReadDeps, max(PendingPT, OldPrepTime)}, DepDict),
            {noreply, SD0#state{dep_dict=DepDict1}};
        error ->
            {noreply, SD0}
    end;

%handle_cast({prepared, _OtherTxId, _}, SD0=#state{stage=local_cert}) ->
%    {noreply, SD0}; 

%% TODO: if we don't direclty speculate after getting all local prepared, maybe we can wait a littler more
%%       and here we should check if the transaction can be directly committed or not. 
handle_cast({read_valid, PendingTxId, PendedTxId}, SD0=#state{dep_dict=DepDict, 
            committed=Committed, sender=Sender}) ->
     %lager:warning("Got read valid for ~w of ~w", [PendingTxId, PendedTxId]),
    case dict:find(PendingTxId, DepDict) of
        {ok, {read_only, [PendedTxId], ReadOnlyTs}} ->
            gen_server:reply(Sender, {ok, {committed, ReadOnlyTs}}),
            {noreply, SD0#state{dep_dict=dict:erase(PendingTxId, DepDict), committed=Committed+1}};
        {ok, {read_only, MoreDeps, ReadOnlyTs}} ->
            {noreply, SD0#state{dep_dict=dict:store(PendingTxId, {read_only, lists:delete(PendedTxId, MoreDeps), ReadOnlyTs}, DepDict)}};
        {ok, {0, SolvedReadDeps, 0}} -> 
            %% Txn is still reading!!
             %lager:warning("Inserting read valid for ~w, old deps are ~w",[PendingTxId, SolvedReadDeps]),
            {noreply, SD0#state{dep_dict=dict:store(PendingTxId, {0, [PendedTxId|SolvedReadDeps], 0}, DepDict)}};
        {ok, {0, [PendedTxId], OldPrepTime}} -> %% Maybe the transaction can commit 
            try_commit_pending(OldPrepTime, PendingTxId, SD0);
        {ok, {PrepDeps, ReadDepTxs, OldPrepTime}} -> 
         %lager:warning("Can not commit... Remaining prepdep is ~w, read dep is ~w", [PrepDeps, lists:delete(PendedTxId, ReadDepTxs)]),
            {noreply, SD0#state{dep_dict=dict:store(PendingTxId, {PrepDeps, lists:delete(PendedTxId, ReadDepTxs), 
                OldPrepTime}, DepDict)}};
        error ->
           %lager:warning("WTF, no info?"),
            {noreply, SD0}
    end;

handle_cast({read_invalid, MyCommitTime, TxId}, SD0) ->
    read_abort(read_invalid, MyCommitTime, TxId, SD0);
handle_cast({read_aborted, MyCommitTime, TxId}, SD0) ->
    read_abort(read_aborted, MyCommitTime, TxId, SD0);
            
handle_cast({aborted, TxId, FromNode}, SD0=#state{tx_id=TxId, local_updates=LocalParts, 
        sender=Sender, dep_dict=DepDict, stage=local_cert, cert_aborted=CertAborted, rep_dict=RepDict}) ->
    abort_tx(TxId, lists:delete(FromNode, LocalParts), [], RepDict),
    DepDict1 = dict:erase(TxId, DepDict),
    %lager:warning("~w local abort! Cert aborted is now ~w", [TxId, CertAborted+1]),
    gen_server:reply(Sender, {aborted, local}),
    {noreply, SD0#state{tx_id=?NO_TXN, dep_dict=DepDict1, cert_aborted=CertAborted+1}};
%% Aborting the current transaction
handle_cast({aborted, TxId, FromNode}, 
	    SD0=#state{tx_id=TxId, pending_txs=PendingTxs,  local_updates=LocalParts, rep_dict=RepDict, 
            remote_updates=RemoteUpdates, dep_dict=DepDict, sender=Sender, cert_aborted=CertAborted, stage=remote_cert}) ->
    %lager:warning("Aborting ~w remote", [TxId]),
    RemoteParts = [P || {P, _} <- RemoteUpdates],
    abort_tx(TxId, LocalParts, lists:delete(FromNode, RemoteParts), RepDict),
    DepDict1 = dict:erase(PendingTxs, DepDict),
    %lager:warning("~w remote abort! Cert aborted is now ~w", [TxId, CertAborted+1]),
    gen_server:reply(Sender, {aborted, remote}),
    {noreply, SD0#state{tx_id=?NO_TXN, dep_dict=DepDict1, cert_aborted=CertAborted+1}};
%% Not aborting current transaction
handle_cast({aborted, PendingTxId, FromNode}, 
	    SD0=#state{pending_list=PendingList, rep_dict=RepDict, tx_id=CurrentTxId, sender=Sender,
            dep_dict=DepDict, pending_txs=PendingTxs, stage=Stage, cascade_aborted=CascadeAborted,
            cert_aborted=CertAborted, local_updates=LocalParts, remote_updates=RemoteUpdates}) ->
    %lager:warning("Aborting ~w remote, not current transaction", [PendingTxId]),
    case start_from_list(PendingTxId, PendingList) of
        [] ->
          %lager:warning("Not aborting anything"),
            {noreply, SD0};
        {Prev, L} ->
            Length = length(L),
            DepDict1 = abort_specula_list(L, RepDict, DepDict, PendingTxs, FromNode),
            case CurrentTxId of
                ?NO_TXN ->
                    %lager:warning("No current txn, Pendinglist is ~w, NumAborted is ~w", [PendingTxId, Length-1]),
                    {noreply, SD0#state{dep_dict=DepDict1, 
                        pending_list=Prev,
                        cert_aborted=CertAborted+1, cascade_aborted=CascadeAborted+Length-1}};
                _ ->
                  %lager:warning("Trying to abort local txn ~w", [CurrentTxId]),
                    RemoteParts = [P || {P, _} <- RemoteUpdates],
                    case Stage of
                        read ->
                            %% The current transaction is cert_aborted! So replying to client.
                             %lager:warning("~w txn is in reading !!", [CurrentTxId]),
                            {noreply, SD0#state{dep_dict=DepDict1, invalid_ts=?FLOW_ABORT, 
                                pending_list=Prev,
                                cert_aborted=CertAborted+1, cascade_aborted=CascadeAborted+Length-1}};
                        _ ->
                            abort_tx(CurrentTxId, LocalParts, RemoteParts, RepDict),
                            DepDict2 = dict:erase(CurrentTxId, DepDict1),
                            %lager:warning("~w remote abort current! Cert aborted is now ~w", [CurrentTxId, CertAborted+1]),
                            gen_server:reply(Sender, {aborted, CurrentTxId}),
                            %% The current transaction is cert_aborted! So replying to client.
                            %% But don't count it!
                            {noreply, SD0#state{dep_dict=DepDict2, tx_id=?NO_TXN, 
                                pending_list=Prev,
                                cert_aborted=CertAborted+1, cascade_aborted=CascadeAborted+Length}}
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
try_commit_pending(NowPrepTime, PendingTxId, SD0=#state{pending_txs=PendingTxs, rep_dict=RepDict, dep_dict=DepDict, 
            pending_list=PendingList, read_invalid=ReadInvalid, tx_id=CurrentTxId, committed=Committed,
            lp_start=LT, rp_start=RT, min_commit_ts=MinCommitTS, sender=Sender, stage=Stage, 
            cascade_aborted=CascadAborted, txn_type=TxnType, local_updates=LocalParts, remote_updates=RemoteUpdates}) ->
    case PendingList of
        [] -> %% This is the just report_committed txn.. But new txn has not come yet.
            CurCommitTime = max(MinCommitTS+1, NowPrepTime),
            RemoteParts = [P||{P, _} <-RemoteUpdates],
            DepDict2 = dict:erase(PendingTxId, DepDict),
            {DepDict3, _} = commit_tx(PendingTxId, CurCommitTime, LocalParts, RemoteParts,
                DepDict2, RepDict),
            gen_server:reply(Sender, {ok, {committed, CurCommitTime}}),
            {noreply, SD0#state{min_commit_ts=CurCommitTime, tx_id=?NO_TXN,
                 pending_list=[], dep_dict=DepDict3, committed=Committed+1}};
        [PendingTxId|Rest] ->
            CommitTime = max(NowPrepTime, MinCommitTS+1),
            {DepDict1, ToAbortTxs} = commit_specula_tx(PendingTxId, CommitTime,
                    dict:erase(PendingTxId, DepDict), RepDict, PendingTxs),		 
            {NewPendingList, NewMaxPT, DepDict2, NumAborted, NewCommitted} 
                = case ToAbortTxs of
                    [] ->
                        try_to_commit(CommitTime, Rest, RepDict, DepDict1, PendingTxs, 0, Committed+1);
                    _ ->
                        {TDepDict, TPendingList, TNumAborted}
                            = try_to_abort(Rest, ToAbortTxs, DepDict1, RepDict, PendingTxs, 0),
                        try_to_commit(CommitTime, TPendingList, RepDict, TDepDict, PendingTxs, TNumAborted, Committed+1)
            end,
             %% Only needs local parts and remote parts when need to commit/abort/specula_commit,
            %% so identify when needs to abort.
            case decide_after_cascade(NewPendingList, DepDict2, NumAborted, CurrentTxId, Stage) of
                invalid -> %% Should be cert_aborted later
                   %lager:warning("Current tx invalid"),
                    {noreply, SD0#state{pending_list=NewPendingList, dep_dict=DepDict2, invalid_ts=?FLOW_ABORT,
                          min_commit_ts=NewMaxPT, read_invalid=ReadInvalid+min(NumAborted, 1),
                              cascade_aborted=CascadAborted+max(0, NumAborted-1), committed=NewCommitted}};
                {abort_local, CFlowAbort, CReadInvalid} ->
                   %lager:warning("Abort local txn"),
                    abort_tx(CurrentTxId, LocalParts, [], RepDict),
                    DepDict3 = dict:erase(CurrentTxId, DepDict2),
                    gen_server:reply(Sender, {aborted, CurrentTxId}),
                    {noreply, SD0#state{
                      pending_list=NewPendingList, tx_id=?NO_TXN, dep_dict=DepDict3,
                        min_commit_ts=NewMaxPT, read_invalid=ReadInvalid+min(NumAborted, 1)+CReadInvalid,
                              cascade_aborted=CascadAborted+max(NumAborted-1, 0)+CFlowAbort,  committed=NewCommitted}};
                {abort_remote, CFlowAbort, CReadInvalid} ->
                   %lager:warning("Abort remote txn"),
                    RemoteParts = [P||{P, _} <-RemoteUpdates],
                    abort_tx(CurrentTxId, LocalParts, RemoteParts, RepDict),
                    DepDict3 = dict:erase(CurrentTxId, DepDict2),
                    gen_server:reply(Sender, {aborted, CurrentTxId}),
                    {noreply, SD0#state{
                      pending_list=NewPendingList, tx_id=?NO_TXN, dep_dict=DepDict3,
                        min_commit_ts=NewMaxPT,  read_invalid=ReadInvalid+min(NumAborted, 1)+CReadInvalid,
                              cascade_aborted=CascadAborted+max(NumAborted-1,0)+CFlowAbort, committed=NewCommitted}};
                wait -> 
                   %lager:warning("Local txn waits"),
                    {noreply, SD0#state{pending_list=NewPendingList, dep_dict=DepDict2, 
                            read_invalid=ReadInvalid+min(NumAborted, 1), min_commit_ts=NewMaxPT,
                              cascade_aborted=CascadAborted+max(0, NumAborted-1), committed=NewCommitted}};
                {commit, OldCurPrepTime} ->
                   %lager:warning("Commit local txn"),
                    CurCommitTime = max(NewMaxPT+1, OldCurPrepTime),
                    RemoteParts = [P||{P, _} <-RemoteUpdates],
                    %true = ets:delete(PendingTxs, CurrentTxId),
                    {DepDict3, _} = commit_tx(CurrentTxId, CurCommitTime, LocalParts, RemoteParts, 
                        dict:erase(CurrentTxId, DepDict2), RepDict),
                    gen_server:reply(Sender, {ok, {committed, CurCommitTime}}),
                    {noreply, SD0#state{min_commit_ts=CurCommitTime, tx_id=?NO_TXN, 
                          pending_list=[], committed=NewCommitted+1, dep_dict=DepDict3}};
                specula -> %% Can not commit, but can specualte (due to the number of specula txns decreased)
                    {ok, {Prep, Read, OldCurPrepTime}} = dict:find(CurrentTxId, DepDict2),
                    SpeculaPrepTime = max(NewMaxPT+1, OldCurPrepTime),
                    {ProposeTS, AvoidNum} = add_to_table(RemoteUpdates, CurrentTxId, SpeculaPrepTime, RepDict),
                    DepDict3=dict:store(CurrentTxId, {Prep-AvoidNum, Read, max(SpeculaPrepTime, ProposeTS)}, DepDict2),
                   %lager:warning("Specula current txn ~w, got propose time ~w, avoided ~w", [CurrentTxId, ProposeTS, AvoidNum]),
                    gen_server:reply(Sender, {ok, {specula_commit, SpeculaPrepTime}}),
                    PendingList1 = NewPendingList ++ [CurrentTxId],
                    RemoteParts = [P||{P, _} <-RemoteUpdates],
                    ets:insert(PendingTxs, {CurrentTxId, {LocalParts, RemoteParts, LT, RT, TxnType}}),
                    {noreply, SD0#state{tx_id=?NO_TXN, dep_dict=DepDict3,
                            pending_list=PendingList1, min_commit_ts=NewMaxPT, min_snapshot_ts=SpeculaPrepTime,
                                      committed=NewCommitted}}
            end;
        _ ->
            DepDict1 = dict:store(PendingTxId, {0, [], NowPrepTime}, DepDict),
            %lager:warning("got all replies, but I am not the first! PendingList is ~w", [PendingList]),
            {noreply, SD0#state{dep_dict=DepDict1}}
    end.

read_abort(Type, MyCommitTime, TxId, SD0=#state{tx_id=CurrentTxId, sender=Sender, read_aborted=ReadAborted,
        dep_dict=DepDict, pending_list=PendingList, cascade_aborted=CascadAborted, stage=Stage,
        rep_dict=RepDict, local_updates=LocalParts, remote_updates=RemoteUpdates, pending_txs=PendingTxs,
        invalid_ts=InvalidTS, read_invalid=ReadInvalid}) ->
   %lager:warning("Got read invalid for ~w", [TxId]),
    {RAD1, RID1} = case Type of read_aborted -> {ReadAborted+1, ReadInvalid};
                                read_invalid -> {ReadAborted, ReadInvalid+1}
                   end,
    %lager:warning("Tx ~w aborted! ReadAborted is ~w, ReadInvalid is ~w", [TxId, RAD1, RID1]),
    case dict:find(TxId, DepDict) of
        {ok, {read_only, _, _}} ->
            {noreply, SD0#state{dep_dict=dict:erase(TxId, DepDict), read_aborted=ReadAborted+1}};
        _ ->
        case start_from_list(TxId, PendingList) of
        [] ->
            case TxId of
                CurrentTxId ->
                    case Stage of
                        local_cert ->
                            abort_tx(TxId, LocalParts, [], RepDict),
                            RD1 = dict:erase(TxId, DepDict),
                            gen_server:reply(Sender, {aborted, TxId}),
                            {noreply, SD0#state{dep_dict=RD1, tx_id=?NO_TXN, read_aborted=RAD1, read_invalid=RID1}};
                        remote_cert ->
                            RemoteParts = [P||{P, _} <-RemoteUpdates],
                            abort_tx(TxId, LocalParts, RemoteParts, RepDict),
                            RD1 = dict:erase(TxId, DepDict),
                            gen_server:reply(Sender, {aborted, TxId}),
                            {noreply, SD0#state{dep_dict=RD1, tx_id=?NO_TXN, read_aborted=RAD1, read_invalid=RID1}};
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
        {Prev, L} ->
            RD = abort_specula_list(L, RepDict, DepDict, PendingTxs),
            Length = length(L),
            %lager:warning("Abort due to read invalid, Pending list is ~w, PendingTxId is ~w, List is ~w", [PendingList, TxId, L]),
            case CurrentTxId of
                ?NO_TXN ->
                    %lager:warning("No current txn, Pendinglist is ~w, NumAborted is ~w", [PendingList, Length-1]),
                    {noreply, SD0#state{dep_dict=RD, 
                        pending_list=Prev,
                        read_aborted=RAD1, read_invalid=RID1, cascade_aborted=CascadAborted+Length-1}};
                _ ->
                  %lager:warning("Current tx is ~w, Stage is ~w", [CurrentTxId, Stage]),
                    case Stage of
                        local_cert -> 
                            abort_tx(CurrentTxId, LocalParts, [], RepDict),
                            gen_server:reply(Sender, {aborted, CurrentTxId}),
                            RD1 = dict:erase(CurrentTxId, RD),
                            {noreply, SD0#state{dep_dict=RD1, tx_id=?NO_TXN, 
                                pending_list=Prev,
                                read_aborted=RAD1, read_invalid=RID1, cascade_aborted=CascadAborted+Length}};
                        remote_cert -> 
                            %lager:warning("Has current txn remote, Pendinglist is ~w, NumAborted is ~w", [PendingList, Length]),
                            RemoteParts = [P||{P, _} <-RemoteUpdates],
                            abort_tx(CurrentTxId, LocalParts, RemoteParts, RepDict),
                            gen_server:reply(Sender, {aborted, CurrentTxId}),
                            RD1 = dict:erase(CurrentTxId, RD),
                            {noreply, SD0#state{dep_dict=RD1, tx_id=?NO_TXN, 
                                pending_list=Prev,
                                read_aborted=RAD1, read_invalid=RID1, cascade_aborted=CascadAborted+Length}};
                        read ->
                            %lager:warning("Has current txn read, Pendinglist is ~w, NumAborted is ~w", [PendingList, Length-1]),
                            case (MyCommitTime == -1) or (InvalidTS == -1) of
                                true ->
                                    {noreply, SD0#state{dep_dict=RD, invalid_ts=-1,
                                        pending_list=Prev,
                                        read_aborted=RAD1, read_invalid=RID1, cascade_aborted=CascadAborted+Length-1}};
                                _ -> 
                                    {noreply, SD0#state{dep_dict=RD, invalid_ts=max(MyCommitTime, InvalidTS), 
                                        pending_list=lists:sublist(PendingList, length(PendingList)-Length),
                                        read_aborted=RAD1, read_invalid=RID1, cascade_aborted=CascadAborted+Length-1}}
                            end
                    end
            end
        end
    end.

decide_after_cascade(PendingList, DepDict, NumAborted, TxId, Stage) ->
  %lager:warning("PendingList ~w, DepDict ~w, NumAborted ~w, TxId ~w, Stage ~w", [PendingList, DepDict, NumAborted, TxId, Stage]),
    case TxId of
        ?NO_TXN -> wait;
        _ -> 
            case NumAborted > 0 of
                true -> %lager:warning("Abort due to flow dep"),
                        case Stage of read -> invalid; %% Abort due to flow dependency. Second indicate flow abort, third read invalid 
                                     local_cert -> {abort_local, 1, 0};
                                     remote_cert -> {abort_remote, 1, 0}
                        end;
                false ->
                    case dict:find(TxId, DepDict) of %% TODO: should give a invalid_ts 
                        error -> %lager:warning("Abort due to read dep"),
                                 case Stage of read -> invalid;
                                             local_cert -> {abort_local, 0, 1};
                                             remote_cert -> {abort_remote, 0, 1}
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

try_to_abort(PendingList, ToAbortTxs, DepDict, RepDict, PendingTxs, ReadAborted) ->
  %lager:warning("Trying to abort ~w ~n", [ToAbortTxs]),
    case find_min_index(ToAbortTxs, PendingList) of
        ?MAX ->
            DepDict1 = lists:foldl(fun(T, D) -> dict:erase(T, D) end, DepDict, ToAbortTxs),
            {DepDict1, PendingList, ReadAborted};
        MinIndex ->
            FullLength = length(PendingList),
            L = lists:sublist(PendingList, MinIndex, FullLength),
            RD = abort_specula_list(L, RepDict, DepDict, PendingTxs),
            %% Remove entries so that the current txn (which is not in pending list) knows that it should be cert_aborted. 
            RD1 = lists:foldl(fun(T, D) -> dict:erase(T, D) end, RD, ToAbortTxs),
            {RD1, lists:sublist(PendingList, MinIndex-1), ReadAborted+FullLength-MinIndex+1}
    end.

%% Same reason, no need for RemoteParts
commit_tx(TxId, CommitTime, LocalParts, RemoteParts, DepDict, RepDict) ->
    DepList = ets:lookup(dependency, TxId),
    %lager:warning("Committing tx ~w with ~w", [TxId, CommitTime]),
  %lager:warning("~w: My read dependncy are ~w", [TxId, DepList]),
    {DepDict1, ToAbortTxs} = solve_read_dependency(CommitTime, DepDict, DepList),
  %lager:warning("Commit ~w to ~w, ~w", [TxId, LocalParts, RemoteParts]),
    ?CLOCKSI_VNODE:commit(LocalParts, TxId, CommitTime),
    %?REPL_FSM:repl_commit(LocalParts, TxId, CommitTime, DoRepl),
    ?REPL_FSM:repl_commit(LocalParts, TxId, CommitTime, false, RepDict),
    ?CLOCKSI_VNODE:commit(RemoteParts, TxId, CommitTime),
    %?REPL_FSM:repl_commit(RemoteParts, TxId, CommitTime, DoRepl),
    ?REPL_FSM:repl_commit(RemoteParts, TxId, CommitTime, false, RepDict),
    {DepDict1, ToAbortTxs}.

commit_specula_tx(TxId, CommitTime, DepDict, RepDict, PendingTxs) ->
   %ning("Committing specula tx ~w with ~w", [TxId, CommitTime]),
    [{TxId, {LocalParts, RemoteParts, LP, RP, TxnType}}] = ets:lookup(PendingTxs, TxId),
    true = ets:delete(PendingTxs, TxId),
    %%%%%%%%% Time stat %%%%%%%%%%%
    LPDiff = get_time_diff(LP, RP),
    RPDiff = get_time_diff(RP, os:timestamp()),
    ets:update_counter(PendingTxs, {TxnType, commit}, [{2, LPDiff}, {3, RPDiff}, {4, 1}]),
    %%%%%%%%% Time stat %%%%%%%%%%%
    DepList = ets:lookup(dependency, TxId),
   %lager:warning("~w: My read dependncy are ~w", [TxId, DepList]),
    {DepDict1, ToAbortTxs} = solve_read_dependency(CommitTime, DepDict, DepList),

    ?CLOCKSI_VNODE:commit(LocalParts, TxId, CommitTime),
    ?REPL_FSM:repl_commit(LocalParts, TxId, CommitTime, false, RepDict),
    %?REPL_FSM:repl_commit(LocalParts, TxId, CommitTime, DoRepl),
    ?CLOCKSI_VNODE:commit(RemoteParts, TxId, CommitTime),
    %?REPL_FSM:repl_commit(RemoteParts, TxId, CommitTime, DoRepl),
    ?REPL_FSM:repl_commit(RemoteParts, TxId, CommitTime, true, RepDict),
    %lager:warning("Specula commit ~w to ~w, ~w", [TxId, LocalParts, RemoteParts]),
    %io:format(user, "Calling commit to table with ~w, ~w, ~w ~n", [RemoteParts, TxId, CommitTime]),
    %commit_to_table(RemoteParts, TxId, CommitTime, RepDict),
    {DepDict1, ToAbortTxs}.

abort_specula_tx(TxId, PendingTxs, RepDict, DepDict, ExceptNode) ->
          [{TxId, {LocalParts, RemoteParts, LP, RP, TxnType}}] = ets:lookup(PendingTxs, TxId),
      %lager:warning("Trying to abort specula ~w to ~w, ~w", [TxId, LocalParts, RemoteParts]),
      ets:delete(PendingTxs, TxId),
      %%%%%%%%% Time stat %%%%%%%%%%%
      LPDiff = get_time_diff(LP, RP),
      RPDiff = get_time_diff(RP, os:timestamp()),
      ets:update_counter(PendingTxs, {TxnType, abort}, [{2, LPDiff}, {3, RPDiff}, {4, 1}]),
      %%%%%%%%% Time stat %%%%%%%%%%%
      DepList = ets:lookup(dependency, TxId),
      %lager:warning("~w: My read dependncy are ~w", [TxId, DepList]),
      Self = self(),
      lists:foreach(fun({_, DepTxId}) ->
                                TxServer = DepTxId#tx_id.server_pid,
                                ets:delete_object(dependency, {TxId, DepTxId}),
                                case TxServer of
                                    Self ->
                                        ok;
                                    _ ->
                                      %lager:warning("~w is not my own, read invalid", [DepTxId]),
                                        ?READ_ABORTED(TxServer, -1, DepTxId)
                                end
                       end, DepList),
      ?CLOCKSI_VNODE:abort(LocalParts, TxId),
      ?REPL_FSM:repl_abort(LocalParts, TxId, false, RepDict),
      ?CLOCKSI_VNODE:abort(lists:delete(ExceptNode, RemoteParts), TxId),
      ?REPL_FSM:repl_abort(RemoteParts, TxId, true, RepDict),
      dict:erase(TxId, DepDict).

abort_specula_tx(TxId, PendingTxs, RepDict, DepDict) ->
    [{TxId, {LocalParts, RemoteParts, LP, RP, TxnType}}] = ets:lookup(PendingTxs, TxId), 
    %lager:warning("Trying to abort specula ~w to ~w, ~w", [TxId, LocalParts, RemoteParts]),
    ets:delete(PendingTxs, TxId),
    DepList = ets:lookup(dependency, TxId),
    %lager:warning("~w: My read dependncy are ~w", [TxId, DepList]),
    Self = self(),
    lists:foreach(fun({_, DepTxId}) ->
                              TxServer = DepTxId#tx_id.server_pid,
                              ets:delete_object(dependency, {TxId, DepTxId}),
                              case TxServer of
                                  Self ->
                                      ok;
                                  _ ->
                                    %lager:warning("~w is not my own, read invalid", [DepTxId]),
                                      ?READ_ABORTED(TxServer, -1, DepTxId)
                              end
                     end, DepList),
    %%%%%%%%% Time stat %%%%%%%%%%%
    LPDiff = get_time_diff(LP, RP),
    RPDiff = get_time_diff(RP, os:timestamp()),
    ets:update_counter(PendingTxs, {TxnType, abort}, [{2, LPDiff}, {3, RPDiff}, {4, 1}]),
    %%%%%%%%% Time stat %%%%%%%%%%%
    %abort_tx(TxId, LocalParts, RemoteParts, DoRepl),
    ?CLOCKSI_VNODE:abort(LocalParts, TxId),
    ?REPL_FSM:repl_abort(LocalParts, TxId, false, RepDict),
    ?CLOCKSI_VNODE:abort(RemoteParts, TxId),
    ?REPL_FSM:repl_abort(RemoteParts, TxId, true, RepDict),
    %abort_to_table(RemoteParts, TxId, RepDict),
    dict:erase(TxId, DepDict).

abort_tx(TxId, LocalParts, RemoteParts, RepDict) ->
   %lager:warning("Trying to abort ~w to ~w, ~w", [TxId, LocalParts, RemoteParts]),
    %true = ets:delete(PendingTxs, TxId),
    DepList = ets:lookup(dependency, TxId),
   %lager:warning("~w: My read dependncy are ~w", [TxId, DepList]),
    Self = self(),
    lists:foreach(fun({_, DepTxId}) ->
                            TxServer = DepTxId#tx_id.server_pid,
                            ets:delete_object(dependency, {TxId, DepTxId}),
                            case TxServer of
                                Self ->
                                    ok;
                                _ ->
                                  %lager:warning("~w is not my own, read invalid", [DepTxId]),
                                    ?READ_ABORTED(TxServer, -1, DepTxId)
                            end
                    end, DepList),
    ?CLOCKSI_VNODE:abort(LocalParts, TxId),
    ?REPL_FSM:repl_abort(LocalParts, TxId, false, RepDict),
    ?CLOCKSI_VNODE:abort(RemoteParts, TxId),
    ?REPL_FSM:repl_abort(RemoteParts, TxId, false, RepDict).

add_to_table(WriteSet, TxId, PrepareTime, RepDict) ->
    {Repled, ProposeTS} = lists:foldl(fun({{Part, Node}, Updates}, {ToDelete, GotTS}) ->
                    case dict:find({rep, Node}, RepDict) of
                        {ok, DataReplServ} ->
                            case ?DATA_REPL_SERV:get_ts(DataReplServ, TxId, Part, Updates) of
                                exist ->
                                    {[{{Part, Node}, Updates}|ToDelete], GotTS};
                                TS ->
                                    {ToDelete, max(TS, GotTS)}
                            end;
                        _ ->
                            {ToDelete, GotTS}
                    end end, {[], PrepareTime}, WriteSet),
    NewWriteSet = WriteSet -- Repled, 
    specula_prepare(NewWriteSet, TxId, ProposeTS, RepDict, 0).

specula_prepare([], _, PrepareTime, _, NumParts) ->
    {PrepareTime, NumParts};
specula_prepare([{{Partition, Node}, Updates}|Rest], TxId, PrepareTime, RepDict, NumParts) ->
    case dict:find({rep, Node}, RepDict) of
            {ok, DataReplServ} ->
                %lager:warning("Prepareing specula for ~w ~w", [TxId, Partition]),
                ?DATA_REPL_SERV:prepare_specula(DataReplServ, TxId, Partition, Updates, PrepareTime),
                specula_prepare(Rest, TxId, PrepareTime, RepDict, NumParts+1);
            _ ->
                %% Send to local cache.
                ?CACHE_SERV:prepare_specula(TxId, Partition, Updates, PrepareTime),
                specula_prepare(Rest, TxId, PrepareTime, RepDict, NumParts)
    end.
    %lager:info("Got ts is ~w", [Ts]),

%commit_to_table([], _, _, _) ->
%    ok;
%commit_to_table([{Partition, Node}|Rest], TxId, CommitTime, RepDict) ->
%    %io:format(user, "Part is ~w, Node is ~w, Rest is ~w ~n", [Partition, Node, Rest]),
%  %lager:warning("Committing to table for ~w", [Partition]),
%    case dict:find(Node, RepDict) of
%        error ->
%            %% Send to local cache.
%            ?CACHE_SERV:commit_specula(TxId, Partition, CommitTime);
%        _ -> %{ok, DataReplServ} ->
%           %lager:warning("Sending to ~w", [DataReplServ]),
%            ok
%            %?DATA_REPL_SERV:commit_specula(DataReplServ, TxId, Partition, CommitTime)
%    end,
%    commit_to_table(Rest, TxId, CommitTime, RepDict).

%abort_to_table([], _, _) ->
%    ok;
%abort_to_table([{Partition, Node}|Rest], TxId, RepDict) ->
%    case dict:find(Node, RepDict) of
%        error ->
%            %% Send to local cache.
%            ?CACHE_SERV:abort_specula(TxId, Partition);
%        _ -> %{ok, DataReplServ} ->
%            ok
%             %lager:warning("Aborting specula for ~w ~w", [TxId, Partition]),
            %?DATA_REPL_SERV:abort_specula(DataReplServ, TxId, Partition);
%    end,
%    abort_to_table(Rest, TxId, RepDict).

%% Deal dependencies and check if any following transactions can be committed.
try_to_commit(LastCommitTime, [], _RepDict, DepDict, _PendingTxs, NumAborted, Committed) ->
    %lager:warning("Returning ~w ~w ~w ~w ~w", [[], LastCommitTime, DepDict, NumAborted, Committed]),
    {[], LastCommitTime, DepDict, NumAborted, Committed};
try_to_commit(LastCommitTime, [H|Rest]=PendingList, RepDict, DepDict, 
                        PendingTxs, NumAborted, Committed) ->
    Result = case dict:find(H, DepDict) of
                {ok, {0, [], PendingMaxPT}} ->
                    {true, max(PendingMaxPT, LastCommitTime+1)};
                _ ->
                    false
            end,
    %lager:warning("If can commit decision for ~w is ~w", [H, Result]),
    case Result of
        false ->
            %lager:warning("Returning ~w ~w ~w ~w ~w", [PendingList, LastCommitTime, DepDict, NumAborted, Committed]),
            {PendingList, LastCommitTime, DepDict, NumAborted, Committed};
        {true, CommitTime} ->
            {DepDict1, ToAbortTxs} = commit_specula_tx(H, CommitTime, 
               dict:erase(H, DepDict), RepDict, PendingTxs),
            %lager:warning("Before commit specula tx, ToAbortTs are ~w, dict is ~w", [ToAbortTxs, DepDict1]),
            case ToAbortTxs of
                [] ->
                    try_to_commit(CommitTime, Rest, RepDict, DepDict1, PendingTxs, NumAborted, Committed+1);
                _ ->
                    {DepDict2, PendingList1, NumAborted1}
                            = try_to_abort(Rest, ToAbortTxs, DepDict1, RepDict, PendingTxs, NumAborted),
                    try_to_commit(CommitTime, PendingList1, RepDict, DepDict2, PendingTxs, NumAborted1, Committed+1)
            end
    end.

abort_specula_list([H|T], RepDict, DepDict, PendingTxs, ExceptNode) ->
    %lager:warning("Trying to abort ~w", [H]),
    DepDict1 = abort_specula_tx(H, PendingTxs, RepDict, DepDict, ExceptNode),
    abort_specula_list(T, RepDict, DepDict1, PendingTxs). 

abort_specula_list([], _RepDict, DepDict, _) ->
    DepDict;
abort_specula_list([H|T], RepDict, DepDict, PendingTxs) ->
   %lager:warning("Trying to abort ~w", [H]),
    DepDict1 = abort_specula_tx(H, PendingTxs, RepDict, DepDict),
    abort_specula_list(T, RepDict, DepDict1, PendingTxs). 

%% Deal with reads that depend on this txn
solve_read_dependency(CommitTime, ReadDep, DepList) ->
    Self = self(),
    lists:foldl(fun({TxId, DepTxId}, {RD, ToAbort}) ->
                    TxServer = DepTxId#tx_id.server_pid,
                    ets:delete_object(dependency, {TxId, DepTxId}), 
                    case DepTxId#tx_id.snapshot_time >= CommitTime of
                        %% This read is still valid
                        true ->
                         %lager:warning("Read still valid for ~w, CommitTime is ~w", [DepTxId, CommitTime]),
                            case TxServer of
                                Self ->
                                 %lager:warning("~w is my own, read valid", [DepTxId]),
                                    case dict:find(DepTxId, RD) of
                                        {ok, {0, _SolvedReadDeps, 0}} -> %% Local transaction is still reading
                                         %lager:warning("Deleting {~w, ~w} from antidep", [DepTxId, TxId]),
                                            ets:delete_object(anti_dep, {DepTxId, TxId}), 
                                            {RD, ToAbort};
                                        {ok, {PrepDeps, ReadDeps, PrepTime}} ->
                                         %lager:warning("Storing ~w for ~w", [lists:delete(TxId, ReadDeps), DepTxId]),
                                            {dict:store(DepTxId, {PrepDeps, lists:delete(TxId, ReadDeps), 
                                                PrepTime}, RD), ToAbort};
                                        error -> %% This txn hasn't even started certifying 
                                                 %% or has been cert_aborted already
                                          %lager:warning("This txn has not even started"),
                                            {RD, ToAbort}
                                    end;
                                _ ->
                                  %lager:warning("~w is not my own, read valid", [DepTxId]),
                                    ?READ_VALID(TxServer, DepTxId, TxId),
                                    {RD, ToAbort}
                            end;
                        false ->
                            %% Read is not valid
                            case TxServer of
                                Self ->
                                    %lager:warning("~w is my own, read invalid", [DepTxId]),
                                    {RD, [DepTxId|ToAbort]};
                                _ ->
                                 %lager:warning("~w is not my own, read invalid", [DepTxId]),
                                    ?READ_INVALID(TxServer, CommitTime, DepTxId),
                                    {RD, ToAbort}
                            end
                    end
                end, {ReadDep, []}, DepList).

%start_from_list(TxId, L) ->
%   start_from_list(TxId, L).

start_from_list(TxId, [TxId|_]=L) ->
    {[], L};
start_from_list(TxId, [H|_T]) when TxId#tx_id.snapshot_time < H#tx_id.snapshot_time ->
    [];
start_from_list(TxId, L) ->
    start_from(TxId, L, []).

%start_from_list(TxId, L) ->
%   start_from_list(TxId, L).
    
start_from(_, [], _Prev) ->
    [];
start_from(H, [H|_]=L, Prev)->
    {lists:reverse(Prev), L};
start_from(H, [NH|T], Prev) ->
    start_from(H, T, [NH|Prev]).

find_min_index(_, []) ->
    ?MAX;
find_min_index(FirstList, FullList) ->
    Set = sets:from_list(FirstList),
    {_, MinIndex} = lists:foldl(fun(E, {Index, Num}) ->
                case sets:is_element(E, Set) of
                    true ->  {Index+1, min(Index, Num)};
                    false -> {Index+1, Num}
                end end, {1, ?MAX}, FullList),
    MinIndex.

get_time_diff({A1, B1, C1}, {A2, B2, C2}) ->
    ((A2-A1)*1000000+ (B2-B1))*1000000+ C2-C1.

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
    ?assertEqual({[], [1,2,3,4,5,6]}, start_from(1, L, [])),
    ?assertEqual({[1,2], [3,4,5,6]}, start_from(3, L, [])),
    ?assertEqual({[1,2,3,4,5], [6]}, start_from(6, L, [])),
    ?assertEqual([], start_from(7, L, [])).

abort_tx_test() ->
    MyTable = ets:new(whatever, [private, set]),
    ets:new(dependency, [private, set, named_table]),
    mock_partition_fsm:start_link(),
    LocalParts = [lp1, lp2],
    TxId1 = tx_utilities:create_tx_id(0),
    abort_tx(TxId1, LocalParts, [], dict:new()),
    ?assertEqual(true, mock_partition_fsm:if_applied({abort, TxId1, LocalParts}, nothing)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_abort, TxId1, LocalParts}, nothing)),
    true = ets:delete(dependency),
    true = ets:delete(MyTable).

commit_tx_test() ->
    ets:new(dependency, [private, set, named_table]),
    LocalParts = [lp1, lp2],
    TxId1 = tx_utilities:create_tx_id(0),
    CT = 10000,
    commit_tx(TxId1, CT, LocalParts, [], dict:new(), dict:new()),
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
    ets:insert(MyTable, {{general, abort}, 0,0,0}),
    ets:insert(MyTable, {{general, commit}, 0,0,0}),
    ets:insert(MyTable, {T1, {[{p1, n1}], [{p2, n2}], now(), now(), general}}),
    ets:insert(MyTable, {T2, {[{p1, n1}], [{p2, n2}], now(), now(), general}}),
    ets:insert(MyTable, {T3, {[{p1, n1}], [{p2, n2}], now(), now(), general}}),
    DepDict4 = abort_specula_list([T1, T2, T3], RepDict, DepDict3, MyTable),
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
    %% T1, T2 should be cert_aborted, T3, T4 should get read_valid.
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
    ets:insert(MyTable, {{general, abort}, 0,0,0}),
    ets:insert(MyTable, {{general, commit}, 0,0,0}),
    RepDict00 = dict:store(n1, n1rep, dict:new()),
    RepDict01 = dict:store(n2, n2rep, RepDict00),
    RepDict = dict:store(n3, n3rep, RepDict01),
    DepDict1 = dict:store(T1, {1, [], 2}, dict:new()),
    DepDict2 = dict:store(T2, {2, [1,2], 2}, DepDict1),
    DepDict3 = dict:store(T3, {2, [], 2}, DepDict2),
    %% T1 can not commit due to having read dependency 
    io:format(user, "Abort ~w ~n", [MaxPT1]),
    {PD1, MPT1, RD1, Aborted, Committed} =  try_to_commit(MaxPT1, [T1, T2], RepDict, DepDict2, 
                                    MyTable, 0, 0),
    ?assertEqual(Aborted, 0),
    ?assertEqual(Committed, 0),
    ?assertEqual(RD1, DepDict2),
    ?assertEqual(PD1, [T1, T2]),
    ?assertEqual(MPT1, MaxPT1),

    ets:insert(MyTable, {T1, {[{lp1, n1}], [{rp1, n3}], now(), now(), general}}), 
    ets:insert(MyTable, {T2, {[{lp1, n1}], [{rp1, n3}], now(), now(), general}}), 
    ets:insert(MyTable, {T3, {[{lp2, n2}], [{rp2, n4}], now(), now(), general}}), 
    DepDict4 = dict:store(T1, {0, [], 2}, DepDict3),
    %% T1 can commit because of read_dep ok. 
    {PD2, MPT2, RD2, Aborted1, Committed1} =  try_to_commit(MaxPT1, [T1, T2], RepDict, DepDict4, 
                                    MyTable, 0, 0),
    ?assertEqual(Aborted1, 0),
    ?assertEqual(Committed1, 1),
    %?assertEqual(true, mock_partition_fsm:if_applied({commit_specula, n3rep, T1, rp1}, MPT2)),
    io:format(user, "RD2 ~w, Dict4 ~w", [dict:to_list(RD2), dict:to_list(dict:erase(T1, DepDict4))]),
    ?assertEqual(RD2, dict:erase(T1, DepDict4)),
    ?assertEqual(PD2, [T2]),
    ?assertEqual(MPT2, MaxPT1+1),

    %% T2 can commit becuase of prepare OK
    DepDict5 = dict:store(T2, {0, [], 2}, RD2),
    {_PD3, MPT3, RD3, Aborted2, Committed2} =  try_to_commit(MPT2, [T2, T3], RepDict, DepDict5, 
                                    MyTable, Aborted1, Committed1),
    ?assertEqual([], ets:lookup(MyTable, T2)),
    ?assertEqual(Aborted2, 0),
    ?assertEqual(Committed2, 2),
    %?assertEqual(true, mock_partition_fsm:if_applied({repl_commit, TxId1, LocalParts}, CT)),
    ?assertEqual(true, mock_partition_fsm:if_applied({commit, T2, [{lp1, n1}]}, MPT3)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_commit, T2, [{rp1, n3}]}, MPT3)),
    %?assertEqual(true, mock_partition_fsm:if_applied({commit_specula, n3rep, T2, rp1}, MPT3)),

    %% T3, T4 get committed and T5 gets cert_aborted.
    ets:insert(MyTable, {T3, {[{lp2, n2}], [{rp2, n4}], now(), now(), general}}), 
    ets:insert(MyTable, {T4, {[{lp2, n2}], [{rp2, n4}], now(), now(), general}}), 
    ets:insert(MyTable, {T5, {[{lp2, n2}], [{rp2, n4}], now(), now(), general}}), 
    ets:insert(dependency, {T3, T4}),
    ets:insert(dependency, {T3, T5}),
    ets:insert(dependency, {T4, T5}),
    DepDict6 = dict:store(T3, {0, [], T3#tx_id.snapshot_time+1}, RD3),
    DepDict7 = dict:store(T4, {0, [T3], T5#tx_id.snapshot_time+1}, DepDict6),
    DepDict8 = dict:store(T5, {0, [T4,T5], 6}, DepDict7),
    io:format(user, "My commit time is ~w, Txns are ~w ~w ~w", [0, T3, T4, T5]),
    {PD4, MPT4, RD4, Aborted3, Committed3} =  try_to_commit(0, [T3, T4, T5], RepDict, DepDict8, 
                                    MyTable, Aborted2, Committed2),
    ?assertEqual(Aborted3, 1),
    ?assertEqual(Committed3, 4),
    ?assertEqual(RD4, dict:new()),
    ?assertEqual(PD4, []),
    ?assertEqual(MPT4, T5#tx_id.snapshot_time+1),
    ?assertEqual(ets:lookup(MyTable, T4),  []), 
    ?assertEqual(ets:lookup(MyTable, T5),  []),

    %?assertEqual(true, mock_partition_fsm:if_applied({commit_specula, cache_serv, T3, rp2}, T3#tx_id.snapshot_time+1)),
    %?assertEqual(true, mock_partition_fsm:if_applied({commit_specula, cache_serv, T4, rp2}, MPT4)),
    %?assertEqual(true, mock_partition_fsm:if_applied({abort_specula, cache_serv, T5, rp2}, nothing)),
    ?assertEqual(true, mock_partition_fsm:if_applied({commit, T4, [{lp2, n2}]}, MPT4)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_commit, T4, [{rp2, n4}]}, MPT4)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_abort, T5, [{rp2, n4}]}, nothing )).
    
-endif.
