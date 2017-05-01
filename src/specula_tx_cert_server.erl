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
-define(MAX, 9999).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(CLOCKSI_VNODE, mock_partition_fsm).
-define(REPL_FSM, mock_partition_fsm).
-define(SPECULA_TX_CERT_SERVER, mock_partition_fsm).
-define(CACHE_SERV, mock_partition_fsm).
-define(DATA_REPL_SERV, mock_partition_fsm).
-define(READ_VALID(SEND, RTXID, WTXID, LOC), mock_partition_fsm:read_valid(SEND, RTXID, WTXID, LOC)).
-define(READ_INVALID(SEND, CT, TXID), mock_partition_fsm:read_invalid(SEND, CT, TXID)).
-define(READ_ABORTED(SEND, CT, TXID), mock_partition_fsm:read_aborted(SEND, CT, TXID)).
-else.
-define(CLOCKSI_VNODE, clocksi_vnode).
-define(REPL_FSM, repl_fsm).
-define(SPECULA_TX_CERT_SERVER, specula_tx_cert_server).
-define(CACHE_SERV, cache_serv).
-define(DATA_REPL_SERV, data_repl_serv).
-define(READ_VALID(SEND, RTXID, WTXID, LOC), gen_server:cast(SEND, {read_valid, RTXID, WTXID, LOC})).
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
        name :: atom(),
        rep_dict :: dict(),
        dep_dict :: dict(),
        specula_length :: non_neg_integer(),
        pending_txs :: dict(),
        client_dict :: dict(),
        total_repl_factor :: non_neg_integer(),
        specula_read :: term(),
        min_commit_ts = 0 :: non_neg_integer(),
        min_snapshot_ts = 0 :: non_neg_integer(),
        hit_counter=0,
        num_specula_read=0 :: non_neg_integer(),
        time_blocked = 0 :: non_neg_integer(),
        num_blocked = 0 :: non_neg_integer()
        }).

-record(c_state, {
        tx_id =?NO_TXN :: txid(),
        invalid_aborted=0 :: non_neg_integer(),
        local_updates = []:: [],
        remote_updates =[] :: [],
        pending_list=[] :: [txid()],
        loc_list,
        ffc,
        %% New stat
        aborted_reads=[],
        committed_reads=[],
        committed_updates=[],
        aborted_update = ?NO_TXN,
        msg_id=0,
        sender,
        %% Pend prepare contains: 1. Pending prepare of local partitions
        %%                        2. Needed prepare from slave partitions
        pending_prepares=0 :: non_neg_integer(),
        stage=read :: read|local_cert|remote_cert
        }).
         

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name) ->
      lager:warning("Specula tx cert started wit name ~w, id is ~p", [Name, self()]),
    gen_server:start_link({local,Name},
             ?MODULE, [Name], []).

%%%===================================================================
%%% Internal
%%%===================================================================

init([Name]) ->
    RepDict = hash_fun:build_rep_dict(true),
    PendingTxs = dict:new(), %tx_utilities:open_private_table(client_dict), 
    ClientDict = dict:new(), %tx_utilities:open_private_table(client_dict), 
    {SpeculaLength, SpeculaRead} = load_config(), 
    [{_, Replicas}] = ets:lookup(meta_info, node()),
    TotalReplFactor = length(Replicas)+1,
      lager:warning("TotalReplFactor is ~w", [TotalReplFactor]),
    {ok, #state{pending_txs=PendingTxs, client_dict=ClientDict, dep_dict=dict:new(), total_repl_factor=TotalReplFactor, name=Name, 
            specula_length=SpeculaLength, specula_read=SpeculaRead, rep_dict=RepDict}}.

handle_call({append_values, Node, KeyValues, CommitTime}, Sender, SD0) ->
    clocksi_vnode:append_values(Node, KeyValues, CommitTime, Sender),
    {noreply, SD0};

handle_call({get_pid}, _Sender, SD0) ->
    {reply, self(), SD0};

handle_call({start_read_tx}, _Sender, SD0) ->
    TxId = tx_utilities:create_tx_id(0),
    {reply, TxId, SD0};

handle_call({get_hitcounter}, _Sender, SD0=#state{hit_counter=HitCounter}) ->
    {reply, HitCounter, SD0}; 

handle_call({start_tx}, Sender, SD0) ->
    {Client, _} = Sender,
    handle_call({start_tx, 0, Client}, Sender, SD0);
handle_call({start_tx, TxnSeq}, Sender, SD0) ->
    {Client, _} = Sender,
    handle_call({start_tx, TxnSeq, Client}, Sender, SD0);
handle_call({start_tx, TxnSeq, Client}, _Sender, SD0=#state{dep_dict=D, min_snapshot_ts=MinSnapshotTS, min_commit_ts=MinCommitTS, client_dict=ClientDict}) ->
    NewSnapshotTS = max(MinSnapshotTS, MinCommitTS) + 1, 
    TxId = tx_utilities:create_tx_id(NewSnapshotTS, Client, TxnSeq),
     lager:warning("Start tx is ~p, TxSeq is ~w", [TxId, TxnSeq]),
    ClientState = case dict:find(Client, ClientDict) of
                    error ->
                        #c_state{};
                    {ok, SenderState} ->
                        SenderState
                 end,
    D1 = dict:store(TxId, {0, [], [], 0}, D),
    OldTxId = ClientState#c_state.tx_id,
    PendingList = ClientState#c_state.pending_list, 
    Stage = ClientState#c_state.stage, 
    LocalParts = ClientState#c_state.local_updates, 
    RemoteUpdates = ClientState#c_state.remote_updates, 
    case OldTxId of
        ?NO_TXN ->
            ClientState1 = ClientState#c_state{tx_id=TxId, stage=read, pending_prepares=0, loc_list=[],
                ffc=0},
            {reply, TxId, SD0#state{client_dict=dict:store(Client, ClientState1, ClientDict), dep_dict=D1, 
                min_snapshot_ts=NewSnapshotTS}};
        _ ->
            case Stage of 
                read ->
                    ClientState1 = ClientState#c_state{tx_id=TxId, stage=read, pending_prepares=0, invalid_aborted=0,
                            loc_list=[], ffc=0},
                    {reply, TxId, SD0#state{client_dict=dict:store(Client, ClientState1, ClientDict),
                        min_snapshot_ts=NewSnapshotTS, dep_dict=dict:erase(OldTxId, D1)}}; 
                _ ->
                    RemoteParts = [P||{P, _} <-RemoteUpdates],
                    ClientDict1 = dict:store(TxId, {LocalParts, RemoteParts}, ClientDict),
                    ClientState1 = ClientState#c_state{tx_id=TxId, stage=read, pending_list=PendingList++[OldTxId], pending_prepares=0, invalid_aborted=0, loc_list=[], ffc=0},
                    {reply, TxId, SD0#state{dep_dict=D1, 
                        min_snapshot_ts=NewSnapshotTS, client_dict=dict:store(Client, ClientState1, ClientDict1)}}
            end
    end;

handle_call({get_cdf}, _Sender, SD0) ->
    {reply, ok, SD0};

handle_call({get_stat}, _Sender, SD0=#state{num_blocked= NB, time_blocked=TB}) ->
    {reply, [NB, TB], SD0};
    %{reply, [ReadAborted, ReadInvalid, CertAborted, CascadeAborted, Committed, 0, NumSpeculaRead], SD0};

handle_call({set_int_data, Type, Param}, _Sender, SD0)->
    case Type of
        last_commit_time ->
               lager:warning("Set lct to ~w", [Param]),
            {noreply, SD0#state{min_commit_ts=Param}}
    end;

handle_call({get_int_data, Type, Param}, _Sender, SD0=#state{specula_length=SpeculaLength, 
        client_dict=ClientDict, dep_dict=DepDict, min_commit_ts=LastCommitTs})->
    case Type of
        pending_list ->
            {reply, dict:to_list(ClientDict), SD0};
        pending_tab ->
            {reply, dict:to_list(ClientDict), SD0};
        %time_list ->
        %    {reply, TimeList, SD0};
        dep_dict ->
            {reply, dict:to_list(DepDict), SD0};
        length ->
            {reply, SpeculaLength, SD0};
        specula_time ->
            {reply, {0,0}, SD0};
        client_dict ->
            case dict:find(param, ClientDict) of
                error ->
                    {reply, [], SD0};
                Content ->
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
            {reply, LastCommitTs, SD0}
    end;

handle_call({certify_read, TxId, ClientMsgId}, Sender, SD0) ->
    {Client, _} = Sender,
    handle_call({certify_read, TxId, ClientMsgId, Client}, Sender, SD0);
handle_call({certify_read, TxId, ClientMsgId, Client}, Sender, SD0=#state{min_commit_ts=LastCommitTs, dep_dict=DepDict, client_dict=ClientDict}) ->
    %% If there was a legacy ongoing transaction.
    ReadDepTxs = case ets:lookup(anti_dep, TxId) of
                    [] -> [];
                    [{TxId, _, _, Deps}] -> Deps
                 end,
    true = ets:delete(anti_dep, TxId),
     lager:warning("Start certifying ~w, readDepTxs is ~w", [TxId, ReadDepTxs]),
    ClientState = dict:fetch(Client, ClientDict),
    InvalidAborted = ClientState#c_state.invalid_aborted,
    TxId = ClientState#c_state.tx_id, 
    SentMsgId = ClientState#c_state.msg_id,
    AbortedReads = ClientState#c_state.aborted_reads,
    CommittedReads = ClientState#c_state.committed_reads,
    CommittedUpdates = ClientState#c_state.committed_updates,
    case SentMsgId == ClientMsgId of
        true ->
            case {InvalidAborted, dict:find(TxId, DepDict)} of
                {1, _} ->
                    case ClientState#c_state.aborted_update of
                        ?NO_TXN -> 
                            lager:warning("Aborted ~w", [TxId]),
                            ClientDict1 = dict:store(Client, ClientState#c_state{tx_id=?NO_TXN, aborted_update=?NO_TXN, aborted_reads=[],
                                    committed_updates=[], committed_reads=[], invalid_aborted=0}, ClientDict),
                            DepDict1 = dict:erase(TxId, DepDict),
                            {reply, {aborted, {AbortedReads, rev(CommittedUpdates), CommittedReads}}, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}};
                        AbortedTxId ->
                            lager:warning("Cascade aborted aborted txid is ~w, TxId is  ~w", [AbortedTxId, TxId]),
                            ClientDict1 = dict:store(Client, ClientState#c_state{tx_id=?NO_TXN, aborted_update=?NO_TXN, aborted_reads=[],
                                            committed_updates=[], committed_reads=[], invalid_aborted=0}, ClientDict),
                            DepDict1 = dict:erase(TxId, DepDict),
                            {reply, {cascade_abort, {AbortedTxId, [TxId|AbortedReads], rev(CommittedUpdates), CommittedReads}}, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}}
                    end;
                    %% Some read is invalid even before the txn starts.. If invalid_aborted is larger than 0, it can possibly be saved.
                {0, {ok, {0, B, ToDeleteLOC, 0}}} ->
                    case ReadDepTxs of
                        B ->
                            lager:warning("Returning specula_commit for ~w: ~w", [TxId, os:timestamp()]),
                            gen_server:reply(Sender, {ok, {specula_commit, LastCommitTs, {AbortedReads, rev(CommittedUpdates), [TxId|CommittedReads]}}}),
                            DepDict1 = dict:erase(TxId, DepDict),
                            ClientDict1 = dict:store(Client, ClientState#c_state{tx_id=?NO_TXN, aborted_reads=
                                [], committed_updates=[], committed_reads=[]}, ClientDict),
                            {noreply, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}};
                        _ ->
                            lager:warning("Returning specula_commit for ~w: ~w", [TxId, os:timestamp()]),
                            gen_server:reply(Sender, {ok, {specula_commit, LastCommitTs, {AbortedReads, rev(CommittedUpdates), CommittedReads}}}),
                            DepDict1 = dict:store(TxId, 
                                    {read_only, delete_some_elems(B, ReadDepTxs), ToDeleteLOC, 0}, DepDict),
                            ClientDict1 = dict:store(Client, ClientState#c_state{tx_id=?NO_TXN, aborted_reads=
                                [], committed_updates=[], committed_reads=[]}, ClientDict),
                            {noreply, SD0#state{client_dict=ClientDict1, dep_dict=DepDict1}}
                    end
            end;
        false ->
            ClientDict1 = dict:store(Client, ClientState#c_state{tx_id=?NO_TXN}, ClientDict),
            DepDict1 = dict:erase(TxId, DepDict),
            {reply, wrong_msg, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}}
            %case ClientState#c_state.aborted_update of
            %    ?NO_TXN -> 
            %        ClientDict1 = dict:store(Client, ClientState#c_state{tx_id=?NO_TXN, aborted_update=?NO_TXN, aborted_reads=[],
            %                committed_updates=[], committed_reads=[], invalid_aborted=0}, ClientDict),
            %        DepDict1 = dict:erase(TxId, DepDict),
            %        {reply, {aborted, {AbortedReads, rev(CommittedUpdates), rev(CommittedReads)}}, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}};
            %    AbortedTxId ->
            %        ClientDict1 = dict:store(Client, ClientState#c_state{tx_id=?NO_TXN, aborted_update=?NO_TXN, aborted_reads=[],
            %                        committed_updates=[], committed_reads=[], invalid_aborted=0}, ClientDict),
            %        DepDict1 = dict:erase(TxId, DepDict),
            %        {reply, {cascade_abort, {AbortedTxId, AbortedReads, rev(CommittedUpdates), rev(CommittedReads)}}, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}}
            %end
    end;

handle_call({abort_txn, TxId}, Sender, SD0) ->
    {Client, _} = Sender,
    handle_call({abort_txn, TxId, Client}, Sender, SD0);
handle_call({abort_txn, TxId, Client}, _Sender, SD0=#state{dep_dict=DepDict, client_dict=ClientDict}) ->
    %% If there was a legacy ongoing transaction.
    true = ets:delete(anti_dep, TxId),
    ClientState = dict:fetch(Client, ClientDict),
    DepDict1 = dict:erase(TxId, DepDict),
    AbortedReads = ClientState#c_state.aborted_reads,
    CommittedReads = ClientState#c_state.committed_reads,
    CommittedUpdates = ClientState#c_state.committed_updates,
    ClientDict1 = dict:store(Client, ClientState#c_state{tx_id=?NO_TXN, aborted_update=?NO_TXN, aborted_reads=[],
            committed_updates=[], committed_reads=[], invalid_aborted=0}, ClientDict),
    lager:warning("Aborting ~w, committed updates are ~w", [TxId, CommittedUpdates]),
    {reply, {aborted, {AbortedReads, rev(CommittedUpdates), CommittedReads}}, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}};

handle_call({certify_update, TxId, LocalUpdates, RemoteUpdates, ClientMsgId, NewSpeculaLength}, Sender, SD0=#state{specula_length=SpeculaLength}) ->
    case NewSpeculaLength of
        SpeculaLength ->
            handle_call({certify_update, TxId, LocalUpdates, RemoteUpdates, ClientMsgId}, Sender, SD0);
        ignore ->
            handle_call({certify_update, TxId, LocalUpdates, RemoteUpdates, ClientMsgId}, Sender, SD0);
        _ ->
            handle_call({certify_update, TxId, LocalUpdates, RemoteUpdates, ClientMsgId}, Sender, SD0#state{specula_length=NewSpeculaLength})
    end;
handle_call({certify_update, TxId, LocalUpdates, RemoteUpdates, ClientMsgId}, Sender, SD0=#state{rep_dict=RepDict, pending_txs=PendingTxs, total_repl_factor=ReplFactor, min_commit_ts=LastCommitTs, specula_length=SpeculaLength, dep_dict=DepDict, client_dict=ClientDict, hit_counter=HitCounter}) ->
    %% If there was a legacy ongoing transaction.
    {Client, _} = Sender,
    {MyLOCList, MyFFC, ReadDepTxs} = case ets:lookup(anti_dep, TxId) of
                    [] -> {[], 0, []};
                    [{TxId, {_LOC, LOCList}, FFC, Deps}] -> {LOCList, FFC, Deps} 
                 end,
    true = ets:delete(anti_dep, TxId),
      lager:warning("Start certifying ~w, readDepTxs is ~w, Sender is ~w, local parts are ~w, remote parts are ~w", [TxId, ReadDepTxs, Sender, LocalUpdates, RemoteUpdates]),
    ClientState = dict:fetch(Client, ClientDict),
    PendingList = ClientState#c_state.pending_list, 
    InvalidAborted = ClientState#c_state.invalid_aborted,
    TxId = ClientState#c_state.tx_id, 
    SentMsgId = ClientState#c_state.msg_id,
    AbortedReads = ClientState#c_state.aborted_reads,
    CommittedReads = ClientState#c_state.committed_reads,
    CommittedUpdates = ClientState#c_state.committed_updates,
    case SentMsgId == ClientMsgId of
        true ->
            case {InvalidAborted, dict:find(TxId, DepDict)} of
                {1, _} ->
                      lager:warning("InvalidAborted ~w", [InvalidAborted]),
                    case ClientState#c_state.aborted_update of
                        ?NO_TXN -> 
                            ClientDict1 = dict:store(Client, ClientState#c_state{tx_id=?NO_TXN, aborted_update=?NO_TXN, aborted_reads=[],
                                    committed_updates=[], committed_reads=[], invalid_aborted=0}, ClientDict),
                            DepDict1 = dict:erase(TxId, DepDict),
                            {reply, {aborted, {AbortedReads, rev(CommittedUpdates), CommittedReads}}, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}};
                        AbortedTxId ->
                            ClientDict1 = dict:store(Client, ClientState#c_state{tx_id=?NO_TXN, aborted_update=?NO_TXN, aborted_reads=[],
                                            committed_updates=[], committed_reads=[], invalid_aborted=0}, ClientDict),
                            DepDict1 = dict:erase(TxId, DepDict),
                            {reply, {cascade_abort, {AbortedTxId, AbortedReads, rev(CommittedUpdates), CommittedReads}}, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}}
                    end;
                {0, {ok, {_, B, ToRemoveLOC, _}}} ->
                    case (LocalUpdates == []) and (RemoteUpdates == []) of 
                        true ->
                            case (ReadDepTxs == B) and (PendingList == []) of
                                true ->
                                    gen_server:reply(Sender, {ok, {committed, LastCommitTs, {AbortedReads, rev(CommittedUpdates), CommittedReads}}}),
                                    DepDict1 = dict:erase(TxId, DepDict),
                                    ClientDict1 = dict:store(Client, ClientState#c_state{tx_id=?NO_TXN, aborted_reads=
                                          [], committed_updates=[], committed_reads=[]}, ClientDict),
                                    {noreply, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}}; 
                                false ->
                                    case length(PendingList) >= SpeculaLength of
                                        true -> %% Wait instead of speculate.
                                            DepDict1 = dict:store(TxId, 
                                                    {0, delete_some_elems(B, ReadDepTxs), ToRemoveLOC, LastCommitTs+1}, DepDict),
                                            ClientDict1 = dict:store(Client, ClientState#c_state{tx_id=TxId, local_updates=[], 
                                                remote_updates=[], stage=remote_cert, sender=Sender}, ClientDict),
                                            {noreply, SD0#state{dep_dict=DepDict1, hit_counter=HitCounter+1, client_dict=ClientDict1}};
                                        false -> %% Can speculate. After replying, removing TxId
                                            %% Update specula data structure, and clean the txid so we know current txn is already replied
                                             lager:warning("Returning specula_commit for ~w, ReadDepTxs are ~w, B is ~w, time is ~w", [TxId, ReadDepTxs, B, os:timestamp()]),
                                            gen_server:reply(Sender, {ok, {specula_commit, LastCommitTs+1, {AbortedReads,
                                                      rev(CommittedUpdates), CommittedReads}}}),
                                            DepDict1 = dict:store(TxId, 
                                                    {0, delete_some_elems(B, ReadDepTxs), ToRemoveLOC, LastCommitTs+1}, DepDict),
                                            PendingTxs1 = dict:store(TxId, {[], []}, PendingTxs),
                                            ClientDict1 = dict:store(Client, ClientState#c_state{tx_id=?NO_TXN,
                                                            pending_list = PendingList ++ [TxId],  aborted_reads=
                                                    [], committed_updates=[], committed_reads=[]}, ClientDict),
                                            %ets:insert(ClientDict, {TxId, {[], RemotePartitions, StartTime, os:timestamp(), no_wait}}),
                                            {noreply, SD0#state{dep_dict=DepDict1, min_snapshot_ts=LastCommitTs+1, 
                                                client_dict=ClientDict1, pending_txs=PendingTxs1}}
                                    end
                            end;
                        false ->
                            %% Certify local partitions that are updated:
                            %% 1. Master partitions of upated kyes
                            %% 2. Slave partitions of updated keys
                            %% 3. Cache partition if the txn updated non-local keys
                            {LocalParts, NumLocalParts, NumRemoteParts, NumCacheParts} 
                                    = local_certify(LocalUpdates, RemoteUpdates, TxId, RepDict),
                            NumToAck = NumLocalParts + NumRemoteParts + NumCacheParts,

                            RemainLOC = delete_some_elems(ToRemoveLOC, MyLOCList),

                            {PendingPrepares, NewLOCList} = case NumCacheParts of 
                                                0 -> {(NumLocalParts+NumRemoteParts)*(ReplFactor-1), RemainLOC};
                                                _ -> {(NumLocalParts+NumRemoteParts)*(ReplFactor-1) + ReplFactor*NumCacheParts,
                                                        [TxId#tx_id.snapshot_time|RemainLOC]}
                                              end,
                            lager:warning("NumToAck is ~w, Pending prepares are ~w", [NumToAck, PendingPrepares]),

                            DepDict1 = dict:store(TxId, {NumToAck, delete_some_elems(B, ReadDepTxs), [], LastCommitTs+1}, DepDict),
                            ClientDict1 = dict:store(Client, ClientState#c_state{pending_prepares=PendingPrepares,
                                local_updates=LocalParts, remote_updates=RemoteUpdates, 
                                stage=local_cert, sender=Sender, ffc=MyFFC, loc_list=NewLOCList}, ClientDict),
                            {noreply, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}}
                    end
            end;
        false ->
            ClientDict1 = dict:store(Client, ClientState#c_state{tx_id=?NO_TXN}, ClientDict),
            DepDict1 = dict:erase(TxId, DepDict),
            {reply, wrong_msg, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}}
            % lager:warning("~w: invalid message id!! My is ~w, from client is ~w, aborted update is ~w", [TxId, SentMsgId, ClientMsgId, ClientState#c_state.aborted_update]),
            %case ClientState#c_state.aborted_update of
            %    ?NO_TXN -> 
            %        ClientDict1 = dict:store(Client, ClientState#c_state{tx_id=?NO_TXN, aborted_update=?NO_TXN, aborted_reads=[],
            %                committed_updates=[], committed_reads=[], invalid_aborted=0}, ClientDict),
            %        DepDict1 = dict:erase(TxId, DepDict),
            %        {reply, {aborted, {AbortedReads, rev(CommittedUpdates), rev(CommittedReads)}}, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}};
            %    AbortedTxId ->
            %        ClientDict1 = dict:store(Client, ClientState#c_state{tx_id=?NO_TXN, aborted_update=?NO_TXN, aborted_reads=[],
            %                        committed_updates=[], committed_reads=[], invalid_aborted=0}, ClientDict),
            %        DepDict1 = dict:erase(TxId, DepDict),
            %        {reply, {cascade_abort, {AbortedTxId, AbortedReads, rev(CommittedUpdates), rev(CommittedReads)}}, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}}
            %end
    end;

handle_call({read, Key, TxId, Node}, Sender, SD0=#state{specula_read=SpeculaRead}) ->
    handle_call({read, Key, TxId, Node, SpeculaRead}, Sender, SD0);
handle_call({read, Key, TxId, Node, SpeculaRead}, Sender, SD0) ->
    %% Add logic to put SSS and LOC
    ?CLOCKSI_VNODE:relay_read(Node, Key, TxId, Sender, SpeculaRead),
    {noreply, SD0};

handle_call({get_oldest}, _Sender, SD0=#state{client_dict=ClientDict}) ->
    PendList = dict:to_list(ClientDict),
    Result = lists:foldl(fun({Key, Value}, Oldest) ->
                case is_pid(Key) of
                    true ->
                            case Value#c_state.pending_list of [] -> 
                                case Value#c_state.tx_id of 
                                    ?NO_TXN -> Oldest;
                                    TId ->
                                        case (Oldest == ?NO_TXN) of
                                            true -> TId;
                                            false -> case TId#tx_id.snapshot_time < Oldest#tx_id.snapshot_time of
                                                            true -> TId;  false -> Oldest 
                                                     end
                                        end
                                end;
                            [H|_T] -> case Oldest of ?NO_TXN -> H;
                                    _ -> case H#tx_id.snapshot_time < Oldest#tx_id.snapshot_time of
                                              true -> H;  false -> Oldest
                                         end 
                                     end
                            end;
                    false -> Oldest
                end end, ?NO_TXN, PendList),
    case Result of nil -> {reply, nil, SD0};
                   _->  {reply, Result, SD0}
    end;

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

%handle_cast({trace_pending}, SD0=#state{pending_list=PendingList, dep_dict=DepDict}) ->
%    case PendingList of [] ->  lager:info("No pending!");
%                        [H|_T] -> send_prob(DepDict, H, [], self())
%    end,
%    {noreply, SD0}; 

handle_cast({load, Sup, Type, Param}, SD0) ->
    lager:info("Got load req!"),
    case Type of tpcc -> tpcc_load:load(Param);
                 micro -> micro_load:load(Param);
                 rubis -> rubis_load:load(Param)
    end,
    lager:info("Finished loading!"),
    Sup ! done,
    lager:info("Replied!"),
    {noreply, SD0};

handle_cast({read_blocked, TxId, LastLOC, LastFFC, Value, Sender}, SD0=#state{dep_dict=DepDict, client_dict=ClientDict, num_blocked=NB}) ->
    lager:warning("Read is blocked for ~w, LastLOC is ~w, LastFFC is ~w !", [TxId, LastLOC, LastFFC]),
    ClientState = dict:fetch(TxId#tx_id.client_pid, ClientDict),
    case ClientState#c_state.invalid_aborted of
        1 -> 
            case ClientState#c_state.tx_id of
                TxId ->
                    ets:insert(anti_dep, {TxId, {inf, []}, 0, []}),
                    gen_server:reply(Sender, {ok, Value}),
                    {noreply, SD0#state{dep_dict=dict:store(TxId, {0, [], [], 0}, DepDict)}}; 
                _ -> ok
            end;
        _ ->
            case ets:lookup(anti_dep, TxId) of
                [] -> lager:warning("Anti dep is empty!!!"), TxId=error,
                    {noreply, SD0};
                [{TxId, {_LOC, LOCList}, FFC, Deps}] ->
                    case dict:fetch(TxId, DepDict) of 
                        {0, SolvedReadDeps, ToRemoveLOC, 0} -> 
                            NewFFC = max(FFC, LastFFC),
                            RemainLOC = case LastLOC of inf -> delete_some_elems(ToRemoveLOC, LOCList); 
                                                        _ -> delete_some_elems(ToRemoveLOC, [LastLOC|LOCList])
                                        end,
                            CurrentLOC = case RemainLOC of [] -> inf; _ -> lists:min(RemainLOC) end,
                            RemainDeps = delete_some_elems(SolvedReadDeps, Deps),
                            case CurrentLOC >= NewFFC of
                                true ->
                                   lager:warning("LastFFC ~w, New ~w, LOC ~w, RemainLOC ~w, Replying to ~w", [LastFFC, NewFFC, LOCList, RemainLOC, Sender]),
                                    ets:insert(anti_dep, {TxId, {CurrentLOC, RemainLOC}, NewFFC, RemainDeps}),
                                    gen_server:reply(Sender, {ok, Value}),
                                    {noreply, SD0#state{dep_dict=dict:store(TxId, {0, [], [], 0}, DepDict)}};
                                false ->
                                    lager:warning("LastFFC ~w, New ~w, LOC ~w, RemainLOC ~w, reader is blocked",[LastFFC, NewFFC, LOCList, RemainLOC]),
                                    ClientState = dict:fetch(TxId#tx_id.client_pid, ClientDict),
                                    case ClientState#c_state.invalid_aborted of
                                        1 -> 
                                            lager:warning("Reply directly for ~w to ~w", [TxId, Sender]),
                                            ets:insert(anti_dep, {TxId, {inf, []}, 0, []}),
                                            gen_server:reply(Sender, {ok, Value}),
                                            {noreply, SD0#state{dep_dict=dict:store(TxId, {0, [], [], 0}, DepDict)}}; 
                                        _ ->
                                            lager:warning("~w blocked", [TxId]),
                                            {noreply, SD0#state{dep_dict=dict:store(TxId, {0, RemainDeps, RemainLOC, NewFFC, 0, {ok, Value}, Sender, os:timestamp()}, DepDict), num_blocked=NB+1}}
                                    end
                            end
                    end
            end
    end;

handle_cast({rr_value, TxId, Sender, TS, Value}, SD0=#state{dep_dict=DepDict, client_dict=ClientDict, num_blocked=NB}) ->
    lager:warning("Remote read result for ~w is ~w!", [TxId, Value]),
    ClientState = dict:fetch(TxId#tx_id.client_pid, ClientDict),
    case ClientState#c_state.invalid_aborted of
        1 -> 
            case ClientState#c_state.tx_id of
                TxId ->
                    ets:insert(anti_dep, {TxId, {inf, []}, 0, []}),
                    gen_server:reply(Sender, {ok, Value}),
                    {noreply, SD0#state{dep_dict=dict:store(TxId, {0, [], [], 0}, DepDict)}}; 
                _ -> ok
            end;
        _ ->
            case ets:lookup(anti_dep, TxId) of
                [] -> 
                    ets:insert(anti_dep, {TxId, {inf, []}, TS, []}),
                    lager:warning("First time replying, Sender is ~w", [Sender]),
                    gen_server:reply(Sender, {ok,Value}),
                    {noreply, SD0};
                [{TxId, {_LOC, LOCList}, FFC, Deps}]=_AntiDep ->
                    lager:warning("Get blocked, LOCList is ~w, FFC is ~w", [LOCList, FFC]),
                    case TS =< FFC of
                        true -> lager:warning("Directly replying to ~w", [Sender]),
                                gen_server:reply(Sender, {ok,Value}), 
                                {noreply, SD0};
                        false ->
                            case dict:fetch(TxId, DepDict) of 
                                {0, SolvedReadDeps, ToRemoveLOC, 0}=_Entry -> 
                                    RemainLOC = delete_some_elems(ToRemoveLOC, LOCList),
                                    CurrentLOC = case RemainLOC of [] -> inf; _ -> lists:min(RemainLOC) end,
                                    RemainDeps = delete_some_elems(SolvedReadDeps, Deps),
                                    case CurrentLOC >= TS of
                                        true ->
                                            lager:warning("Actually unblocked, CurrentLOC is ~w, TS is ~w", [CurrentLOC, TS]),
                                            ets:insert(anti_dep, {TxId, {CurrentLOC, RemainLOC}, TS, RemainDeps}),
                                            gen_server:reply(Sender, {ok, Value}),
                                            {noreply, SD0#state{dep_dict=dict:store(TxId, {0, [], [], 0}, DepDict)}};
                                        false ->
                                            lager:warning("Get blocked, TS is ~w, AntiDep is ~w, Entry is ~w", [TS, _AntiDep, _Entry]),
                                            ClientState = dict:fetch(TxId#tx_id.client_pid, ClientDict),
                                            case ClientState#c_state.invalid_aborted of
                                                1 -> 
                                                    ets:insert(anti_dep, {TxId, {inf, []}, 0, []}),
                                                    gen_server:reply(Sender, {ok, Value}),
                                                    {noreply, SD0#state{dep_dict=dict:store(TxId, {0, [], [], 0}, DepDict)}}; 
                                                _ ->
                                                    {noreply, SD0#state{dep_dict=dict:store(TxId, {0, RemainDeps, RemainLOC, TS, 0, {ok, Value}, Sender, os:timestamp()}, DepDict), num_blocked=NB+1}}
                                            end
                                    end
                            end
                    end
            end
    end;

handle_cast({end_trace, TList, InfoList}, SD0) ->
    lager:info("List txn is ~p, info is ~p", [TList, lists:flatten(InfoList)]),
    {noreply, SD0};

handle_cast({clean_data, Sender}, #state{name=Name}) ->
    antidote_config:load("antidote.config"),
    {SpeculaLength, SpeculaRead} = load_config(), 
    %DoRepl = antidote_config:get(do_repl),
    RepDict = hash_fun:build_rep_dict(true),
    PendingTxs = dict:new(),
    ClientDict = dict:new(),
    [{_, Replicas}] = ets:lookup(meta_info, node()),
    TotalReplFactor = length(Replicas)+1,
    %SpeculaData = tx_utilities:open_private_table(specula_data),
    Sender ! cleaned,
    {noreply, #state{pending_txs=PendingTxs, dep_dict=dict:new(), name=Name, client_dict=ClientDict, 
            specula_length=SpeculaLength, specula_read=SpeculaRead, rep_dict=RepDict, total_repl_factor=TotalReplFactor, 
            time_blocked=0, num_blocked=0}};

%% Receiving local prepare. Can only receive local prepare for two kinds of transaction
%%  Current transaction that has not finished local cert phase
%%  Transaction that has already cert_aborted.
%% Has to add local_cert here, because may receive pending_prepared, prepared from the same guy.
handle_cast({pending_prepared, TxId, PrepareTime}, 
	    SD0=#state{dep_dict=DepDict, specula_length=SpeculaLength, client_dict=ClientDict, hit_counter=HitCounter,
         pending_txs=PendingTxs,rep_dict=RepDict}) ->  
    Client = TxId#tx_id.client_pid,
    case dict:find(Client, ClientDict) of
        error ->
            lager:warning("Got pending preapr for ~w, but nothing", [TxId]),
            {noreply, SD0};
        {ok, ClientState} ->
    Stage = ClientState#c_state.stage,
    MyTxId = ClientState#c_state.tx_id, 
    case (Stage == local_cert) and (MyTxId == TxId) of
        true -> 
            Sender = ClientState#c_state.sender,
            PendingList = ClientState#c_state.pending_list,
            LocalParts = ClientState#c_state.local_updates, 
            RemoteUpdates = ClientState#c_state.remote_updates, 
            PendingPrepares = ClientState#c_state.pending_prepares, 
            LOCList = ClientState#c_state.loc_list,
            FFC = ClientState#c_state.ffc,
             lager:warning("Speculative receive pending_prepared for ~w, current pp is ~w", [TxId, PendingPrepares+1]),
            case dict:find(TxId, DepDict) of
                %% Maybe can commit already.
                {ok, {1, ReadDepTxs, ToDeleteLOC, OldPrepTime}} ->
                    NewMaxPrep = max(PrepareTime, OldPrepTime),
                    RemoteParts = [P || {P, _} <- RemoteUpdates],
                    LOC = case delete_some_elems(ToDeleteLOC, LOCList) of [] -> inf; List -> lists:min(List) end,
                    case length(PendingList) >= SpeculaLength of
                        true ->
                              lager:warning("Pending prep: decided to wait and prepare ~w, pending list is ~w!!", [TxId, PendingList]),
                            ?CLOCKSI_VNODE:prepare(RemoteUpdates, TxId, {remote, node()}),
                            pre_commit(LocalParts, RemoteParts, TxId, NewMaxPrep, RepDict, LOC, FFC),
                            DepDict1 = dict:store(TxId, {PendingPrepares+1, ReadDepTxs, LOC, NewMaxPrep}, DepDict),
                            ClientDict1 = dict:store(Client, ClientState#c_state{stage=remote_cert, remote_updates=RemoteParts}, ClientDict),
                            {noreply, SD0#state{dep_dict=DepDict1, hit_counter=HitCounter+1, client_dict=ClientDict1}};
                        false ->
                              lager:warning("Pending prep: decided to speculate ~w and prepare to ~w pending list is ~w!!", [TxId, RemoteUpdates, PendingList]),
                            AbortedReads = ClientState#c_state.aborted_reads,
                            CommittedReads = ClientState#c_state.committed_reads,
                            CommittedUpdates = ClientState#c_state.committed_updates,
                            PendingTxs1 = dict:store(TxId, {LocalParts, RemoteParts}, PendingTxs),
                            pre_commit(LocalParts, RemoteParts, TxId, NewMaxPrep, RepDict, LOC, FFC),
                            ?CLOCKSI_VNODE:prepare(RemoteUpdates, NewMaxPrep, TxId, {remote, node()}),
                             lager:warning("Returning specula_commit for ~w, time is ~w", [TxId, os:timestamp()]),
                            gen_server:reply(Sender, {ok, {specula_commit, NewMaxPrep, {AbortedReads,
                                                      rev(CommittedUpdates), CommittedReads}}}),
                            DepDict1 = dict:store(TxId, {PendingPrepares+1, ReadDepTxs, LOC, NewMaxPrep}, DepDict),
                            ClientDict1 = dict:store(Client, ClientState#c_state{tx_id=?NO_TXN, pending_list=PendingList++[TxId],committed_updates=[], committed_reads=[], aborted_reads=[]}, ClientDict),
                            {noreply, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1, pending_txs=PendingTxs1}}
                    end;
                {ok, {N, ReadDeps, ToDeleteLOC, OldPrepTime}} ->
                       lager:warning("~w needs ~w local prep replies", [TxId, N-1]),
                    DepDict1 = dict:store(TxId, {N-1, ReadDeps, ToDeleteLOC, max(PrepareTime, OldPrepTime)}, DepDict),
                    ClientDict1 = dict:store(Client, ClientState#c_state{pending_prepares=PendingPrepares+1}, ClientDict),
                    {noreply, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}};
                error ->
                    {noreply, SD0}
            end;
        _ ->
            lager:warning("Got pending preapr for ~w, but nothing", [TxId]),
            {noreply, SD0}
    end
    end;

handle_cast({solve_pending_prepared, TxId, PrepareTime, _From}, 
	    SD0=#state{dep_dict=DepDict, client_dict=ClientDict}) ->
     lager:warning("Got solve pending prepare for ~w from ~w", [TxId, _From]),
    Client = TxId#tx_id.client_pid,
    case dict:find(Client, ClientDict) of
        error -> {noreply, SD0};
        {ok, ClientState} ->
    Stage = ClientState#c_state.stage,
    MyTxId = ClientState#c_state.tx_id, 

    case (Stage == local_cert) and (MyTxId == TxId) of
        true -> 
            PendingPrepares = ClientState#c_state.pending_prepares, 
            ClientDict1 = dict:store(Client, ClientState#c_state{pending_prepares=PendingPrepares-1}, ClientDict),
            {noreply, SD0#state{client_dict=ClientDict1}};
        false ->
            case dict:find(TxId, DepDict) of
                {ok, {1, [], LOC, OldPrepTime}} -> %% Maybe the transaction can commit 
                    NowPrepTime =  max(OldPrepTime, PrepareTime),
                    DepDict1 = dict:store(TxId, {0, [], LOC, NowPrepTime}, DepDict),
                    {noreply, try_solve_pending([{NowPrepTime, LOC, TxId}], [], SD0#state{dep_dict=DepDict1}, [])};
                {ok, {PrepDeps, ReadDeps, LOC, OldPrepTime}} -> %% Maybe the transaction can commit 
                       lager:warning("~w not enough.. Prep ~w, Read ~w", [TxId, PrepDeps, ReadDeps]),
                    DepDict1=dict:store(TxId, {PrepDeps-1, ReadDeps, LOC, max(PrepareTime, OldPrepTime)}, DepDict),
                    {noreply, SD0#state{dep_dict=DepDict1}};
                error ->
                    {noreply, SD0}
            end
    end
    end;

handle_cast({prepared, TxId, PrepareTime, _From}, 
	    SD0=#state{dep_dict=DepDict, specula_length=SpeculaLength, hit_counter=HitCounter, 
            client_dict=ClientDict, rep_dict=RepDict, pending_txs=PendingTxs}) ->
    Client = TxId#tx_id.client_pid,
    case dict:find(Client, ClientDict) of
        error -> {noreply, SD0};
        {ok, ClientState} ->
    Stage = ClientState#c_state.stage,
    Sender = ClientState#c_state.sender,
    MyTxId = ClientState#c_state.tx_id, 
    AbortedRead = ClientState#c_state.aborted_reads,
    CommittedUpdated = ClientState#c_state.committed_updates,
    CommittedReads = ClientState#c_state.committed_reads,

    case (Stage == local_cert) and (MyTxId == TxId) of
        true -> 
            lager:warning("Got prepare for ~w, prepare time is ~w for local from ~w", [TxId, PrepareTime, _From]),
            PendingList = ClientState#c_state.pending_list,
            LocalParts = ClientState#c_state.local_updates, 
            RemoteUpdates = ClientState#c_state.remote_updates, 
            PendingPrepares = ClientState#c_state.pending_prepares, 
            LOCList = ClientState#c_state.loc_list, 
            FFC = ClientState#c_state.ffc, 
            case dict:find(TxId, DepDict) of
                %% Maybe can commit already.
                {ok, {1, ReadDepTxs, ToDeleteLOC, OldPrepTime}} ->
                    NewMaxPrep = max(PrepareTime, OldPrepTime),
                    RemoteParts = [P || {P, _} <- RemoteUpdates],
                    RemoteToAck = length(RemoteParts),
                    LOC = case delete_some_elems(ToDeleteLOC, LOCList) of [] -> inf; List -> lists:min(List) end,
                    case (RemoteToAck == 0) and (ReadDepTxs == [])
                          and (PendingList == []) and (PendingPrepares == 0) of
                        true ->
                            SD1 = try_solve_pending([{NewMaxPrep, LOC, TxId}], [], SD0, []), 
                            {noreply, SD1}; 
                        false ->
                              lager:warning("Pending list is ~w, pending prepares is ~w, ReadDepTxs is ~w", [PendingList, PendingPrepares, ReadDepTxs]),
                            case length(PendingList) >= SpeculaLength of
                                true -> 
                                    %%In wait stage, only prepare and doesn't add data to table
                                      lager:warning("Decided to wait and prepare ~w, pending list is ~w, sending to ~w!!", [TxId, PendingList, RemoteParts]),
                                    pre_commit(LocalParts, RemoteParts, TxId, NewMaxPrep, RepDict, LOC, FFC),
                                    ?CLOCKSI_VNODE:prepare(RemoteUpdates, TxId, {remote, node()}),
                                    DepDict1 = dict:store(TxId, {PendingPrepares, ReadDepTxs, LOC, NewMaxPrep}, DepDict),
                                    ClientDict1 = dict:store(Client, ClientState#c_state{stage=remote_cert, 
                                        remote_updates=RemoteParts}, ClientDict),
                                    {noreply, SD0#state{dep_dict=DepDict1, hit_counter=HitCounter+1, client_dict=ClientDict1}};
                                false ->
                                    %% Add dependent data into the table
                                    PendingTxs1 = dict:store(TxId, {LocalParts, RemoteParts}, PendingTxs),
                                    pre_commit(LocalParts, RemoteParts, TxId, NewMaxPrep, RepDict, LOC, FFC),
                                    ?CLOCKSI_VNODE:prepare(RemoteUpdates, NewMaxPrep, TxId, {remote, node()}),
                                    lager:warning("Returning specula_commit for ~w, time is ~w", [TxId, os:timestamp()]),
                                    gen_server:reply(Sender, {ok, {specula_commit, NewMaxPrep, {rev(AbortedRead), rev(CommittedUpdated), CommittedReads}}}),
                                    DepDict1 = dict:store(TxId, {PendingPrepares, ReadDepTxs, LOC, NewMaxPrep}, DepDict),
                                    ClientDict1 = dict:store(Client, ClientState#c_state{pending_list=PendingList++[TxId], tx_id=?NO_TXN, 
                                        aborted_update=?NO_TXN, aborted_reads=[], committed_reads=[], committed_updates=[]}, ClientDict),
                                    {noreply, SD0#state{dep_dict=DepDict1, min_snapshot_ts=NewMaxPrep, client_dict=ClientDict1, pending_txs=PendingTxs1}}
                            end
                        end;
                {ok, {N, ReadDeps, ToDeleteLOC, OldPrepTime}} ->
                      lager:warning("~w needs ~w local prep replies", [TxId, N-1]),
                    DepDict1 = dict:store(TxId, {N-1, ReadDeps, ToDeleteLOC, max(PrepareTime, OldPrepTime)}, DepDict),
                    {noreply, SD0#state{dep_dict=DepDict1}};
                error ->
                    {noreply, SD0}
            end;
        false ->
            lager:warning("Got prepare for ~w, prepare time is ~w  not in local from ~w", [TxId, PrepareTime, _From]),
            case dict:find(TxId, DepDict) of
                {ok, {1, [], LOC, OldPrepTime}} -> %% Maybe the transaction can commit 
                    NowPrepTime =  max(OldPrepTime, PrepareTime),
                    DepDict1 = dict:store(TxId, {0, [], LOC, NowPrepTime}, DepDict),
                    {noreply, try_solve_pending([{NowPrepTime, LOC, TxId}], [], SD0#state{dep_dict=DepDict1}, [])};
                {ok, {PrepDeps, ReadDeps, LOC, OldPrepTime}} -> %% Maybe the transaction can commit 
                        lager:warning("~w not enough.. Prep ~w, Read ~w", [TxId, PrepDeps, ReadDeps]),
                    DepDict1=dict:store(TxId, {PrepDeps-1, ReadDeps, LOC, max(PrepareTime, OldPrepTime)}, DepDict),
                    {noreply, SD0#state{dep_dict=DepDict1}};
                error ->
                    {noreply, SD0}
            end
    end
    end;

%handle_cast({prepared, _OtherTxId, _}, SD0=#state{stage=local_cert}) ->
%    {noreply, SD0}; 

%% TODO: if we don't direclty speculate after getting all local prepared, maybe we can wait a littler more
%%       and here we should check if the transaction can be directly committed or not. 
handle_cast({read_valid, PendingTxId, PendedTxId, PendedLOC}, SD0=#state{dep_dict=DepDict, client_dict=ClientDict, time_blocked=TB}) ->
      lager:warning("Got read valid for ~w of ~w", [PendingTxId, PendedTxId]),
    case dict:find(PendingTxId, DepDict) of
        {ok, {read_only, [PendedTxId], _, _ReadOnlyTs}} ->
            %gen_server:reply(Sender, {ok, {committed, ReadOnlyTs}}),
            ClientState = dict:fetch(PendingTxId#tx_id.client_pid, ClientDict),
            lager:warning("Committed reads are ~w, ~w", [PendingTxId, ClientState#c_state.committed_reads]),
            ClientState1 = ClientState#c_state{committed_reads=[PendingTxId|ClientState#c_state.committed_reads]},
            {noreply, SD0#state{dep_dict=dict:erase(PendingTxId, DepDict), client_dict=dict:store(PendingTxId#tx_id.client_pid,
                    ClientState1, ClientDict)}};
        {ok, {read_only, MoreDeps, ToRemoveLOC, ReadOnlyTs}} ->
            {noreply, SD0#state{dep_dict=dict:store(PendingTxId, {read_only, delete_elem(PendedTxId, MoreDeps), ToRemoveLOC, ReadOnlyTs}, DepDict)}};
        {ok, {0, ReadDeps, LOCList, FFC, 0, Value, Sender, Blocked}} ->
            RemainLOCList = delete_elem(PendedLOC, LOCList),
            MinLOC = case RemainLOCList of [] -> inf; _ -> lists:min(RemainLOCList) end,
            RemainDeps = delete_elem(PendedTxId, ReadDeps),
            case MinLOC >= FFC of
                true ->
                    lager:warning("Successfully Reduced blocked txn ~w, MinLOC is ~w, FFC is ~w", [PendingTxId, MinLOC, FFC]),
                    ets:insert(anti_dep, {PendingTxId, {MinLOC, RemainLOCList}, FFC, RemainDeps}),
                    gen_server:reply(Sender, Value),
                    DepDict1 = dict:store(PendingTxId, {0, [], [], 0}, DepDict), 
                    {noreply, SD0#state{dep_dict=DepDict1, time_blocked=TB+timer:now_diff(os:timestamp(), Blocked)}};
                false ->
                    lager:warning("Didn't reduce blocked txn ~w, LOCList is ~w, RemainLOCList is ~w, FFC is ~w", [PendingTxId, LOCList, RemainLOCList, FFC]),
                    DepDict1 = dict:store(PendingTxId, {0, RemainDeps, RemainLOCList, FFC, 0, Value, Sender, Blocked}, DepDict), 
                    {noreply, SD0#state{dep_dict=DepDict1}}
            end;
        {ok, {0, SolvedReadDeps, ToRemoveLOC, 0}} -> 
            %% Txn is still reading!!
            lager:warning("Still reading!"),
            {noreply, SD0#state{dep_dict=dict:store(PendingTxId, {0, [PendedTxId|SolvedReadDeps], [PendedLOC|ToRemoveLOC], 0}, DepDict)}};
        {ok, {0, [PendedTxId], LOC, OldPrepTime}} -> %% Maybe the transaction can commit, no need to update LOC again 
            lager:warning("Removign blocked txn"),
            SD1 = SD0#state{dep_dict=dict:store(PendingTxId, {0, [], LOC, OldPrepTime}, DepDict)},
            {noreply, try_solve_pending([{OldPrepTime, LOC, PendingTxId}], [], SD1, [])};
        {ok, {PrepDeps, ReadDepTxs, LOC, OldPrepTime}} ->  %% Still certifying, but not sure if local_cert or remote_cert
            lager:warning("Can not commit... Remaining prepdep is ~w, read dep is ~w, LOC is ~w", [PrepDeps, delete_elem(PendedTxId, ReadDepTxs), LOC]),
            case is_list(LOC) of
                true -> %% Before pre_commit
                    {noreply, SD0#state{dep_dict=dict:store(PendingTxId, {PrepDeps, delete_elem(PendedTxId, ReadDepTxs), [PendedLOC|LOC], 
                        OldPrepTime}, DepDict)}};
                false ->
                    {noreply, SD0#state{dep_dict=dict:store(PendingTxId, {PrepDeps, delete_elem(PendedTxId, ReadDepTxs), LOC, 
                        OldPrepTime}, DepDict)}}
            end;
        error ->
            {noreply, SD0}
    end;

handle_cast({read_invalid, _MyCommitTime, TxId}, SD0) ->
   lager:warning("Read invalid for ~w", [TxId]),
    {noreply, try_solve_pending([], [{ignore, TxId}], SD0, [])};
    %read_abort(read_invalid, MyCommitTime, TxId, SD0);
handle_cast({read_aborted, _MyCommitTime, TxId}, SD0) ->
    lager:warning("Read aborted for ~w", [TxId]),
    {noreply, try_solve_pending([], [{ignore, TxId}], SD0, [])};
    %read_abort(read_aborted, MyCommitTime, TxId, SD0);
            
handle_cast({aborted, TxId}, SD0) ->
    SD1 = try_solve_pending([], [{ignore, TxId}], SD0, []),
    {noreply, SD1};
handle_cast({aborted, TxId, FromNode}, SD0) ->
    SD1 = try_solve_pending([], [{FromNode, TxId}], SD0, []),
    {noreply, SD1};

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

delete_elem(_E, []) ->
    [];
delete_elem(inf, L) ->
    L;
delete_elem(E, [E|L]) ->
    delete_elem(E, L);
delete_elem(E, [H|L]) ->
    [H|delete_elem(E, L)].

delete_some_elems(ToDelete, List) ->
    lists:foldl(fun(E, LL) -> delete_elem(E, LL) end, List, ToDelete).

%%%%%%%%%%%%%%%%%%%%%
try_solve_pending([], [], SD0=#state{client_dict=ClientDict, rep_dict=RepDict, dep_dict=DepDict, pending_txs=PendingTxs,
            min_commit_ts=MinCommitTs, time_blocked=OldTimeBlocked}, ClientsOfCommTxns) ->
    {ClientDict1, DepDict1, PendingTxs1, NewMayCommit, NewToAbort, NewCommitTime, NewTimeBlocked} = 
        lists:foldl(fun(Client, {CD, DD, PD, MayCommit, ToAbort, PCommitTime, AccTB}) ->
            CState = dict:fetch(Client, CD),
            TxId = CState#c_state.tx_id,
            case TxId of
                ?NO_TXN -> {CD, DD, PD, MayCommit, ToAbort, PCommitTime, AccTB};
                _ ->
                    case CState#c_state.committed_updates of [] -> {CD, DD, PD, MayCommit, ToAbort, PCommitTime, AccTB};
                        _ ->
                            PendingList = CState#c_state.pending_list,
                            Sender = CState#c_state.sender,
                            LocalParts = CState#c_state.local_updates, 
                            case dict:fetch(TxId, DepDict) of 
                            {Prep, Read, LOC, OldCurPrepTime} ->
                                case CState#c_state.stage of
                                    remote_cert ->
                                        SpeculaPrepTime = max(PCommitTime+1, OldCurPrepTime),
                                        RemoteParts = CState#c_state.remote_updates,
                                          lager:warning("Spec comm curr txn: ~p, remote updates are ~p", [TxId, RemoteParts]),
                                        %specula_commit(LocalParts, RemoteParts, TxId, SpeculaPrepTime, RepDict),
                                        case (Prep == 0) and (Read == []) and (PendingList == []) of
                                            true ->   
                                                  lager:warning("Can already commit ~w!!", [TxId]),
                                                {DD1, NewMayCommit, NewToAbort, CD1, TB} = commit_tx(TxId, SpeculaPrepTime, 
                                                    LocalParts, RemoteParts, dict:erase(TxId, DD), RepDict, LOC, CD),
                                                CS1 = dict:fetch(Client, CD1),
                                                gen_server:reply(Sender, {ok, {committed, SpeculaPrepTime, {rev(CS1#c_state.aborted_reads), rev(CS1#c_state.committed_updates), CS1#c_state.committed_reads}}}),
                                                CD2 = dict:store(Client, CS1#c_state{committed_updates=[], committed_reads=[], aborted_reads=[],
                                                    pending_list=[], tx_id=?NO_TXN}, CD1),
                                                {CD2, DD1, PD, MayCommit++NewMayCommit, ToAbort++NewToAbort, SpeculaPrepTime, AccTB+TB};
                                            false ->
                                                lager:warning("Returning specula_commit for ~w, time is ~w", [TxId, os:timestamp()]),
                                                gen_server:reply(Sender, {ok, {specula_commit, SpeculaPrepTime, {rev(CState#c_state.aborted_reads), rev(CState#c_state.committed_updates), CState#c_state.committed_reads}}}),
                                                PD1 = dict:store(TxId, {LocalParts, RemoteParts}, PD),
                                                CD1 = dict:store(Client, CState#c_state{committed_updates=[], committed_reads=[], aborted_reads=[], pending_list=PendingList ++ [TxId], tx_id=?NO_TXN}, CD),
                                                {CD1, DD, PD1, MayCommit, ToAbort, PCommitTime, AccTB}
                                        end;
                                    _ -> {CD, DD, PD, MayCommit, ToAbort, PCommitTime, AccTB}
                                end;
                            _ -> {CD, DD, PD, MayCommit, ToAbort, PCommitTime, AccTB}
                            end
                    end
            end end, {ClientDict, DepDict, PendingTxs, [], [], MinCommitTs, 0}, ClientsOfCommTxns),
    SD1 = SD0#state{client_dict=ClientDict1, dep_dict=DepDict1, pending_txs=PendingTxs1, min_commit_ts=NewCommitTime, 
        time_blocked=OldTimeBlocked+NewTimeBlocked},
    case (NewMayCommit == []) and (NewToAbort == []) of
        true -> SD1;
        false ->
            try_solve_pending(NewMayCommit, NewToAbort, SD1, [])
    end;
try_solve_pending(ToCommitTxs, [{FromNode, TxId}|Rest], SD0=#state{client_dict=ClientDict, rep_dict=RepDict, pending_txs=PendingTxs, dep_dict=DepDict, time_blocked=TB}, ClientsOfCommTxns) ->
    Client = TxId#tx_id.client_pid,
    ClientState = dict:fetch(Client, ClientDict),
    CurrentTxId = ClientState#c_state.tx_id, 
    Sender = ClientState#c_state.sender, 
    PendingList = ClientState#c_state.pending_list, 
    Stage = ClientState#c_state.stage, 
    LocalParts = ClientState#c_state.local_updates, 
    RemoteUpdates = ClientState#c_state.remote_updates, 
    AbortedUpdate = ClientState#c_state.aborted_update,
    AbortedReads = ClientState#c_state.aborted_reads,
    CommittedReads = ClientState#c_state.committed_reads,
    CommittedUpdates = ClientState#c_state.committed_updates,
    case dict:find(TxId, DepDict) of
        {ok, {read_only, _, _, _}} ->
            ClientState1 = ClientState#c_state{aborted_reads=[TxId|ClientState#c_state.aborted_reads]},
            try_solve_pending(ToCommitTxs, Rest, SD0#state{dep_dict=dict:erase(TxId, DepDict), 
                    client_dict=dict:store(Client, ClientState1, ClientDict)}, ClientsOfCommTxns);
        DepEntry ->
            lager:warning("For ~w, DepEntry is ~w", [TxId, DepEntry]),
        case start_from_list(TxId, PendingList) of
        [] ->
            lager:warning("Did not find the tx in list, list is ~w", [PendingList]),
            case TxId of
                CurrentTxId ->
                    case Stage of
                        local_cert ->
                            RemoteParts = [P|| {P,_}<-RemoteUpdates],
                             lager:warning("~w abort local!", [TxId]),
                            NewToAbort = case FromNode of ignore -> abort_tx(TxId, LocalParts, RemoteParts, RepDict, local_cert);
                                                          _ ->  abort_tx(TxId, lists:delete(FromNode, LocalParts), RemoteParts, RepDict, local_cert)
                            end,
                            RD1 = dict:erase(TxId, DepDict),
                            gen_server:reply(Sender, {aborted, {AbortedReads, rev(CommittedUpdates), CommittedReads}}),
                            ClientDict1 = dict:store(Client, ClientState#c_state{tx_id=?NO_TXN, committed_updates=[], committed_reads=[], aborted_reads=[]}, ClientDict), 
                            try_solve_pending(ToCommitTxs, Rest++NewToAbort, 
                                SD0#state{dep_dict=RD1, client_dict=ClientDict1}, ClientsOfCommTxns);
                            %{noreply, SD0#state{dep_dict=RD1, read_aborted=RAD1, read_invalid=RID1, client_dict=ClientDict1}};
                        remote_cert ->
                              lager:warning("~w abort remote!", [TxId]),
                            RemoteParts = RemoteUpdates,
                            NewToAbort = case FromNode of [] -> abort_tx(TxId, LocalParts, RemoteParts, RepDict, remote_cert);
                                                          _ -> abort_tx(TxId, LocalParts, lists:delete(FromNode, RemoteParts), RepDict, remote_cert, FromNode)
                            end,
                            RD1 = dict:erase(TxId, DepDict),
                            gen_server:reply(Sender, {aborted, {AbortedReads, rev(CommittedUpdates), CommittedReads}}),
                            ClientDict1 = dict:store(Client, ClientState#c_state{tx_id=?NO_TXN, committed_updates=[], committed_reads=[], aborted_reads=[]}, ClientDict), 
                            try_solve_pending(ToCommitTxs, Rest++NewToAbort, 
                                SD0#state{dep_dict=RD1, client_dict=ClientDict1}, ClientsOfCommTxns);
                            %{noreply, SD0#state{dep_dict=RD1, client_dict=ClientDict1, read_aborted=RAD1, read_invalid=RID1}};
                        read ->
                            case DepEntry of
                                {ok, {0, RemainReadDeps, _RemainLOC, FFC, 0, Value, ReadSender, TimeBlocked}} ->
                                    %% Should actually abort here!!!!!
                                    ets:insert(anti_dep, {TxId, {inf, []}, FFC, RemainReadDeps}),
                                    gen_server:reply(ReadSender, Value),
                                    DepDict1 = dict:store(TxId, {0, [], [], 0}, DepDict),
                                    ClientDict1 = dict:store(Client, ClientState#c_state{invalid_aborted=1}, ClientDict), 
                                    try_solve_pending(ToCommitTxs, Rest, SD0#state{client_dict=ClientDict1, dep_dict=DepDict1, time_blocked=timer:now_diff(os:timestamp(), TimeBlocked)+TB}, ClientsOfCommTxns);
                                  _ -> 
                                    ClientDict1 = dict:store(Client, ClientState#c_state{invalid_aborted=1}, ClientDict), 
                                    try_solve_pending(ToCommitTxs, Rest, SD0#state{client_dict=ClientDict1}, ClientsOfCommTxns)
                            end
                    end;
                _ -> %% The transaction has already been aborted or whatever
                    lager:warning("~w is not current tx ~w", [TxId, CurrentTxId]),
                    try_solve_pending(ToCommitTxs, Rest, SD0, ClientsOfCommTxns)
            end;
        {Prev, L} ->
            lager:warning("List is ~w, Current txn is ~w", [L, CurrentTxId]),
            {PendingTxs1, RD, NewToAbort} = abort_specula_list(L, RepDict, DepDict, PendingTxs, FromNode, []),
            case CurrentTxId of
                ?NO_TXN ->
                     lager:warning("Abort due to read invlalid, pend tx is ~w, No current txn, Pendinglist is ~w, Prev is ~w", [PendingList, TxId, Prev]),
                    %% The clien is sleeping now! Need to reply msg.
                    MsgId = ClientState#c_state.msg_id,
                    %Client ! {final_abort, MsgId+1, TxId, AbortedReads, rev(CommittedUpdates), rev(CommittedReads)},
                    gen_fsm:send_event(Client, {final_abort, MsgId+1, TxId, AbortedReads, rev(CommittedUpdates), CommittedReads}),
                    ClientDict1 = dict:store(Client, ClientState#c_state{pending_list=Prev, msg_id=MsgId+1,
                            aborted_update=?NO_TXN, aborted_reads=[], committed_updates=[], committed_reads=[]}, ClientDict), 
                    try_solve_pending(ToCommitTxs, Rest++NewToAbort, SD0#state{dep_dict=RD, client_dict=ClientDict1, pending_txs=PendingTxs1}, ClientsOfCommTxns);
                    %{noreply, SD0#state{dep_dict=RD, client_dict=ClientDict2,
                    %    read_aborted=RAD1, read_invalid=RID1, cascade_aborted=CascadAborted+Length-1}};
                _ ->
                    case Stage of
                        local_cert -> 
                             lager:warning("Read invalid cascade Local abort: current tx is ~w, local parts are ~w, repdict is ~p", [CurrentTxId, LocalParts, dict:to_list(RepDict)]),
                            RemoteParts=  [P||{P,_}<-RemoteUpdates],
                            NewToAbort1 = abort_tx(CurrentTxId, LocalParts, RemoteParts, RepDict, local_cert),
                            gen_server:reply(Sender, {cascade_abort, {TxId, AbortedReads, rev(CommittedUpdates), CommittedReads}}),
                            RD1 = dict:erase(CurrentTxId, RD),
                            ClientDict1 = dict:store(Client, ClientState#c_state{tx_id=?NO_TXN, pending_list=Prev,
                                    aborted_update=?NO_TXN, aborted_reads=[], committed_updates=[], committed_reads=[]}, ClientDict), 
                            try_solve_pending(ToCommitTxs, Rest++NewToAbort++NewToAbort1, SD0#state{dep_dict=RD1, client_dict=ClientDict1, pending_txs=PendingTxs1}, ClientsOfCommTxns);
                            %{noreply, SD0#state{dep_dict=RD1, client_dict=ClientDict2,
                            %    read_aborted=RAD1, read_invalid=RID1, cascade_aborted=CascadAborted+Length}};
                        remote_cert -> 
                             lager:warning("Read invalid Remote abort: TxId is ~w, Pendinglist is ~w", [CurrentTxId, PendingList]),
                            RemoteParts = RemoteUpdates,
                            NewToAbort1 = abort_tx(CurrentTxId, LocalParts, RemoteParts, RepDict, remote_cert),
                            gen_server:reply(Sender, {cascade_abort, {TxId, AbortedReads, rev(CommittedUpdates), CommittedReads}}),
                            RD1 = dict:erase(CurrentTxId, RD),
                            ClientDict1 = dict:store(Client, ClientState#c_state{tx_id=?NO_TXN, pending_list=Prev,
                                aborted_update=?NO_TXN, aborted_reads=[], committed_updates=[], committed_reads=[]}, ClientDict), 
                            try_solve_pending(ToCommitTxs, Rest++NewToAbort++NewToAbort1, SD0#state{dep_dict=RD1, 
                                  client_dict=ClientDict1, pending_txs=PendingTxs1}, ClientsOfCommTxns);
                            %{noreply, SD0#state{dep_dict=RD1, client_dict=ClientDict2,
                            %    read_aborted=RAD1, read_invalid=RID1, cascade_aborted=CascadAborted+Length}};
                        read ->
                            lager:warning("Has current txn ~w read, Pendinglist is ~w, Rest is ~w", [CurrentTxId, PendingList, Rest]),
                            case dict:find(CurrentTxId, DepDict) of
                                {ok, {0, RemainReadDeps, _RemainLOC, FFC, 0, Value, ReadSender, BlockedTime}} ->
                                    %% Should actually abort here!!!!!
                                    lager:warning("Trying to reply ~w of Value ~w", [ReadSender, Value]),
                                    ets:insert(anti_dep, {CurrentTxId, {inf, []}, FFC, RemainReadDeps}),
                                    gen_server:reply(ReadSender, Value),
                                    RD1 = dict:store(CurrentTxId, {0, [], [], 0}, RD),
                                    ClientDict1 = dict:store(Client, ClientState#c_state{pending_list=Prev, invalid_aborted=1,
                                        aborted_update=get_older(AbortedUpdate, TxId)}, ClientDict), 
                                    try_solve_pending(ToCommitTxs, Rest++NewToAbort, SD0#state{dep_dict=RD1, 
                                          pending_txs=PendingTxs1, client_dict=ClientDict1, time_blocked=TB+timer:now_diff(os:timestamp(), BlockedTime)}, ClientsOfCommTxns);
                                _Entry -> 
                                    lager:warning("Entry is ~w", [_Entry]),
                                    ClientDict1 = dict:store(Client, ClientState#c_state{pending_list=Prev, invalid_aborted=1}, ClientDict), 
                                    try_solve_pending(ToCommitTxs, Rest++NewToAbort, SD0#state{pending_txs=PendingTxs1, 
                                        client_dict=ClientDict1}, ClientsOfCommTxns)
                           end
                            %{noreply, SD0#state{dep_dict=RD, client_dict=ClientDict2,
                            %    read_aborted=RAD1, read_invalid=RID1, cascade_aborted=CascadAborted+Length-1}}
                    end
            end
        end
    end;
try_solve_pending([{NowPrepTime, LOC, PendingTxId}|Rest], [], SD0=#state{client_dict=ClientDict, rep_dict=RepDict, dep_dict=DepDict, 
            pending_txs=PendingTxs, min_commit_ts=MinCommitTS, time_blocked=TB}, ClientsOfCommTxns) ->
    Client = PendingTxId#tx_id.client_pid,
    ClientState = dict:fetch(Client, ClientDict),
    PendingList = ClientState#c_state.pending_list, 
      lager:warning("Pending list is ~w, tx is ~w", [PendingList, PendingTxId]),
    Sender = ClientState#c_state.sender, 

    PendingList = ClientState#c_state.pending_list,
    LocalParts = ClientState#c_state.local_updates, 
    RemoteUpdates = ClientState#c_state.remote_updates, 
    CommittedUpdates = ClientState#c_state.committed_updates,
    AbortedReads = ClientState#c_state.aborted_reads,
    Stage = ClientState#c_state.stage,
    case PendingList of
        [] -> %% This is the just report_committed txn.. But new txn has not come yet.
              lager:warning("Pending list no me ~p, commit current tx!",[PendingTxId]),
            RemoteParts = case Stage of remote_cert -> RemoteUpdates; local_cert -> [P||{P, _} <-RemoteUpdates] end,
            DepDict2 = dict:erase(PendingTxId, DepDict),
            CurCommitTime = max(NowPrepTime, MinCommitTS+1),
            {DepDict3, MayCommTxs, ToAbortTxs, ClientDict1, NewTB} = commit_tx(PendingTxId, CurCommitTime, LocalParts, RemoteParts,
                DepDict2, RepDict, LOC, ClientDict),
            CS = dict:fetch(Client, ClientDict1),
            gen_server:reply(Sender, {ok, {committed, CurCommitTime, {AbortedReads, rev(CommittedUpdates), CS#c_state.committed_reads}}}),
            ClientDict2 = dict:store(Client, ClientState#c_state{tx_id=?NO_TXN, pending_list=[], aborted_reads=[],
                    committed_updates=[], committed_reads=[], aborted_update=?NO_TXN}, ClientDict1),
            try_solve_pending(Rest++MayCommTxs, ToAbortTxs, SD0#state{min_commit_ts=CurCommitTime, dep_dict=DepDict3, 
                          client_dict=ClientDict2, time_blocked=TB+NewTB}, ClientsOfCommTxns);
        [PendingTxId|ListRest] ->
             lager:warning("Pending list contain me ~p!",[PendingTxId]),
            CommitTime = max(NowPrepTime, MinCommitTS+1),
            {PendingTxs1, DepDict1, ToAbortTxs, MayCommitTxs, ClientDict1, NewTB} = commit_specula_tx(PendingTxId, CommitTime,
                    dict:erase(PendingTxId, DepDict), RepDict, PendingTxs, LOC, ClientDict),
            Now = os:timestamp(),

            {PendingTxs2, NewPendingList, NewMaxPT, DepDict2, NewToAbort, MyNext, ClientDict2, CommittedTxs, NewTB1}= 
                try_commit_follower(CommitTime, ListRest, RepDict, DepDict1, PendingTxs1, ClientDict1, [], [], [], 0), 
            CS2 = dict:fetch(Client, ClientDict2),
            lager:warning("~w committed, time is ~w", [PendingTxId, Now]),
            ClientDict3 = dict:store(Client, CS2#c_state{pending_list=NewPendingList, 
                    committed_updates=CommittedTxs++[{PendingTxId, Now}|CommittedUpdates]}, ClientDict2),
            try_solve_pending(Rest++MayCommitTxs++MyNext, Rest++ToAbortTxs++NewToAbort, SD0#state{min_commit_ts=NewMaxPT, dep_dict=DepDict2, 
                          pending_txs=PendingTxs2, client_dict=ClientDict3, time_blocked=TB+NewTB+NewTB1}, [Client|ClientsOfCommTxns]);
        _ ->
            %DepDict1 = dict:update(PendingTxId, fun({_, _, _}) ->
            %                            {0, [], NowPrepTime} end, DepDict),
               lager:warning("got all replies, but I am not the first! PendingList is ~w", [PendingList]),
            try_solve_pending(Rest, [], SD0, ClientsOfCommTxns)
            %SD0#state{dep_dict=DepDict1}
    end.

%decide_after_cascade(PendingList, DepDict, NumAborted, TxId, Stage) ->
%       lager:warning("PendingList ~w, DepDict ~w, NumAborted ~w, TxId ~w, Stage ~w", [PendingList, DepDict, NumAborted, TxId, Stage]),
%    case TxId of
%        ?NO_TXN -> wait;
%        _ -> 
%            case NumAborted > 0 of
%                true ->    lager:warning("Abort due to flow dep"),
%                        case Stage of read -> invalid; %% Abort due to flow dependency. Second indicate flow abort, third read invalid 
%                                     local_cert -> abort_local;
%                                     remote_cert -> abort_remote
%                        end;
%                false ->
%                    case dict:find(TxId, DepDict) of %% TODO: should give a invalid_aborted 
%                        error ->      lager:warning("Abort due to read dep"),
%                                 case Stage of read -> invalid;
%                                             local_cert -> abort_local;
%                                             remote_cert -> abort_remote
%                                 end;
%                        R ->
%                            case Stage of 
%                                remote_cert ->
%                                    case PendingList of 
%                                        [] -> 
%                                            case R of            
%                                                {ok, {0, [], PrepTime}} -> {commit, PrepTime};
%                                                 _ -> specula
%
%                                            end;
%                                        _ -> specula 
%                                    end;
%                                %% Can not specula commit during read or local_cert stage
%                                _ -> wait
%                            end
%                    end
%            end
%    end.

%% Same reason, no need for RemoteParts
commit_tx(TxId, CommitTime, LocalParts, RemoteParts, DepDict, RepDict, LOC, ClientDict) ->
    DepList = ets:lookup(dependency, TxId),
    {DepDict1, MayCommTxs, ToAbortTxs, ClientDict1, TimeBlocked} = solve_read_dependency(CommitTime, DepDict, DepList, LOC, ClientDict),
      lager:warning("Commit ~w, local parts ~w, remote parts ~w", [TxId, LocalParts, RemoteParts]),
    ?CLOCKSI_VNODE:commit(LocalParts, TxId, LOC, CommitTime),
    ?REPL_FSM:repl_commit(LocalParts, TxId, LOC, CommitTime, RepDict),
    ?CLOCKSI_VNODE:commit(RemoteParts, TxId, LOC, CommitTime),
    ?REPL_FSM:repl_commit(RemoteParts, TxId, LOC, CommitTime, RepDict),
    {DepDict1, MayCommTxs, ToAbortTxs, ClientDict1, TimeBlocked}.

commit_specula_tx(TxId, CommitTime, DepDict, RepDict, PendingTxs, LOC, ClientDict) ->
    {LocalParts, RemoteParts} = dict:fetch(TxId, PendingTxs),
    PendingTxs1 = dict:erase(TxId, PendingTxs),
    %%%%%%%%% Time stat %%%%%%%%%%%
    DepList = ets:lookup(dependency, TxId),
    lager:warning("Committing ~w: My read dependncy are ~w, local parts are ~w, remote parts are ~w", [TxId, DepList, LocalParts, RemoteParts]),
    {DepDict1, MayCommTxs, ToAbortTxs, ClientDict1, TimeBlocked} = solve_read_dependency(CommitTime, DepDict, DepList, LOC, ClientDict),

    ?CLOCKSI_VNODE:commit(LocalParts, TxId, LOC, CommitTime),
    ?REPL_FSM:repl_commit(LocalParts, TxId, LOC, CommitTime, RepDict),
    %?REPL_FSM:repl_commit(LocalParts, TxId, CommitTime, DoRepl),
    ?CLOCKSI_VNODE:commit(RemoteParts, TxId, LOC, CommitTime),
    %?REPL_FSM:repl_commit(RemoteParts, TxId, CommitTime, DoRepl),
    ?REPL_FSM:repl_commit(RemoteParts, TxId, LOC, CommitTime, RepDict),
    {PendingTxs1, DepDict1, ToAbortTxs, MayCommTxs, ClientDict1, TimeBlocked}.

abort_specula_tx(TxId, PendingTxs, RepDict, DepDict, ExceptNode) ->
    {LocalParts, RemoteParts} = dict:fetch(TxId, PendingTxs),
    PendingTxs1 = dict:erase(TxId, PendingTxs),
    %%%%%%%%% Time stat %%%%%%%%%%%
    DepList = ets:lookup(dependency, TxId),
     lager:warning("Abort specula ~w: My read dependncy are ~w", [TxId, DepList]),
    Self = self(),
    ToAbortTxs = lists:foldl(fun({_, DepTxId}, AccToAbort) ->
                              TxServer = DepTxId#tx_id.server_pid,
                              ets:delete_object(dependency, {TxId, DepTxId}),
                              case TxServer == Self of
                                  true ->
                                    case DepTxId#tx_id.client_pid == TxId#tx_id.client_pid of
                                        true ->     AccToAbort;
                                        false ->  [{[], DepTxId}|AccToAbort]
                                    end;
                                  _ ->
                                        lager:warning("~w is not my own, read invalid", [DepTxId]),
                                      ?READ_ABORTED(TxServer, -1, DepTxId), AccToAbort
                              end
                    end, [], DepList),
    ?CLOCKSI_VNODE:abort(LocalParts, TxId),
    ?REPL_FSM:repl_abort(LocalParts, TxId, RepDict),
    ?CLOCKSI_VNODE:abort(lists:delete(ExceptNode, RemoteParts), TxId),
    ?REPL_FSM:repl_abort(RemoteParts, TxId, RepDict),
    {PendingTxs1, dict:erase(TxId, DepDict), ToAbortTxs}.

abort_specula_tx(TxId, PendingTxs, RepDict, DepDict) ->
    %[{TxId, {LocalParts, RemoteParts, _LP, _RP, IfWaited}}] = ets:lookup(ClientDict, TxId), 
    %ets:delete(ClientDict, TxId),
    {LocalParts, RemoteParts} = dict:fetch(TxId, PendingTxs),
    PendingTxs1 = dict:erase(TxId, PendingTxs),
    DepList = ets:lookup(dependency, TxId),
     lager:warning("Specula abort ~w: My read dependncy are ~w", [TxId, DepList]),
    Self = self(),
    ToAbortTxs = lists:foldl(fun({_, DepTxId}, AccToAbort) ->
                              TxServer = DepTxId#tx_id.server_pid,
                              ets:delete_object(dependency, {TxId, DepTxId}),
                              case TxServer == Self of
                                  true ->
                                    case DepTxId#tx_id.client_pid == TxId#tx_id.client_pid of
                                        true ->     AccToAbort;
                                        false ->  [{[], DepTxId}|AccToAbort]
                                    end;
                                  _ ->
                                        lager:warning("~w is not my own, read invalid", [DepTxId]),
                                      ?READ_ABORTED(TxServer, -1, DepTxId), AccToAbort
                              end
                    end, [], DepList),
    ?CLOCKSI_VNODE:abort(LocalParts, TxId),
    ?REPL_FSM:repl_abort(LocalParts, TxId, RepDict),
    ?CLOCKSI_VNODE:abort(RemoteParts, TxId),
    ?REPL_FSM:repl_abort(RemoteParts, TxId, RepDict),
    %abort_to_table(RemoteParts, TxId, RepDict),
    {PendingTxs1, dict:erase(TxId, DepDict), ToAbortTxs}.

abort_tx(TxId, LocalParts, RemoteParts, RepDict, Stage) ->
    abort_tx(TxId, LocalParts, RemoteParts, RepDict, Stage, ignore).

abort_tx(TxId, LocalParts, RemoteParts, RepDict, Stage, LocalOnlyPart) ->
    %true = ets:delete(ClientDict, TxId),
    DepList = ets:lookup(dependency, TxId),
     lager:warning("Abort ~w: My read dependncy are ~w, RemoteParts ~w, Stage is ~w", [TxId, DepList, RemoteParts, Stage]),
    Self = self(),
    ToAbortTxs = lists:foldl(fun({_, DepTxId}, AccTxs) ->
                            TxServer = DepTxId#tx_id.server_pid,
                            ets:delete_object(dependency, {TxId, DepTxId}),
                            case TxServer == Self of 
                                true ->
                                    case (DepTxId#tx_id.client_pid == TxId#tx_id.client_pid) of
                                        true ->
                                            AccTxs;
                                        false ->
                                            [{[], DepTxId}|AccTxs]
                                    end;
                                _ ->
                                     lager:warning("~w is not my own, read invalid", [DepTxId]),
                                    ?READ_ABORTED(TxServer, -1, DepTxId),
                                    AccTxs
                            end
                    end, [], DepList),
    ?CLOCKSI_VNODE:abort(LocalParts, TxId),
    case Stage of 
        remote_cert ->
            ?REPL_FSM:repl_abort(LocalParts, TxId, RepDict),
            ?CLOCKSI_VNODE:abort(RemoteParts, TxId),
            ?REPL_FSM:repl_abort(RemoteParts, TxId, RepDict),
            case LocalOnlyPart of 
                ignore -> ok;
                {LP, LN} ->
                    case dict:find({rep, LN}, RepDict) of
                        {ok, DataReplServ} ->
                            gen_server:cast({global, DataReplServ}, {repl_abort, TxId, [LP]});
                        _ ->
                            ?CACHE_SERV:abort(TxId, [LP])
                    end
            end;
        local_cert ->
            ?REPL_FSM:repl_abort(LocalParts, TxId, RepDict),
            NodeParts = build_node_parts(RemoteParts),
            lists:foreach(fun({Node, Partitions}) ->
                case dict:find({rep, Node}, RepDict) of
                    {ok, DataReplServ} ->
                        gen_server:cast({global, DataReplServ}, {repl_abort, TxId, Partitions, local});
                    _ ->
                        ?CACHE_SERV:abort(TxId, Partitions)
            end end, NodeParts)
    end,
    ToAbortTxs.

local_certify(LocalUpdates, RemoteUpdates, TxId, RepDict) ->
    {LocalParts, NumLocalParts} = ?CLOCKSI_VNODE:prepare(LocalUpdates, TxId, local),
    {NumSlaveParts, NumCacheParts} = lists:foldl(fun({{Part, Node}, Updates}, {NumSParts, NumCParts}) ->
                    case dict:find({rep, Node}, RepDict) of
                        {ok, DataReplServ} ->
                            ?DATA_REPL_SERV:local_certify(DataReplServ, TxId, Part, Updates),
                            {NumSParts+1, NumCParts}; 
                        _ ->
                            ?CACHE_SERV:local_certify(TxId, Part, Updates),
                            {NumSParts, NumCParts+1}
                    end end, {0, 0}, RemoteUpdates),
    {LocalParts, NumLocalParts, NumSlaveParts, NumCacheParts}.

pre_commit(LocalPartitions, RemotePartitions, TxId, SpeculaCommitTs, RepDict, LOC, FFC) ->
    ?CLOCKSI_VNODE:pre_commit(LocalPartitions, TxId, SpeculaCommitTs, LOC, FFC),
    lists:foreach(fun({Part, Node}) ->
            case dict:find({rep, Node}, RepDict) of
                {ok, DataReplServ} ->
                    ?DATA_REPL_SERV:pre_commit(DataReplServ, TxId, Part, SpeculaCommitTs, LOC, FFC); 
                _ ->
                    ?CACHE_SERV:pre_commit(TxId, Part, SpeculaCommitTs, LOC, FFC)
    end end, RemotePartitions).

%% Deal dependencies and check if any following transactions can be committed.
try_commit_follower(LastCommitTime, [], _RepDict, DepDict, PendingTxs, ClientDict, ToAbortTxs, MayCommitTxs, CommittedTxs, OldTB) ->
    {PendingTxs, [], LastCommitTime, DepDict, ToAbortTxs, MayCommitTxs, ClientDict, CommittedTxs, OldTB};
try_commit_follower(LastCommitTime, [H|Rest]=PendingList, RepDict, DepDict, 
                        PendingTxs, ClientDict, LastAbortTxs, LastMayCommit, CommittedTxs, OldTB) ->
    Result = case dict:find(H, DepDict) of
                {ok, {0, [], LOC, PendingMaxPT}} ->
                    {true, max(PendingMaxPT, LastCommitTime+1), LOC};
                _R ->
                    false
            end,
    case Result of
        false ->
            {PendingTxs, PendingList, LastCommitTime, DepDict, LastAbortTxs, LastMayCommit, ClientDict, CommittedTxs, OldTB};
        {true, CommitTime, MyLOC} ->
             lager:warning("Commit pending specula ~w", [H]),
            {PendingTxs1, DepDict1, ToAbortTxs, MayCommitTxs, ClientDict1, TB} = commit_specula_tx(H, CommitTime, 
               dict:erase(H, DepDict), RepDict, PendingTxs, MyLOC, ClientDict),
            try_commit_follower(CommitTime, Rest, RepDict, DepDict1, PendingTxs1, ClientDict1, 
                    LastAbortTxs++ToAbortTxs, LastMayCommit++MayCommitTxs, [{H, os:timestamp()}|CommittedTxs], TB+OldTB)
    end.

abort_specula_list([H|T], RepDict, DepDict, PendingTxs, ExceptNode, []) ->
    {PendingTxs1, DepDict1, ToAbortTxs} = abort_specula_tx(H, PendingTxs, RepDict, DepDict, ExceptNode),
    abort_specula_list(T, RepDict, DepDict1, PendingTxs1, ToAbortTxs). 

abort_specula_list([], _RepDict, DepDict, PendingTxs, ToAbortTxs) ->
    {PendingTxs, DepDict, ToAbortTxs};
abort_specula_list([H|T], RepDict, DepDict, PendingTxs, ToAbortTxs) ->
    {PendingTxs1, DepDict1, NewToAbort} = abort_specula_tx(H, PendingTxs, RepDict, DepDict),
    abort_specula_list(T, RepDict, DepDict1, PendingTxs1, ToAbortTxs ++ NewToAbort). 

%% Deal with reads that depend on this txn
solve_read_dependency(CommitTime, ReadDep, DepList, LOC, ClientDict) ->
    Self = self(),
    lists:foldl(fun({TxId, DepTxId}, {RD, MaybeCommit, ToAbort, CD, AccTB}) ->
                    TxServer = DepTxId#tx_id.server_pid,
                    ets:delete_object(dependency, {TxId, DepTxId}), 
                    case DepTxId#tx_id.snapshot_time >= CommitTime of
                        %% This read is still valid
                        true ->
                               lager:warning("Read still valid for ~w, CommitTime is ~w", [DepTxId, CommitTime]),
                            case TxServer == Self of
                                true ->
                                     lager:warning("~w is my own, read valid", [DepTxId]),
                                    case dict:find(DepTxId, RD) of
                                        {ok, {0, SolvedReadDeps, ToRemoveLOC, 0}} -> %% Local transaction is still reading
                                            lager:warning("Deleting {~w, ~w} from antidep", [DepTxId, TxId]),
                                            RD1 = dict:store(DepTxId, {0, [TxId|SolvedReadDeps], [LOC|ToRemoveLOC], 0}, RD), 
                                            {RD1, MaybeCommit, ToAbort, ClientDict, AccTB};
                                        {ok, {read_only, [TxId], _, _}} ->
                                            CPid = DepTxId#tx_id.client_pid,
                                            CS = dict:fetch(CPid, CD),
                                            lager:warning("Committed reads are ~w, ~w", [CS#c_state.committed_reads, DepTxId]),
                                            CS1 = CS#c_state{committed_reads=[DepTxId|CS#c_state.committed_reads]},
                                            {dict:erase(DepTxId, RD), MaybeCommit, ToAbort, dict:store(CPid, CS1, CD), AccTB};
                                        {ok, {0, [TxId], DepLOC, PrepTime}} ->
                                            lager:warning("Here!!!"),
                                            case DepTxId#tx_id.client_pid == TxId#tx_id.client_pid of
                                                false ->
                                                    {dict:store(DepTxId, {0, [], DepLOC, PrepTime}, RD), 
                                                        [{PrepTime, DepLOC, DepTxId}|MaybeCommit], ToAbort, CD, AccTB};
                                                true ->
                                                    {dict:store(DepTxId, {0, [], DepLOC, PrepTime}, RD), 
                                                          MaybeCommit, ToAbort, CD, AccTB}
                                            end;
                                        {ok, {PrepDeps, ReadDeps, DepLOC, PrepTime}} ->
                                              lager:warning("Prepdeps is ~p, Storing ~w for ~w", [PrepDeps, delete_elem(TxId, ReadDeps), DepTxId]),
                                            {dict:store(DepTxId, {PrepDeps, delete_elem(TxId, ReadDeps), DepLOC, 
                                                PrepTime}, RD), MaybeCommit, ToAbort, CD, AccTB};
                                        {ok, {0, RDeps, RLOC, FFC, 0, Value, Sender, BlockedTime}} ->
                                            lager:warning("~w is blocked!, RDeps is ~w, RLOC is ~w, FFC is ~w", [DepTxId, RDeps, RLOC, FFC]),
                                            RemainDeps = delete_elem(TxId, RDeps),
                                            RemainLOC = delete_elem(LOC, RLOC),
                                            MinLOC = case RemainLOC of [] -> inf; _ -> lists:min(RemainLOC) end, 
                                            case MinLOC >= FFC of
                                                true -> 
                                                    ets:insert(anti_dep, {DepTxId, {MinLOC, RemainLOC}, FFC, RemainDeps}),
                                                    gen_server:reply(Sender, Value),
                                                    lager:warning("~w Replying to reader ~w, inserted remainLOC is ~w, FFC is ~w", [DepTxId, Sender, RemainLOC, FFC]),
                                                    {dict:store(DepTxId, {0, [], [], 0}, RD), MaybeCommit, ToAbort, CD, AccTB+timer:now_diff(os:timestamp(), BlockedTime)};
                                                false ->
                                                    lager:warning("Updated ~w, but can not reply ~w, ~w", [DepTxId, RemainLOC, FFC]),
                                                    {dict:store(DepTxId, {0, RemainDeps, RemainLOC, FFC, 0, Value, Sender, BlockedTime}, RD), 
                                                        MaybeCommit, ToAbort, CD, AccTB}
                                            end;
                                        error -> %% This txn hasn't even started certifying 
                                                 %% or has been cert_aborted already
                                                   lager:warning("This txn has not even started"),
                                            {RD, MaybeCommit, ToAbort, CD, AccTB}
                                    end;
                                _ ->
                                      lager:warning("~w is not my own, read valid", [DepTxId]),
                                    ?READ_VALID(TxServer, DepTxId, TxId, LOC),
                                    {RD, MaybeCommit, ToAbort, CD, AccTB}
                            end;
                        false ->
                            %% Read is not valid
                            case TxServer == Self of
                                true ->
                                    lager:warning("~w is my own, read invalid", [DepTxId]),
                                    {RD, MaybeCommit, [{[], DepTxId}|ToAbort], CD, AccTB};
                                _ ->
                                     lager:warning("~w is not my own, read invalid", [DepTxId]),
                                    ?READ_INVALID(TxServer, CommitTime, DepTxId),
                                    {RD, MaybeCommit, ToAbort, CD, AccTB}
                            end
                    end
                end, {ReadDep, [], [], ClientDict, 0}, DepList).

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

rev(L) ->
    lists:reverse(L).


%get_time_diff({A1, B1, C1}, {A2, B2, C2}) ->
%    ((A2-A1)*1000000+ (B2-B1))*1000000+ C2-C1.

get_older(?NO_TXN, TxId2) ->
    TxId2;
get_older(TxId1, TxId2) ->
    case TxId1#tx_id.snapshot_time < TxId2#tx_id.snapshot_time of
        true -> TxId1;
        false -> TxId2
    end.

load_config() ->
    {antidote_config:get(specula_length), antidote_config:get(specula_read)}.

%send_prob(DepDict, TxId, InfoList, Sender) ->
%    case dict:fetch(TxId, DepDict) of
%        {N, [], _} ->
%            lager:info("PendTx ~w no pending reading!", [TxId, N]);
%        {N, ReadDep, _} ->
%            lists:foreach(fun(BTxId) ->
%                Info = io_lib:format("~p: blocked by ~p, N is ~p, read deps are ~p \n", [TxId, BTxId, N, ReadDep]),
%            gen_server:cast(BTxId#tx_id.server_pid, {trace, [TxId], BTxId, Sender, InfoList ++ [Info]}) end, ReadDep)
%    end.

build_node_parts(Parts) ->
    D = lists:foldl(fun({Partition, Node}, Acc) ->
                      dict:append(Node, Partition, Acc)
                   end,
                    dict:new(), Parts),
    dict:to_list(D).

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
    abort_tx(TxId1, LocalParts, [], dict:new(), local_cert),
    ?assertEqual(true, mock_partition_fsm:if_applied({abort, TxId1, LocalParts}, nothing)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_abort, TxId1, LocalParts}, nothing)),
    true = ets:delete(dependency),
    true = ets:delete(MyTable).

commit_tx_test() ->
    ets:new(dependency, [private, set, named_table]),
    LocalParts = [lp1, lp2],
    TxId1 = tx_utilities:create_tx_id(0),
    CT = 10000,
    commit_tx(TxId1, CT, LocalParts, [], dict:new(), dict:new(), 0, dict:new()),
    ?assertEqual(true, mock_partition_fsm:if_applied({commit, TxId1, LocalParts}, CT)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_commit, TxId1, LocalParts}, CT)),
    ets:delete(dependency).

abort_specula_list_test() ->
    MyTable = dict:new(), 
    ets:new(dependency, [public, bag, named_table]),
    DepDict = dict:new(),
    RepDict0 = dict:store(n1, n1rep, dict:new()),
    RepDict = dict:store(n2, n2rep, RepDict0),
    T1 = tx_utilities:create_tx_id(0),
    T2 = tx_utilities:create_tx_id(0),
    T3 = tx_utilities:create_tx_id(0),
    DepDict1 = dict:store(T1, {3, 1, 0, 1}, DepDict),
    DepDict2 = dict:store(T2, {1, 1, 0, 1}, DepDict1),
    DepDict3 = dict:store(T3, {0, 1, 0, 1}, DepDict2),
    MyTable1 = dict:store(T1, {[{p1, n1}], [{p2, n2}]}, MyTable),
    MyTable2 = dict:store(T2, {[{p1, n1}], [{p2, n2}]}, MyTable1),
    MyTable3 = dict:store(T3, {[{p1, n1}], [{p2, n2}]}, MyTable2),
    {MyTable4, DepDict4, _} = abort_specula_list([T1, T2, T3], RepDict, DepDict3, MyTable3, []),
    ?assertEqual(error, dict:find(T1, DepDict4)),
    ?assertEqual(error, dict:find(T2, DepDict4)),
    ?assertEqual(error, dict:find(T3, DepDict4)),
    ?assertEqual(error, dict:find(T1, MyTable4)),
    ?assertEqual(error, dict:find(T2, MyTable4)),
    ?assertEqual(error, dict:find(T3, MyTable4)),
    ?assertEqual(true, mock_partition_fsm:if_applied({abort, T1, [{p1, n1}]}, nothing)),
    ?assertEqual(true, mock_partition_fsm:if_applied({abort, T1, [{p2, n2}]}, nothing)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_abort, T1, [{p1, n1}]}, nothing)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_abort, T1, [{p2, n2}]}, nothing)),
    ?assertEqual(true, mock_partition_fsm:if_applied({abort, T2, [{p1, n1}]}, nothing)),
    ?assertEqual(true, mock_partition_fsm:if_applied({abort, T2, [{p2, n2}]}, nothing)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_abort, T2, [{p1, n1}]}, nothing)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_abort, T2, [{p2, n2}]}, nothing)).

solve_read_dependency_test() ->
    T0 = tx_utilities:create_tx_id(0), 
    T1 = tx_utilities:create_tx_id(0), 
    T2 = T1#tx_id{server_pid=test_fsm}, 
    CommitTime = tx_utilities:now_microsec(),
    T3 = tx_utilities:create_tx_id(0),
    T4 = T3#tx_id{server_pid=test_fsm}, 
    %% T1, T2 should be cert_aborted, T3, T4 should get read_valid.
    DepDict = dict:new(),
    DepDict1 = dict:store(T3, {2, [T0, T2,T1], 0, 4}, DepDict),
    {RD2, _, T, _, 0} = solve_read_dependency(CommitTime, DepDict1, [{T0, T1}, {T0, T2}, {T0, T3}, {T0, T4}], 0, dict:new()),
    ?assertEqual({[{T3,{2,[T2,T1],0,4}}], [{[], T1}]}, {dict:to_list(RD2), T}),
    ?assertEqual(true, mock_partition_fsm:if_applied({read_valid, T4}, T0)),
    ?assertEqual(true, mock_partition_fsm:if_applied({read_invalid, T2}, nothing)).

try_commit_follower_test() ->
    ClientDict = dict:store(self(), #c_state{}, dict:new()),
    PendingTxs = dict:new(),
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
    DepDict1 = dict:store(T1, {1, [], 0, 2}, dict:new()),
    DepDict2 = dict:store(T2, {2, [1,2], 0, 2}, DepDict1),
    DepDict3 = dict:store(T3, {2, [], 0, 2}, DepDict2),
    %% T1 can not commit due to having read dependency 
    Result =  
            try_commit_follower(MaxPT1, [T1, T2], RepDict, DepDict2, PendingTxs, ClientDict, [], [], [], 0),
    {PendingTxs1, PD1, MPT1, RD1, ToAbortTxs, MayCommitTxs, ClientDict1, CommittedTxs, 0}=Result,

    ?assertEqual(ToAbortTxs, []),
    ?assertEqual(MayCommitTxs, []),
    ?assertEqual(CommittedTxs, []),
    ?assertEqual(RD1, DepDict2),
    ?assertEqual(PD1, [T1, T2]),
    ?assertEqual(MPT1, MaxPT1),

    %ets:insert(MyTable, {T1, {[{lp1, n1}], [{rp1, n3}], now(), now(), no_wait}}), 
    %ets:insert(MyTable, {T2, {[{lp1, n1}], [{rp1, n3}], now(), now(), no_wait}}), 
    %ets:insert(MyTable, {T3, {[{lp2, n2}], [{rp2, n4}], now(), now(), no_wait}}), 
    PendingTxs2 = dict:store(T1, {[{lp1, n1}], [{rp1, n3}]}, PendingTxs1), 
    PendingTxs3 = dict:store(T2, {[{lp1, n1}], [{rp1, n3}]}, PendingTxs2), 
    PendingTxs4 = dict:store(T3, {[{lp2, n2}], [{rp2, n4}]}, PendingTxs3), 
    DepDict4 = dict:store(T1, {0, [], 0, 2}, DepDict3),
    %% T1 can commit because of read_dep ok. 
    {PendingTxs5, PD2, MPT2, RD2, ToAbortTxs1, MayCommitTxs1, ClientDict2, CommittedTxs1, 0} =  try_commit_follower(MaxPT1, [T1, T2], RepDict, DepDict4, PendingTxs4, ClientDict1, [], [], [], 0),
    ?assertEqual(ToAbortTxs1, []),
    ?assertEqual(MayCommitTxs1, []),
    ?assertMatch([{T1, _}], CommittedTxs1),
    %?assertEqual(true, mock_partition_fsm:if_applied({commit_specula, n3rep, T1, rp1}, MPT2)),
    io:format(user, "RD2 ~w, Dict4 ~w", [dict:to_list(RD2), dict:to_list(dict:erase(T1, DepDict4))]),
    ?assertEqual(RD2, dict:erase(T1, DepDict4)),
    ?assertEqual(PD2, [T2]),
    ?assertEqual(MPT2, MaxPT1+1),

    %% T2 can commit becuase of prepare OK
    DepDict5 = dict:store(T2, {0, [], 0, 2}, RD2),
    {PendingTxs6, _PD3, MPT3, RD3, ToAbortedTxs2, MayCommitTxs2, ClientDict3, CommittedTxs2, 1} =  try_commit_follower(MPT2, [T2, T3], RepDict, DepDict5, PendingTxs5, ClientDict2, [], [], [], 1),
    ?assertEqual(error, dict:find(T2, PendingTxs6)),
    ?assertEqual(MayCommitTxs2, []),
    ?assertEqual(ToAbortedTxs2, []),
    ?assertMatch([{T2, _}], CommittedTxs2),
    %?assertEqual(true, mock_partition_fsm:if_applied({repl_commit, TxId1, LocalParts}, CT)),
    ?assertEqual(true, mock_partition_fsm:if_applied({commit, T2, [{lp1, n1}]}, MPT3)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_commit, T2, [{rp1, n3}]}, MPT3)),
    %?assertEqual(true, mock_partition_fsm:if_applied({commit_specula, n3rep, T2, rp1}, MPT3)),

    %% T3, T4 get committed and T5 gets cert_aborted.
    PendingTxs7 = dict:store(T3, {[{lp2, n2}], [{rp2, n4}]}, PendingTxs6), 
    PendingTxs8 = dict:store(T4, {[{lp2, n2}], [{rp2, n4}]}, PendingTxs7), 
    PendingTxs9 = dict:store(T5, {[{lp2, n2}], [{rp2, n4}]}, PendingTxs8), 
    ets:insert(dependency, {T3, T4}),
    ets:insert(dependency, {T3, T5}),
    ets:insert(dependency, {T4, T5}),
    DepDict6 = dict:store(T3, {0, [], 0, T3#tx_id.snapshot_time+1}, RD3),
    DepDict7 = dict:store(T4, {0, [T3], 0, T5#tx_id.snapshot_time+1}, DepDict6),
    DepDict8 = dict:store(T5, {0, [T4,T5], 0, 6}, DepDict7),
    io:format(user, "My commit time is ~w, Txns are ~w ~w ~w", [0, T3, T4, T5]),
    {PendingTxs10, PD4, MPT4, RD4, ToAbortTxs3, MayCommitTxs3, _ClientDict4, CommittedTxs3, 2} 
            =  try_commit_follower(0, [T3, T4, T5], RepDict, DepDict8, PendingTxs9, ClientDict3, [], [], [], 2),
    ?assertMatch([], MayCommitTxs),
    ?assertEqual(ToAbortTxs3, [{[], T5}]),
    ?assertEqual(MayCommitTxs3, []),
    ?assertMatch([{T4, _}, {T3, _}], CommittedTxs3),
    ?assertEqual(1, dict:size(RD4)),
    ?assertEqual(PD4, [T5]),
    ?assertEqual(MPT4, T5#tx_id.snapshot_time+1),
    ?assertEqual(dict:find(T4, PendingTxs10), error), 

    %?assertEqual(true, mock_partition_fsm:if_applied({commit_specula, cache_serv, T3, rp2}, T3#tx_id.snapshot_time+1)),
    %?assertEqual(true, mock_partition_fsm:if_applied({commit_specula, cache_serv, T4, rp2}, MPT4)),
    %?assertEqual(true, mock_partition_fsm:if_applied({abort_specula, cache_serv, T5, rp2}, nothing)),
    ?assertEqual(true, mock_partition_fsm:if_applied({commit, T4, [{lp2, n2}]}, MPT4)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_commit, T4, [{rp2, n4}]}, MPT4)).
    %?assertEqual(true, mock_partition_fsm:if_applied({repl_abort, T5, [{rp2, n4}]}, nothing )).
    
-endif.
