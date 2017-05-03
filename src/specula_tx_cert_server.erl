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
        num_specula_read=0 :: non_neg_integer()
        }).

-record(client_state, {
        tx_id =?NO_TXN :: txid(),
        invalid_aborted=0 :: non_neg_integer(),
        specula_abort_count=0 :: non_neg_integer(),
        local_updates = []:: [],
        remote_updates =[] :: [],
        pending_list=[] :: [txid()],
        spec_commit_time :: non_neg_integer(),
        %% New stat
        aborted_reads=[],
        committed_reads=[],
        committed_updates=[],
        aborted_update = ?NO_TXN,
        msg_id=0,
        %% New stat
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
     %lgaer:warning("Specula tx cert started wit name ~w, id is ~p", [Name, self()]),
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
     %lgaer:warning("TotalReplFactor is ~w", [TotalReplFactor]),
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
    %lgaer:warning("Start tx is ~p", [TxId]),
    ClientState = case dict:find(Client, ClientDict) of
                    error ->
                        #client_state{};
                    {ok, SenderState} ->
                        SenderState
                 end,
    D1 = dict:store(TxId, {0, [], 0}, D),
    OldTxId = ClientState#client_state.tx_id,
    PendingList = ClientState#client_state.pending_list, 
    Stage = ClientState#client_state.stage, 
    LocalParts = ClientState#client_state.local_updates, 
    RemoteUpdates = ClientState#client_state.remote_updates, 
    case OldTxId of
        ?NO_TXN ->
            ClientState1 = ClientState#client_state{tx_id=TxId, stage=read, pending_prepares=0},
            {reply, TxId, SD0#state{client_dict=dict:store(Client, ClientState1, ClientDict), dep_dict=D1, 
                min_snapshot_ts=NewSnapshotTS}};
        _ ->
            case Stage of 
                read ->
                    ClientState1 = ClientState#client_state{tx_id=TxId, stage=read, pending_prepares=0, invalid_aborted=0},
                    {reply, TxId, SD0#state{client_dict=dict:store(Client, ClientState1, ClientDict),
                        min_snapshot_ts=NewSnapshotTS, dep_dict=dict:erase(OldTxId, D1)}}; 
                _ ->
                    RemoteParts = [P||{P, _} <-RemoteUpdates],
                    ClientDict1 = dict:store(TxId, {LocalParts, RemoteParts, waited}, ClientDict),
                    ClientState1 = ClientState#client_state{tx_id=TxId, stage=read, pending_list=PendingList++[OldTxId], pending_prepares=0, invalid_aborted=0},
                    {reply, TxId, SD0#state{dep_dict=D1, 
                        min_snapshot_ts=NewSnapshotTS, client_dict=dict:store(Client, ClientState1, ClientDict1)}}
            end
    end;

handle_call({get_cdf}, _Sender, SD0) ->
    {reply, ok, SD0};

handle_call({get_stat}, _Sender, SD0) ->
    {reply, [0,0], SD0};
    %{reply, [ReadAborted, ReadInvalid, CertAborted, CascadeAborted, Committed, 0, NumSpeculaRead], SD0};

handle_call({set_int_data, Type, Param}, _Sender, SD0)->
    case Type of
        last_commit_time ->
              %lgaer:warning("Set lct to ~w", [Param]),
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
    ReadDepTxs = [T2  || {_, T2} <- ets:lookup(anti_dep, TxId)],
    true = ets:delete(anti_dep, TxId),
    %lgaer:warning("Start certifying ~w, readDepTxs is ~w", [TxId, ReadDepTxs]),
    ClientState = dict:fetch(Client, ClientDict),
    InvalidAborted = ClientState#client_state.invalid_aborted,
    TxId = ClientState#client_state.tx_id, 
    SentMsgId = ClientState#client_state.msg_id,
    AbortedReads = ClientState#client_state.aborted_reads,
    CommittedReads = ClientState#client_state.committed_reads,
    CommittedUpdates = ClientState#client_state.committed_updates,
    case SentMsgId == ClientMsgId of
        true ->
            case {InvalidAborted, dict:find(TxId, DepDict)} of
                {1, _} ->
                    case ClientState#client_state.aborted_update of
                        ?NO_TXN -> 
                            %lager:warning("Aborted ~w", [TxId]),
                            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, aborted_update=?NO_TXN, aborted_reads=[],
                                    committed_updates=[], committed_reads=[], invalid_aborted=0}, ClientDict),
                            DepDict1 = dict:erase(TxId, DepDict),
                            {reply, {aborted, {true, [TxId|AbortedReads], rev(CommittedUpdates), lists:sort(CommittedReads)}}, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}};
                        AbortedTxId ->
                            %lgaer:warning("Cascade aborted aborted txid is ~w, TxId is  ~w", [AbortedTxId, TxId]),
                            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, aborted_update=?NO_TXN, aborted_reads=[],
                                            committed_updates=[], committed_reads=[], invalid_aborted=0}, ClientDict),
                            DepDict1 = dict:erase(TxId, DepDict),
                            {reply, {cascade_abort, {AbortedTxId, [TxId|AbortedReads], rev(CommittedUpdates), CommittedReads}}, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}}
                    end;
                    %% Some read is invalid even before the txn starts.. If invalid_aborted is larger than 0, it can possibly be saved.
                {0, {ok, {0, B, 0}}} ->
                    case ReadDepTxs of
                        B ->
                            %lgaer:warning("Returning specula_commit for ~w", [TxId]),
                            gen_server:reply(Sender, {ok, {specula_commit, LastCommitTs, {AbortedReads, rev(CommittedUpdates), [TxId|CommittedReads]}}}),
                            DepDict1 = dict:erase(TxId, DepDict),
                            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, aborted_reads=
                                [], committed_updates=[], committed_reads=[]}, ClientDict),
                            {noreply, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}};
                        _ ->
                            %lgaer:warning("Returning specula_commit for ~w", [TxId]),
                            gen_server:reply(Sender, {ok, {specula_commit, LastCommitTs, {AbortedReads, rev(CommittedUpdates), CommittedReads}}}),
                            DepDict1 = dict:store(TxId, 
                                    {read_only, ReadDepTxs--B, 0}, DepDict),
                            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, aborted_reads=
                                [], committed_updates=[], committed_reads=[]}, ClientDict),
                            {noreply, SD0#state{client_dict=ClientDict1, dep_dict=DepDict1}}
                    end
            end;
        false ->
            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN}, ClientDict),
            DepDict1 = dict:erase(TxId, DepDict),
            {reply, wrong_msg, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}}
            %case ClientState#client_state.aborted_update of
            %    ?NO_TXN -> 
            %        ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, aborted_update=?NO_TXN, aborted_reads=[],
            %                committed_updates=[], committed_reads=[], invalid_aborted=0}, ClientDict),
            %        DepDict1 = dict:erase(TxId, DepDict),
            %        {reply, {aborted, {AbortedReads, rev(CommittedUpdates), rev(CommittedReads)}}, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}};
            %    AbortedTxId ->
            %        ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, aborted_update=?NO_TXN, aborted_reads=[],
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
    AbortedReads = ClientState#client_state.aborted_reads,
    CommittedReads = ClientState#client_state.committed_reads,
    CommittedUpdates = ClientState#client_state.committed_updates,
    ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, aborted_update=?NO_TXN, aborted_reads=[],
            committed_updates=[], committed_reads=[], invalid_aborted=0}, ClientDict),
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
handle_call({certify_update, TxId, LocalUpdates, RemoteUpdates, ClientMsgId}, Sender, SD0=#state{rep_dict=RepDict, pending_txs=PendingTxs, total_repl_factor=ReplFactor, min_commit_ts=LastCommitTs, specula_length=SpeculaLength, dep_dict=DepDict, client_dict=ClientDict}) ->
    %% If there was a legacy ongoing transaction.
    {Client, _} = Sender,
    ReadDepTxs = [T2  || {_, T2} <- ets:lookup(anti_dep, TxId)],
    true = ets:delete(anti_dep, TxId),
     %lgaer:warning("Start certifying ~w, readDepTxs is ~w, Sender is ~w, local parts remote parts are ~w", [TxId, ReadDepTxs, Sender, RemoteUpdates]),
    ClientState = dict:fetch(Client, ClientDict),
    PendingList = ClientState#client_state.pending_list, 
    InvalidAborted = ClientState#client_state.invalid_aborted,
    TxId = ClientState#client_state.tx_id, 
    SentMsgId = ClientState#client_state.msg_id,
    AbortedReads = ClientState#client_state.aborted_reads,
    CommittedReads = ClientState#client_state.committed_reads,
    CommittedUpdates = ClientState#client_state.committed_updates,
    case SentMsgId == ClientMsgId of
        true ->
            case {InvalidAborted, dict:find(TxId, DepDict)} of
                {1, _} ->
                     %lgaer:warning("InvalidAborted ~w", [InvalidAborted]),
                    case ClientState#client_state.aborted_update of
                        ?NO_TXN -> 
                            %lager:warning("Cascading abort!"),
                            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, aborted_update=?NO_TXN, aborted_reads=[],
                                    committed_updates=[], committed_reads=[], invalid_aborted=0}, ClientDict),
                            DepDict1 = dict:erase(TxId, DepDict),
                            {reply, {aborted, {true, AbortedReads, rev(CommittedUpdates), CommittedReads}}, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}};
                        AbortedTxId ->
                            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, aborted_update=?NO_TXN, aborted_reads=[],
                                            committed_updates=[], committed_reads=[], invalid_aborted=0}, ClientDict),
                            DepDict1 = dict:erase(TxId, DepDict),
                            {reply, {cascade_abort, {AbortedTxId, AbortedReads, rev(CommittedUpdates), CommittedReads}}, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}}
                    end;
                {0, {ok, {_, B, _}}} ->
                    case LocalUpdates of
                        [] ->
                            RemotePartitions = [P || {P, _} <- RemoteUpdates],
                            case (RemotePartitions == []) and (ReadDepTxs == B) and (PendingList == []) of
                                true -> 
                                    gen_server:reply(Sender, {ok, {committed, LastCommitTs, {rev(AbortedReads), rev(CommittedUpdates), rev(CommittedReads), ClientState#client_state.spec_commit_time}}}),
                                    DepDict1 = dict:erase(TxId, DepDict),
                                    ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, aborted_reads=
                                          [], committed_updates=[], committed_reads=[]}, ClientDict),
                                    {noreply, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}}; 
                                _ ->
                                    %%% !!!!!!! Add to ets if is not read only
                                    %NewTL = add_to_ets(StartTime, Cdf, TimeList, PendingList),
                                    case length(PendingList) >= SpeculaLength of
                                        true -> %% Wait instead of speculate.
                                           %lager:warning("remote prepr for ~w", [TxId]),
                                            ?CLOCKSI_VNODE:prepare(RemoteUpdates, TxId, {remote, ignore}),
                                            DepDict1 = dict:store(TxId, 
                                                    {ReplFactor*length(RemotePartitions), ReadDepTxs--B, LastCommitTs+1}, DepDict),
                                            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=TxId, local_updates=[], 
                                                remote_updates=RemoteUpdates, stage=remote_cert, sender=Sender, spec_commit_time=os:timestamp()}, ClientDict),
                                            {noreply, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}};
                                        false -> %% Can speculate. After replying, removing TxId
                                            %% Update specula data structure, and clean the txid so we know current txn is already replied
                                            {ProposeTS, AvoidNum} = add_to_table(RemoteUpdates, TxId, LastCommitTs, RepDict),
                                           %lager:warning("Add to table: Specula txn ~w, avoided ~w", [TxId, AvoidNum]),
                                            ?CLOCKSI_VNODE:prepare(RemoteUpdates, ProposeTS, TxId, {remote, node()}),
                                            gen_server:reply(Sender, {ok, {specula_commit, ProposeTS, {rev(AbortedReads),
                                                      rev(CommittedUpdates), rev(CommittedReads)}}}),
                                            DepDict1 = dict:store(TxId, 
                                                    {ReplFactor*length(RemotePartitions)-AvoidNum, ReadDepTxs--B, LastCommitTs+1}, DepDict),
                                            PendingTxs1 = dict:store(TxId, {[], RemotePartitions, no_wait}, PendingTxs),
                                            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN,
                                                            pending_list = PendingList ++ [TxId],  aborted_reads=
                                                    [], committed_updates=[], committed_reads=[]}, ClientDict),
                                            %ets:insert(ClientDict, {TxId, {[], RemotePartitions, StartTime, os:timestamp(), no_wait}}),
                                            {noreply, SD0#state{dep_dict=DepDict1, min_snapshot_ts=ProposeTS, 
                                                client_dict=ClientDict1, pending_txs=PendingTxs1}}
                                    end
                                end;
                        _ ->
                            LocalPartitions = [P || {P, _} <- LocalUpdates],
                            NumToAck = length(LocalUpdates),
                            DepDict1 = dict:store(TxId, {NumToAck, ReadDepTxs--B, LastCommitTs+1}, DepDict),
                            ?CLOCKSI_VNODE:prepare(LocalUpdates, TxId, local),
                            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=TxId, 
                                                local_updates=LocalPartitions, pending_prepares=(ReplFactor-1)*NumToAck, 
                                                sender=Sender, remote_updates=RemoteUpdates, stage=local_cert}, ClientDict),
                           %lager:warning("Prepare ~w to ~w, NumToAck is ~w", [TxId, LocalPartitions, NumToAck]),
                            {noreply, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}}
                    end
            end;
        false ->
            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN}, ClientDict),
            DepDict1 = dict:erase(TxId, DepDict),
            {reply, wrong_msg, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}}
    end;

handle_call({read, Key, TxId, Node}, Sender, SD0=#state{specula_read=SpeculaRead}) ->
    ?CLOCKSI_VNODE:relay_read(Node, Key, TxId, Sender, SpeculaRead),
    {noreply, SD0};

handle_call({get_oldest}, _Sender, SD0=#state{client_dict=ClientDict}) ->
    PendList = dict:to_list(ClientDict),
    Result = lists:foldl(fun({Key, Value}, Oldest) ->
                case is_pid(Key) of
                    true ->
                            case Value#client_state.pending_list of [] -> 
                                case Value#client_state.tx_id of 
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

%handle_cast({trace, PrevTxs, TxId, Sender, InfoList}, SD0=#state{dep_dict=DepDict, pending_list=PendingList, client_dict=ClientDict}) ->
%    case dict:find(TxId, DepDict) of
%        error ->    Info = io_lib:format("~p: can not find this txn!!\n", [TxId]),
%                    gen_server:cast(Sender, {end_trace, PrevTxs++[TxId], InfoList++[Info]});
%        {ok, {0, [], _}} -> Info = io_lib:format("~p: probably blocked by predecessors, pend list is ~p\n", [TxId, PendingList]),
%                            [Head|_T] = PendingList,
%                            send_prob(DepDict, Head, InfoList++[Info], Sender);
%        {ok, {N, [], _}} -> 
%                            {LocalParts, RemoteParts, _, _, _} = dict:fetch(TxId, ClientDict),
%                            Info = io_lib:format("~p: blocked with ~w remaining prepare, local parts are ~w, remote parts are ~w \n", [TxId, N, LocalParts, RemoteParts]),
%                            gen_server:cast(Sender, {end_trace, PrevTxs++[TxId], InfoList++[Info]}); 
%        {ok, {N, ReadDeps, _}} -> lists:foreach(fun(BTxId) ->
%                    Info = io_lib:format("~p: blocked by ~p, N is ~p, read deps are ~p \n", [TxId, BTxId, N, ReadDeps]),
%                    gen_server:cast(BTxId#tx_id.server_pid, {trace, PrevTxs++[TxId], BTxId, Sender, InfoList ++ [Info]}) end, ReadDeps)
%    end,
%    {noreply, SD0};

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
            specula_length=SpeculaLength, specula_read=SpeculaRead, rep_dict=RepDict, total_repl_factor=TotalReplFactor}};

%% Receiving local prepare. Can only receive local prepare for two kinds of transaction
%%  Current transaction that has not finished local cert phase
%%  Transaction that has already cert_aborted.
%% Has to add local_cert here, because may receive pending_prepared, prepared from the same guy.
handle_cast({pending_prepared, TxId, PrepareTime}, 
        SD0=#state{dep_dict=DepDict, specula_length=SpeculaLength, total_repl_factor=TotalReplFactor, client_dict=ClientDict, pending_txs=PendingTxs,rep_dict=RepDict}) ->  
    Client = TxId#tx_id.client_pid,
   %lager:warning("Got pending preapre for ~w", [TxId]),
    case dict:find(Client, ClientDict) of
        error -> {noreply, SD0};
        {ok, ClientState} ->
    Stage = ClientState#client_state.stage,
    MyTxId = ClientState#client_state.tx_id, 
    case (Stage == local_cert) and (MyTxId == TxId) of
        true -> 
            Sender = ClientState#client_state.sender,
            PendingList = ClientState#client_state.pending_list,
            LocalParts = ClientState#client_state.local_updates, 
            RemoteUpdates = ClientState#client_state.remote_updates, 
            PendingPrepares = ClientState#client_state.pending_prepares, 
            %lager:warning("Speculative receive pending_prepared for ~w, current pp is ~w", [TxId, PendingPrepares+1]),
            case dict:find(TxId, DepDict) of
                %% Maybe can commit already.
                {ok, {1, ReadDepTxs, OldPrepTime}} ->
                    NewMaxPrep = max(PrepareTime, OldPrepTime),
                    RemoteParts = [P || {P, _} <- RemoteUpdates],
                    RemoteToAck = length(RemoteParts),
                    case length(PendingList) >= SpeculaLength of
                        true ->
                            %lager:warning("Pending prep: decided to wait and prepare ~w, pending list is ~w!!", [TxId, PendingList]),
                            ?CLOCKSI_VNODE:prepare(RemoteUpdates, TxId, {remote, ignore}),
                            DepDict1 = dict:store(TxId, {RemoteToAck*TotalReplFactor+PendingPrepares+1, ReadDepTxs, NewMaxPrep}, DepDict),
                            ClientDict1 = dict:store(Client, ClientState#client_state{stage=remote_cert, spec_commit_time=os:timestamp()}, ClientDict),
                            {noreply, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}};
                        false ->
                             %lager:warning("Pending prep: decided to speculate ~w and prepare to ~w pending list is ~w!!", [TxId, RemoteUpdates, PendingList]),
                            AbortedReads = ClientState#client_state.aborted_reads,
                            CommittedReads = ClientState#client_state.committed_reads,
                            CommittedUpdates = ClientState#client_state.committed_updates,
                            PendingTxs1 = dict:store(TxId, {LocalParts, RemoteParts, no_wait}, PendingTxs),
                            {ProposeTS, AvoidNum} = add_to_table(RemoteUpdates, TxId, NewMaxPrep, RepDict),
                            %lager:warning("Add table: specula txn ~w, avoided ~w, still need ~w, pending list is ~w", [TxId, AvoidNum, TotalReplFactor*RemoteToAck+PendingPrepares+1-AvoidNum, PendingList]),
                             %lager:warning("Proposets for ~w is ~w", [ProposeTS, TxId]),
                            ?CLOCKSI_VNODE:prepare(RemoteUpdates, ProposeTS, TxId, {remote, node()}),
                            gen_server:reply(Sender, {ok, {specula_commit, NewMaxPrep, {rev(AbortedReads),
                                                      rev(CommittedUpdates), rev(CommittedReads)}}}),
                            DepDict1 = dict:store(TxId, {TotalReplFactor*RemoteToAck+PendingPrepares+1-AvoidNum, ReadDepTxs, NewMaxPrep}, DepDict),
                            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, pending_list=PendingList++[TxId],committed_updates=[], committed_reads=[], aborted_reads=[]}, ClientDict),
                            {noreply, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1, pending_txs=PendingTxs1}}
                    end;
                {ok, {N, ReadDeps, OldPrepTime}} ->
                    %lager:warning("~w needs ~w local prep replies", [TxId, N-1]),
                    DepDict1 = dict:store(TxId, {N-1, ReadDeps, max(PrepareTime, OldPrepTime)}, DepDict),
                    ClientDict1 = dict:store(Client, ClientState#client_state{pending_prepares=PendingPrepares+1}, ClientDict),
                    {noreply, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}};
                error ->
                    {noreply, SD0}
            end;
        _ ->
            {noreply, SD0}
    end
    end;

handle_cast({solve_pending_prepared, TxId, PrepareTime, _From}, 
	    SD0=#state{dep_dict=DepDict, client_dict=ClientDict}) ->
    %lgaer:warning("Got prepare for ~w from ~w", [TxId, From]),
    Client = TxId#tx_id.client_pid,
    case dict:find(Client, ClientDict) of
        error -> {noreply, SD0};
        {ok, ClientState} ->
    Stage = ClientState#client_state.stage,
    MyTxId = ClientState#client_state.tx_id, 

    case (Stage == local_cert) and (MyTxId == TxId) of
        true -> 
            PendingPrepares = ClientState#client_state.pending_prepares, 
            ClientDict1 = dict:store(Client, ClientState#client_state{pending_prepares=PendingPrepares-1}, ClientDict),
            {noreply, SD0#state{client_dict=ClientDict1}};
        false ->
            case dict:find(TxId, DepDict) of
                {ok, {1, [], OldPrepTime}} -> %% Maybe the transaction can commit 
                    NowPrepTime =  max(OldPrepTime, PrepareTime),
                    DepDict1 = dict:store(TxId, {0, [], NowPrepTime}, DepDict),
                    {noreply, try_solve_pending([{NowPrepTime, TxId}], [], SD0#state{dep_dict=DepDict1}, [])};
                {ok, {PrepDeps, ReadDeps, OldPrepTime}} -> %% Maybe the transaction can commit 
                      %lgaer:warning("~w not enough.. Prep ~w, Read ~w", [TxId, PrepDeps, ReadDeps]),
                    DepDict1=dict:store(TxId, {PrepDeps-1, ReadDeps, max(PrepareTime, OldPrepTime)}, DepDict),
                    {noreply, SD0#state{dep_dict=DepDict1}};
                error ->
                    {noreply, SD0}
            end
    end
    end;

handle_cast({prepared, TxId, PrepareTime}, 
        SD0=#state{total_repl_factor=TotalReplFactor, dep_dict=DepDict, specula_length=SpeculaLength, 
            client_dict=ClientDict, rep_dict=RepDict, pending_txs=PendingTxs}) ->
   %lager:warning("Got prepare for ~w", [TxId]),
    Client = TxId#tx_id.client_pid,
    case dict:find(Client, ClientDict) of
        error -> {noreply, SD0};
        {ok, ClientState} ->
    Stage = ClientState#client_state.stage,
    Sender = ClientState#client_state.sender,
    MyTxId = ClientState#client_state.tx_id, 
    AbortedRead = ClientState#client_state.aborted_reads,
    CommittedUpdated = ClientState#client_state.committed_updates,
    CommittedReads = ClientState#client_state.committed_reads,

    case (Stage == local_cert) and (MyTxId == TxId) of
        true -> 
            PendingList = ClientState#client_state.pending_list,
            LocalParts = ClientState#client_state.local_updates, 
            RemoteUpdates = ClientState#client_state.remote_updates, 
            PendingPrepares = ClientState#client_state.pending_prepares, 
            case dict:find(TxId, DepDict) of
                %% Maybe can commit already.
                {ok, {1, ReadDepTxs, OldPrepTime}} ->
                    NewMaxPrep = max(PrepareTime, OldPrepTime),
                    RemoteParts = [P || {P, _} <- RemoteUpdates],
                    RemoteToAck = length(RemoteParts),
                    case (RemoteToAck == 0) and (ReadDepTxs == [])
                          and (PendingList == []) and (PendingPrepares == 0) of
                        true ->
                            SD1 = try_solve_pending([{NewMaxPrep, TxId}], [], SD0, []), 
                            {noreply, SD1}; 
                        false ->
                            %lager:warning("Pending list is ~w, pending prepares is ~w, ReadDepTxs is ~w", [PendingList, PendingPrepares, ReadDepTxs]),
                            case length(PendingList) >= SpeculaLength of
                                true -> 
                                    %%In wait stage, only prepare and doesn't add data to table
                                   %lager:warning("Decided to wait and prepare ~w, pending list is ~w!!", [TxId, PendingList]),
                                    ?CLOCKSI_VNODE:prepare(RemoteUpdates, TxId, {remote, ignore}),
                                    DepDict1 = dict:store(TxId, {TotalReplFactor*RemoteToAck+PendingPrepares, ReadDepTxs, NewMaxPrep}, DepDict),
                                    ClientDict1 = dict:store(Client, ClientState#client_state{stage=remote_cert, spec_commit_time=os:timestamp()}, ClientDict),
                                    {noreply, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}};
                                false ->
                                    %% Add dependent data into the table
                                    PendingTxs1 = dict:store(TxId, {LocalParts, RemoteParts, no_wait}, PendingTxs),
                                    {ProposeTS, NumAvoid} = add_to_table(RemoteUpdates, TxId, NewMaxPrep, RepDict),
                                   %lager:warning("Specula txn ~w, got propose time ~w, avoided ~w, still need ~w, Replying specula_commit", [TxId, ProposeTS, NumAvoid, TotalReplFactor*RemoteToAck+PendingPrepares-NumAvoid]),
                                    ?CLOCKSI_VNODE:prepare(RemoteUpdates, ProposeTS, TxId, {remote, node()}),
                                    gen_server:reply(Sender, {ok, {specula_commit, NewMaxPrep, {rev(AbortedRead), rev(CommittedUpdated), rev(CommittedReads)}}}),
                                    DepDict1 = dict:store(TxId, {TotalReplFactor*RemoteToAck+PendingPrepares-NumAvoid, ReadDepTxs, NewMaxPrep}, DepDict),
                                    ClientDict1 = dict:store(Client, ClientState#client_state{pending_list=PendingList++[TxId], tx_id=?NO_TXN, 
                                        aborted_update=?NO_TXN, aborted_reads=[], committed_reads=[], committed_updates=[]}, ClientDict),
                                    {noreply, SD0#state{dep_dict=DepDict1, min_snapshot_ts=NewMaxPrep, client_dict=ClientDict1, pending_txs=PendingTxs1}}
                            end
                        end;
                {ok, {N, ReadDeps, OldPrepTime}} ->
                  %lager:warning("~w needs ~w local prep replies", [TxId, N-1]),
                    DepDict1 = dict:store(TxId, {N-1, ReadDeps, max(PrepareTime, OldPrepTime)}, DepDict),
                    {noreply, SD0#state{dep_dict=DepDict1}};
                error ->
                    {noreply, SD0}
            end;
        false ->
            case dict:find(TxId, DepDict) of
                {ok, {1, [], OldPrepTime}} -> %% Maybe the transaction can commit 
                    NowPrepTime =  max(OldPrepTime, PrepareTime),
                    DepDict1 = dict:store(TxId, {0, [], NowPrepTime}, DepDict),
                    {noreply, try_solve_pending([{NowPrepTime, TxId}], [], SD0#state{dep_dict=DepDict1}, [])};
                {ok, {PrepDeps, ReadDeps, OldPrepTime}} -> %% Maybe the transaction can commit 
                     %lager:warning("~w not enough.. Prep ~w, Read ~w", [TxId, PrepDeps, ReadDeps]),
                    DepDict1=dict:store(TxId, {PrepDeps-1, ReadDeps, max(PrepareTime, OldPrepTime)}, DepDict),
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
handle_cast({read_valid, PendingTxId, PendedTxId}, SD0=#state{dep_dict=DepDict, client_dict=ClientDict}) ->
     %lgaer:warning("Got read valid for ~w of ~w", [PendingTxId, PendedTxId]),
    case dict:find(PendingTxId, DepDict) of
        {ok, {read_only, [PendedTxId], _ReadOnlyTs}} ->
            %gen_server:reply(Sender, {ok, {committed, ReadOnlyTs}}),
            ClientState = dict:fetch(PendingTxId#tx_id.client_pid, ClientDict),
           %lgaer:warning("Committed reads are ~w, ~w", [PendingTxId, ClientState#client_state.committed_reads]),
            ClientState1 = ClientState#client_state{committed_reads=[PendingTxId|ClientState#client_state.committed_reads]},
            {noreply, SD0#state{dep_dict=dict:erase(PendingTxId, DepDict), client_dict=dict:store(PendingTxId#tx_id.client_pid,
                    ClientState1, ClientDict)}};
        {ok, {read_only, MoreDeps, ReadOnlyTs}} ->
            {noreply, SD0#state{dep_dict=dict:store(PendingTxId, {read_only, lists:delete(PendedTxId, MoreDeps), ReadOnlyTs}, DepDict)}};
        {ok, {0, SolvedReadDeps, 0}} -> 
            %% Txn is still reading!!
            {noreply, SD0#state{dep_dict=dict:store(PendingTxId, {0, [PendedTxId|SolvedReadDeps], 0}, DepDict)}};
        {ok, {0, [PendedTxId], OldPrepTime}} -> %% Maybe the transaction can commit 
            SD1 = SD0#state{dep_dict=dict:store(PendingTxId, {0, [],OldPrepTime}, DepDict)},
            {noreply, try_solve_pending([{OldPrepTime, PendingTxId}], [], SD1, [])};
        {ok, {PrepDeps, ReadDepTxs, OldPrepTime}} -> 
            %lgaer:warning("Can not commit... Remaining prepdep is ~w, read dep is ~w", [PrepDeps, lists:delete(PendedTxId, ReadDepTxs)]),
            {noreply, SD0#state{dep_dict=dict:store(PendingTxId, {PrepDeps, lists:delete(PendedTxId, ReadDepTxs), 
                OldPrepTime}, DepDict)}};
        error ->
            {noreply, SD0}
    end;

handle_cast({read_invalid, _MyCommitTime, TxId}, SD0) ->
    {noreply, try_solve_pending([], [{ignore, TxId}], SD0, [])};
    %read_abort(read_invalid, MyCommitTime, TxId, SD0);
handle_cast({read_aborted, _MyCommitTime, TxId}, SD0) ->
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

%%%%%%%%%%%%%%%%%%%%%
try_solve_pending([], [], SD0=#state{client_dict=ClientDict, rep_dict=RepDict, dep_dict=DepDict, pending_txs=PendingTxs,
            min_commit_ts=MinCommitTs}, ClientsOfCommTxns) ->
    {ClientDict1, DepDict1, PendingTxs1, NewMayCommit, NewToAbort, NewCommitTime} = 
            lists:foldl(fun(Client, {CD, DD, PD, MayCommit, ToAbort, PCommitTime}) ->
                    CState = dict:fetch(Client, CD),
                    TxId = CState#client_state.tx_id,
                    case TxId of
                        ?NO_TXN -> {CD, DD, PD, MayCommit, ToAbort, PCommitTime};
                        _ ->
                            case CState#client_state.committed_updates of [] -> {CD, DD, PD, MayCommit, ToAbort, PCommitTime};
                                _ ->
                                    %AbortedReads = CState#client_state.aborted_reads,
                                    %CommittedUpdates = CState#client_state.committed_updates,
                                    %CommittedReads = CState#client_state.committed_reads,
                                    PendingList = CState#client_state.pending_list,
                                    Sender = CState#client_state.sender,
                                    LocalParts = CState#client_state.local_updates, 
                                    TxState = dict:fetch(TxId, DepDict),
                                    case TxState of 
                                        {0, [], 0} -> {CD, DD, PD, MayCommit, ToAbort, PCommitTime}; %%Still reading 
                                        {Prep, Read, OldCurPrepTime} -> %% Can not commit, but can specualte (due to the number of specula txns decreased)
                                            case CState#client_state.stage of
                                                remote_cert ->
                                                    SpeculaPrepTime = max(PCommitTime+1, OldCurPrepTime),
                                                    RemoteParts = [P||{P, _} <- CState#client_state.remote_updates],
                                                     %lgaer:warning("Spec comm curr txn: ~p, remote updates are ~p", [TxId, RemoteParts]),
                                                    %specula_commit(LocalParts, RemoteParts, TxId, SpeculaPrepTime, RepDict),
                                                    case (Prep == 0) and (Read == []) and (PendingList == []) of
                                                        true ->   
                                                             %lgaer:warning("Can already commit ~w!!", [TxId]),
                                                            {DD1, NewMayCommit, NewToAbort, CD1} = commit_tx(TxId, SpeculaPrepTime, 
                                                                LocalParts, RemoteParts, dict:erase(TxId, DD), RepDict, CD, waited),
                                                            CS1 = dict:fetch(Client, CD1),
                                                            gen_server:reply(Sender, {ok, {committed, SpeculaPrepTime, {rev(CS1#client_state.aborted_reads), rev(CS1#client_state.committed_updates), CS1#client_state.committed_reads, CS1#client_state.spec_commit_time}}}),
                                                            CD2 = dict:store(Client, CS1#client_state{committed_updates=[], committed_reads=[], aborted_reads=[],
                                                                pending_list=[], tx_id=?NO_TXN}, CD1),
                                                            {CD2, DD1, PD, MayCommit++NewMayCommit, ToAbort++NewToAbort, SpeculaPrepTime};
                                                        false ->
                                                             %lgaer:warning("Returning specula_commit for ~w", [TxId]),
                                                            gen_server:reply(Sender, {ok, {specula_commit, SpeculaPrepTime, {rev(CState#client_state.aborted_reads), rev(CState#client_state.committed_updates), CState#client_state.committed_reads}}}),
                                                            PD1 = dict:store(TxId, {LocalParts, RemoteParts, waited}, PD),
                                                            CD1 = dict:store(Client, CState#client_state{committed_updates=[], committed_reads=[], aborted_reads=[], pending_list=PendingList ++ [TxId], tx_id=?NO_TXN}, CD),
                                                            {CD1, DD, PD1, MayCommit, ToAbort, PCommitTime}
                                                    end;
                                                _ -> {CD, DD, PD, MayCommit, ToAbort, PCommitTime}
                                            end
                                    end
                            end
                    end end, {ClientDict, DepDict, PendingTxs, [], [], MinCommitTs}, ClientsOfCommTxns),
    SD1 = SD0#state{client_dict=ClientDict1, dep_dict=DepDict1, pending_txs=PendingTxs1, min_commit_ts=NewCommitTime},
    case (NewMayCommit == []) and (NewToAbort == []) of
        true -> SD1;
        false ->
            try_solve_pending(NewMayCommit, NewToAbort, SD1, [])
    end;
try_solve_pending(ToCommitTxs, [{FromNode, TxId}|Rest], SD0=#state{client_dict=ClientDict, rep_dict=RepDict, pending_txs=PendingTxs, dep_dict=DepDict}, ClientsOfCommTxns) ->
    Client = TxId#tx_id.client_pid,
    ClientState = dict:fetch(Client, ClientDict),
    CurrentTxId = ClientState#client_state.tx_id, 
    Sender = ClientState#client_state.sender, 
    PendingList = ClientState#client_state.pending_list, 
    Stage = ClientState#client_state.stage, 
    LocalParts = ClientState#client_state.local_updates, 
    RemoteUpdates = ClientState#client_state.remote_updates, 
    AbortedUpdate = ClientState#client_state.aborted_update,
    AbortedReads = ClientState#client_state.aborted_reads,
    CommittedReads = ClientState#client_state.committed_reads,
    CommittedUpdates = ClientState#client_state.committed_updates,
    case dict:find(TxId, DepDict) of
        {ok, {read_only, _, _}} ->
            ClientState1 = ClientState#client_state{aborted_reads=[TxId|ClientState#client_state.aborted_reads]},
            try_solve_pending(ToCommitTxs, Rest, SD0#state{dep_dict=dict:erase(TxId, DepDict), 
                    client_dict=dict:store(Client, ClientState1, ClientDict)}, ClientsOfCommTxns);
        _ ->
        case start_from_list(TxId, PendingList) of
        [] ->
            case TxId of
                CurrentTxId ->
                    case Stage of
                        local_cert ->
                            %lager:warning("~w abort local!", [TxId]),
                            NewToAbort = case FromNode of [] -> abort_tx(TxId, LocalParts, [], RepDict);
                                                          _ ->  abort_tx(TxId, lists:delete(FromNode, LocalParts), [], RepDict)
                            end,
                            RD1 = dict:erase(TxId, DepDict),
                            gen_server:reply(Sender, {aborted, {false, AbortedReads, rev(CommittedUpdates), CommittedReads}}),
                            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, committed_updates=[], committed_reads=[], aborted_reads=[]}, ClientDict), 
                            try_solve_pending(ToCommitTxs, Rest++NewToAbort, 
                                SD0#state{dep_dict=RD1, client_dict=ClientDict1}, ClientsOfCommTxns);
                            %{noreply, SD0#state{dep_dict=RD1, read_aborted=RAD1, read_invalid=RID1, client_dict=ClientDict1}};
                        remote_cert ->
                            %lager:warning("~w abort remote!", [TxId]),
                            RemoteParts = [P||{P, _} <-RemoteUpdates],
                            NewToAbort = case FromNode of [] -> abort_tx(TxId, LocalParts, RemoteParts, RepDict);
                                                          _ -> abort_tx(TxId, LocalParts, lists:delete(FromNode, RemoteParts), RepDict)
                            end,
                            RD1 = dict:erase(TxId, DepDict),
                            gen_server:reply(Sender, {aborted, {true, AbortedReads, rev(CommittedUpdates), CommittedReads}}),
                            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, committed_updates=[], committed_reads=[], aborted_reads=[]}, ClientDict), 
                            try_solve_pending(ToCommitTxs, Rest++NewToAbort, 
                                SD0#state{dep_dict=RD1, client_dict=ClientDict1}, ClientsOfCommTxns);
                            %{noreply, SD0#state{dep_dict=RD1, client_dict=ClientDict1, read_aborted=RAD1, read_invalid=RID1}};
                        read ->
                            ClientDict1 = dict:store(Client, ClientState#client_state{invalid_aborted=1}, ClientDict), 
                            try_solve_pending(ToCommitTxs, Rest, SD0#state{client_dict=ClientDict1}, ClientsOfCommTxns)
                            %{noreply, SD0#state{client_dict=ClientDict1}}
                    end;
                _ -> %% The transaction has already been aborted or whatever
                    try_solve_pending(ToCommitTxs, Rest, SD0, ClientsOfCommTxns)
            end;
        {Prev, L} ->
            {PendingTxs1, RD, NewToAbort} = abort_specula_list(L, RepDict, DepDict, PendingTxs, FromNode, []),
            case CurrentTxId of
                ?NO_TXN ->
                    %lgaer:warning("Abort due to read invlalid, pend tx is ~w, No current txn, Pendinglist is ~w, Prev is ~w", [PendingList, TxId, Prev]),
                    %% The clien is sleeping now! Need to reply msg.
                    MsgId = ClientState#client_state.msg_id,
                    %Client ! {final_abort, MsgId+1, TxId, AbortedReads, rev(CommittedUpdates), rev(CommittedReads)},
                    gen_fsm:send_event(Client, {final_abort, MsgId+1, TxId, AbortedReads, rev(CommittedUpdates), CommittedReads}),
                    ClientDict1 = dict:store(Client, ClientState#client_state{pending_list=Prev, msg_id=MsgId+1,
                            aborted_update=?NO_TXN, aborted_reads=[], committed_updates=[], committed_reads=[]}, ClientDict), 
                    try_solve_pending(ToCommitTxs, Rest++NewToAbort, SD0#state{dep_dict=RD, client_dict=ClientDict1, pending_txs=PendingTxs1}, ClientsOfCommTxns);
                    %{noreply, SD0#state{dep_dict=RD, client_dict=ClientDict2,
                    %    read_aborted=RAD1, read_invalid=RID1, cascade_aborted=CascadAborted+Length-1}};
                _ ->
                    case Stage of
                        local_cert -> 
                            %lgaer:warning("Read invalid cascade Local abort: current tx is ~w, local parts are ~w, repdict is ~p", [CurrentTxId, LocalParts, dict:to_list(RepDict)]),
                            NewToAbort1 = abort_tx(CurrentTxId, LocalParts, [], RepDict),
                            gen_server:reply(Sender, {cascade_abort, {TxId, AbortedReads, rev(CommittedUpdates), CommittedReads}}),
                            RD1 = dict:erase(CurrentTxId, RD),
                            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, pending_list=Prev,
                                    aborted_update=?NO_TXN, aborted_reads=[], committed_updates=[], committed_reads=[]}, ClientDict), 
                            try_solve_pending(ToCommitTxs, Rest++NewToAbort++NewToAbort1, SD0#state{dep_dict=RD1, client_dict=ClientDict1, pending_txs=PendingTxs1}, ClientsOfCommTxns);
                            %{noreply, SD0#state{dep_dict=RD1, client_dict=ClientDict2,
                            %    read_aborted=RAD1, read_invalid=RID1, cascade_aborted=CascadAborted+Length}};
                        remote_cert -> 
                            %lgaer:warning("Read invalid Remote abort: TxId is ~w, Pendinglist is ~w", [CurrentTxId, PendingList]),
                            RemoteParts = [P||{P, _} <-RemoteUpdates],
                            NewToAbort1 = abort_tx(CurrentTxId, LocalParts, RemoteParts, RepDict),
                            gen_server:reply(Sender, {cascade_abort, {TxId, AbortedReads, rev(CommittedUpdates), CommittedReads}}),
                            RD1 = dict:erase(CurrentTxId, RD),
                            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, pending_list=Prev,
                                aborted_update=?NO_TXN, aborted_reads=[], committed_updates=[], committed_reads=[]}, ClientDict), 
                            try_solve_pending(ToCommitTxs, Rest++NewToAbort++NewToAbort1, SD0#state{dep_dict=RD1, 
                                  client_dict=ClientDict1, pending_txs=PendingTxs1}, ClientsOfCommTxns);
                            %{noreply, SD0#state{dep_dict=RD1, client_dict=ClientDict2,
                            %    read_aborted=RAD1, read_invalid=RID1, cascade_aborted=CascadAborted+Length}};
                        read ->
                            %lgaer:warning("Has current txn ~w read, Pendinglist is ~w, Rest is ~w", [TxId, PendingList, Rest]),
                            ClientDict1 = dict:store(Client, ClientState#client_state{pending_list=Prev, invalid_aborted=1,
                                aborted_update=get_older(AbortedUpdate, TxId)}, ClientDict), 
                            try_solve_pending(ToCommitTxs, Rest++NewToAbort, SD0#state{dep_dict=RD, 
                                  pending_txs=PendingTxs1, client_dict=ClientDict1}, ClientsOfCommTxns)
                            %{noreply, SD0#state{dep_dict=RD, client_dict=ClientDict2,
                            %    read_aborted=RAD1, read_invalid=RID1, cascade_aborted=CascadAborted+Length-1}}
                    end
            end
        end
    end;
try_solve_pending([{NowPrepTime, PendingTxId}|Rest], [], SD0=#state{client_dict=ClientDict, rep_dict=RepDict, dep_dict=DepDict, 
            pending_txs=PendingTxs, min_commit_ts=MinCommitTS}, ClientsOfCommTxns) ->
    Client = PendingTxId#tx_id.client_pid,
    ClientState = dict:fetch(Client, ClientDict),
    PendingList = ClientState#client_state.pending_list, 
     %lgaer:warning("Pending list is ~w, tx is ~w", [PendingList, PendingTxId]),
    Sender = ClientState#client_state.sender, 

    PendingList = ClientState#client_state.pending_list,
    LocalParts = ClientState#client_state.local_updates, 
    RemoteUpdates = ClientState#client_state.remote_updates, 
    CommittedUpdates = ClientState#client_state.committed_updates,
    AbortedReads = ClientState#client_state.aborted_reads,
    case PendingList of
        [] -> %% This is the just report_committed txn.. But new txn has not come yet.
             %lgaer:warning("Pending list no me ~p, commit current tx!",[PendingTxId]),
            RemoteParts = [P||{P, _} <-RemoteUpdates],
            DepDict2 = dict:erase(PendingTxId, DepDict),
            CurCommitTime = max(NowPrepTime, MinCommitTS+1),
            {DepDict3, MayCommTxs, ToAbortTxs, ClientDict1} = commit_tx(PendingTxId, CurCommitTime, LocalParts, RemoteParts,
                DepDict2, RepDict, ClientDict),
            CS = dict:fetch(Client, ClientDict1),
            gen_server:reply(Sender, {ok, {committed, CurCommitTime, {AbortedReads, rev(CommittedUpdates), CS#client_state.committed_reads, CS#client_state.spec_commit_time}}}),
            ClientDict2 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, pending_list=[], aborted_reads=[],
                    committed_updates=[], committed_reads=[], aborted_update=?NO_TXN}, ClientDict1),
            try_solve_pending(Rest++MayCommTxs, ToAbortTxs, SD0#state{min_commit_ts=CurCommitTime, dep_dict=DepDict3, 
                          client_dict=ClientDict2}, ClientsOfCommTxns);
        [PendingTxId|ListRest] ->
            %lgaer:warning("Pending list contain me ~p!",[PendingTxId]),
            CommitTime = max(NowPrepTime, MinCommitTS+1),
            {PendingTxs1, DepDict1, ToAbortTxs, MayCommitTxs, ClientDict1} = commit_specula_tx(PendingTxId, CommitTime,
                    dict:erase(PendingTxId, DepDict), RepDict, PendingTxs, ClientDict),		 
            Now = os:timestamp(),

            {PendingTxs2, NewPendingList, NewMaxPT, DepDict2, NewToAbort, MyNext, ClientDict2, CommittedTxs}= 
                try_commit_follower(CommitTime, ListRest, RepDict, DepDict1, PendingTxs1, ClientDict1, [], [], []),
            CS2 = dict:fetch(Client, ClientDict2),
            ClientDict3 = dict:store(Client, CS2#client_state{pending_list=NewPendingList, 
                    committed_updates=CommittedTxs++[{PendingTxId, Now}|CommittedUpdates]}, ClientDict2),
            try_solve_pending(Rest++MayCommitTxs++MyNext, Rest++ToAbortTxs++NewToAbort, SD0#state{min_commit_ts=NewMaxPT, dep_dict=DepDict2, 
                          pending_txs=PendingTxs2, client_dict=ClientDict3}, [Client|ClientsOfCommTxns]);
        _ ->
            %DepDict1 = dict:update(PendingTxId, fun({_, _, _}) ->
            %                            {0, [], NowPrepTime} end, DepDict),
              %lgaer:warning("got all replies, but I am not the first! PendingList is ~w", [PendingList]),
            try_solve_pending(Rest, [], SD0, ClientsOfCommTxns)
            %SD0#state{dep_dict=DepDict1}
    end.

%decide_after_cascade(PendingList, DepDict, NumAborted, TxId, Stage) ->
%      %lgaer:warning("PendingList ~w, DepDict ~w, NumAborted ~w, TxId ~w, Stage ~w", [PendingList, DepDict, NumAborted, TxId, Stage]),
%    case TxId of
%        ?NO_TXN -> wait;
%        _ -> 
%            case NumAborted > 0 of
%                true ->   %lgaer:warning("Abort due to flow dep"),
%                        case Stage of read -> invalid; %% Abort due to flow dependency. Second indicate flow abort, third read invalid 
%                                     local_cert -> abort_local;
%                                     remote_cert -> abort_remote
%                        end;
%                false ->
%                    case dict:find(TxId, DepDict) of %% TODO: should give a invalid_aborted 
%                        error ->     %lgaer:warning("Abort due to read dep"),
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
commit_tx(TxId, CommitTime, LocalParts, RemoteParts, DepDict, RepDict, ClientDict) ->
    commit_tx(TxId, CommitTime, LocalParts, RemoteParts, DepDict, RepDict, ClientDict, no_wait).

commit_tx(TxId, CommitTime, LocalParts, RemoteParts, DepDict, RepDict, ClientDict, IfWaited) ->
    DepList = ets:lookup(dependency, TxId),
    {DepDict1, MayCommTxs, ToAbortTxs, ClientDict1} = solve_read_dependency(CommitTime, DepDict, DepList, ClientDict),
   %lager:warning("Commit ~w", [TxId]),
    ?CLOCKSI_VNODE:commit(LocalParts, TxId, CommitTime),
    ?REPL_FSM:repl_commit(LocalParts, TxId, CommitTime, false, RepDict),
    ?CLOCKSI_VNODE:commit(RemoteParts, TxId, CommitTime),
    case IfWaited of
        no_wait -> ?REPL_FSM:repl_commit(RemoteParts, TxId, CommitTime, false, RepDict, no_wait);
        waited -> ?REPL_FSM:repl_commit(RemoteParts, TxId, CommitTime, true, RepDict, waited)
    end,
    {DepDict1, MayCommTxs, ToAbortTxs, ClientDict1}.

commit_specula_tx(TxId, CommitTime, DepDict, RepDict, PendingTxs, ClientDict) ->
   %lager:warning("Committing specula tx ~w with ~w", [TxId, CommitTime]),
    {LocalParts, RemoteParts, IfWaited} = dict:fetch(TxId, PendingTxs),
    PendingTxs1 = dict:erase(TxId, PendingTxs),
    %%%%%%%%% Time stat %%%%%%%%%%%
    DepList = ets:lookup(dependency, TxId),
     %lager:warning("~w: My read dependncy are ~w", [TxId, DepList]),
    {DepDict1, MayCommTxs, ToAbortTxs, ClientDict1} = solve_read_dependency(CommitTime, DepDict, DepList, ClientDict),

    ?CLOCKSI_VNODE:commit(LocalParts, TxId, CommitTime),
    ?REPL_FSM:repl_commit(LocalParts, TxId, CommitTime, false, RepDict),
    %?REPL_FSM:repl_commit(LocalParts, TxId, CommitTime, DoRepl),
    ?CLOCKSI_VNODE:commit(RemoteParts, TxId, CommitTime),
    %?REPL_FSM:repl_commit(RemoteParts, TxId, CommitTime, DoRepl),
    ?REPL_FSM:repl_commit(RemoteParts, TxId, CommitTime, true, RepDict, IfWaited),
     %lager:warning("Specula commit ~w to ~w, ~w", [TxId, LocalParts, RemoteParts]),
    {PendingTxs1, DepDict1, ToAbortTxs, MayCommTxs, ClientDict1}.

abort_specula_tx(TxId, PendingTxs, RepDict, DepDict, ExceptNode) ->
    {LocalParts, RemoteParts, IfWaited} = dict:fetch(TxId, PendingTxs),
    PendingTxs1 = dict:erase(TxId, PendingTxs),
    %%%%%%%%% Time stat %%%%%%%%%%%
    DepList = ets:lookup(dependency, TxId),
    %lgaer:warning("Abort specula ~w: My read dependncy are ~w", [TxId, DepList]),
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
                                       %lgaer:warning("~w is not my own, read invalid", [DepTxId]),
                                      ?READ_ABORTED(TxServer, -1, DepTxId), AccToAbort
                              end
                    end, [], DepList),
    ?CLOCKSI_VNODE:abort(LocalParts, TxId),
    ?REPL_FSM:repl_abort(LocalParts, TxId, false, RepDict),
    ?CLOCKSI_VNODE:abort(lists:delete(ExceptNode, RemoteParts), TxId),
    ?REPL_FSM:repl_abort(RemoteParts, TxId, true, RepDict, IfWaited),
    {PendingTxs1, dict:erase(TxId, DepDict), ToAbortTxs}.

abort_specula_tx(TxId, PendingTxs, RepDict, DepDict) ->
    %[{TxId, {LocalParts, RemoteParts, _LP, _RP, IfWaited}}] = ets:lookup(ClientDict, TxId), 
    %ets:delete(ClientDict, TxId),
    {LocalParts, RemoteParts, IfWaited} = dict:fetch(TxId, PendingTxs),
    PendingTxs1 = dict:erase(TxId, PendingTxs),
    DepList = ets:lookup(dependency, TxId),
      %lgaer:warning("Specula abort ~w: My read dependncy are ~w", [TxId, DepList]),
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
                                       %lgaer:warning("~w is not my own, read invalid", [DepTxId]),
                                      ?READ_ABORTED(TxServer, -1, DepTxId), AccToAbort
                              end
                    end, [], DepList),
    ?CLOCKSI_VNODE:abort(LocalParts, TxId),
    ?REPL_FSM:repl_abort(LocalParts, TxId, false, RepDict),
    ?CLOCKSI_VNODE:abort(RemoteParts, TxId),
    ?REPL_FSM:repl_abort(RemoteParts, TxId, true, RepDict, IfWaited),
    %abort_to_table(RemoteParts, TxId, RepDict),
    {PendingTxs1, dict:erase(TxId, DepDict), ToAbortTxs}.

abort_tx(TxId, LocalParts, RemoteParts, RepDict) ->
    %true = ets:delete(ClientDict, TxId),
    DepList = ets:lookup(dependency, TxId),
   %lager:warning("Abort ~w: My read dependncy are ~w", [TxId, DepList]),
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
                                    %lager:warning("~w is not my own, read invalid", [DepTxId]),
                                    ?READ_ABORTED(TxServer, -1, DepTxId),
                                    AccTxs
                            end
                    end, [], DepList),
    ?CLOCKSI_VNODE:abort(LocalParts, TxId),
    ?REPL_FSM:repl_abort(LocalParts, TxId, false, RepDict),
    ?CLOCKSI_VNODE:abort(RemoteParts, TxId),
    ?REPL_FSM:repl_abort(RemoteParts, TxId, false, RepDict),
    ToAbortTxs.

%% Deal dependencies and check if any following transactions can be committed.
try_commit_follower(LastCommitTime, [], _RepDict, DepDict, PendingTxs, ClientDict, ToAbortTxs, MayCommitTxs, CommittedTxs) ->
    {PendingTxs, [], LastCommitTime, DepDict, ToAbortTxs, MayCommitTxs, ClientDict, CommittedTxs};
try_commit_follower(LastCommitTime, [H|Rest]=PendingList, RepDict, DepDict, 
                        PendingTxs, ClientDict, LastAbortTxs, LastMayCommit, CommittedTxs) ->
    Result = case dict:find(H, DepDict) of
                {ok, {0, [], PendingMaxPT}} ->
                    {true, max(PendingMaxPT, LastCommitTime+1)};
                _R ->
                    false
            end,
    case Result of
        false ->
            {PendingTxs, PendingList, LastCommitTime, DepDict, LastAbortTxs, LastMayCommit, ClientDict, CommittedTxs};
        {true, CommitTime} ->
            %lgaer:warning("Commit pending specula ~w", [H]),
            {PendingTxs1, DepDict1, ToAbortTxs, MayCommitTxs, ClientDict1} = commit_specula_tx(H, CommitTime, 
               dict:erase(H, DepDict), RepDict, PendingTxs, ClientDict),
            try_commit_follower(CommitTime, Rest, RepDict, DepDict1, PendingTxs1, ClientDict1, 
                    LastAbortTxs++ToAbortTxs, LastMayCommit++MayCommitTxs, [{H, os:timestamp()}|CommittedTxs])
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
solve_read_dependency(CommitTime, ReadDep, DepList, ClientDict) ->
    Self = self(),
    lists:foldl(fun({TxId, DepTxId}, {RD, MaybeCommit, ToAbort, CD}) ->
                    TxServer = DepTxId#tx_id.server_pid,
                    ets:delete_object(dependency, {TxId, DepTxId}), 
                    case DepTxId#tx_id.snapshot_time >= CommitTime of
                        %% This read is still valid
                        true ->
                              %lgaer:warning("Read still valid for ~w, CommitTime is ~w", [DepTxId, CommitTime]),
                            case TxServer == Self of
                                true ->
                                    %lgaer:warning("~w is my own, read valid", [DepTxId]),
                                    case dict:find(DepTxId, RD) of
                                        {ok, {0, _SolvedReadDeps, 0}} -> %% Local transaction is still reading
                                           %lgaer:warning("Deleting {~w, ~w} from antidep", [DepTxId, TxId]),
                                            true = ets:delete_object(anti_dep, {DepTxId, TxId}), 
                                            {RD, MaybeCommit, ToAbort, ClientDict};
                                        {ok, {read_only, [TxId], _}} ->
                                            CPid = DepTxId#tx_id.client_pid,
                                            CS = dict:fetch(CPid, CD),
                                           %lgaer:warning("Committed reads are ~w, ~w", [CS#client_state.committed_reads, DepTxId]),
                                            CS1 = CS#client_state{committed_reads=[DepTxId|CS#client_state.committed_reads]},
                                            {dict:erase(DepTxId, RD), MaybeCommit, ToAbort, dict:store(CPid, CS1, CD)};
                                        {ok, {0, [TxId], PrepTime}} ->
                                            case DepTxId#tx_id.client_pid == TxId#tx_id.client_pid of
                                                false ->
                                                    {dict:store(DepTxId, {0, [], PrepTime}, RD), 
                                                        [{PrepTime, DepTxId}|MaybeCommit], ToAbort, CD};
                                                true ->
                                                    {dict:store(DepTxId, {0, [], PrepTime}, RD), 
                                                          MaybeCommit, ToAbort, CD}
                                            end;
                                        {ok, {PrepDeps, ReadDeps, PrepTime}} ->
                                             %lgaer:warning("Prepdeps is ~p, Storing ~w for ~w", [PrepDeps, lists:delete(TxId, ReadDeps), DepTxId]),
                                            {dict:store(DepTxId, {PrepDeps, lists:delete(TxId, ReadDeps), 
                                                PrepTime}, RD), MaybeCommit, ToAbort, CD};
                                        error -> %% This txn hasn't even started certifying 
                                                 %% or has been cert_aborted already
                                                  %lgaer:warning("This txn has not even started"),
                                            {RD, MaybeCommit, ToAbort, CD}
                                    end;
                                _ ->
                                     %lgaer:warning("~w is not my own, read valid", [DepTxId]),
                                    ?READ_VALID(TxServer, DepTxId, TxId),
                                    {RD, MaybeCommit, ToAbort, CD}
                            end;
                        false ->
                            %% Read is not valid
                            case TxServer == Self of
                                true ->
                                       %lgaer:warning("~w is my own, read invalid", [DepTxId]),
                                    {RD, MaybeCommit, [{[], DepTxId}|ToAbort], CD};
                                _ ->
                                    %lgaer:warning("~w is not my own, read invalid", [DepTxId]),
                                    ?READ_INVALID(TxServer, CommitTime, DepTxId),
                                    {RD, MaybeCommit, ToAbort, CD}
                            end
                    end
                end, {ReadDep, [], [], ClientDict}, DepList).


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

add_to_table(WriteSet, TxId, PrepareTime, RepDict) ->
    {Repled, ProposeTS} = lists:foldl(fun({{Part, Node}, Updates}, {ToDelete, GotTS}) ->
                    case dict:find({rep, Node}, RepDict) of
                        {ok, DataReplServ} ->
                           %lager:warning("Data repl serveris ~p, ~p", [DataReplServ, Part]),
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
                  %lager:warning("Prepareing specula for ~w ~w to ~w", [TxId, Partition, Node]),
                ?DATA_REPL_SERV:prepare_specula(DataReplServ, TxId, Partition, Updates, PrepareTime),
                specula_prepare(Rest, TxId, PrepareTime, RepDict, NumParts+1);
            _ ->
                %% Send to local cache.
                ?CACHE_SERV:prepare_specula(TxId, Partition, Updates, PrepareTime),
                specula_prepare(Rest, TxId, PrepareTime, RepDict, NumParts)
    end.

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
    commit_tx(TxId1, CT, LocalParts, [], dict:new(), dict:new(), dict:new()),
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
    DepDict1 = dict:store(T1, {3, 1, 1}, DepDict),
    DepDict2 = dict:store(T2, {1, 1, 1}, DepDict1),
    DepDict3 = dict:store(T3, {0, 1, 1}, DepDict2),
    MyTable1 = dict:store(T1, {[{p1, n1}], [{p2, n2}], no_wait}, MyTable),
    MyTable2 = dict:store(T2, {[{p1, n1}], [{p2, n2}], no_wait}, MyTable1),
    MyTable3 = dict:store(T3, {[{p1, n1}], [{p2, n2}], no_wait}, MyTable2),
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
    DepDict1 = dict:store(T3, {2, [T0, T2,T1], 4}, DepDict),
    {RD2, _, T, _} = solve_read_dependency(CommitTime, DepDict1, [{T0, T1}, {T0, T2}, {T0, T3}, {T0, T4}], dict:new()),
    ?assertEqual({[{T3,{2,[T2,T1],4}}], [{[], T1}]}, {dict:to_list(RD2), T}),
    ?assertEqual(true, mock_partition_fsm:if_applied({read_valid, T4}, T0)),
    ?assertEqual(true, mock_partition_fsm:if_applied({read_invalid, T2}, nothing)).

try_commit_follower_test() ->
    ClientDict = dict:store(self(), #client_state{}, dict:new()),
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
    DepDict1 = dict:store(T1, {1, [], 2}, dict:new()),
    DepDict2 = dict:store(T2, {2, [1,2], 2}, DepDict1),
    DepDict3 = dict:store(T3, {2, [], 2}, DepDict2),
    %% T1 can not commit due to having read dependency 
    Result =  
            try_commit_follower(MaxPT1, [T1, T2], RepDict, DepDict2, PendingTxs, ClientDict, [], [], []),
    {PendingTxs1, PD1, MPT1, RD1, ToAbortTxs, MayCommitTxs, ClientDict1, CommittedTxs}=Result,

    ?assertEqual(ToAbortTxs, []),
    ?assertEqual(MayCommitTxs, []),
    ?assertEqual(CommittedTxs, []),
    ?assertEqual(RD1, DepDict2),
    ?assertEqual(PD1, [T1, T2]),
    ?assertEqual(MPT1, MaxPT1),

    %ets:insert(MyTable, {T1, {[{lp1, n1}], [{rp1, n3}], now(), now(), no_wait}}), 
    %ets:insert(MyTable, {T2, {[{lp1, n1}], [{rp1, n3}], now(), now(), no_wait}}), 
    %ets:insert(MyTable, {T3, {[{lp2, n2}], [{rp2, n4}], now(), now(), no_wait}}), 
    PendingTxs2 = dict:store(T1, {[{lp1, n1}], [{rp1, n3}], no_wait}, PendingTxs1), 
    PendingTxs3 = dict:store(T2, {[{lp1, n1}], [{rp1, n3}], no_wait}, PendingTxs2), 
    PendingTxs4 = dict:store(T3, {[{lp2, n2}], [{rp2, n4}], no_wait}, PendingTxs3), 
    DepDict4 = dict:store(T1, {0, [], 2}, DepDict3),
    %% T1 can commit because of read_dep ok. 
    {PendingTxs5, PD2, MPT2, RD2, ToAbortTxs1, MayCommitTxs1, ClientDict2, CommittedTxs1} =  try_commit_follower(MaxPT1, [T1, T2], RepDict, DepDict4, PendingTxs4, ClientDict1, [], [], []),
    ?assertEqual(ToAbortTxs1, []),
    ?assertEqual(MayCommitTxs1, []),
    ?assertMatch([{T1, _}], CommittedTxs1),
    %?assertEqual(true, mock_partition_fsm:if_applied({commit_specula, n3rep, T1, rp1}, MPT2)),
    io:format(user, "RD2 ~w, Dict4 ~w", [dict:to_list(RD2), dict:to_list(dict:erase(T1, DepDict4))]),
    ?assertEqual(RD2, dict:erase(T1, DepDict4)),
    ?assertEqual(PD2, [T2]),
    ?assertEqual(MPT2, MaxPT1+1),

    %% T2 can commit becuase of prepare OK
    DepDict5 = dict:store(T2, {0, [], 2}, RD2),
    {PendingTxs6, _PD3, MPT3, RD3, ToAbortedTxs2, MayCommitTxs2, ClientDict3, CommittedTxs2} =  try_commit_follower(MPT2, [T2, T3], RepDict, DepDict5, 
                                    PendingTxs5, ClientDict2, [], [], []),
    ?assertEqual(error, dict:find(T2, PendingTxs6)),
    ?assertEqual(MayCommitTxs2, []),
    ?assertEqual(ToAbortedTxs2, []),
    ?assertMatch([{T2, _}], CommittedTxs2),
    %?assertEqual(true, mock_partition_fsm:if_applied({repl_commit, TxId1, LocalParts}, CT)),
    ?assertEqual(true, mock_partition_fsm:if_applied({commit, T2, [{lp1, n1}]}, MPT3)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_commit, T2, [{rp1, n3}]}, MPT3)),
    %?assertEqual(true, mock_partition_fsm:if_applied({commit_specula, n3rep, T2, rp1}, MPT3)),

    %% T3, T4 get committed and T5 gets cert_aborted.
    PendingTxs7 = dict:store(T3, {[{lp2, n2}], [{rp2, n4}], no_wait}, PendingTxs6), 
    PendingTxs8 = dict:store(T4, {[{lp2, n2}], [{rp2, n4}], no_wait}, PendingTxs7), 
    PendingTxs9 = dict:store(T5, {[{lp2, n2}], [{rp2, n4}], no_wait}, PendingTxs8), 
    ets:insert(dependency, {T3, T4}),
    ets:insert(dependency, {T3, T5}),
    ets:insert(dependency, {T4, T5}),
    DepDict6 = dict:store(T3, {0, [], T3#tx_id.snapshot_time+1}, RD3),
    DepDict7 = dict:store(T4, {0, [T3], T5#tx_id.snapshot_time+1}, DepDict6),
    DepDict8 = dict:store(T5, {0, [T4,T5], 6}, DepDict7),
    io:format(user, "My commit time is ~w, Txns are ~w ~w ~w", [0, T3, T4, T5]),
    {PendingTxs10, PD4, MPT4, RD4, ToAbortTxs3, MayCommitTxs3, _ClientDict4, CommittedTxs3} 
            =  try_commit_follower(0, [T3, T4, T5], RepDict, DepDict8, PendingTxs9, ClientDict3, [], [], []),
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
