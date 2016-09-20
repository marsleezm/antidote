%
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
        dc_id :: non_neg_integer(),
        num_specula_read=0 :: non_neg_integer()
        }).

-record(client_state, {
        tx_id =?NO_TXN :: txid(),
        invalid_aborted=0 :: non_neg_integer(),
        specula_abort_count=0 :: non_neg_integer(),
        local_updates = []:: [],
        remote_updates =[] :: [],
        pending_list=[] :: [txid()],
        %% New stat
        read_deps = 0,
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
    [{dc_id, DcId}] = ets:lookup(meta_info, dc_id),
    TotalReplFactor = length(Replicas)+1,
    lager:warning("TotalReplFactor is ~w, RepDict is ~w", [TotalReplFactor, dict:to_list(RepDict)]),
    {ok, #state{pending_txs=PendingTxs, client_dict=ClientDict, dep_dict=dict:new(), total_repl_factor=TotalReplFactor, name=Name, 
            specula_length=SpeculaLength, specula_read=SpeculaRead, rep_dict=RepDict, dc_id=DcId}}.

handle_call({append_values, Node, KeyValues, CommitTime}, Sender, SD0) ->
    clocksi_vnode:append_values(Node, KeyValues, CommitTime, Sender),
    {noreply, SD0};

handle_call({get_pid}, _Sender, SD0) ->
    {reply, self(), SD0};

handle_call({start_read_tx}, _Sender, SD0) ->
    TxId = tx_utilities:create_tx_id(0),
    {reply, TxId, SD0};

handle_call({start_tx}, Sender, SD0) ->
    {Client, _} = Sender,
    handle_call({start_tx, 0, Client}, Sender, SD0);
handle_call({start_tx, TxnSeq}, Sender, SD0) ->
    {Client, _} = Sender,
    handle_call({start_tx, TxnSeq, Client}, Sender, SD0);
handle_call({start_tx, TxnSeq, Client}, Sender, SD0=#state{dep_dict=D, min_snapshot_ts=MinSnapshotTS, min_commit_ts=MinCommitTS, client_dict=ClientDict, dc_id=DcId}) ->
    NewSnapshotTS = max(MinSnapshotTS, MinCommitTS) + 1, 
    TxId = tx_utilities:create_tx_id(NewSnapshotTS, Client, TxnSeq, DcId),
     lager:warning("Start tx is ~p, Sender is ~p", [TxId, Sender]),
    ClientState = case dict:find(Client, ClientDict) of
                    error ->
                        #client_state{};
                    {ok, SenderState} ->
                        SenderState
                 end,
    D1 = dict:store(TxId, {0, 0, 0, 0}, D),
    OldTxId = ClientState#client_state.tx_id,
    PendingList = ClientState#client_state.pending_list, 
    Stage = ClientState#client_state.stage, 
    LocalParts = ClientState#client_state.local_updates, 
    RemoteUpdates = ClientState#client_state.remote_updates, 
    case OldTxId of
        ?NO_TXN ->
            ClientState1 = ClientState#client_state{tx_id=TxId, stage=read, pending_prepares=0},
               lager:warning("~p client state is ~p", [Client, ClientState1]),
            {reply, TxId, SD0#state{client_dict=dict:store(Client, ClientState1, ClientDict), dep_dict=D1, 
                min_snapshot_ts=NewSnapshotTS}};
        _ ->
            Stage = 1000000,
            case Stage of 
                read ->
                    ClientState1 = ClientState#client_state{tx_id=TxId, stage=read, pending_prepares=0, invalid_aborted=0},
                       lager:warning("~p client state is ~p", [Client, ClientState1]),
                    {reply, TxId, SD0#state{client_dict=dict:store(Client, ClientState1, ClientDict),
                        min_snapshot_ts=NewSnapshotTS, dep_dict=dict:erase(OldTxId, D1)}}; 
                _ ->
                    RemoteParts = [P||{P, _} <-RemoteUpdates],
                    ClientDict1 = dict:store(TxId, {LocalParts, RemoteParts, waited}, ClientDict),
                    ClientState1 = ClientState#client_state{tx_id=TxId, stage=read, pending_list=PendingList++[OldTxId], pending_prepares=0, invalid_aborted=0},
                       lager:warning("~p client state is ~p", [Client, ClientState1]),
                    {reply, TxId, SD0#state{dep_dict=D1, 
                        min_snapshot_ts=NewSnapshotTS, client_dict=dict:store(Client, ClientState1, ClientDict1)}}
            end
    end;

handle_call({get_cdf}, _Sender, SD0) ->
    {reply, ok, SD0};

handle_call({get_stat}, _Sender, SD0) ->
    {reply, ok, SD0};
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
     lager:warning("Start certifying ~w", [TxId]),
    ClientState = dict:fetch(Client, ClientDict),
    InvalidAborted = ClientState#client_state.invalid_aborted,
    TxId = ClientState#client_state.tx_id, 
    ReadDeps = ClientState#client_state.read_deps,
    SentMsgId = ClientState#client_state.msg_id,
    AbortedReads = ClientState#client_state.aborted_reads,
    CommittedReads = ClientState#client_state.committed_reads,
    CommittedUpdates = ClientState#client_state.committed_updates,
    case SentMsgId == ClientMsgId of
        true ->
            case {InvalidAborted, dict:find(TxId, DepDict)} of
                {1, _} ->
                    %% Some read is invalid even before the txn starts.. If invalid_aborted is larger than 0, it can possibly be saved.
                     lager:warning("~w aborted!", [TxId]),
                    ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, committed_updates=[], committed_reads=[], aborted_reads=[], invalid_aborted=0}, ClientDict), 
                     lager:warning("Returning specula_commit for ~w", [TxId]),
                    {reply, {ok, {specula_commit, LastCommitTs, {rev([TxId|AbortedReads]), rev(CommittedUpdates), rev(CommittedReads)}}}, SD0#state{dep_dict=dict:erase(TxId, DepDict), client_dict=ClientDict1}};
                {0, {ok, {_, B, _}}} ->
                    case ReadDeps of
                        B ->
                             lager:warning("Returning specula_commit for ~w", [TxId]),
                            gen_server:reply(Sender, {ok, {specula_commit, LastCommitTs, {rev(AbortedReads), rev(CommittedUpdates), rev([TxId|CommittedReads])}}}),
                            DepDict1 = dict:erase(TxId, DepDict),
                            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, aborted_reads=
                                [], committed_updates=[], committed_reads=[]}, ClientDict),
                            {noreply, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}};
                        _ ->
                             lager:warning("Returning specula_commit for ~w", [TxId]),
                            gen_server:reply(Sender, {ok, {specula_commit, LastCommitTs, {rev(AbortedReads), rev(CommittedUpdates), rev(CommittedReads)}}}),
                            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, aborted_reads=
                                [], committed_updates=[], committed_reads=[]}, ClientDict),
                            {noreply, SD0#state{client_dict=ClientDict1}}
                    end
            end;
        false ->
            case ClientState#client_state.aborted_update of
                ?NO_TXN -> 
                    ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, aborted_update=?NO_TXN, aborted_reads=[],
                            committed_updates=[], committed_reads=[]}, ClientDict),
                    DepDict1 = dict:erase(TxId, DepDict),
                    {reply, {aborted, {rev(AbortedReads), rev(CommittedUpdates), rev(CommittedReads)}}, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}};
                AbortedTxId ->
                    ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, aborted_update=?NO_TXN, aborted_reads=[],
                                    committed_updates=[], committed_reads=[]}, ClientDict),
                    DepDict1 = dict:erase(TxId, DepDict),
                    {reply, {cascade_abort, {AbortedTxId, rev(AbortedReads), rev(CommittedUpdates), rev(CommittedReads)}}, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}}
            end
    end;

handle_call({certify_update, TxId, LocalUpdates, RemoteUpdates, ReadDeps, ClientMsgId}, Sender, SD0) ->
    {Client, _} = Sender,
    handle_call({certify_update, TxId, LocalUpdates, RemoteUpdates, ReadDeps, ClientMsgId, Client}, Sender, SD0);
handle_call({certify_update, TxId, LocalUpdates, RemoteUpdates, ReadDeps, ClientMsgId, Client}, Sender, SD0=#state{rep_dict=RepDict, pending_txs=PendingTxs, total_repl_factor=ReplFactor, min_commit_ts=LastCommitTs, specula_length=SpeculaLength, dep_dict=DepDict, client_dict=ClientDict}) ->
    %% If there was a legacy ongoing transaction.
     lager:warning("Start certifying ~w, readDepTxs is ~w, Sender is ~w, local parts are ~w, remote parts are ~w", [TxId, ReadDeps, Sender, LocalUpdates, RemoteUpdates]),
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
                    lager:warning("Abort due to invalid abort, aborted update is ~w", [ClientState#client_state.aborted_update]),
                    case ClientState#client_state.aborted_update of
                        ?NO_TXN -> 
                            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, aborted_update=?NO_TXN, aborted_reads=[],
                                    committed_updates=[], committed_reads=[], invalid_aborted=0}, ClientDict),
                            DepDict1 = dict:erase(TxId, DepDict),
                            {reply, {aborted, {rev(AbortedReads), rev(CommittedUpdates), rev(CommittedReads)}}, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}};
                        AbortedTxId ->
                            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, aborted_update=?NO_TXN, aborted_reads=[],
                                            committed_updates=[], committed_reads=[], invalid_aborted=0}, ClientDict),
                            DepDict1 = dict:erase(TxId, DepDict),
                            {reply, {cascade_abort, {AbortedTxId, rev(AbortedReads), rev(CommittedUpdates), rev(CommittedReads)}}, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}}
                    end;
                {0, {ok, {_, B, _}}} ->
                    case (LocalUpdates == []) and (RemoteUpdates == []) of 
                        true ->
                            case (ReadDeps == 0) and (PendingList == []) of
                                true ->
                                    B = 0,
                                     lager:warning("Replying committed for ~w", [TxId]),
                                    gen_server:reply(Sender, {ok, {committed, LastCommitTs, {rev(AbortedReads), rev(CommittedUpdates), rev(CommittedReads)}}}),
                                    DepDict1 = dict:erase(TxId, DepDict),
                                    ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, aborted_reads=
                                          [], committed_updates=[], committed_reads=[]}, ClientDict),
                                    {noreply, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}}; 
                                false ->
                                    case length(PendingList) >= SpeculaLength of
                                        true -> %% Wait instead of speculate.
                                             lager:warning("remote prepr for ~w", [TxId]),
                                            DepDict1 = dict:store(TxId, 
                                                    {0, ReadDeps-B, LastCommitTs+1}, DepDict),
                                            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=TxId, local_updates=[], 
                                                remote_updates=[], stage=remote_cert, sender=Sender}, ClientDict),
                                            {noreply, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}};
                                        false -> %% Can speculate. After replying, removing TxId
                                            %% Update specula data structure, and clean the txid so we know current txn is already replied
                                             lager:warning("Returning specula_commit for ~w", [TxId]),
                                            gen_server:reply(Sender, {ok, {specula_commit, LastCommitTs+1, {rev(AbortedReads),
                                                      rev(CommittedUpdates), rev(CommittedReads)}}}),
                                            DepDict1 = dict:store(TxId, 
                                                    {0, ReadDeps-B, LastCommitTs+1}, DepDict),
                                            PendingTxs1 = dict:store(TxId, {[], [], no_wait, 0, 0}, PendingTxs),
                                            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN,
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
                            PendingPrepares = case NumCacheParts of 
                                                0 -> (NumLocalParts+NumRemoteParts)*(ReplFactor-1);
                                                _ -> (NumLocalParts+NumRemoteParts)*(ReplFactor-1) + ReplFactor*NumCacheParts
                                              end,
                             lager:warning("NumToAck is ~w, Pending prepares are ~w", [NumToAck, PendingPrepares]),

                            DepDict1 = dict:store(TxId, {NumToAck, ReadDeps-B, LastCommitTs+1, NumToAck, to_binary(NumToAck)}, DepDict),
                            ClientDict1 = dict:store(Client, ClientState#client_state{pending_prepares=PendingPrepares,
                                local_updates=LocalParts, remote_updates=RemoteUpdates, 
                                stage=local_cert, sender=Sender}, ClientDict),
                            {noreply, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}}
                    end
            end;
        false ->
             lager:warning("~w: invalid message id!! My is ~w, from client is ~w", [TxId, SentMsgId, ClientMsgId]),
            case ClientState#client_state.aborted_update of
                ?NO_TXN -> 
                    ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, aborted_update=?NO_TXN, aborted_reads=[],
                            committed_updates=[], committed_reads=[], invalid_aborted=0}, ClientDict),
                    DepDict1 = dict:erase(TxId, DepDict),
                    {reply, {aborted, {rev(AbortedReads), rev(CommittedUpdates), rev(CommittedReads)}}, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}};
                AbortedTxId ->
                    ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, aborted_update=?NO_TXN, aborted_reads=[],
                                    committed_updates=[], committed_reads=[], invalid_aborted=0}, ClientDict),
                    DepDict1 = dict:erase(TxId, DepDict),
                    {reply, {cascade_abort, {AbortedTxId, rev(AbortedReads), rev(CommittedUpdates), rev(CommittedReads)}}, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}}
            end
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
    [{dc_id, DcId}] = ets:lookup(meta_info, dc_id),
    [{_, Replicas}] = ets:lookup(meta_info, node()),
    TotalReplFactor = length(Replicas)+1,
    %SpeculaData = tx_utilities:open_private_table(specula_data),
    Sender ! cleaned,
    {noreply, #state{pending_txs=PendingTxs, dep_dict=dict:new(), name=Name, client_dict=ClientDict, dc_id=DcId, 
            specula_length=SpeculaLength, specula_read=SpeculaRead, rep_dict=RepDict, total_repl_factor=TotalReplFactor}};

handle_cast({can_remove, TxId, PartitionId, PartitionSum}, State=#state{dep_dict=DepDict}) ->
    {Partitions, Type, PartSum, RemainReply} = dict:fetch(TxId, DepDict),
    case PartSum band PartitionId of
        0 -> 
            case RemainReply of
                1 -> 
                    ?REPL_FSM:send_to_remove(Type, Partitions, TxId),
                    {noreply, State#state{dep_dict=dict:erase(TxId, DepDict)}};
                _ -> 
                    DepDict1 = dict:store(TxId, {Partitions, Type, PartSum, RemainReply-1}, DepDict),
                    {noreply, State#state{dep_dict=DepDict1}}
            end; 
        _ ->
            DepDict1 = dict:store(TxId, {Partitions, Type, PartSum-PartitionId, RemainReply+PartitionSum-1}, DepDict),
            {noreply, State#state{dep_dict=DepDict1}}
    end;

%% Receiving local prepare. Can only receive local prepare for two kinds of transaction
%%  Current transaction that has not finished local cert phase
%%  Transaction that has already cert_aborted.
%% Has to add local_cert here, because may receive pending_prepared, prepared from the same guy.
handle_cast({pending_prepared, TxId, PrepareTime, From}, 
	    SD0=#state{dep_dict=DepDict, specula_length=SpeculaLength, client_dict=ClientDict, pending_txs=PendingTxs,rep_dict=RepDict}) ->  
    Client = TxId#tx_id.client_pid,
     lager:warning("Got pending prepare for ~w, from ~w", [TxId, From]),
    ClientState = dict:fetch(Client, ClientDict),
    Stage = ClientState#client_state.stage,
    MyTxId = ClientState#client_state.tx_id, 
    case (Stage == local_cert) and (MyTxId == TxId) of
        true -> 
            Sender = ClientState#client_state.sender,
            PendingList = ClientState#client_state.pending_list,
            LocalParts = ClientState#client_state.local_updates, 
            RemoteUpdates = ClientState#client_state.remote_updates, 
            PendingPrepares = ClientState#client_state.pending_prepares, 
              lager:warning("Speculative receive pending_prepared for ~w, current pp is ~w", [TxId, PendingPrepares+1]),
            case dict:find(TxId, DepDict) of
                %% Maybe can commit already.
                {ok, {1, ReadDepTxs, OldPrepTime, LCNum, LCBinary}} ->
                    NewMaxPrep = max(PrepareTime, OldPrepTime),
                    RemoteParts = [P || {P, _} <- RemoteUpdates],
                    case length(PendingList) >= SpeculaLength of
                        true ->
                             lager:warning("Pending prep: decided to wait and prepare ~w, pending list is ~w!!", [TxId, PendingList]),
                            ?CLOCKSI_VNODE:prepare(RemoteUpdates, TxId, {remote, node()}),
                            DepDict1 = dict:store(TxId, {PendingPrepares+1, ReadDepTxs, NewMaxPrep, LCNum, LCBinary}, DepDict),
                            ClientDict1 = dict:store(Client, ClientState#client_state{stage=remote_cert, remote_updates=RemoteParts}, ClientDict),
                            {noreply, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}};
                        false ->
                             lager:warning("Pending prep: decided to speculate ~w and prepare to ~w pending list is ~w!!", [TxId, RemoteUpdates, PendingList]),
                            AbortedReads = ClientState#client_state.aborted_reads,
                            CommittedReads = ClientState#client_state.committed_reads,
                            CommittedUpdates = ClientState#client_state.committed_updates,
                            PendingTxs1 = dict:store(TxId, {LocalParts, RemoteParts, no_wait, LCNum, LCBinary}, PendingTxs),
                            specula_commit(LocalParts, RemoteParts, TxId, NewMaxPrep, RepDict),
                            ?CLOCKSI_VNODE:prepare(RemoteUpdates, NewMaxPrep, TxId, {remote, node()}),
                             lager:warning("Returning specula_commit for ~w", [TxId]),
                            gen_server:reply(Sender, {ok, {specula_commit, NewMaxPrep, {rev(AbortedReads),
                                                      rev(CommittedUpdates), rev(CommittedReads)}}}),
                            DepDict1 = dict:store(TxId, {PendingPrepares+1, ReadDepTxs, NewMaxPrep}, DepDict),
                            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, pending_list=PendingList++[TxId],committed_updates=[], committed_reads=[], aborted_reads=[]}, ClientDict),
                            {noreply, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1, pending_txs=PendingTxs1}}
                    end;
                {ok, {N, ReadDeps, OldPrepTime, LCNum, LCBinary}} ->
                      lager:warning("~w needs ~w local prep replies", [TxId, N-1]),
                    DepDict1 = dict:store(TxId, {N-1, ReadDeps, max(PrepareTime, OldPrepTime), LCNum, LCBinary}, DepDict),
                    ClientDict1 = dict:store(Client, ClientState#client_state{pending_prepares=PendingPrepares+1}, ClientDict),
                    {noreply, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}};
                error ->
                    {noreply, SD0}
            end;
        _ ->
            {noreply, SD0}
    end;

handle_cast({remove_pend_prepare, TxId, PrepareTime, From},
        SD0=#state{dep_dict=DepDict, client_dict=ClientDict}) ->
    lager:warning("Got solve_pending_prepare for ~w from ~w", [TxId, From]),
    Client = TxId#tx_id.client_pid,
    ClientState = dict:fetch(Client, ClientDict),
    Stage = ClientState#client_state.stage,
    MyTxId = ClientState#client_state.tx_id,

    case (Stage == local_cert) and (MyTxId == TxId) of
        true ->
            PendingPrepares = ClientState#client_state.pending_prepares,
            ClientDict1 = dict:store(Client, ClientState#client_state{pending_prepares=PendingPrepares-1}, ClientDict),
            {noreply, SD0#state{client_dict=ClientDict1}};
        false ->
            case dict:find(TxId, DepDict) of
                {ok, {1, 0, OldPrepTime}} -> %% Maybe the transaction can commit 
                    NowPrepTime =  max(OldPrepTime, PrepareTime),
                    DepDict1 = dict:store(TxId, {0, 0, NowPrepTime}, DepDict),
                    {noreply, try_solve_pending({NowPrepTime, TxId}, [], SD0#state{dep_dict=DepDict1}, [])};
                {ok, {PrepDeps, ReadDeps, OldPrepTime}} -> %% Maybe the transaction can commit 
                    lager:warning("~w not enough.. Prep ~w, Read ~w", [TxId, PrepDeps, ReadDeps]),
                    DepDict1=dict:store(TxId, {PrepDeps-1, ReadDeps, max(PrepareTime, OldPrepTime)}, DepDict),
                    {noreply, SD0#state{dep_dict=DepDict1}};
                error ->
                    {noreply, SD0}
            end
    end;

handle_cast({prepared, TxId, PrepareTime, From}, 
	    SD0=#state{dep_dict=DepDict, specula_length=SpeculaLength, 
            client_dict=ClientDict, rep_dict=RepDict, pending_txs=PendingTxs}) ->
     lager:warning("Got prepare for ~w, prepare time is ~w, from ~w", [TxId, PrepareTime, From]),
    Client = TxId#tx_id.client_pid,
    ClientState = dict:fetch(Client, ClientDict),
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
                {ok, {1, ReadDepTxs, OldPrepTime, LCNum, LCBinary}} ->
                    NewMaxPrep = max(PrepareTime, OldPrepTime),
                    RemoteParts = [P || {P, _} <- RemoteUpdates],
                    RemoteToAck = length(RemoteParts),
                    case (RemoteToAck == 0) and (ReadDepTxs == 0)
                          and (PendingList == []) and (PendingPrepares == 0) of
                        true ->
                            SD1 = try_solve_pending({NewMaxPrep, TxId}, [], SD0, []), 
                            {noreply, SD1}; 
                        false ->
                              lager:warning("Pending list is ~w, pending prepares is ~w, ReadDepTxs is ~w", [PendingList, PendingPrepares, ReadDepTxs]),
                            case length(PendingList) >= SpeculaLength of
                                true -> 
                                    %%In wait stage, only prepare and doesn't add data to table
                                     lager:warning("Decided to wait and prepare ~w, pending list is ~w, sending to ~w!!", [TxId, PendingList, RemoteParts]),
                                    ?CLOCKSI_VNODE:prepare(RemoteUpdates, TxId, {remote, node()}),
                                    DepDict1 = dict:store(TxId, {PendingPrepares, ReadDepTxs, NewMaxPrep, LCNum, LCBinary}, DepDict),
                                    ClientDict1 = dict:store(Client, ClientState#client_state{stage=remote_cert, 
                                        remote_updates=RemoteParts}, ClientDict),
                                    {noreply, SD0#state{dep_dict=DepDict1, client_dict=ClientDict1}};
                                false ->
                                    %% Add dependent data into the table
                                    PendingTxs1 = dict:store(TxId, {LocalParts, RemoteParts, no_wait, LCNum, LCBinary}, PendingTxs),
                                    specula_commit(LocalParts, RemoteParts, TxId, NewMaxPrep, RepDict),
                                    ?CLOCKSI_VNODE:prepare(RemoteUpdates, NewMaxPrep, TxId, {remote, node()}),
                                     lager:warning("Returning specula_commit for ~w", [TxId]),
                                    gen_server:reply(Sender, {ok, {specula_commit, NewMaxPrep, {rev(AbortedRead), rev(CommittedUpdated), rev(CommittedReads)}}}),
                                    DepDict1 = dict:store(TxId, {PendingPrepares, ReadDepTxs, NewMaxPrep}, DepDict),
                                    ClientDict1 = dict:store(Client, ClientState#client_state{pending_list=PendingList++[TxId], tx_id=?NO_TXN, 
                                        aborted_update=?NO_TXN, aborted_reads=[], committed_reads=[], committed_updates=[]}, ClientDict),
                                    {noreply, SD0#state{dep_dict=DepDict1, min_snapshot_ts=NewMaxPrep, client_dict=ClientDict1, pending_txs=PendingTxs1}}
                            end
                        end;
                {ok, {N, ReadDeps, OldPrepTime, LCNum, LCBinary}} ->
                     lager:warning("~w needs ~w local prep replies", [TxId, N-1]),
                    DepDict1 = dict:store(TxId, {N-1, ReadDeps, max(PrepareTime, OldPrepTime), LCNum, LCBinary}, DepDict),
                    {noreply, SD0#state{dep_dict=DepDict1}};
                error ->
                    {noreply, SD0}
            end;
        false ->
            case dict:find(TxId, DepDict) of
                {ok, {1, 0, OldPrepTime}} -> %% Maybe the transaction can commit 
                    NowPrepTime =  max(OldPrepTime, PrepareTime),
                    DepDict1 = dict:store(TxId, {0, 0, NowPrepTime}, DepDict),
                    {noreply, try_solve_pending({NowPrepTime, TxId}, [], SD0#state{dep_dict=DepDict1}, [])};
                {ok, {PrepDeps, ReadDeps, OldPrepTime}} -> %% Maybe the transaction can commit 
                       lager:warning("~w not enough.. Prep ~w, Read ~w", [TxId, PrepDeps, ReadDeps]),
                    DepDict1=dict:store(TxId, {PrepDeps-1, ReadDeps, max(PrepareTime, OldPrepTime)}, DepDict),
                    {noreply, SD0#state{dep_dict=DepDict1}};
                error ->
                    {noreply, SD0}
            end
    end;

%handle_cast({prepared, _OtherTxId, _}, SD0=#state{stage=local_cert}) ->
%    {noreply, SD0}; 

%% TODO: if we don't direclty speculate after getting all local prepared, maybe we can wait a littler more
%%       and here we should check if the transaction can be directly committed or not. 
handle_cast({read_valid, PendingTxId, SolveNum}, SD0=#state{dep_dict=DepDict, client_dict=ClientDict}) ->
      lager:warning("Got read valid for ~w, solved ~w", [PendingTxId, SolveNum]),
    case dict:find(PendingTxId, DepDict) of
        {ok, {read_only, SolveNum, _ReadOnlyTs}} ->
            %gen_server:reply(Sender, {ok, {committed, ReadOnlyTs}}),
            ClientState = dict:fetch(PendingTxId#tx_id.client_pid, ClientDict),
            ClientState1 = ClientState#client_state{committed_reads=[PendingTxId|ClientState#client_state.committed_reads]},
            {noreply, SD0#state{dep_dict=dict:erase(PendingTxId, DepDict), client_dict=dict:store(PendingTxId#tx_id.client_pid,
                    ClientState1, ClientDict)}};
        {ok, {read_only, N, ReadOnlyTs}} ->
            {noreply, SD0#state{dep_dict=dict:store(PendingTxId, {read_only, N-SolveNum, ReadOnlyTs}, DepDict)}};
        {ok, {0, N, 0}} -> 
            %% Txn is still reading!!
                lager:warning("Inserting read valid for ~w, old deps are ~w",[PendingTxId, N]),
            {noreply, SD0#state{dep_dict=dict:store(PendingTxId, {0, N+SolveNum, 0}, DepDict)}};
        {ok, {0, SolveNum, OldPrepTime}} -> %% Maybe the transaction can commit 
            SD1 = SD0#state{dep_dict=dict:store(PendingTxId, {0, 0, OldPrepTime}, DepDict)},
            {noreply, try_solve_pending({OldPrepTime, PendingTxId}, [], SD1, [])};
        {ok, {PrepDeps, N, OldPrepTime}} -> 
             lager:warning("Can not commit... Remaining prepdep is ~w, read dep is ~w", [PrepDeps, N-SolveNum]),
            {noreply, SD0#state{dep_dict=dict:store(PendingTxId, {PrepDeps, N-SolveNum, 
                OldPrepTime}, DepDict)}};
        error ->
             lager:warning("WTF, no info?"),
            {noreply, SD0}
    end;

handle_cast({read_invalid, _MyCommitTime, TxId}, SD0) ->
    {noreply, try_solve_pending([], [{ignore, TxId}], SD0, [])};
    %read_abort(read_invalid, MyCommitTime, TxId, SD0);
handle_cast({read_aborted, _MyCommitTime, TxId}, SD0) ->
    {noreply, try_solve_pending([], [{ignore, TxId}], SD0, [])};
    %read_abort(read_aborted, MyCommitTime, TxId, SD0);
            
handle_cast({aborted, TxId}, SD0) ->
    lager:warning("Received aborted for ~w", [TxId]),
    SD1 = try_solve_pending([], [{ignore, TxId}], SD0, []),
    {noreply, SD1};
handle_cast({aborted, TxId, FromNode}, SD0) ->
    lager:warning("Received aborted for ~w from ~w", [TxId, FromNode]),
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
     lager:warning("Try to check current txn, ClientsOfCommTxns are ~w", [ClientsOfCommTxns]),
    {ClientDict1, DepDict1, PendingTxs1, NewCommitTime} = 
            lists:foldl(fun(Client, {CD, DD, PD, PCommitTime}) ->
                    CState = dict:fetch(Client, CD),
                    TxId = CState#client_state.tx_id,
                    case TxId of
                        ?NO_TXN -> {CD, DD, PD, PCommitTime};
                        _ ->
                             lager:warning("Curent txn id is ~w", [TxId]),
                            case CState#client_state.committed_updates of [] -> {CD, DD, PD, PCommitTime};
                                _ ->
                                    AbortedReads = CState#client_state.aborted_reads,
                                    CommittedUpdates = CState#client_state.committed_updates,
                                    CommittedReads = CState#client_state.committed_reads,
                                    PendingList = CState#client_state.pending_list,
                                    Sender = CState#client_state.sender,
                                    LocalParts = CState#client_state.local_updates, 
                                    TxState = dict:fetch(TxId, DepDict),
                                    case TxState of 
                                        {0, 0, 0} -> {CD, DD, PD, PCommitTime}; %%Still reading 
                                        {Prep, Read, OldCurPrepTime, LCNum, LCBinary} -> %% Can not commit, but can specualte (due to the number of specula txns decreased)
                                            case CState#client_state.stage of
                                                remote_cert ->
                                                    SpeculaPrepTime = max(PCommitTime+1, OldCurPrepTime),
                                                    RemoteParts = CState#client_state.remote_updates,
                                                     lager:warning("Spec comm curr txn: ~p, remote updates are ~p", [TxId, RemoteParts]),
                                                    specula_commit(LocalParts, RemoteParts, TxId, SpeculaPrepTime, RepDict),
                                                    case (Prep == 0) and (Read == 0) and (PendingList == []) of
                                                        true ->   
                                                             lager:warning("Can already commit ~w!!", [TxId]),
                                                            DD1 = dict:erase(TxId, DD),
                                                            commit_tx(TxId, SpeculaPrepTime, 
                                                                LocalParts, RemoteParts, RepDict, waited),
                                                            gen_server:reply(Sender, {ok, {committed, SpeculaPrepTime, {rev(AbortedReads), rev(CommittedUpdates), rev(CommittedReads)}}}),
                                                            CD1 = dict:store(Client, CState#client_state{committed_updates=[], committed_reads=[], aborted_reads=[],
                                                                pending_list=[], tx_id=?NO_TXN}, ClientDict),
                                                            {CD1, DD1, PD, SpeculaPrepTime};
                                                        false ->
                                                             lager:warning("Returning specula_commit for ~w", [TxId]),
                                                            gen_server:reply(Sender, {ok, {specula_commit, SpeculaPrepTime, {rev(AbortedReads), rev(CommittedUpdates), rev(CommittedReads)}}}),
                                                            PD1 = dict:store(TxId, {LocalParts, RemoteParts, waited, LCNum, LCBinary}, PD),
                                                            CD1 = dict:store(Client, CState#client_state{committed_updates=[], committed_reads=[], aborted_reads=[], pending_list=PendingList ++ [TxId], tx_id=?NO_TXN}, CD),
                                                            {CD1, DD, PD1, PCommitTime}
                                                    end;
                                                _ -> {CD, DD, PD, PCommitTime}
                                            end
                                    end
                            end
                    end end, {ClientDict, DepDict, PendingTxs, MinCommitTs}, ClientsOfCommTxns),
    SD0#state{client_dict=ClientDict1, dep_dict=DepDict1, pending_txs=PendingTxs1, min_commit_ts=NewCommitTime};
try_solve_pending(ToCommitTx, [{FromNode, TxId}|Rest], SD0=#state{client_dict=ClientDict, rep_dict=RepDict, pending_txs=PendingTxs, dep_dict=DepDict}, ClientsOfCommTxns) ->
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
            try_solve_pending(ToCommitTx, Rest, SD0#state{dep_dict=dict:erase(TxId, DepDict), 
                    client_dict=dict:store(Client, ClientState1, ClientDict)}, ClientsOfCommTxns);
        _ ->
        case start_from_list(TxId, PendingList) of
        [] ->
            case TxId of
                CurrentTxId ->
                    case Stage of
                        local_cert ->
                            RemoteParts = [P|| {P,_}<-RemoteUpdates],
                              lager:warning("~w abort local!", [TxId]),
                            case FromNode of 
                                ignore -> abort_tx(TxId, LocalParts, RemoteParts, RepDict, local_cert);
                                _ ->  abort_tx(TxId, lists:delete(FromNode, LocalParts), RemoteParts, RepDict, local_cert)
                            end,
                            RD1 = dict:erase(TxId, DepDict),
                            gen_server:reply(Sender, {aborted, {rev(AbortedReads), rev(CommittedUpdates), rev(CommittedReads)}}),
                             lager:warning("Replyed aborted"),
                            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, committed_updates=[], committed_reads=[], aborted_reads=[]}, ClientDict), 
                            try_solve_pending(ToCommitTx, Rest, 
                                SD0#state{dep_dict=RD1, client_dict=ClientDict1}, ClientsOfCommTxns);
                            %{noreply, SD0#state{dep_dict=RD1, read_aborted=RAD1, read_invalid=RID1, client_dict=ClientDict1}};
                        remote_cert ->
                             lager:warning("~w abort remote!", [TxId]),
                            RemoteParts = RemoteUpdates,
                            case FromNode of 
                                [] -> abort_tx(TxId, LocalParts, RemoteParts, RepDict, remote_cert);
                                _ -> abort_tx(TxId, LocalParts, lists:delete(FromNode, RemoteParts), RepDict, remote_cert, FromNode)
                            end,
                            RD1 = dict:erase(TxId, DepDict),
                            gen_server:reply(Sender, {aborted, {rev(AbortedReads), rev(CommittedUpdates), rev(CommittedReads)}}),
                            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, committed_updates=[], committed_reads=[], aborted_reads=[]}, ClientDict), 
                            try_solve_pending(ToCommitTx, Rest, 
                                SD0#state{dep_dict=RD1, client_dict=ClientDict1}, ClientsOfCommTxns);
                            %{noreply, SD0#state{dep_dict=RD1, client_dict=ClientDict1, read_aborted=RAD1, read_invalid=RID1}};
                        read ->
                            ClientDict1 = dict:store(Client, ClientState#client_state{invalid_aborted=1}, ClientDict), 
                            try_solve_pending(ToCommitTx, Rest, SD0#state{client_dict=ClientDict1}, ClientsOfCommTxns)
                            %{noreply, SD0#state{client_dict=ClientDict1}}
                    end;
                _ -> %% The transaction has already been aborted or whatever
                    try_solve_pending(ToCommitTx, Rest, SD0, ClientsOfCommTxns)
            end;
        {Prev, L} ->
            {PendingTxs1, RD} = abort_specula_list(L, RepDict, DepDict, PendingTxs, FromNode),
             lager:warning("Abort due to read invalid, Pending list is ~w, PendingTxId is ~w, List is ~w", [PendingList, TxId, L]),
            case CurrentTxId of
                ?NO_TXN ->
                     lager:warning("No current txn, Pendinglist is ~w, Prev is ~w", [PendingList, Prev]),
                    %% The clien is sleeping now! Need to reply msg.
                    MsgId = ClientState#client_state.msg_id,
                    gen_fsm:send_event(Client, {final_abort, MsgId+1, TxId, rev(AbortedReads), rev(CommittedUpdates), rev(CommittedReads)}),
                    ClientDict1 = dict:store(Client, ClientState#client_state{pending_list=Prev, msg_id=MsgId+1,
                            aborted_update=?NO_TXN, aborted_reads=[], committed_updates=[], committed_reads=[]}, ClientDict), 
                    try_solve_pending(ToCommitTx, Rest, SD0#state{dep_dict=RD, client_dict=ClientDict1, pending_txs=PendingTxs1}, ClientsOfCommTxns);
                _ ->
                     lager:warning("Current tx is ~w, Stage is ~w", [CurrentTxId, Stage]),
                    case Stage of
                        local_cert -> 
                            lager:warning("Local abort: current tx is ~w, local parts are ~w, repdict is ~p", [CurrentTxId, LocalParts, dict:to_list(RepDict)]),
                            RemoteParts=  [P||{P,_}<-RemoteUpdates],
                            abort_tx(CurrentTxId, LocalParts, RemoteParts, RepDict, local_cert),
                            gen_server:reply(Sender, {cascade_abort, {TxId, rev(AbortedReads), rev(CommittedUpdates), rev(CommittedReads)}}),
                            RD1 = dict:erase(CurrentTxId, RD),
                            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, pending_list=Prev,
                                    aborted_update=?NO_TXN, aborted_reads=[], committed_updates=[], committed_reads=[]}, ClientDict), 
                            try_solve_pending(ToCommitTx, Rest, SD0#state{dep_dict=RD1, client_dict=ClientDict1, pending_txs=PendingTxs1}, ClientsOfCommTxns);
                            %{noreply, SD0#state{dep_dict=RD1, client_dict=ClientDict2,
                            %    read_aborted=RAD1, read_invalid=RID1, cascade_aborted=CascadAborted+Length}};
                        remote_cert -> 
                                 lager:warning("Remote abort: TxId is ~w, Pendinglist is ~w", [CurrentTxId, PendingList]),
                            RemoteParts = RemoteUpdates,
                            abort_tx(CurrentTxId, LocalParts, RemoteParts, RepDict, remote_cert),
                            gen_server:reply(Sender, {cascade_abort, {TxId, rev(AbortedReads), rev(CommittedUpdates), rev(CommittedReads)}}),
                            RD1 = dict:erase(CurrentTxId, RD),
                            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, pending_list=Prev,
                                aborted_update=?NO_TXN, aborted_reads=[], committed_updates=[], committed_reads=[]}, ClientDict), 
                            try_solve_pending(ToCommitTx, Rest, SD0#state{dep_dict=RD1, 
                                  client_dict=ClientDict1, pending_txs=PendingTxs1}, ClientsOfCommTxns);
                            %{noreply, SD0#state{dep_dict=RD1, client_dict=ClientDict2,
                            %    read_aborted=RAD1, read_invalid=RID1, cascade_aborted=CascadAborted+Length}};
                        read ->
                                 lager:warning("Has current txn read, Pendinglist is ~w, Rest is ~w, aborted_update ~w", [PendingList, Rest,  get_older(AbortedUpdate, TxId)]),
                            ClientDict1 = dict:store(Client, ClientState#client_state{pending_list=Prev, invalid_aborted=1,
                                aborted_update=get_older(AbortedUpdate, TxId)}, ClientDict), 
                            try_solve_pending(ToCommitTx, Rest, SD0#state{dep_dict=RD, 
                                  pending_txs=PendingTxs1, client_dict=ClientDict1}, ClientsOfCommTxns)
                            %{noreply, SD0#state{dep_dict=RD, client_dict=ClientDict2,
                            %    read_aborted=RAD1, read_invalid=RID1, cascade_aborted=CascadAborted+Length-1}}
                    end
            end
        end
    end;
try_solve_pending({NowPrepTime, PendingTxId}, [], SD0=#state{client_dict=ClientDict, rep_dict=RepDict, dep_dict=DepDict, 
            pending_txs=PendingTxs, min_commit_ts=MinCommitTS}, ClientsOfCommTxns) ->
    Client = PendingTxId#tx_id.client_pid,
    ClientState = dict:fetch(Client, ClientDict),
    PendingList = ClientState#client_state.pending_list, 
     lager:warning("Pending list is ~w, tx is ~w, pending tx is ~p", [PendingList, PendingTxId, PendingTxId]),
    Sender = ClientState#client_state.sender, 

    PendingList = ClientState#client_state.pending_list,
    LocalParts = ClientState#client_state.local_updates, 
    RemoteUpdates = ClientState#client_state.remote_updates, 
    CommittedUpdates = ClientState#client_state.committed_updates,
    CommittedReads = ClientState#client_state.committed_reads,
    AbortedReads = ClientState#client_state.aborted_reads,
    Stage = ClientState#client_state.stage,
    case PendingList of
        [] -> %% This is the just report_committed txn.. But new txn has not come yet.
             lager:warning("Pending list no me ~p, current tx!",[PendingTxId]),
            RemoteParts = case Stage of remote_cert -> RemoteUpdates; local_cert -> [P||{P, _} <-RemoteUpdates] end,
            DepDict1 = dict:erase(PendingTxId, DepDict),
            CurCommitTime = max(NowPrepTime, MinCommitTS+1),
             lager:warning("Commit txn"),
            commit_tx(PendingTxId, CurCommitTime, LocalParts, RemoteParts, RepDict),
            gen_server:reply(Sender, {ok, {committed, CurCommitTime, {rev(AbortedReads), rev(CommittedUpdates), rev(CommittedReads)}}}),
            ClientDict1 = dict:store(Client, ClientState#client_state{tx_id=?NO_TXN, pending_list=[], aborted_reads=[],
                    committed_updates=[], committed_reads=[], aborted_update=?NO_TXN}, ClientDict),
            try_solve_pending([], [], SD0#state{min_commit_ts=CurCommitTime, dep_dict=DepDict1, 
                          client_dict=ClientDict1}, ClientsOfCommTxns);
        [PendingTxId|ListRest] ->
            lager:warning("Pending list contain me ~p!",[PendingTxId]),
            CommitTime = max(NowPrepTime, MinCommitTS+1),
            DepDict1 = dict:erase(PendingTxId, DepDict),
            PendingTxs1 = commit_specula_tx(PendingTxId, CommitTime, RepDict, PendingTxs),		 
            Now = os:timestamp(),

            {PendingTxs2, NewPendingList, NewMaxPT, DepDict2, ClientDict2, CommittedTxs}= 
                try_commit_follower(CommitTime, ListRest, RepDict, DepDict1, PendingTxs1, ClientDict, []),
             lager:warning("Committed Txns are ~w", [CommittedTxs++[{PendingTxId, Now}|ClientState#client_state.committed_updates]]),
            ClientDict3 = dict:store(Client, ClientState#client_state{pending_list=NewPendingList, 
                    committed_updates=CommittedTxs++[{PendingTxId, Now}|ClientState#client_state.committed_updates]}, ClientDict2),
            try_solve_pending([], [], SD0#state{min_commit_ts=NewMaxPT, dep_dict=DepDict2, 
                          pending_txs=PendingTxs2, client_dict=ClientDict3}, [Client|ClientsOfCommTxns]);
        _ ->
            %DepDict1 = dict:update(PendingTxId, fun({_, _, _}) ->
            %                            {0, [], NowPrepTime} end, DepDict),
              lager:warning("got all replies, but I am not the first! PendingList is ~w", [PendingList]),
            try_solve_pending([], [], SD0, ClientsOfCommTxns)
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
commit_tx(TxId, CommitTime, LocalParts, RemoteParts, RepDict) ->
    commit_tx(TxId, CommitTime, LocalParts, RemoteParts, RepDict, no_wait).

commit_tx(TxId, CommitTime, LocalParts, RemoteParts, RepDict, IfWaited) ->
    lager:warning("Commit ~w, local parts ~w, remote parts ~w, if waited is ~w", [TxId, LocalParts, RemoteParts, IfWaited]),
    ?CLOCKSI_VNODE:commit(LocalParts, TxId, CommitTime),
    ?CLOCKSI_VNODE:commit(RemoteParts, TxId, CommitTime),
    LocalNodeRepl = repl_fsm:build_node_parts(LocalParts),
    lists:foreach(fun({Node, Partitions}) ->
            Replicas = dict:fetch(Node, RepDict),
            lager:info("repl commit of ~w for node ~w to replicas ~w for partitions ~w", [TxId, Node, Replicas, Partitions]),
            lists:foreach(fun(R) -> gen_server:cast({global, R}, {repl_commit, TxId, CommitTime, Partitions, no_wait})
                end, Replicas) end,
        LocalNodeRepl),
    RemoteNodeRepl = repl_fsm:build_node_parts(RemoteParts),
    lists:foreach(fun({Node, Partitions}) ->
        Replicas = dict:fetch(Node, RepDict),
        lager:info("repl commit of ~w for node ~w to replicas ~w for partitions ~w", [TxId, Node, Replicas, Partitions]),
        lists:foreach(fun(R) ->
            case R of cache -> ?CACHE_SERV:commit(TxId, Partitions, CommitTime);
                {local_dc, S} -> gen_server:cast({global, S}, {repl_commit, TxId, CommitTime, Partitions, IfWaited})
            end end, Replicas)
        end, RemoteNodeRepl).

commit_specula_tx(TxId, CommitTime, RepDict, PendingTxs) ->
    {LocalParts, RemoteParts, IfWaited, LCNum, LCBinary} = dict:fetch(TxId, PendingTxs),
    %%%%%%%%% Time stat %%%%%%%%%%%
    lager:warning("~w specula commit: local parts are ~w, remote parts are ~w", [TxId, LocalParts, RemoteParts]),

    LocalNodeRepl = repl_fsm:build_node_parts(LocalParts),
    lists:foreach(fun({Node, Partitions}) ->
            Replicas = dict:fetch(Node, RepDict),
            lager:info("repl commit of ~w for node ~w to replicas ~w for partitions ~w", [TxId, Node, Replicas, Partitions]),
            lists:foreach(fun(R) -> gen_server:cast({global, R}, {repl_commit, TxId, CommitTime, Partitions, no_wait})
                end, Replicas) end,
        LocalNodeRepl),
    ?CLOCKSI_VNODE:commit(RemoteParts, TxId, CommitTime),

    TempBN = lists:foldl(fun(Node, BN) ->
          riak_core_vnode_master:command(Node,
                             {prev_remove, TxId, BN, CommitTime},
                             {server, undefined, self()},
                             ?CLOCKSI_MASTER),
          BN*2
      end, 1, LocalParts),

    RemoteNodeParts = build_node_parts(RemoteParts),
    {RemoteCumParts, LCBinary} = lists:foldl(fun({Node, Partitions}, {Acc, BN}) ->
        Replicas = dict:fetch(Node, RepDict),
        lists:foldl(fun(R, {Acc1, TBN}) ->
            case R of
                cache -> ?CACHE_SERV:prev_remove(prev_remove, TxId, TBN, Partitions, CommitTime),
                         {[{cache, Partitions}|Acc1], 2*TBN};
                {local_dc, S} ->
                    gen_server:cast({global, S}, {prev_remove, commit, TxId, TBN, CommitTime, Partitions, IfWaited}),
                    {[{slave, S, Partitions}|Acc1], 2*TBN}
            end end, {Acc, BN}, Replicas) end,
            {[], TempBN}, RemoteNodeParts),
    dict:store(TxId, {[{master, LocalParts}|RemoteCumParts], commit, LCNum, LCBinary}, PendingTxs).

abort_specula_tx(TxId, PendingTxs, RepDict, ExceptNode) ->
      lager:warning("Trying to abort specula ~w", [TxId]),
    {LocalParts, RemoteParts, IfWaited, LCNum, LCBinary} = dict:fetch(TxId, PendingTxs),
    %%%%%%%%% Time stat %%%%%%%%%%%
    ?CLOCKSI_VNODE:abort(LocalParts, TxId),
    ?REPL_FSM:repl_abort(LocalParts, TxId, false, RepDict),
    ?CLOCKSI_VNODE:abort(lists:delete(ExceptNode, RemoteParts), TxId),
    RemoteCumParts = ?REPL_FSM:repl_abort(RemoteParts, TxId, true, RepDict, IfWaited),
    dict:store(TxId, {[{master, LocalParts}|RemoteCumParts] , abort, LCNum, LCBinary}, PendingTxs).

abort_specula_tx(TxId, PendingTxs, RepDict) ->
    {LocalParts, RemoteParts, IfWaited, LCNum, LCBinary} = dict:fetch(TxId, PendingTxs),
    lager:warning("Specula abort ~w", [TxId]),
    ?CLOCKSI_VNODE:abort(LocalParts, TxId),
    ?REPL_FSM:repl_abort(LocalParts, TxId, false, RepDict),
    ?CLOCKSI_VNODE:abort(RemoteParts, TxId),
    RemoteCumParts = ?REPL_FSM:repl_abort(RemoteParts, TxId, true, RepDict, IfWaited),
    dict:store(TxId, {[{master, LocalParts}|RemoteCumParts] , abort, LCNum, LCBinary}, PendingTxs).
    %abort_to_table(RemoteParts, TxId, RepDict),

abort_tx(TxId, LocalParts, RemoteParts, RepDict, Stage) ->
    abort_tx(TxId, LocalParts, RemoteParts, RepDict, Stage, ignore).

abort_tx(TxId, LocalParts, RemoteParts, RepDict, Stage, LocalOnlyPart) ->
    lager:warning("Abort ~w: RemoteParts ~w, Stage is ~w", [TxId, RemoteParts, Stage]),
    ?CLOCKSI_VNODE:abort(LocalParts, TxId),
    ?REPL_FSM:repl_abort(LocalParts, TxId, false, RepDict),
    case Stage of 
        remote_cert ->
            ?CLOCKSI_VNODE:abort(RemoteParts, TxId),
            ?REPL_FSM:repl_abort(RemoteParts, TxId, false, RepDict),
            case LocalOnlyPart of 
                ignore -> ok;
                {LP, LN} ->
                    case dict:find({local_dc, LN}, RepDict) of
                        {ok, DataReplServ} ->
                            gen_server:cast({global, DataReplServ}, {repl_abort, TxId, [LP], no_wait});
                        _ ->
                            ?CACHE_SERV:abort(TxId, [LP])
                    end
            end;
        local_cert ->
            NodeParts = build_node_parts(RemoteParts),
            lists:foreach(fun({Node, Partitions}) ->
                case dict:find({local_dc, Node}, RepDict) of
                    {ok, DataReplServ} ->
                        gen_server:cast({global, DataReplServ}, {repl_abort, TxId, Partitions, no_wait});
                    _ ->
                        ?CACHE_SERV:abort(TxId, Partitions)
            end end, NodeParts)
    end.

local_certify(LocalUpdates, RemoteUpdates, TxId, RepDict) ->
    {LocalParts, NumLocalParts} = ?CLOCKSI_VNODE:prepare(LocalUpdates, TxId, local),
     lager:warning("Local Parts are ~w, Num local parts are ~w", [LocalParts, NumLocalParts]),
    {NumSlaveParts, NumCacheParts} = lists:foldl(fun({{Part, Node}, Updates}, {NumSParts, NumCParts}) ->
                    case dict:find({local_dc, Node}, RepDict) of
                        {ok, DataReplServ} ->
                             lager:warning("LocalCert to ~p, ~p", [DataReplServ, Part]),
                            ?DATA_REPL_SERV:local_certify(DataReplServ, TxId, Part, Updates),
                            {NumSParts+1, NumCParts}; 
                        _ ->
                             lager:warning("LocalCert to cache"),
                            ?CACHE_SERV:local_certify(TxId, Part, Updates),
                            {NumSParts, NumCParts+1}
                    end end, {0, 0}, RemoteUpdates),
     lager:warning("NumSlaveParts are ~w, NumCacheParts are ~w", [NumSlaveParts, NumCacheParts]),
    {LocalParts, NumLocalParts, NumSlaveParts, NumCacheParts}.

specula_commit(LocalPartitions, RemotePartitions, TxId, SpeculaCommitTs, RepDict) ->
    ?CLOCKSI_VNODE:specula_commit(LocalPartitions, TxId, SpeculaCommitTs),
    lists:foreach(fun({Part, Node}) ->
            case dict:find({local_dc, Node}, RepDict) of
                {ok, DataReplServ} ->
                    ?DATA_REPL_SERV:specula_commit(DataReplServ, TxId, Part, SpeculaCommitTs); 
                _ ->
                    ?CACHE_SERV:specula_commit(TxId, Part, SpeculaCommitTs)
    end end, RemotePartitions).

%% Deal dependencies and check if any following transactions can be committed.
try_commit_follower(LastCommitTime, [], _RepDict, DepDict, PendingTxs, ClientDict, CommittedTxs) ->
       lager:warning("Returning ~w ~w ~w ~p ~p", [PendingTxs, LastCommitTime, DepDict, ClientDict, CommittedTxs]),
    {PendingTxs, [], LastCommitTime, DepDict, ClientDict, CommittedTxs};
try_commit_follower(LastCommitTime, [H|Rest]=PendingList, RepDict, DepDict, PendingTxs, ClientDict, CommittedTxs) ->
    Result = case dict:find(H, DepDict) of
                {ok, {0, 0, PendingMaxPT}} ->
                     lager:warning("If can commit decision for ~w is true", [H]),
                    {true, max(PendingMaxPT, LastCommitTime+1)};
                R ->
                     lager:warning("If can commit decision for ~w is ~w", [H, R]),
                    false
            end,
    case Result of
        false ->
              lager:warning("Returning ~w ~w ~w ~w ~w ~w", [PendingTxs, PendingList, LastCommitTime, DepDict, ClientDict, CommittedTxs]),
            {PendingTxs, PendingList, LastCommitTime, DepDict, ClientDict, CommittedTxs};
        {true, CommitTime} ->
             lager:warning("Commit pending specula ~w", [H]),
            DepDict1 = dict:erase(H, DepDict),
            PendingTxs1 = commit_specula_tx(H, CommitTime, RepDict, PendingTxs),
            try_commit_follower(CommitTime, Rest, RepDict, DepDict1, PendingTxs1, ClientDict, 
                    [{H, os:timestamp()}|CommittedTxs])
    end.

abort_specula_list([H|T], RepDict, DepDict, PendingTxs, ExceptNode) ->
    lager:warning("Trying to abort ~w", [H]),
    PendingTxs1 = abort_specula_tx(H, PendingTxs, RepDict, ExceptNode),
    DepDict1 = dict:erase(H, DepDict),
    abort_specula_list(T, RepDict, DepDict1, PendingTxs1). 

abort_specula_list([], _RepDict, DepDict, PendingTxs) ->
    {PendingTxs, DepDict};
abort_specula_list([H|T], RepDict, DepDict, PendingTxs) ->
    lager:warning("Trying to abort ~w", [H]),
    PendingTxs1 = abort_specula_tx(H, PendingTxs, RepDict),
    DepDict1 = dict:erase(H, DepDict),
    abort_specula_list(T, RepDict, DepDict1, PendingTxs1). 

to_binary(Num) ->
    to_binary(Num, 1).

to_binary(0, _) ->
    0;
to_binary(N, M) ->
    M+to_binary(N-1, M*2). 

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
    %ets:new(dependency, [private, set, named_table]),
    mock_partition_fsm:start_link(),
    LocalParts = [lp1, lp2],
    TxId1 = tx_utilities:create_tx_id(0),
    abort_tx(TxId1, LocalParts, [], dict:new(), local_cert),
    ?assertEqual(true, mock_partition_fsm:if_applied({abort, TxId1, LocalParts}, nothing)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_abort, TxId1, LocalParts}, nothing)),
    %true = ets:delete(dependency),
    true = ets:delete(MyTable).

commit_tx_test() ->
    %ets:new(dependency, [private, set, named_table]),
    LocalParts = [lp1, lp2],
    TxId1 = tx_utilities:create_tx_id(0),
    CT = 10000,
    commit_tx(TxId1, CT, LocalParts, [], dict:new()),
    ?assertEqual(true, mock_partition_fsm:if_applied({commit, TxId1, LocalParts}, CT)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_commit, TxId1, LocalParts}, CT)).
    %ets:delete(dependency).

abort_specula_list_test() ->
    MyTable = dict:new(), 
    %ets:new(dependency, [public, bag, named_table]),
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
    {MyTable4, DepDict4} = abort_specula_list([T1, T2, T3], RepDict, DepDict3, MyTable3, []),
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

%solve_read_dependency_test() ->
%    T0 = tx_utilities:create_tx_id(0), 
%    T1 = tx_utilities:create_tx_id(0), 
%    T2 = T1#tx_id{server_pid=test_fsm}, 
%    CommitTime = tx_utilities:now_microsec(),
%    T3 = tx_utilities:create_tx_id(0),
%    T4 = T3#tx_id{server_pid=test_fsm}, 
%    %% T1, T2 should be cert_aborted, T3, T4 should get read_valid.
%    DepDict = dict:new(),
%    DepDict1 = dict:store(T3, {2, [T0, T2,T1], 4}, DepDict),
%    {RD2, _, T, _} = solve_read_dependency(CommitTime, DepDict1, [{T0, T1}, {T0, T2}, {T0, T3}, {T0, T4}], dict:new()),
%    ?assertEqual({[{T3,{2,[T2,T1],4}}], [{[], T1}]}, {dict:to_list(RD2), T}),
%    ?assertEqual(true, mock_partition_fsm:if_applied({read_valid, T4}, T0)),
%    ?assertEqual(true, mock_partition_fsm:if_applied({read_invalid, T2}, nothing)).

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
            try_commit_follower(MaxPT1, [T1, T2], RepDict, DepDict2, PendingTxs, ClientDict, []),
    {PendingTxs1, PD1, MPT1, RD1, ClientDict1, CommittedTxs}=Result,

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
    DepDict4 = dict:store(T1, {0, 0, 2}, DepDict3),
    %% T1 can commit because of read_dep ok. 
    {PendingTxs5, PD2, MPT2, RD2, ClientDict2, CommittedTxs1} = try_commit_follower(MaxPT1, [T1, T2], RepDict, DepDict4, PendingTxs4, ClientDict1,  []),
    ?assertMatch([{T1, _}], CommittedTxs1),
    %?assertEqual(true, mock_partition_fsm:if_applied({commit_specula, n3rep, T1, rp1}, MPT2)),
    io:format(user, "RD2 ~w, Dict4 ~w", [dict:to_list(RD2), dict:to_list(dict:erase(T1, DepDict4))]),
    ?assertEqual(RD2, dict:erase(T1, DepDict4)),
    ?assertEqual(PD2, [T2]),
    ?assertEqual(MPT2, MaxPT1+1),

    %% T2 can commit becuase of prepare OK
    DepDict5 = dict:store(T2, {0, 0, 2}, RD2),
    {PendingTxs6, _PD3, MPT3, RD3, ClientDict3, CommittedTxs2} = try_commit_follower(MPT2, [T2, T3], RepDict, DepDict5, 
                                    PendingTxs5, ClientDict2, []),
    ?assertEqual(error, dict:find(T2, PendingTxs6)),
    ?assertMatch([{T2, _}], CommittedTxs2),
    %?assertEqual(true, mock_partition_fsm:if_applied({repl_commit, TxId1, LocalParts}, CT)),
    ?assertEqual(true, mock_partition_fsm:if_applied({commit, T2, [{lp1, n1}]}, MPT3)),
    ?assertEqual(true, mock_partition_fsm:if_applied({repl_commit, T2, [{rp1, n3}]}, MPT3)),
    %?assertEqual(true, mock_partition_fsm:if_applied({commit_specula, n3rep, T2, rp1}, MPT3)),

    %% T3, T4 get committed and T5 gets cert_aborted.
    PendingTxs7 = dict:store(T3, {[{lp2, n2}], [{rp2, n4}], no_wait}, PendingTxs6), 
    PendingTxs8 = dict:store(T4, {[{lp2, n2}], [{rp2, n4}], no_wait}, PendingTxs7), 
    PendingTxs9 = dict:store(T5, {[{lp2, n2}], [{rp2, n4}], no_wait}, PendingTxs8), 
    %ets:insert(dependency, {T3, T4}),
    %ets:insert(dependency, {T3, T5}),
    %ets:insert(dependency, {T4, T5}),
    DepDict6 = dict:store(T3, {0, 0, T3#tx_id.snapshot_time+1}, RD3),
    DepDict7 = dict:store(T4, {0, 1, T5#tx_id.snapshot_time+1}, DepDict6),
    DepDict8 = dict:store(T5, {0, 2, 6}, DepDict7),
    io:format(user, "My commit time is ~w, Txns are ~w ~w ~w", [0, T3, T4, T5]),
    {_PendingTxs10, PD4, _MPT4, RD4, _ClientDict4, CommittedTxs3} 
            =  try_commit_follower(0, [T3, T4, T5], RepDict, DepDict8, PendingTxs9, ClientDict3, []),
    ?assertMatch([{T3, _}], CommittedTxs3),
    ?assertEqual(2, dict:size(RD4)),
    ?assertEqual(PD4, [T4, T5]).
    %?assertEqual(MPT4, T5#tx_id.snapshot_time+1).

    %?assertEqual(true, mock_partition_fsm:if_applied({commit_specula, cache_serv, T3, rp2}, T3#tx_id.snapshot_time+1)),
    %?assertEqual(true, mock_partition_fsm:if_applied({commit_specula, cache_serv, T4, rp2}, MPT4)),
    %?assertEqual(true, mock_partition_fsm:if_applied({abort_specula, cache_serv, T5, rp2}, nothing)),
    %?assertEqual(true, mock_partition_fsm:if_applied({commit, T4, [{lp2, n2}]}, MPT4)),
    %?assertEqual(true, mock_partition_fsm:if_applied({repl_commit, T4, [{rp2, n4}]}, MPT4)).
    %?assertEqual(true, mock_partition_fsm:if_applied({repl_abort, T5, [{rp2, n4}]}, nothing )).
    
-endif.
