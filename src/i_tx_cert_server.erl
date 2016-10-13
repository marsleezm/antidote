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
        name :: atom(),
        rep_dict :: dict(),
        do_repl :: boolean(),
        client_dict :: dict(),
        total_repl_factor :: non_neg_integer(),
        last_commit_ts :: non_neg_integer()}).

-record(client_state, {
        tx_id :: txid(),
        local_parts = []:: [],
        remote_parts =[] :: [],
        pending_to_ack :: non_neg_integer(),
        sender,
        to_ack = 0 :: non_neg_integer(),
        prepare_time = 0 :: non_neg_integer(),
        stage :: read | local_cert | remote_cert
        }).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name) ->
   %lager:warning("Specula tx cert started wit name ~w, id is ~p", [Name, self()]),
    gen_server:start_link({local, Name},
             ?MODULE, [Name], []).

%%%===================================================================
%%% Internal
%%%===================================================================

init([Name]) ->
    RepDict = hash_fun:build_rep_dict(true),
    %PendingMetadata = tx_utilities:open_private_table(pending_metadata),
    [{_, Replicas}] = ets:lookup(meta_info, node()),
    TotalReplFactor = length(Replicas)+1,
    %Cdf = case antidote_config:get(cdf, false) of
    %            true -> [];
    %            _ -> false
    %        end,
    {ok, #state{do_repl=antidote_config:get(do_repl), name=Name, total_repl_factor=TotalReplFactor, last_commit_ts=0, rep_dict=RepDict, client_dict=dict:new()}}.

handle_call({get_cdf}, _Sender, SD0) ->
    %case Cdf of false -> ok;
    %            _ ->
    %                case Cdf of [] -> ok;
    %                            _ ->
    %                                FinalFileName = atom_to_list(Name) ++ "final-latency",
    %                                {ok, FinalFile} = file:open(FinalFileName, [raw, binary, write]),
    %                                lists:foreach(fun(Lat) -> file:write(FinalFile,  io_lib:format("~w\n", [Lat]))
    %                                              end, Cdf),
    %                                file:close(FinalFile)
    %                end
    %end,
    %{reply, ok, SD0};
    {reply, ok, SD0};

handle_call({get_stat}, _Sender, SD0) ->
    {reply, [0, 0, 0, 0, 0, 0, 0], SD0};

handle_call({append_values, Node, KeyValues, CommitTime}, Sender, SD0) ->
    clocksi_vnode:append_values(Node, KeyValues, CommitTime, Sender),
    {noreply, SD0};

handle_call({get_pid}, _Sender, SD0) ->
      {reply, self(), SD0};

handle_call({get_hash_fun}, _Sender, SD0) ->
    L = hash_fun:get_hash_fun(),
    {reply, L, SD0};

handle_call({start_tx, _}, Sender, SD0) ->
    handle_call({start_tx}, Sender, SD0);
handle_call({start_tx}, Sender, SD0=#state{last_commit_ts=LastCommitTS, client_dict=ClientDict}) ->
    {Client, _} = Sender,
    TxId = tx_utilities:create_tx_id(LastCommitTS+1, Client),
   %lager:warning("Starting new txn ~w", [TxId]),
    ClientState = case dict:find(Client, ClientDict) of
                    error ->
                        #client_state{};
                    {ok, SenderState} ->
                        SenderState
                 end,
    ClientState1 = ClientState#client_state{tx_id=TxId, stage=read, pending_to_ack=0},
    {reply, TxId, SD0#state{last_commit_ts=LastCommitTS+1, client_dict=dict:store(Client, ClientState1, ClientDict)}};

handle_call({start_read_tx}, _Sender, SD0) ->
    TxId = tx_utilities:create_tx_id(0),
    {reply, TxId, SD0};

handle_call({read, Key, TxId, Node}, Sender, SD0) ->
   %lager:warning("Trying to relay read ~w for key ~w", [TxId, Key]),
    clocksi_vnode:relay_read(Node, Key, TxId, Sender, false),
    {noreply, SD0};

handle_call({certify_read, TxId, 0},  Sender, SD0) ->
    handle_call({certify, TxId, [], [], ignore},  Sender, SD0);
handle_call({certify_update, TxId, LocalUpdates, RemoteUpdates, 0, _},  Sender, SD0) ->
    handle_call({certify, TxId, LocalUpdates, RemoteUpdates, ignore, ignore},  Sender, SD0);
handle_call({certify, TxId, LocalUpdates, RemoteUpdates, _, _},  Sender, SD0=#state{client_dict=ClientDict, last_commit_ts=LastCommitTs, total_repl_factor=TotalReplFactor}) ->
   %lager:warning("Certifying txn ~w", [TxId]),
    {Client, _} = Sender,
    ClientState = dict:fetch(Client, ClientDict),
    case length(LocalUpdates) of
        0 ->
            case length(RemoteUpdates) of
                0 -> gen_server:reply(Sender, {ok, {committed, LastCommitTs, {[],[],[]}}}),
                    {noreply, SD0};
                _ -> 
                    RemoteParts = [P || {P, _} <- RemoteUpdates],
                    clocksi_vnode:prepare(RemoteUpdates, TxId, {remote,ignore}),
                    %Now = os:timestamp(),
                    ClientState1 = ClientState#client_state{tx_id=TxId, to_ack=length(RemoteUpdates)*TotalReplFactor, local_parts=[], remote_parts=RemoteParts,
                                    sender=Sender, stage=remote_cert},
                    %{noreply, SD0#state{tx_id=TxId, pend_txn_start=Now, to_ack=length(RemoteUpdates)*TotalReplFactor, local_parts=[], remote_parts=RemoteParts, sender=Sender, stage=remote_cert}}
                    {noreply, SD0#state{client_dict=dict:store(Client, ClientState1, ClientDict)}}
            end;
        N ->
            LocalParts = [Part || {Part, _} <- LocalUpdates],
            %Now = os:timestamp(),
            clocksi_vnode:prepare(LocalUpdates, TxId, local),
            ClientState1 = ClientState#client_state{tx_id=TxId, to_ack=N, pending_to_ack=N*(TotalReplFactor-1), local_parts=LocalParts, remote_parts=RemoteUpdates,
                                    sender=Sender, stage=local_cert},
            {noreply, SD0#state{client_dict=dict:store(Client, ClientState1, ClientDict)}}
    end;

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

handle_cast({load, Sup, Type, Param}, SD0) ->
    case Type of tpcc -> tpcc_load:load(Param);
                 micro -> micro_load:load(Param);
                 rubis -> rubis_load:load(Param)
    end,
    Sup ! done,
    {noreply, SD0};

handle_cast({clean_data, Sender}, SD0) ->
    ClientDict = dict:new(),
    Sender ! cleaned,
    {noreply, SD0#state{client_dict=ClientDict}};

handle_cast({prepared, TxId, PrepareTime}, 
	    SD0=#state{total_repl_factor=TotalReplFactor, rep_dict=RepDict, client_dict=ClientDict}) ->
   %lager:warning("Receive prepared for txn ~w", [TxId]),
    Client = TxId#tx_id.client_pid,
    ClientState = dict:fetch(Client, ClientDict),
    MyTxId = ClientState#client_state.tx_id,
    N = ClientState#client_state.to_ack,
    PN = ClientState#client_state.pending_to_ack,
    LocalParts = ClientState#client_state.local_parts,
    RemoteUpdates = ClientState#client_state.remote_parts,
    Sender = ClientState#client_state.sender,
    OldPrepTime = ClientState#client_state.prepare_time,
    case {TxId, ClientState#client_state.stage} of
        {MyTxId, local_cert} ->
            case N of
                1 -> 
                    RemoteParts = [Part || {Part, _} <- RemoteUpdates],
                    case length(RemoteParts) of
                        0 ->
                            case PN of
                                0 ->
                                    CommitTime = max(PrepareTime, OldPrepTime),
                                    clocksi_vnode:commit(LocalParts, TxId, CommitTime),
                                    repl_fsm:repl_commit(LocalParts, TxId, CommitTime, false, RepDict),
                                   %lager:warning("~w committed", [TxId]),
                                    gen_server:reply(Sender, {ok, {committed, CommitTime, {[],[],[]}}}),
                                    %ets:insert(Cdf, {TxId, get_time_diff(PendStart, os:timestamp())}),
                                    %Cdf1 =  case Cdf of false -> false;
                                    %            _ -> [get_time_diff(PendStart, os:timestamp())]
                                    %        end, 
                                    ClientState1 = ClientState#client_state{tx_id={}, prepare_time=CommitTime},
                                    ClientDict1 = dict:store(Client, ClientState1, ClientDict),
                                    {noreply, SD0#state{last_commit_ts=CommitTime, client_dict=ClientDict1}};
                                _ ->
                                    ClientState1 = ClientState#client_state{to_ack=PN, pending_to_ack=0, prepare_time=max(PrepareTime, OldPrepTime), stage=remote_cert},
                                    ClientDict1 = dict:store(Client, ClientState1, ClientDict),
                                    {noreply, SD0#state{client_dict=ClientDict1}}
                            end;
                        L ->
                            MaxPrepTime = max(PrepareTime, OldPrepTime),
                            clocksi_vnode:prepare(RemoteUpdates, TxId, {remote,ignore}),
                            ClientState1 = ClientState#client_state{prepare_time=MaxPrepTime, to_ack=L*TotalReplFactor+PN, pending_to_ack=0, 
                                    remote_parts=RemoteParts, stage=remote_cert},
                            ClientDict1 = dict:store(Client, ClientState1, ClientDict),
                            {noreply, SD0#state{client_dict=ClientDict1}}
                        end;
                _ ->
                    MaxPrepTime = max(OldPrepTime, PrepareTime),
                    ClientState1 = ClientState#client_state{to_ack=N-1, prepare_time=MaxPrepTime},
                    ClientDict1 = dict:store(Client, ClientState1, ClientDict),
                    {noreply, SD0#state{client_dict=ClientDict1}}
            end;
%handle_cast({prepared, TxId, PrepareTime}, 
%	    SD0=#state{remote_parts=RemoteParts, sender=Sender, tx_id=TxId, rep_dict=RepDict, prepare_time=MaxPrepTime, to_ack=N, cdf=Cdf, local_parts=LocalParts, stage=remote_cert}) ->
        {MyTxId, remote_cert} ->
            case N of
                1 ->
                    CommitTime = max(OldPrepTime, PrepareTime),
                    clocksi_vnode:commit(LocalParts, TxId, CommitTime),
                    %% Remote updates should be just partitions here
                    clocksi_vnode:commit(RemoteUpdates, TxId, CommitTime),
                    repl_fsm:repl_commit(LocalParts, TxId, CommitTime, false, RepDict),
                    repl_fsm:repl_commit(RemoteUpdates, TxId, CommitTime, false, RepDict),
                   %lager:warning("~w committed", [TxId]),
                    gen_server:reply(Sender, {ok, {committed, CommitTime, {[],[],[]}}}),
                    %ets:insert(Cdf, {TxId, get_time_diff(PendStart, os:timestamp())}),
                    %Cdf1 = case Cdf of false -> false;
                    %            _ -> [get_time_diff(PendStart, os:timestamp())|Cdf]
                    %end,
                    %ets:insert(Cdf, {TxId, [os:timestamp()|PendStart]}),
                    ClientState1 = ClientState#client_state{prepare_time=CommitTime, tx_id={}},
                    ClientDict1 = dict:store(Client, ClientState1, ClientDict),
                    {noreply, SD0#state{last_commit_ts=CommitTime, client_dict=ClientDict1}};
                N ->
                    ClientState1 = ClientState#client_state{to_ack=N-1, prepare_time=max(OldPrepTime, PrepareTime)},
                    ClientDict1 = dict:store(Client, ClientState1, ClientDict),
                    {noreply, SD0#state{client_dict=ClientDict1}}
            end;
        _ ->
            {noreply, SD0}
    end;
%handle_cast({prepared, _OtherTxId, _}, 
%	    SD0=#state{stage=local_cert}) ->
%      %lager:info("Received prepare of previous prepared txn! ~w", [OtherTxId]),
%    {noreply, SD0};

handle_cast({aborted, TxId, FromNode}, SD0=#state{client_dict=ClientDict, rep_dict=RepDict}) ->
   %lager:warning("Receive aborted for txn ~w", [TxId]),
    Client = TxId#tx_id.client_pid,
    ClientState = dict:fetch(Client, ClientDict),
    MyTxId = ClientState#client_state.tx_id,
    LocalParts = ClientState#client_state.local_parts,
    RemoteParts = ClientState#client_state.remote_parts,
    Sender = ClientState#client_state.sender,
    Stage = ClientState#client_state.stage,
    case {MyTxId, Stage} of
        {TxId, local_cert} ->
            LocalParts1 = lists:delete(FromNode, LocalParts), 
            clocksi_vnode:abort(LocalParts1, TxId),
            repl_fsm:repl_abort(LocalParts1, TxId, false, RepDict),
            gen_server:reply(Sender, {aborted, {[],[],[]}}),
            ClientState1 = ClientState#client_state{tx_id={}},
            ClientDict1 = dict:store(Client, ClientState1, ClientDict),
            {noreply, SD0#state{client_dict=ClientDict1}};
        {TxId, remote_cert} ->
            RemoteParts1 = lists:delete(FromNode, RemoteParts),
            clocksi_vnode:abort(LocalParts, TxId),
            clocksi_vnode:abort(RemoteParts1, TxId),
            repl_fsm:repl_abort(LocalParts, TxId, false, RepDict),
            repl_fsm:repl_abort(RemoteParts1, TxId, false, RepDict),
            gen_server:reply(Sender, {aborted, {[],[],[]}}),
            ClientState1 = ClientState#client_state{tx_id={}},
            ClientDict1 = dict:store(Client, ClientState1, ClientDict),
            {noreply, SD0#state{client_dict=ClientDict1}};
        _ ->
        {noreply, SD0}
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

