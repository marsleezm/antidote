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
-module(repl_fsm).

-behavior(gen_server).

-include("antidote.hrl").

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
-export([start_link/0]).

%% Callbacks
-export([init/1,
	    handle_call/3,
	    handle_cast/2,
         code_change/3,
        prev_remove/7,
        get_dc_id/1,
         handle_event/3,
         handle_info/2,
         handle_sync_event/4,
         terminate/2]).

%% States
-export([repl_prepare/4,
	    check_table/0,
         repl_abort/4,
         repl_abort/5,
         repl_commit/5,
         repl_commit/6,
         %repl_abort/4,
         %repl_commit/5,
         quorum_replicate/7,
         ack_pending_prep/2,
         abort_pending_prep/2,
         chain_replicate/5,
         send_after/3]).

%% Spawn

-record(state, {
        pending_log :: cache_id(),
        log_size :: non_neg_integer(),
        replicas :: [atom()],
        local_rep_set :: set(),
        except_replicas :: dict(),

        mode :: atom(),
        repl_factor :: non_neg_integer(),
        delay :: non_neg_integer(),
		self :: atom()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, repl_fsm},
             ?MODULE, [repl_fsm], []).

repl_prepare(Partition, PrepType, TxId, LogContent) ->
    gen_server:cast(repl_fsm, {repl_prepare, Partition, PrepType, TxId, LogContent}).

ack_pending_prep(TxId, Partition) ->
    gen_server:cast(repl_fsm, {ack_pending_prep, TxId, Partition}).

abort_pending_prep(TxId, Partition) ->
    gen_server:cast(repl_fsm, {abort_pending_prep, TxId, Partition}).

check_table() ->
    gen_server:call(repl_fsm, {check_table}).


%repl_abort(UpdatedParts, TxId, DoRepl) ->
%    repl_abort(UpdatedParts, TxId, DoRepl, false). 

%repl_abort(_, _, false) ->
%    ok;
%repl_abort([], _, true) ->
%    ok;
%repl_abort(UpdatedParts, TxId, true) ->
%    gen_server:cast({global, get_repl_name()}, {repl_abort, TxId, UpdatedParts}).

%repl_commit(UpdatedParts, TxId, CommitTime, DoRepl) ->
%    repl_commit(UpdatedParts, TxId, CommitTime, DoRepl). 

%repl_commit(_, _, _, false) ->
%    ok;
%repl_commit([], _, _, true) ->
%    ok;
%repl_commit(UpdatedParts, TxId, CommitTime, true) ->
%    gen_server:cast({global, get_repl_name()}, {repl_commit, TxId, UpdatedParts, CommitTime}).

quorum_replicate(Replicas, Type, TxId, Partition, WriteSet, TimeStamp, Sender) ->
    lager:warning("~w of part ~w send repl_update to ~w", [TxId, Partition, Replicas]),
    lists:foreach(fun(Replica) ->
            gen_server:cast({global, Replica}, {repl_prepare, 
                Type, TxId, Partition, WriteSet, TimeStamp, Sender})
            end, Replicas).

chain_replicate(_Replicas, _TxId, _WriteSet, _TimeStamp, _ToReply) ->
    %% Not implemented yet.
    ok.

%%%===================================================================
%%% Internal
%%%===================================================================


init([_Name]) ->
    hash_fun:build_rev_replicas(),
    [{_, Replicas}] = ets:lookup(meta_info, node()), 
   %lager:warning("Replicas are ~w", [Replicas]),
    %PendingLog = tx_utilities:open_private_table(pending_log),
    NewDict = generate_except_replicas(Replicas),
    Lists = antidote_config:get(to_repl),
    [LocalRepList] = [LocalReps || {Node, LocalReps} <- Lists, Node == node()],
    LocalRepNames = [list_to_atom(atom_to_list(node())++"repl"++atom_to_list(L))  || L <- LocalRepList ],
    lager:info("NewDict is ~w, LocalRepNames is ~w", [NewDict, LocalRepNames]),
    {ok, #state{replicas=Replicas, mode=quorum, repl_factor=length(Replicas), local_rep_set=sets:from_list(LocalRepNames), 
                except_replicas=NewDict}}.

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0};

handle_call({check_table}, _Sender, SD0=#state{pending_log=PendingLog}) ->
    lager:info("Log info: ~w", [ets:tab2list(PendingLog)]),
    {reply, ok, SD0}.

%% RepMode can only be prepared for now.
handle_cast({repl_prepare, Partition, prepared, TxId, LogContent}, 
	    SD0=#state{replicas=Replicas, except_replicas=ExceptReplicas, 
            mode=Mode}) ->
    %lager:info("Send preparing of Tx ~w", [TxId]),
            {Sender, RepMode, WriteSet, PrepareTime} = LogContent,
            case Mode of
                quorum ->
                    case RepMode of
                        %% This is for non-specula version
                        {remote, ignore} ->
                           %lager:warning("Ignor Remote prepared request for {~w, ~w}, Sending to ~w", [TxId, Partition, Replicas]),
                            quorum_replicate(Replicas, prepared, TxId, Partition, WriteSet, PrepareTime, Sender);
                        %% This is for specula.. So no need to replicate the msg to the partition that sent the prepare(because due to specula,
                        %% the msg is replicated already).
                        {remote, _SenderName} ->
                            SenderDcId = TxId#tx_id.dc_id,
                            case dict:find(SenderDcId, ExceptReplicas) of
                                {ok, []} ->
                                    ok;
                                {ok, R} -> 
                                   %lager:warning("Remote prepared request for {~w, ~w}, Sending to ~w", [TxId, Partition, R]),
                                    quorum_replicate(R, prepared, TxId, Partition, WriteSet, PrepareTime, Sender);
                                error ->
                                   %lager:warning("Remote prepared request for {~w, ~w}, Sending to ~w", [TxId, Partition, Replicas]),
                                    quorum_replicate(Replicas, prepared, TxId, Partition, WriteSet, PrepareTime, Sender)
                            end;
                        _ ->
                            %lager:warning("Local prepared request for {~w, ~w}, Sending to ~w, ReplFactor is ~w", [TxId, Partition, Replicas, ReplFactor]),
                            quorum_replicate(Replicas, prepared, TxId, Partition, WriteSet, PrepareTime, Sender)
                    end;
                chain ->
                    ToReply = {prepared, TxId, PrepareTime, RepMode},
                    chain_replicate(Replicas, TxId, WriteSet, PrepareTime, {Sender, ToReply})
            end,
    {noreply, SD0};

%handle_cast({ack_pending_prep, TxId, Partition}, SD0=#state{pending_log=PendingLog}) ->
%    case ets:lookup(PendingLog, {TxId, Partition}) of
%        [{{TxId, Partition}, Sender, Timestamp, pending_prepared, N}] ->
%            %lager:warning("Pending prep of ~w still waiting.. With time ~w", [TxId, Timestamp]),
%            ets:insert(PendingLog, {{TxId, Partition}, Sender, Timestamp, remote, N});
%        [{{TxId, Partition}, Sender, Timestamp}] ->
%            ets:delete(PendingLog, {TxId, Partition}),
%            %lager:warning("~w ack pending with time ~w", [TxId, Timestamp]),
%            gen_server:cast(Sender, {prepared, TxId, Timestamp})
%    end,
%    {noreply, SD0};

handle_cast({abort_pending_prep, TxId, Partition}, SD0=#state{pending_log=PendingLog, replicas=Replicas}) ->
    ets:delete(PendingLog, {TxId, Partition}),
    lists:foreach(fun(Replica) ->
              gen_server:cast({global, Replica}, {repl_abort,
                  TxId, [Partition]})
              end, Replicas),
    {noreply, SD0};

%handle_cast({ack, Partition, TxId, ProposedTs}, SD0=#state{pending_log=PendingLog}) ->
%    %lager:info("Got proposed ts ~w fo ~w", [ProposedTs, TxId]),
%    case ets:lookup(PendingLog, {TxId, Partition}) of
%        [{{TxId, Partition}, Sender, Timestamp, RepMode, 1}] ->
%           %lager:warning("Got all replis, replying for ~w, ~w", [TxId, Partition]),
%            case RepMode of
%                false -> true = ets:delete(PendingLog, {TxId, Partition}), ok;
%                %local_fast -> true = ets:delete(PendingLog, {TxId, Partition}),
%                %              gen_server:cast(Sender, {real_prepared, TxId, max(Timestamp, ProposedTs)});
%                pending_prepared -> ets:insert(PendingLog, {{TxId, Partition}, Sender, max(Timestamp,ProposedTs)});
%                _ -> true = ets:delete(PendingLog, {TxId, Partition}),
%                     %lager:warning("Replying for ~w", [TxId]),
%                     gen_server:cast(Sender, {prepared, TxId, max(Timestamp, ProposedTs)})
%            end;
%        [{{TxId, Partition}, Sender, Timestamp, RepMode, N}] ->

handle_cast(_Info, StateData) ->
    {noreply,StateData}.

handle_info(_Info, StateData) ->
    {noreply,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

terminate(_Reason, _SD) ->
    ok.

send_after(Delay, To, Msg) ->
    timer:sleep(Delay),
    %lager:warning("Sending info after ~w", [Delay]),
    gen_server:cast(To, Msg).

    %list_to_atom("repl"++atom_to_list(node())).

build_node_parts(Parts) ->
    D = lists:foldl(fun({Partition, Node}, Acc) ->
                      dict:append(Node, Partition, Acc)
                   end,
                    dict:new(), Parts),
    dict:to_list(D).


generate_except_replicas(Replicas) ->
    lists:foldl(fun(Rep, D) ->
            Except = lists:delete(Rep, Replicas),
            NodeName = get_name(atom_to_list(Rep), 1),
            NodeDcId = get_dc_id(list_to_atom(NodeName)),
            dict:store(NodeDcId, Except, D)
        end, dict:new(), Replicas).

get_name(ReplName, N) ->
    case lists:sublist(ReplName, N, 4) of
        "repl" ->  lists:sublist(ReplName, 1, N-1);
         _ -> get_name(ReplName, N+1)
    end.

get_dc_id(Node) ->
    NumDcs = antidote_config:get(num_dcs),
    ReplList = antidote_config:get(to_repl),
    NumNodes = length(ReplList),  
    NumNodesPerDc = NumNodes div NumDcs,
    NodeIndex = index([N||{N, _} <- ReplList], Node),
    (NodeIndex-1) div NumNodesPerDc + 1.
    
index([], _N) ->
    -100;
index([N|_], N) ->
    1;
index([_M|R], N) ->
    1+index(R, N).    

    
%% No replication
repl_commit(UpdatedParts, TxId, CommitTime, ToCache, RepDict) ->
    repl_commit(UpdatedParts, TxId, CommitTime, ToCache, RepDict, no_wait).

repl_commit([], _, _, _, _, _) ->
    ok;
repl_commit(UpdatedParts, TxId, CommitTime, ToCache, RepDict, IfWaited) ->
    NodeParts = build_node_parts(UpdatedParts),
    lists:foldl(fun({Node, Partitions}, Acc) ->
        Replicas = dict:fetch(Node, RepDict),
        lager:info("repl commit of ~w for node ~w to replicas ~w for partitions ~w", [TxId, Node, Replicas, Partitions]),
        lists:foldl(fun(R, Acc1) ->
            case R of
                cache -> 
                    %lager:warning("~w: Sending to cache is ~w", [TxId, ToCache]),
                    case ToCache of 
                        false -> Acc1;
                        true -> ?CACHE_SERV:commit(TxId, Partitions, CommitTime),
                                [{cache, Partitions}|Acc1]
                    end; 
                {local_dc, S} ->
                    gen_server:cast({global, S}, {repl_commit, TxId, CommitTime, Partitions, IfWaited}),
                    [{slave, S, Partitions}|Acc1];
                S ->
                    gen_server:cast({global, S}, {repl_commit, TxId, CommitTime, Partitions, no_wait}),
                    Acc1
            end end, Acc, Replicas) end,
            [], NodeParts).

repl_abort(UpdatedParts, TxId, ToCache, RepDict) -> 
    repl_abort(UpdatedParts, TxId, ToCache, RepDict, no_wait). 

repl_abort([], _, _, _, _) ->
    ok;
repl_abort(UpdatedParts, TxId, ToCache, RepDict, IfWaited) ->
   %lager:warning("Aborting to ~w", [UpdatedParts]),
    NodeParts = build_node_parts(UpdatedParts),
    lists:foldl(fun({Node, Partitions}, Acc) ->
        Replicas = dict:fetch(Node, RepDict),
        %lager:info("repl abort of ~w for node ~w to replicas ~w for partitions ~w", [TxId, Node, Replicas, Partitions]),
        lists:foldl(fun(R, Acc1) ->
            case R of
                cache -> case ToCache of false -> Acc1;
                                        true -> ?CACHE_SERV:abort(TxId, Partitions), [{cache, Partitions}|Acc1] 
                         end;
                {local_dc, S} ->
                    %lager:warning("Aborting to repl ~w, Parts are ~w", [S, Partitions]),
                    gen_server:cast({global, S}, {repl_abort, TxId, Partitions, IfWaited}),
                    [{slave, S, Partitions}|Acc1];
                S ->
                    %lager:warning("Aborting to repl ~w", [S]),
                    gen_server:cast({global, S}, {repl_abort, TxId, Partitions, no_wait}),
                    Acc1
            end end,  Acc, Replicas) end,
            [], NodeParts).

prev_remove(TxId, Type, CommitTime, LocalParts, RemoteParts, LCBinary, RepDict) ->
    TempBN = lists:foldl(fun(Node, BN) ->
                riak_core_vnode_master:command(Node,
                             {prev_remove, TxId, Type, BN, CommitTime},
                             {server, undefined, self()},
                             ?CLOCKSI_MASTER),
             BN*2
            end, 1, LocalParts),

    RemoteNodeParts = build_node_parts(RemoteParts),
    {RemoteCumParts, LCBinary} = lists:foldl(fun({Node, Partitions}, {Acc, BN}) ->
        Replicas = dict:fetch(Node, RepDict),
        lists:foldl(fun(R, {Acc1, TBN}) ->
            case R of
                cache -> ?CACHE_SERV:prev_remove(prev_remove, TxId, Type, TBN, CommitTime, Partitions),
                         {[{cache, Partitions}|Acc1], 2*TBN};
                {local_dc, S} ->
                    gen_server:cast({global, S}, {prev_remove, TxId, Type, TBN, CommitTime, Partitions}),
                    {[{slave, S, Partitions}|Acc1], 2*TBN};
                S -> %% Non-local replica
                    case Type of 
                        commit ->
                            gen_server:cast({global, S}, {repl_commit, TxId, CommitTime, Partitions}),
                            {Acc1, TBN};
                        abort -> 
                            gen_server:cast({global, S}, {repl_abort, TxId, Partitions}),
                            {Acc1, TBN}
                    end
            end end, {Acc, BN}, Replicas) end,
            {[], TempBN}, RemoteNodeParts),
    RemoteCumParts.
