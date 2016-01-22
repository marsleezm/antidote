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
         handle_event/3,
         handle_info/2,
         handle_sync_event/4,
         terminate/2]).

%% States
-export([repl_prepare/4,
	    check_table/0,
         repl_abort/5,
         repl_commit/6,
         %repl_abort/4,
         %repl_commit/5,
         quorum_replicate/7,
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
        fast_reply :: boolean(),
        delay :: non_neg_integer(),
        my_name :: atom(),
		self :: atom()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    Name = get_repl_name(),
    gen_server:start_link({global, Name},
             ?MODULE, [Name], []).

repl_prepare(Partition, PrepType, TxId, LogContent) ->
    gen_server:cast({global, get_repl_name()}, {repl_prepare, Partition, PrepType, TxId, LogContent}).

check_table() ->
    gen_server:call({global, get_repl_name()}, {check_table}).

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

quorum_replicate(Replicas, Type, TxId, Partition, WriteSet, TimeStamp, MyName) ->
    lists:foreach(fun(Replica) ->
            gen_server:cast({global, Replica}, {repl_prepare, 
                Type, TxId, Partition, WriteSet, TimeStamp, MyName})
            end, Replicas).

chain_replicate(_Replicas, _TxId, _WriteSet, _TimeStamp, _ToReply) ->
    %% Not implemented yet.
    ok.

%%%===================================================================
%%% Internal
%%%===================================================================


init([Name]) ->
    hash_fun:build_rev_replicas(),
    [{_, Replicas}] = ets:lookup(meta_info, node()), 
    PendingLog = tx_utilities:open_private_table(pending_log),
    NewDict = generate_except_replicas(Replicas),
    Lists = antidote_config:get(to_repl),
    FastReply = antidote_config:get(fast_reply),
    [LocalRepList] = [LocalReps || {Node, LocalReps} <- Lists, Node == node()],
    LocalRepNames = [list_to_atom(atom_to_list(node())++"repl"++atom_to_list(L))  || L <- LocalRepList ],
    lager:info("NewDict is ~w, LocalRepNames is ~w", [NewDict, LocalRepNames]),
    {ok, #state{replicas=Replicas, mode=quorum, repl_factor=length(Replicas), local_rep_set=sets:from_list(LocalRepNames), 
                pending_log=PendingLog, my_name=Name, except_replicas=NewDict, fast_reply=FastReply}}.

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0};

handle_call({check_table}, _Sender, SD0=#state{pending_log=PendingLog}) ->
    lager:info("Log info: ~w", [ets:tab2list(PendingLog)]),
    {reply, ok, SD0}.

%% RepMode can only be prepared for now.
handle_cast({repl_prepare, Partition, PrepType, TxId, LogContent}, 
	    SD0=#state{replicas=Replicas, pending_log=PendingLog, except_replicas=ExceptReplicas, 
            my_name=MyName, mode=Mode, repl_factor=ReplFactor, fast_reply=_FastReply}) ->
    case PrepType of
        single_commit ->
            {Sender, WriteSet, CommitTime} = LogContent,
            case Mode of
                quorum ->
                    %lager:warning("Got single_commit request for ~w, Sending to ~w", [TxId, Replicas]),
                    ets:insert(PendingLog, {{TxId, Partition}, {{single_commit, Sender, 
                            CommitTime, ignore}, ReplFactor}}),
                    quorum_replicate(Replicas, single_commit, TxId, Partition, WriteSet, CommitTime, MyName);
                chain ->
                    ToReply = {single_commit, TxId, CommitTime, PrepType},
                    chain_replicate(Replicas, TxId, WriteSet, CommitTime, {Sender, ToReply})
            end;
        prepared ->
            {Sender, RepMode, WriteSet, PrepareTime} = LogContent,
            case Mode of
                quorum ->
                    case RepMode of
                        %% This is for non-specula version
                        {remote, ignore} ->
                            ets:insert(PendingLog, {{TxId, Partition}, {{prepared, Sender, 
                                    PrepareTime, remote}, ReplFactor}}),
                            quorum_replicate(Replicas, prepared, TxId, Partition, WriteSet, PrepareTime, MyName);
                        %% This is for specula.. So no need to replicate the msg to the partition that sent the prepare(because due to specula,
                        %% the msg is replicated already).
                        {remote, SenderName} ->
                            %lager:warning("RepMode is ~w", [RepMode]),
                            case dict:find(SenderName, ExceptReplicas) of
                                {ok, R} -> 
                                    %lager:warning("Remote prepared request for {~w, ~w}, Sending to ~w", [TxId, Partition, R]),
                                    ets:insert(PendingLog, {{TxId, Partition}, {{prepared, Sender, 
                                            PrepareTime, remote}, ReplFactor-1}}),
                                    quorum_replicate(R, prepared, TxId, Partition, WriteSet, PrepareTime, MyName);
                                error ->
                                    %lager:warning("Remote prepared request for {~w, ~w}, Sending to ~w", [TxId, Partition, Replicas]),
                                    ets:insert(PendingLog, {{TxId, Partition}, {{prepared, Sender, 
                                            PrepareTime, remote}, ReplFactor}}),
                                    quorum_replicate(Replicas, prepared, TxId, Partition, WriteSet, PrepareTime, MyName)
                            end;
                        _ ->
                            %lager:warning("Local prepared request for {~w, ~w}, Sending to ~w", [TxId, Partition, Replicas]),
                            %case RepMode of
                            %    local_fast -> lager:warning("Replicating local_fast txn! ~w", [TxId]);
                            %    _ -> ok
                            %end,
                            ets:insert(PendingLog, {{TxId, Partition}, {{prepared, Sender, 
                                    PrepareTime, RepMode}, ReplFactor}}),
                            quorum_replicate(Replicas, prepared, TxId, Partition, WriteSet, PrepareTime, MyName)
                    end;
                chain ->
                    ToReply = {prepared, TxId, PrepareTime, RepMode},
                    chain_replicate(Replicas, TxId, WriteSet, PrepareTime, {Sender, ToReply})
            end
    end,
    {noreply, SD0};

handle_cast({ack, Partition, TxId, ProposedTs}, SD0=#state{pending_log=PendingLog}) ->
    case ets:lookup(PendingLog, {TxId, Partition}) of
        [{{TxId, Partition}, {R, 1}}] ->
        %ToReply = {prepared, TxId, PrepareTime, remote},
%                    ets:insert(PendingLog, {{TxId, Partition}, {{Prep, Sender,
%                            PrepareTime, WriteSet, RepMode}, ReplFactor-1}}),
            %%lager:warning("Got req from ~w for ~w, done", [Partition, TxId]),
            {PrepType, Sender, Timestamp, RepMode} = R,
            case PrepType of
                prepared ->
                    lager:warning("Got enough reply.. Replying ~w for [~w, ~w] to ~w", [Partition, RepMode, TxId, Sender]),
                    true = ets:delete(PendingLog, {TxId, Partition}),
                    case RepMode of
                        false -> ok;
                        local_fast -> gen_server:cast(Sender, {real_prepared, TxId, max(Timestamp, ProposedTs)});
                        _ -> gen_server:cast(Sender, {prepared, TxId, max(Timestamp, ProposedTs)})
                    end;
                single_commit ->
                    %lager:warning("Single commit of ~w got enough replies, replying to ~w", [TxId, Sender]),
                    true = ets:delete(PendingLog, {TxId, Partition}),
                    gen_server:reply(Sender, {ok, {committed, Timestamp}})
            end;
        [{{TxId, Partition}, {R, N}}] ->
            {PrepType, Sender, Timestamp, RepMode} = R,
            %lager:warning("Got req from ~w for ~w, ~w more to get", [Partition, TxId, N-1]),
            ets:insert(PendingLog, {{TxId, Partition}, {{PrepType, Sender, max(Timestamp,ProposedTs), RepMode}, N-1}});
        [] -> %%The record is appended already, do nothing
            %lager:warning("~w, ~w: prepare repl disappeared!!", [Partition, TxId]),
            ok
    end,
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

terminate(_Reason, _SD) ->
    ok.

send_after(Delay, To, Msg) ->
    timer:sleep(Delay),
    %lager:warning("Sending info after ~w", [Delay]),
    gen_server:cast(To, Msg).

get_repl_name() ->
    list_to_atom("repl"++atom_to_list(node())).

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
            dict:store(list_to_atom(NodeName), Except, D)
        end, dict:new(), Replicas).

get_name(ReplName, N) ->
    case lists:sublist(ReplName, N, 4) of
        "repl" ->  lists:sublist(ReplName, 1, N-1);
         _ -> get_name(ReplName, N+1)
    end.
    
%% No replication
repl_commit(_, _, _, false, _, _) ->
    ok;
repl_commit([], _, _, true, _, _) ->
    ok;
repl_commit(UpdatedParts, TxId, CommitTime, true, ToCache, RepDict) ->
    NodeParts = build_node_parts(UpdatedParts),
    lists:foreach(fun({Node, Partitions}) ->
        Replicas = dict:fetch(Node, RepDict),
        lists:foreach(fun(R) ->
            case R of
                cache -> 
                    case ToCache of false -> ok;
                                      true -> ?CACHE_SERV:commit_specula(TxId, Partitions, CommitTime)
                    end; 
                {rep, S} ->
                    gen_server:cast({global, S}, {repl_commit, TxId, CommitTime, Partitions});
                S ->
                    gen_server:cast({global, S}, {repl_commit, TxId, CommitTime, Partitions})
            end end,  Replicas) end,
            NodeParts).

repl_abort(_, _, false, _, _) ->
    ok;
repl_abort([], _, true, _, _) ->
    ok;
repl_abort(UpdatedParts, TxId, true, ToCache, RepDict) ->
    NodeParts = build_node_parts(UpdatedParts),
    lists:foreach(fun({Node, Partitions}) ->
        Replicas = dict:fetch(Node, RepDict),
        lists:foreach(fun(R) ->
            case R of
                cache -> case ToCache of false -> ok;
                                        true -> ?CACHE_SERV:abort_specula(TxId, Partitions) 
                         end;
                {rep, S} ->
                    gen_server:cast({global, S}, {repl_abort, TxId, Partitions});
                S ->
                    gen_server:cast({global, S}, {repl_abort, TxId, Partitions})
            end end,  Replicas) end,
            NodeParts).
