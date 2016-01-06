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

-include("antidote.hrl").
-include("speculation.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(CACHE_SERV, mock_partition_fsm).
-define(DATA_REPL_SERV, mock_partition_fsm).
-else.
-define(CACHE_SERV, cache_serv).
-define(DATA_REPL_SERV, data_repl_serv).
-endif.

%% Callbacks
-export([init/0]).

%% States
-export([repl_prepare/5,
         repl_commit/6,
         repl_abort/5,
         ack/2,
         quorum_replicate/7,
         chain_replicate/5]).


%%%===================================================================
%%% API
%%%===================================================================

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


init() ->
    hash_fun:build_rev_replicas(),
    [{_, Replicas}] = ets:lookup(meta_info, node()), 
    PendingLog = dict:new(), 
    NewDict = generate_except_replicas(Replicas),
    %Lists = antidote_config:get(to_repl),
    %[LocalRepList] = [LocalReps || {Node, LocalReps} <- Lists, Node == node()],
    %LocalRepNames = [list_to_atom(atom_to_list(node())++"repl"++atom_to_list(L))  || L <- LocalRepList ],
    %lager:info("NewDict is ~w, LocalRepNames is ~w", [NewDict, LocalRepNames]),
    #repl_state{replicas=Replicas, mode=quorum, repl_factor=length(Replicas), 
                pending_log=PendingLog, except_replicas=NewDict}.

%% RepMode can only be prepared for now.
repl_prepare(Partition, PrepType, TxId, LogContent, ReplState=#repl_state{mode=Mode, 
            replicas=Replicas, repl_factor=ReplFactor, pending_log=PendingLog, except_replicas=ExceptReplicas}) -> 
    Node = {Partition, node()},
    case PrepType of
        single_commit ->
            {Sender, WriteSet, CommitTime} = LogContent,
            case Mode of
                quorum ->
                   %lager:warning("Got single_commit request for ~w, Sending to ~w", [TxId, Replicas]),
                    PendingLog1 = dict:store({TxId, Partition}, {{single_commit, Sender, CommitTime, ignore}, ReplFactor}, 
                        PendingLog),
                    quorum_replicate(Replicas, single_commit, TxId, Partition, WriteSet, CommitTime, Node),
                    ReplState#repl_state{pending_log=PendingLog1};
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
                            PendingLog1 = dict:store({TxId, Partition}, {{prepared, Sender, PrepareTime, remote}, ReplFactor},
                                PendingLog),
                            quorum_replicate(Replicas, prepared, TxId, Partition, WriteSet, PrepareTime, Node),
                            ReplState#repl_state{pending_log=PendingLog1};
                        %% This is for specula.. So no need to replicate the msg to the partition that sent the prepare(because due to specula,
                        %% the msg is replicated already).
                        {remote, SenderName} ->
                            case dict:find(SenderName, ExceptReplicas) of
                                {ok, R} -> 
                                   %lager:warning("Remote prepared request for {~w, ~w}, Sending to ~w", [TxId, Partition, R]),
                                    PendingLog1 = dict:store({TxId, Partition}, {{prepared, Sender, PrepareTime, remote}, ReplFactor-1},
                                        PendingLog),
                                    quorum_replicate(R, prepared, TxId, Partition, WriteSet, PrepareTime, Node),
                                    ReplState#repl_state{pending_log=PendingLog1};
                                error ->
                                   %lager:warning("Remote prepared request for {~w, ~w}, Sending to ~w", [TxId, Partition, Replicas]),
                                    PendingLog1 = dict:store({TxId, Partition}, {{prepared, Sender, PrepareTime, remote}, ReplFactor},
                                          PendingLog),
                                    quorum_replicate(Replicas, prepared, TxId, Partition, WriteSet, PrepareTime, Node),
                                    ReplState#repl_state{pending_log=PendingLog1}
                            end;
                        _ ->
                           %lager:warning("Local prepared request for {~w, ~w}, Sending to ~w", [TxId, Partition, Replicas]),
                            PendingLog1 = dict:store({TxId, Partition}, {{prepared, Sender, PrepareTime, RepMode}, ReplFactor},
                                  PendingLog),
                            %case FastReply of
                            %    true ->
                            %        ets:delete(PendingLog, {TxId, Partition});
                            %    false ->
                            quorum_replicate(Replicas, prepared, TxId, Partition, WriteSet, PrepareTime, Node),
                            ReplState#repl_state{pending_log=PendingLog1}
                            %end
                    end;
                chain ->
                    ToReply = {prepared, TxId, PrepareTime, RepMode},
                    chain_replicate(Replicas, TxId, WriteSet, PrepareTime, {Sender, ToReply})
            end
    end.

repl_commit(_, _, _, false, _, _) ->
    ok;
repl_commit([], _, _, true, _, _) ->
    ok;
repl_commit(UpdatedParts, TxId, CommitTime, true, false, RepDict) -> 
    NodeParts = build_node_parts(UpdatedParts),
    lists:foreach(fun({Node, Partitions}) -> 
        Replicas = dict:fetch(Node, RepDict),
        lists:foreach(fun(R) ->
            case R of
                cache -> ok;
                {rep, Server} ->
                   %lager:warning("Repl commit to ~w for ~w of ~w", [Server, TxId, Partitions]),
                    gen_server:cast({global, Server}, {repl_commit, TxId, CommitTime, Partitions});
                Server ->
                   %lager:warning("Repl commit to ~w for ~w of ~w", [Server, TxId, Partitions]),
                    gen_server:cast({global, Server}, {repl_commit, TxId, CommitTime, Partitions})
            end end,  Replicas) end,
            NodeParts);
repl_commit(UpdatedParts, TxId, CommitTime, true, true, RepDict) -> 
    NodeParts = build_node_parts(UpdatedParts),
    lists:foreach(fun({Node, Partitions}) -> 
        Replicas = dict:fetch(Node, RepDict),
        lists:foreach(fun(R) ->
            case R of
                cache -> 
                   %lager:warning("Commit specula to cache for ~w of ~w", [TxId, Partitions]),
                    ?CACHE_SERV:commit_specula(TxId, Partitions, CommitTime);
                {rep, Server} -> 
                   %lager:warning("Commit specula to ~w for ~w of ~w", [Server, TxId, Partitions]),
                    ?DATA_REPL_SERV:commit_specula(Server, TxId, Partitions, CommitTime);
                Server ->
                   %lager:warning("Repl commit to ~w for ~w of ~w", [Server, TxId, Partitions]),
                    gen_server:cast({global, Server}, {repl_commit, TxId, CommitTime, Partitions})
            end end,  Replicas) end,
            NodeParts).

repl_abort(_, _, false, _, _) ->
    ok;
repl_abort([], _, true, _, _) ->
    ok;
repl_abort(UpdatedParts, TxId, true, false, RepDict) -> 
    NodeParts = build_node_parts(UpdatedParts),
    lists:foreach(fun({Node, Partitions}) ->
        Replicas = dict:fetch(Node, RepDict),
        lists:foreach(fun(R) ->
            case R of
                cache -> ok;
                {rep, Server} ->
                   %lager:warning("Commit specula to ~w for ~w of ~w", [Server, TxId, Partitions]),
                    gen_server:cast({global, Server}, {repl_abort, TxId, Partitions});
                Server ->
                    gen_server:cast({global, Server}, {repl_abort, TxId, Partitions}) 
            end end,  Replicas) end,
            NodeParts);
repl_abort(UpdatedParts, TxId, true, true, RepDict) -> 
    NodeParts = build_node_parts(UpdatedParts),
    lists:foreach(fun({Node, Partitions}) ->
        Replicas = dict:fetch(Node, RepDict),
        lists:foreach(fun(R) ->
            case R of
                cache ->
                   %lager:warning("Abort specula to cache for ~w of ~w", [TxId, Partitions]),
                    ?CACHE_SERV:abort_specula(TxId, Partitions);
                {rep, Server} ->
                   %lager:warning("Abort specula to ~w for ~w of ~w", [Server, TxId, Partitions]),
                    ?DATA_REPL_SERV:abort_specula(Server, TxId, Partitions);
                Server ->
                    gen_server:cast({global, Server}, {repl_abort, TxId, Partitions}) 
            end end,  Replicas) end,
            NodeParts).


ack(TxPart={TxId, _}, ReplState=#repl_state{pending_log=PendingLog}) ->
    case dict:find(TxPart, PendingLog) of
        {ok, {R, 1}} ->
        %ToReply = {prepared, TxId, PrepareTime, remote},
%                    ets:insert(PendingLog, {{TxId, Partition}, {{Prep, Sender,
%                            PrepareTime, WriteSet, RepMode}, ReplFactor-1}}),
           %lager:warning("Got req from ~w, done", [TxPart]),
            {PrepType, Sender, Timestamp, RepMode} = R,
            case PrepType of
                prepared ->
                   %lager:warning("Got enough reply.. Replying ~w for ~w to ~w", [RepMode, TxPart, Sender]),
                    case RepMode of
                        false ->
                            %% In this case, no need to reply
                           %lager:warning("Not really replying.."),
                            ok;
                        _ ->
                            gen_server:cast(Sender, {prepared, TxId, Timestamp, RepMode})
                    end,
                    PendingLog1 = dict:erase(TxPart, PendingLog), 
                    ReplState#repl_state{pending_log=PendingLog1};
                single_commit ->
                   %lager:warning("Single commit of ~w got enough replies, replying to ~w", [TxId, Sender]),
                    PendingLog1 = dict:erase(TxPart, PendingLog), 
                    gen_server:reply(Sender, {ok, {committed, Timestamp}}),
                    ReplState#repl_state{pending_log=PendingLog1}
            end;
        {ok, {R, N}} ->
           %lager:warning("Got req from ~w, ~w more to get", [TxPart, N-1]),
            PendingLog1 = dict:store(TxPart, {R, N-1}, PendingLog), 
            ReplState#repl_state{pending_log=PendingLog1};
        error -> %%The record is appended already, do nothing
            %lager:warning("~w, ~w: prepare repl disappeared!!", [Partition, TxId]),
            ReplState
    end.


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
    

