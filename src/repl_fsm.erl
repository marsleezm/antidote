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
         repl_abort/3,
         repl_commit/4,
         quorum_replicate/7,
         chain_replicate/6,
         send_after/3]).

%% Spawn

-record(state, {
        pending_log :: cache_id(),
        log_size :: non_neg_integer(),
        replicas :: [atom()],
        mode :: atom(),
        repl_factor :: non_neg_integer(),
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

repl_prepare(Partition, TxId, Type, LogContent) ->
    gen_server:cast({global, get_repl_name()}, {repl_prepare, Partition, Type, TxId, LogContent}).

repl_abort(_, _, false) ->
    ok;
repl_abort([], _, true) ->
    ok;
repl_abort(UpdatedParts, TxId, true) ->
    gen_server:cast({global, get_repl_name()}, {repl_abort, TxId, UpdatedParts}).

repl_commit(_, _, _, false) ->
    ok;
repl_commit([], _, _, true) ->
    ok;
repl_commit(UpdatedParts, TxId, CommitTime, true) ->
    gen_server:cast({global, get_repl_name()}, {repl_commit, TxId, UpdatedParts, CommitTime}).

quorum_replicate(Replicas, Type, TxId, Partition, WriteSet, TimeStamp, MyName) ->
    lists:foreach(fun(Replica) ->
            gen_server:cast({global, Replica}, {repl_prepare, 
                Type, TxId, Partition, WriteSet, TimeStamp, MyName})
            end, Replicas).

chain_replicate(_Replicas, _Type, _TxId, _WriteSet, _TimeStamp, _ToReply) ->
    %% Not implemented yet.
    ok.

%%%===================================================================
%%% Internal
%%%===================================================================


init([Name]) ->
    hash_fun:build_rev_replicas(),
    [{_, Replicas}] = ets:lookup(meta_info, node()), 
    PendingLog = tx_utilities:open_private_table(pending_log),
    {ok, #state{replicas=Replicas, mode=quorum, repl_factor=length(Replicas),
                pending_log=PendingLog, my_name=Name}}.

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

handle_cast({repl_prepare, Partition, Type, TxId, LogContent}, 
	    SD0=#state{replicas=Replicas, pending_log=PendingLog, 
            my_name=MyName, mode=Mode, repl_factor=ReplFactor}) ->
    %lager:info("Repl prepared ~w", [TxId]),
    {TxId, Sender, ToReply, WriteSet, TimeStamp} = LogContent,        
    case Mode of
        quorum ->
     %       lager:info("I am ~w, txid is {~w, ~w}", [MyName, TxId, Partition]),
            ets:insert(PendingLog, {{TxId, Partition}, {{Type, Sender, ToReply, WriteSet}, ReplFactor}}),
            quorum_replicate(Replicas, Type, TxId, Partition, WriteSet, TimeStamp, MyName);
        chain ->
            chain_replicate(Replicas, Type, TxId, WriteSet, TimeStamp, {Sender, ToReply})
    end,
    {noreply, SD0};

handle_cast({repl_commit, TxId, UpdatedParts, CommitTime}, 
	    SD0) ->
    AllReplicas = get_all_replicas(UpdatedParts),
    %lager:info("Replicas are ~w", [AllReplicas]),
    lists:foreach(fun({Node, Partitions}) -> 
        [{_, Replicas}] = ets:lookup(meta_info, Node),
        lists:foreach(fun(R) ->
            %lager:info("Sending repl commit to ~w", [R]),
            gen_server:cast({global, R}, {repl_commit, TxId, CommitTime, Partitions}) end,
        Replicas) end,
            AllReplicas),
    {noreply, SD0};

handle_cast({repl_abort, TxId, UpdatedParts}, 
	    SD0) ->
    AllReplicas = get_all_replicas(UpdatedParts),
    %lager:info("Replicas are ~w", [AllReplicas]),
    lists:foreach(fun({Node, Partitions}) -> 
        [{_, Replicas}] = ets:lookup(meta_info, Node),
        lists:foreach(fun(R) ->
            %lager:info("Sending repl commit to ~w", [R]),
            gen_server:cast({global, R}, {repl_abort, TxId, Partitions}) end,
        Replicas) end,
            AllReplicas),
    {noreply, SD0};

handle_cast({ack, Partition, TxId}, SD0=#state{pending_log=PendingLog}) ->
    case ets:lookup(PendingLog, {TxId, Partition}) of
        [{{TxId, Partition}, {R, 1}}] ->
            {Type, Sender, ToReply, _} = R,
            true = ets:delete(PendingLog, {TxId, Partition}),
            case ToReply of
                false ->
                    ok;
                _ ->
                    case Type of
                        prepare ->
                            gen_server:cast(Sender, ToReply);
                        single_commit ->
                            Sender ! ToReply
                    end
            end;
        [{{TxId, Partition}, {R, N}}] ->
            %lager:info("Accumulating"),
            ets:insert(PendingLog, {{TxId, Partition}, {R, N-1}});
        [] -> %%The record is appended already, do nothing
            lager:warning("Prepare repl has disappeared!!!!"),
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
    %lager:info("Sending info after ~w", [Delay]),
    gen_server:cast(To, Msg).

get_repl_name() ->
    list_to_atom("repl"++atom_to_list(node())).

get_all_replicas(Parts) ->
    D = lists:foldl(fun({Partition, Node}, Acc) ->
                   case dict:find(Node, Acc) of 
                        {ok, P} ->
                            dict:store(Node, [Partition|P], Acc);
                        error ->
                            dict:store(Node, [Partition], Acc)
                   end end,
                    dict:new(), Parts),
    dict:to_list(D).

