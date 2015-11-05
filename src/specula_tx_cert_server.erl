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

%% Spawn

-record(state, {partition :: non_neg_integer(),
        tx_id :: txid(),
        prepare_time = 0 :: non_neg_integer(),
        local_updates :: [],
        remote_updates :: [],
        dep_dict :: dict(),
        dep_num :: dict(),
        pending_list=[] :: [txid()],

        specula_data :: cache_id(),
        pending_txs :: cache_id(),
        do_repl :: boolean(),
        is_specula = false :: boolean(),
        committed=0 :: non_neg_integer(),
        aborted=0 :: non_neg_integer(),
        sender :: term(),
        to_ack :: non_neg_integer()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name) ->
    gen_server:start_link({local, Name},
             ?MODULE, [], []).

%%%===================================================================
%%% Internal
%%%===================================================================

init([]) ->
    %PendingMetadata = tx_utilities:open_private_table(pending_metadata),
    PendingTxs = tx_utilities:open_private_table(pending_txs), 
    SpeculaData = tx_utilities:open_private_table(specula_data),
    {ok, #state{pending_txs=PendingTxs, specula_data=SpeculaData,
            dep_dict=dict:new(), dep_num=dict:new(), do_repl=antidote_config:get(do_repl)}}.

handle_call({certify, TxId, LocalUpdates, RemoteUpdates},  Sender, SD0=#state{dep_num=DepNum, dep_dict=DepDict}) ->
    %LocalKeys = lists:map(fun({Node, Ups}) -> {Node, [Key || {Key, _} <- Ups]} end, LocalUpdates),
    %lager:info("Got req: localUpdates ~p, remote updates ~p", [LocalUpdates, RemoteUpdates]),
    {DepNum1, DepDict1} = case dict:find(TxId, DepNum) of
                            {ok, Deps} ->
                                DD = lists:foldl(fun(T, Acc) -> dict:append(T, TxId, Acc) end, 
                                        DepDict, sets:to_list(Deps)),
                                {dict:store(TxId, sets:size(Deps), DepDict), DD};
                            error ->
                                {DepNum, DepDict}
                           end,
    case length(LocalUpdates) of
        0 ->
            clocksi_vnode:prepare(RemoteUpdates, TxId, remote),
            {noreply, SD0#state{tx_id=TxId, to_ack=length(RemoteUpdates), local_updates=LocalUpdates, remote_updates=
                RemoteUpdates, sender=Sender, dep_num=DepNum1, dep_dict=DepDict1}};
        _ ->
            %lager:info("Local updates are ~w", [LocalUpdates]),
            clocksi_vnode:prepare(LocalUpdates, TxId, local),
            {noreply, SD0#state{tx_id=TxId, to_ack=length(LocalUpdates), local_updates=LocalUpdates, remote_updates=
                RemoteUpdates, sender=Sender, dep_num=DepNum1, dep_dict=DepDict1}}
    end;

handle_call({read, Key, TxId, Node}, _Sender, SD0=#state{dep_num=DepNum, specula_data=SpeculaData}) ->
    lager:info("Reading key ~w from ~w", [Key, Node]),
    case ets:lookup(SpeculaData, TxId) of
        [{Key, Value, PendingTxId}] ->
            DepNum1 = case dict:fetch(TxId, DepNum) of
                        {ok, Set} ->
                            dict:store(TxId, sets:add_element(PendingTxId, Set), DepNum);
                        error ->
                            dict:store(TxId, sets:add_element(PendingTxId, sets:new()), DepNum)
                      end,
            {reply, {ok, Value}, SD0#state{dep_num=DepNum1}};
        [] ->
            case Node of 
                {_,_} ->
                    lager:info("Well, from clocksi_vnode"),
                    clocksi_vnode:read_data_item(Node, Key, TxId);
                _ ->
                    data_repl_serv:read(Node, TxId, Key)
            end
    end;

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

handle_cast({prepared, TxId, PrepareTime, local}, 
	    SD0=#state{to_ack=N, tx_id=TxId, local_updates=LocalUpdates, do_repl=DoRepl,
            remote_updates=RemoteUpdates, sender=Sender, pending_list=PendingList,
            prepare_time=OldPrepTime, pending_txs=PendingTxs, specula_data=SpeculaData}) ->
    case N of
        1 -> 
            %lager:info("Got all prepare!"),
            case length(RemoteUpdates) of
                0 ->
                    %lager:info("Trying to commit!!"),
                    CommitTime = max(PrepareTime, OldPrepTime),
                    case PendingList of
                        [] -> 
                            commit_tx(TxId, CommitTime, LocalUpdates, RemoteUpdates, DoRepl,
                                PendingTxs),
                            gen_server:reply(Sender, {ok, {committed, CommitTime}}),
                            {noreply, SD0#state{prepare_time=CommitTime, tx_id={}}};
                        _ ->
                            gen_server:reply(Sender, {ok, {specula_commit, CommitTime}}),
                            ets:insert(PendingTxs, {TxId, {0, CommitTime, LocalUpdates, []}}),
                            add_to_table(LocalUpdates, TxId, SpeculaData),
                            {noreply, SD0#state{prepare_time=CommitTime, tx_id={}, 
                                pending_list=PendingList++[TxId]}}
                    end;
                _ ->
                    MaxPrepTime = max(PrepareTime, OldPrepTime),
                    clocksi_vnode:prepare(RemoteUpdates, TxId, remote),
                    %% Add dependent data into the table
                    gen_server:reply(Sender, {ok, {specula_commit, MaxPrepTime}}),
                    ToAck = length(RemoteUpdates),
                    ets:insert(PendingTxs, {TxId, {ToAck, MaxPrepTime, LocalUpdates, RemoteUpdates}}),
                    add_to_table(LocalUpdates, TxId, SpeculaData),
                    add_to_table(RemoteUpdates, TxId, SpeculaData),
                    {noreply, SD0#state{prepare_time=MaxPrepTime, to_ack=ToAck, local_updates=LocalUpdates,
                        remote_updates=RemoteUpdates, pending_list=PendingList++[TxId]}}
                end;
                %%TODO: Maybe it does not speculatively-commit.
        _ ->
            %lager:info("Not done for ~w", [TxId]),
            MaxPrepTime = max(OldPrepTime, PrepareTime),
            {noreply, SD0#state{to_ack=N-1, prepare_time=MaxPrepTime}}
    end;

handle_cast({prepared, _, _, local}, 
	    SD0) ->
    %lager:info("Received prepare of previous prepared txn! ~w", [OtherTxId]),
    {noreply, SD0};

handle_cast({abort, TxId, local}, SD0=#state{tx_id=TxId, local_updates=LocalUpdates, 
        do_repl=DoRepl, sender=Sender, pending_txs=PendingTxs}) ->
    lager:info("Received abort for ~w", [TxId]),
    abort_tx(TxId, LocalUpdates, [], DoRepl, PendingTxs),
    gen_server:reply(Sender, {aborted, TxId}),
    {noreply, SD0#state{tx_id={}}};
handle_cast({abort, _, local}, SD0) ->
    %lager:info("Received abort for previous txn ~w", [OtherTxId]),
    {noreply, SD0};

handle_cast({prepared, TxId, PrepareTime, remote}, 
	    SD0=#state{sender=Sender, tx_id=TxId, do_repl=DoRepl, 
            prepare_time=MaxPrepTime, remote_updates=RemoteUpdates, pending_txs=PendingTxs, 
            pending_list=PendingList, to_ack=N, local_updates=LocalUpdates}) ->
    %lager:info("Received remote prepare"),
    case N of
        1 ->
            CommitTime = max(MaxPrepTime, PrepareTime),
            case PendingList of
                [] -> 
                    commit_tx(TxId, CommitTime, LocalUpdates, RemoteUpdates, DoRepl, PendingTxs),
                    gen_server:reply(Sender, {ok, {committed, CommitTime}}),
                    {noreply, SD0#state{prepare_time=CommitTime, tx_id={}}};
                _ ->
                    %%This can not happen!!!
                    lager:error("In prepare remote: something is wrong!"),
                    {noreply, SD0}
            end;
        N ->
            {noreply, SD0#state{to_ack=N-1, prepare_time=max(MaxPrepTime, PrepareTime)}}
    end;
handle_cast({prepared, PendingTxId, PendingPT, remote}, 
	    SD0=#state{pending_list=PendingList, do_repl=DoRepl, prepare_time=PrepareTime, pending_txs=PendingTxs,
             committed=Committed, dep_dict=DepDict, dep_num=DepNum, specula_data=SpeculaData}) ->
    case ets:lookup(PendingTxs, PendingTxId) of
        [{PendingTxId, {1, PendingMaxPT, LocalUpdates, RemoteUpdates}}] ->
            MaxPT = max(PendingMaxPT, PendingPT),
            %% To make sure that a transaction commits with timestamp larger than its previous txn
            MaxPT2 = max(PrepareTime+1, MaxPT),
            case hd(PendingList) of
                PendingTxId ->
                    commit_tx(PendingTxId, MaxPT2, LocalUpdates, RemoteUpdates, DoRepl, PendingTxs),
                    {NewPendingList, NewDepDict, NewDepNum, NewMaxPT} = deal_dependency(PendingTxId,
                                MaxPT, tl(PendingList), DepDict, DepNum, MaxPT2, DoRepl, PendingTxs, SpeculaData),
                    {noreply, SD0#state{committed=Committed+1, pending_list=NewPendingList,
                            dep_dict=NewDepDict, dep_num=NewDepNum, prepare_time=NewMaxPT}};
                _ ->
                    ets:insert(PendingTxs, {PendingTxId, {0, PendingMaxPT, LocalUpdates, RemoteUpdates}}),
                    {noreply, SD0}
            end;
        [{PendingTxId, {N, PendingMaxPT, LocalParts, RemoteParts}}] ->
            ets:insert(PendingTxs, {PendingTxId, {N-1, max(PendingMaxPT, PendingPT), 
                LocalParts, RemoteParts}}),
            {noreply, SD0};
        [] ->
            {noreply, SD0}
    end;

handle_cast({abort, TxId, remote}, 
	    SD0=#state{remote_updates=RemoteUpdates, sender=Sender, pending_txs=PendingTxs, 
            do_repl=DoRepl, tx_id=TxId, local_updates=LocalUpdates}) ->
    abort_tx(TxId, LocalUpdates, RemoteUpdates, DoRepl, PendingTxs),
    gen_server:reply(Sender, {aborted, TxId}),
    {noreply, SD0#state{tx_id={}}};

handle_cast({abort, PendingTxId, remote}, 
	    SD0=#state{pending_list=PendingList, dep_dict=DepDict, specula_data=SpeculaData,
            do_repl=DoRepl, dep_num=DepNum, pending_txs=PendingTxs}) ->
    case start_from(PendingTxId, PendingList) of
        [] ->
            {noreply, SD0};
        L ->
            {DD, DN} = abort_specula_list(L, DepDict, DepNum, DoRepl, PendingTxs, SpeculaData),
            {noreply, SD0#state{dep_dict=DD, dep_num=DN}}
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

%%%%%%%%%%%%%%%%%%%%%
commit_tx(TxId, CommitTime, LocalUpdates, RemoteUpdates, DoRepl, PendingTxs) ->
    LocalParts = [Part || {Part, _} <- LocalUpdates],
    RemoteParts = [Part || {Part, _} <- RemoteUpdates],
    true = ets:delete(PendingTxs, TxId),
    clocksi_vnode:commit(LocalParts, TxId, CommitTime),
    clocksi_vnode:commit(RemoteParts, TxId, CommitTime),
    repl_fsm:repl_commit(LocalParts, TxId, CommitTime, DoRepl),
    repl_fsm:repl_commit(RemoteParts, TxId, CommitTime, DoRepl).

commit_specula_tx(TxId, CommitTime, LocalUpdates, RemoteUpdates, DoRepl, PendingTxs, SpeculaData) ->
    commit_tx(TxId, CommitTime, LocalUpdates, RemoteUpdates, DoRepl, PendingTxs),
    remove_from_table(LocalUpdates, TxId, SpeculaData),
    remove_from_table(RemoteUpdates, TxId, SpeculaData).

abort_specula_tx(TxId, LocalUpdates, RemoteUpdates, DoRepl, PendingTxs, SpeculaData) ->
    abort_tx(TxId, LocalUpdates, RemoteUpdates, DoRepl, PendingTxs),
    remove_from_table(LocalUpdates, TxId, SpeculaData),
    remove_from_table(RemoteUpdates, TxId, SpeculaData).

abort_tx(TxId, LocalUpdates, RemoteUpdates, DoRepl, PendingTxs) ->
    LocalParts = [Part || {Part, _} <- LocalUpdates],
    RemoteParts = [Part || {Part, _} <- RemoteUpdates],
    true = ets:delete(PendingTxs, TxId),
    clocksi_vnode:abort(LocalParts, TxId),
    clocksi_vnode:commit(RemoteParts, TxId),
    repl_fsm:repl_abort(LocalParts, TxId, DoRepl),
    repl_fsm:repl_abort(RemoteParts, TxId, DoRepl).

add_to_table([], _, _) ->
    ok;
add_to_table([{_, Updates}|Rest], TxId, SpeculaData) ->
    lists:foreach(fun({K, V}) -> ets:insert(SpeculaData, {K, V, TxId}) end,
        Updates),
    add_to_table(Rest, TxId, SpeculaData).

remove_from_table([], _, _) ->
    ok;
remove_from_table([{_, Updates}|Rest], TxId, SpeculaData) ->
    lists:foreach(fun({K, V}) -> 
                    case ets:lookup(SpeculaData, K) of 
                        [{K, V, TxId}] ->
                            ets:delete(SpeculaData, K);
                        _ ->
                            ok
                    end end, Updates),
    remove_from_table(Rest, TxId, SpeculaData).
   

deal_dependency([], [], [], DepDict, DepNum, MaxPT, _, _, _) ->
    {DepDict, DepNum, [], MaxPT};
deal_dependency(DependentTxId, DependentCT, PendingList, DepDict, DepNum, MaxPT, 
            DoRepl, PendingTxs, SpeculaData) ->
    %%Check all txns depend on itself
    {DepDict1, DepNum1, ToAbort} = case dict:find(DependentTxId, DepDict) of 
                                             %%DepList is an ordered list that the last element is 
                                             %%the oldest transaction.
                                             {ok, DepList} ->
                                                solve_read_dependency(DependentTxId, DependentCT, DepList, 
                                                    DepDict, DepNum);
                                             error ->
                                                %% No txn depends on itself..
                                                {DepDict, DepNum, ignore}
                                          end,
    DepDict2 = dict:erase(DependentTxId, DepDict1),
    {DepDict3, DepNum2, PendingList1}
         = case ToAbort of
                ignore ->
                    {DepDict2, DepNum1, PendingList};
                _ ->
                    L = start_from(ToAbort, PendingList),
                    {DD, DN} = abort_specula_list(L, DepDict2, DepNum1, DoRepl, PendingTxs, SpeculaData),
                    {DD, DN, lists:sublist(PendingList, length(PendingList)-length(L))}
           end,
    %% Try to commit the next txn.
    [H|Rest] = PendingList1,
    case dict:find(H, DepNum2) of
        error ->
            %% The next txn has no dependency
            case ets:lookup(PendingTxs, H) of
                %% The next txn has got all prepare request
                [{H, {0, PendingMaxPT, LocalParts, RemoteParts}}] ->
                    MaxPT2 = max(PendingMaxPT, MaxPT+1),
                    commit_specula_tx(H, MaxPT2, LocalParts, RemoteParts, DoRepl, PendingTxs, SpeculaData),
                    deal_dependency(H, DependentCT, Rest, DepDict3, DepNum2, 
                            MaxPT2, DoRepl, PendingTxs, SpeculaData);
                %% The next txn can not be committed.. Then just return
                _ ->
                    {DepDict3, DepNum2, PendingList1, MaxPT}
            end;
        R ->
            %% The next txn still depends on some txn.. Should not happen!!! 
            lager:error("The next txn still depends on some other txns.. This is wrong! ~w", [R])        
    end.

abort_specula_list([], DepDict, DepNum, _, _, _) ->
    {DepDict, DepNum};
abort_specula_list([H|T], DepDict, DepNum, DoRepl, PendingTxs, SpeculaData) ->
    [{H, {_, _, LocalUpdates, RemoteUpdates}}] = ets:lookup(PendingTxs, H), 
    abort_specula_tx(H, LocalUpdates, RemoteUpdates, DoRepl, PendingTxs, SpeculaData),
    abort_specula_list(T, dict:erase(H, DepDict), dict:erase(H, DepNum), DoRepl, PendingTxs, SpeculaData). 

%% Deal with reads that depend on this txn
solve_read_dependency(_, _, [], DepDict, DepNum) ->
    {DepDict, DepNum, ignore};
solve_read_dependency(TxId, CommitTime, [T|Rest], DepDict, DepNum) ->
    case T#tx_id.snapshot_time >= CommitTime of
        true ->
            case dict:find(T, DepNum) of
                error ->
                    solve_read_dependency(TxId, CommitTime, [Rest], DepDict, DepNum); 
                1 ->
                    solve_read_dependency(TxId, CommitTime, [Rest], DepDict, dict:erase(T, DepNum)); 
                N ->
                    solve_read_dependency(TxId, CommitTime, [Rest], DepDict, dict:store(T, N-1, DepNum)) 
            end;
        false ->
            %%Cascading abort
            {DepDict, DepNum, T}
    end.
    
start_from(_, []) ->
    [];
start_from(H, [H|T])->
    [H|T];
start_from(H, [_|T]) ->
    start_from(H, T).
