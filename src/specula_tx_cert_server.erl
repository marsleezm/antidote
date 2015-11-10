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
-define(CLOCKSI_VNODE, mock_partition_fsm).
-define(REPL_FSM, mock_partition_fsm).
-else.
-define(CLOCKSI_VNODE, clocksi_vnode).
-define(REPL_FSM, repl_fsm).
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
        last_commit_ts = 0 :: non_neg_integer(),
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
        speculated=0 :: non_neg_integer(),
        sender :: term(),
        to_ack :: non_neg_integer()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name) ->
    gen_server:start_link({global, Name},
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

handle_call({get_stat}, _Sender, SD0=#state{aborted=Aborted, committed=Committed}) ->
    lager:info("Num of aborted is ~w, Num of committed is ~w", [Aborted, Committed]),
    {reply, {Aborted, Committed}, SD0};

handle_call({get_internal_data, Type, Param}, _Sender, SD0=#state{pending_list=PendingList, 
        pending_txs=PendingTxs, specula_data=SpeculaData, dep_dict=DepDict, dep_num=DepNum})->
    case Type of
        pending_list ->
            {reply, PendingList, SD0};
        pending_txs ->
            case ets:lookup(PendingTxs, Param) of
                [] ->
                    {reply, [], SD0};
                [Content] ->
                    {reply, Content, SD0}
            end;
        specula_data ->
            case ets:lookup(SpeculaData, Param) of
                [] ->
                    {reply, [], SD0};
                [Content] ->
                    {reply, Content, SD0}
            end;
        all_specula_data ->
            {reply, ets:to_list(SpeculaData), SD0};
        dependency ->
            {Reader, Creator} = Param,
            {reply, {dict:find(Reader, DepNum), dict:find(Creator, DepDict)}, SD0};
        all_denendency ->
            {reply, {dict:to_list(DepDict), dict:to_list(DepNum)}, SD0}
    end;

handle_call({certify, TxId, LocalUpdates, RemoteUpdates},  Sender, SD0=#state{dep_num=DepNum, 
            dep_dict=DepDict}) ->
    %lager:info("TxId is ~w", [TxId]),
    %LocalKeys = lists:map(fun({Node, Ups}) -> {Node, [Key || {Key, _} <- Ups]} end, LocalUpdates),
    %lager:info("Got req: localUpdates ~p, remote updates ~p", [LocalUpdates, RemoteUpdates]),
    %% If there was a legacy ongoing transaction.
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
            ?CLOCKSI_VNODE:prepare(RemoteUpdates, TxId, remote),
            {noreply, SD0#state{tx_id=TxId, to_ack=length(RemoteUpdates), local_updates=LocalUpdates, remote_updates=
                RemoteUpdates, sender=Sender, dep_num=DepNum1, dep_dict=DepDict1}};
        _ ->
            %lager:info("Local updates are ~w", [LocalUpdates]),
            ?CLOCKSI_VNODE:prepare(LocalUpdates, TxId, local),
            {noreply, SD0#state{tx_id=TxId, to_ack=length(LocalUpdates), local_updates=LocalUpdates, remote_updates=
                RemoteUpdates, sender=Sender, dep_num=DepNum1, dep_dict=DepDict1}}
    end;

handle_call({read, Key, TxId, Node}, _Sender, SD0=#state{dep_num=DepNum, specula_data=SpeculaData}) ->
    lager:info("~w Reading key ~w from ~w", [TxId, Key, Node]),
    case ets:lookup(SpeculaData, Key) of
        [{Key, Value, PendingTxId}] ->
            lager:info("Has Value ~w", [Value]),
            DepNum1 = case dict:find(TxId, DepNum) of
                        {ok, Set} ->
                            dict:store(TxId, sets:add_element(PendingTxId, Set), DepNum);
                        error ->
                            dict:store(TxId, sets:add_element(PendingTxId, sets:new()), DepNum)
                      end,
            {reply, {ok, Value}, SD0#state{dep_num=DepNum1}};
        [] ->
            %lager:info("No Value"),
            case Node of 
                {_,_} ->
                    %lager:info("Well, from clocksi_vnode"),
                    ?CLOCKSI_VNODE:read_data_item(Node, Key, TxId);
                _ ->
                    data_repl_serv:read(Node, TxId, Key)
            end
    end;

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

handle_cast({prepared, TxId, PrepareTime, local}, 
	    SD0=#state{to_ack=N, tx_id=TxId, local_updates=LocalUpdates, do_repl=DoRepl, last_commit_ts=LastCommitTS,
            remote_updates=RemoteUpdates, sender=Sender, pending_list=PendingList, speculated=Speculated,
            prepare_time=OldPrepTime, pending_txs=PendingTxs, specula_data=SpeculaData}) ->
    case N of
        1 -> 
            %lager:info("Got all prepare!"),
            NewMaxPrep = max(PrepareTime, OldPrepTime),
            ToAck = length(RemoteUpdates),
            case ToAck of
                0 ->
                    case PendingList of
                        [] -> 
                            CommitTime = max(LastCommitTS+1, NewMaxPrep),
                            %lager:info("Real committing"),
                            commit_tx(TxId, CommitTime, LocalUpdates, RemoteUpdates, DoRepl,
                                PendingTxs),
                            gen_server:reply(Sender, {ok, {committed, CommitTime}}),
                            {noreply, SD0#state{last_commit_ts=CommitTime, tx_id={}}};
                        _ ->
                            %lager:info("Specula committing "),
                            ets:insert(PendingTxs, {TxId, {0, NewMaxPrep, LocalUpdates, RemoteUpdates}}),
                            gen_server:reply(Sender, {ok, {specula_commit, NewMaxPrep}}),
                            add_to_table(LocalUpdates, TxId, SpeculaData),
                            %lager:info("Pending list is ~w, TxId is ~w", [PendingList, TxId]),
                            {noreply, SD0#state{speculated=Speculated+1, 
                                pending_list=PendingList++[TxId]}}
                    end;
                _ ->
                    ?CLOCKSI_VNODE:prepare(RemoteUpdates, TxId, remote),
                    %% Add dependent data into the table
                    gen_server:reply(Sender, {ok, {specula_commit, NewMaxPrep}}),
                    ets:insert(PendingTxs, {TxId, {ToAck, NewMaxPrep, LocalUpdates, RemoteUpdates}}),
                    add_to_table(LocalUpdates, TxId, SpeculaData),
                    add_to_table(RemoteUpdates, TxId, SpeculaData),
                    %lager:info("Pending list is ~w, TxId is ~w", [PendingList, TxId]),
                    %lager:info("Inserting ~w, ~w, ~p, ~p", [ToAck, MaxPrepTime, LocalUpdates, RemoteUpdates]),
                    {noreply, SD0#state{to_ack=ToAck, pending_list=PendingList++[TxId]}}
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
    %lager:info("Received abort for ~w", [TxId]),
    abort_tx(TxId, LocalUpdates, [], DoRepl, PendingTxs),
    gen_server:reply(Sender, {aborted, TxId}),
    {noreply, SD0#state{tx_id=[]}};
handle_cast({abort, _, local}, SD0) ->
    %lager:info("Received abort for previous txn ~w", [OtherTxId]),
    {noreply, SD0};

%% If the last transaction gets a remote prepare right after the reply is sent back to the client and before the
%% next transaction arrives.. We will update the state in place, instead of always going to the ets table.
%% But if we can not commit the transaction immediately, we have to store the updated state to ets table.
handle_cast({prepared, TxId, PrepareTime, remote}, 
	    SD0=#state{sender=Sender, tx_id=TxId, do_repl=DoRepl, specula_data=SpeculaData, 
            prepare_time=OldMaxPrep, last_commit_ts=LastCommitTS, remote_updates=RemoteUpdates, 
            pending_txs=PendingTxs, committed=Committed, 
            pending_list=PendingList, to_ack=N, local_updates=LocalUpdates}) ->
    %lager:info("Received remote prepare"),
    NewMaxPrep = max(OldMaxPrep, PrepareTime),
    case N of
        1 ->
            case PendingList of
                [] -> 
                    CommitTime = max(NewMaxPrep, LastCommitTS+1),
                    lager:info("Not possible! Trying to commit tx ~w", [TxId]),
                    commit_specula_tx(TxId, CommitTime, LocalUpdates, RemoteUpdates, 
                        DoRepl, PendingTxs, SpeculaData),
                    gen_server:reply(Sender, {ok, {committed, CommitTime}}),
                    {noreply, SD0#state{last_commit_ts=CommitTime, tx_id=[], committed=Committed+1}};
                [TxId] -> 
                    %lager:info("Trying to commit tx ~w", [TxId]),
                    CommitTime = max(NewMaxPrep, LastCommitTS+1),
                    commit_specula_tx(TxId, CommitTime, LocalUpdates, RemoteUpdates, 
                        DoRepl, PendingTxs, SpeculaData),
                    %gen_server:reply(Sender, {ok, {committed, CommitTime}}),
                    {noreply, SD0#state{last_commit_ts=CommitTime, tx_id=[], pending_list=[], committed=Committed+1}};
                _ ->
                    %%This can not happen!!!
                    ets:insert(PendingTxs, {TxId, {0, NewMaxPrep, LocalUpdates, RemoteUpdates}}),
                    {noreply, SD0#state{to_ack=0, prepare_time=NewMaxPrep}}
            end;
        N ->
            ets:insert(PendingTxs, {TxId, {N-1, NewMaxPrep, LocalUpdates, RemoteUpdates}}),
            {noreply, SD0#state{to_ack=N-1, prepare_time=NewMaxPrep}}
    end;
handle_cast({prepared, PendingTxId, PendingPT, remote}, 
	    SD0=#state{pending_list=PendingList, do_repl=DoRepl, last_commit_ts=LastCommitTS, pending_txs=PendingTxs,
             committed=Committed, dep_dict=DepDict, dep_num=DepNum, specula_data=SpeculaData}) ->
    case ets:lookup(PendingTxs, PendingTxId) of
        [{PendingTxId, {1, PendingMaxPT, LocalUpdates, RemoteUpdates}}] ->
            MaxPT = max(PendingMaxPT, PendingPT),
            %% To make sure that a transaction commits with timestamp larger than its previous txn
            case hd(PendingList) of
                PendingTxId ->
                    CommitTime = max(LastCommitTS+1, MaxPT),
                    commit_specula_tx(PendingTxId, CommitTime, LocalUpdates, RemoteUpdates, DoRepl,
                             PendingTxs, SpeculaData),
                    {NewDepDict, NewDepNum, NewPendingList, NewMaxPT} = deal_dependency(PendingTxId,
                                CommitTime, tl(PendingList), DepDict, DepNum, DoRepl, PendingTxs, SpeculaData),
                    case NewPendingList of
                        [] ->
                            {noreply, SD0#state{committed=Committed+length(PendingList)-length(NewPendingList), 
                                pending_list=[], tx_id=[], dep_dict=NewDepDict, dep_num=NewDepNum, 
                                last_commit_ts=NewMaxPT}};
                        _ ->
                            {noreply, SD0#state{committed=Committed+length(PendingList)-length(NewPendingList), 
                                pending_list=NewPendingList, dep_dict=NewDepDict, dep_num=NewDepNum, 
                                last_commit_ts=NewMaxPT}}
                    end;
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
	    SD0=#state{remote_updates=RemoteUpdates, pending_txs=PendingTxs, pending_list=PendingList, 
            do_repl=DoRepl, tx_id=TxId, local_updates=LocalUpdates, aborted=Aborted}) ->
    abort_tx(TxId, LocalUpdates, RemoteUpdates, DoRepl, PendingTxs),
    %gen_server:reply(Sender, {aborted, TxId}),
    {L, _} = lists:split(length(PendingList) - 1, PendingList),
    {noreply, SD0#state{tx_id=[], aborted=Aborted+1, 
                pending_list=L}};

handle_cast({abort, PendingTxId, remote}, 
	    SD0=#state{pending_list=PendingList, dep_dict=DepDict, specula_data=SpeculaData,
            do_repl=DoRepl, dep_num=DepNum, pending_txs=PendingTxs, aborted=Aborted}) ->
    case start_from(PendingTxId, PendingList) of
        [] ->
            %lager:info("Not aborting anything"),
            {noreply, SD0};
        L ->
            %lager:info("Pending list is ~w, PendingTxId is ~w, List is ~w", [PendingList, PendingTxId, L]),
            {DD, DN} = abort_specula_list(L, DepDict, DepNum, DoRepl, PendingTxs, SpeculaData),
            {noreply, SD0#state{dep_dict=DD, dep_num=DN, tx_id=[], 
                pending_list=lists:sublist(PendingList, length(PendingList)-length(L)),
                aborted=Aborted+length(PendingList)-length(L)}}
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
    %lager:info("Deleting for ~w", [TxId]),
    ?CLOCKSI_VNODE:commit(LocalParts, TxId, CommitTime),
    ?CLOCKSI_VNODE:commit(RemoteParts, TxId, CommitTime),
    ?REPL_FSM:repl_commit(LocalParts, TxId, CommitTime, DoRepl),
    ?REPL_FSM:repl_commit(RemoteParts, TxId, CommitTime, DoRepl).

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
    %lager:info("Deleting ~w", [TxId]),
    ?CLOCKSI_VNODE:abort(LocalParts, TxId),
    ?CLOCKSI_VNODE:abort(RemoteParts, TxId),
    ?REPL_FSM:repl_abort(LocalParts, TxId, DoRepl),
    ?REPL_FSM:repl_abort(RemoteParts, TxId, DoRepl).

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

deal_dependency([], _, [], DepDict, DepNum, MaxPT, _, _) ->
    {DepDict, DepNum, [], MaxPT};
deal_dependency(DependentTxId, CommitTime, PendingList, DepDict, DepNum, 
                        DoRepl, PendingTxs, SpeculaData) ->
    %%Check all txns depend on itself
    {DepNum1, ToAbort} = case dict:find(DependentTxId, DepDict) of 
                                             %%DepList is an ordered list that the last element is 
                                             %%the oldest transaction.
                                             {ok, DepList} ->
                                                solve_read_dependency(DependentTxId, CommitTime, DepList, 
                                                    DepNum);
                                             error ->
                                                %% No txn depends on itself..
                                                {DepNum, ignore}
                                          end,
    DepDict1 = dict:erase(DependentTxId, DepDict),
    {DepDict2, DepNum2, PendingList1}
         = case ToAbort of
                ignore ->
                    %lager:info("Nothing to abort"),
                    {DepDict1, DepNum1, PendingList};
                _ ->
                    L = start_from(ToAbort, PendingList),
                    {DD, DN} = abort_specula_list(L, DepDict1, DepNum1, DoRepl, PendingTxs, SpeculaData),
                    {DD, DN, lists:sublist(PendingList, length(PendingList)-length(L))}
           end,
    %% Try to commit the next txn.
    case PendingList1 of
        [H|Rest] ->
            case dict:find(H, DepNum2) of
                error ->
                    %% The next txn has no dependency
                    case ets:lookup(PendingTxs, H) of
                        %% The next txn has got all prepare request
                        [{H, {0, PendingMaxPT, LocalParts, RemoteParts}}] ->
                            %io:format(user, "Committing ~w, CommitTime is ~w, Pending PT is ~w ~n", 
                            %        [H, CommitTime, PendingMaxPT]),
                            MaxPT2 = max(PendingMaxPT, CommitTime+1),
                            commit_specula_tx(H, MaxPT2, LocalParts, RemoteParts, DoRepl, PendingTxs, SpeculaData),
                            deal_dependency(H, MaxPT2, Rest, DepDict2, DepNum2, 
                                    DoRepl, PendingTxs, SpeculaData);
                        %% The next txn can not be committed.. Then just return
                        _ ->
                            %lager:info("Next txn not prepared!, pending list 1 is ~w", [PendingList1]),
                            {DepDict2, DepNum2, PendingList1, CommitTime}
                    end;
                R ->
                    %% The next txn still depends on some txn.. Should not happen!!! 
                    lager:error("The next txn still depends on some other txns.. This is wrong! ~w", [R]), 
                    error
            end;
        [] ->
            {DepDict2, DepNum2, [], CommitTime}
    end.

abort_specula_list([], DepDict, DepNum, _, _, _) ->
    {DepDict, DepNum};
abort_specula_list([H|T], DepDict, DepNum, DoRepl, PendingTxs, SpeculaData) ->
    %lager:info("Trying to abort ~w", [H]),
    [{H, {_, _, LocalUpdates, RemoteUpdates}}] = ets:lookup(PendingTxs, H), 
    abort_specula_tx(H, LocalUpdates, RemoteUpdates, DoRepl, PendingTxs, SpeculaData),
    abort_specula_list(T, dict:erase(H, DepDict), dict:erase(H, DepNum), DoRepl, PendingTxs, SpeculaData). 

%% Deal with reads that depend on this txn
solve_read_dependency(_, _, [], DepNum) ->
    {DepNum, ignore};
solve_read_dependency(TxId, CommitTime, [T|Rest], DepNum) ->
    %lager:info("Resolving read dependency: s ~w, c ~w", [T#tx_id.snapshot_time, CommitTime]),
    case T#tx_id.snapshot_time >= CommitTime of
        true ->
            case dict:find(T, DepNum) of
                error ->
                    solve_read_dependency(TxId, CommitTime, Rest, DepNum); 
                {ok, 1} ->
                    solve_read_dependency(TxId, CommitTime, Rest, dict:erase(T, DepNum)); 
                {ok, N} ->
                    solve_read_dependency(TxId, CommitTime, Rest, dict:store(T, N-1, DepNum)) 
            end;
        false ->
            %io:format(user, "Read invalid ~w ~n", [T]),
            %%Cascading abort
            {DepNum, T}
    end.
    
start_from(_, []) ->
    [];
start_from(H, [H|T])->
    [H|T];
start_from(H, [_|T]) ->
    start_from(H, T).

-ifdef(TEST).

add_to_table_test() ->
    MyTable = ets:new(whatever, [private, set]),
    Updates= [{node1, [{key1, 1}, {key2, 2}]}, {node2, [{key3, 2}]}],
    TxId1 = tx_utilities:create_transaction_record(0),
    add_to_table(Updates, TxId1, MyTable),
    ?assertEqual([{key1, 1, TxId1}], ets:lookup(MyTable, key1)),
    ?assertEqual([{key2, 2, TxId1}], ets:lookup(MyTable, key2)),
    ?assertEqual([{key3, 2, TxId1}], ets:lookup(MyTable, key3)),
    TxId2 = tx_utilities:create_transaction_record(0),
    Updates2 =  [{node1, [{key1, 2}]}, {node3, [{key4, 4}]}],
    add_to_table(Updates2, TxId2, MyTable),
    ?assertEqual([{key1, 2, TxId2}], ets:lookup(MyTable, key1)),
    ?assertEqual([{key2, 2, TxId1}], ets:lookup(MyTable, key2)),
    ?assertEqual([{key3, 2, TxId1}], ets:lookup(MyTable, key3)),
    ?assertEqual([{key4, 4, TxId2}], ets:lookup(MyTable, key4)),
    true = ets:delete(MyTable).

remove_from_table_test() ->
    MyTable = ets:new(whatever, [private, set]),
    Updates= [{node1, [{key1, 1}, {key2, 2}]}, {node2, [{key3, 2}]}],
    TxId1 = tx_utilities:create_transaction_record(0),
    add_to_table(Updates, TxId1, MyTable),
    TxId2 = tx_utilities:create_transaction_record(0),
    remove_from_table(Updates, TxId2, MyTable),
    ?assertEqual([{key1, 1, TxId1}], ets:lookup(MyTable, key1)),
    ?assertEqual([{key2, 2, TxId1}], ets:lookup(MyTable, key2)),
    ?assertEqual([{key3, 2, TxId1}], ets:lookup(MyTable, key3)),
    remove_from_table(Updates, TxId1, MyTable),
    ?assertEqual([], ets:lookup(MyTable, key1)),
    ?assertEqual([], ets:lookup(MyTable, key2)),
    ?assertEqual([], ets:lookup(MyTable, key3)),
    true = ets:delete(MyTable).

start_from_test() ->
    L = [1,2,3,4,5,6],
    ?assertEqual([1,2,3,4,5,6], start_from(1, L)),
    ?assertEqual([3,4,5,6], start_from(3, L)),
    ?assertEqual([6], start_from(6, L)),
    ?assertEqual([], start_from(7, L)).

abort_tx_test() ->
    MyTable = ets:new(whatever, [private, set]),
    mock_partition_fsm:start_link(),
    LocalUps=[{lp1, [{key1, 1}, {key2, 2}]}, {lp2, [{key3, 3}]}],
    RemoteUps=[{rp1, [{key4, 1}, {key5, 2}]}, {rp2, [{key6, 3}]}],
    LocalParts = [lp1, lp2],
    RemoteParts = [rp1, rp2],
    TxId1 = tx_utilities:create_transaction_record(0),
    ets:insert(MyTable, {TxId1, whatever}),
    abort_tx(TxId1, LocalUps, RemoteUps, true, MyTable),
    ?assertEqual([], ets:lookup(MyTable, TxId1)),
    ?assertEqual(true, mock_partition_fsm:if_applied(LocalParts, TxId1, nothing, abort)),
    ?assertEqual(true, mock_partition_fsm:if_applied(RemoteParts, TxId1, nothing, abort)),
    ?assertEqual(true, mock_partition_fsm:if_applied(LocalParts, TxId1, nothing, repl_abort)),
    ?assertEqual(true, mock_partition_fsm:if_applied(RemoteParts, TxId1, nothing, repl_abort)),
    true = ets:delete(MyTable).

commit_tx_test() ->
    MyTable = ets:new(whatever, [private, set]),
    LocalUps=[{lp1, [{key1, 1}, {key2, 2}]}, {lp2, [{key3, 3}]}],
    RemoteUps=[{rp1, [{key4, 1}, {key5, 2}]}, {rp2, [{key6, 3}]}],
    LocalParts = [lp1, lp2],
    RemoteParts = [rp1, rp2],
    TxId1 = tx_utilities:create_transaction_record(0),
    ets:insert(MyTable, {TxId1, whatever}),
    CT = 10000,
    commit_tx(TxId1, CT, LocalUps, RemoteUps, true, MyTable),
    ?assertEqual([], ets:lookup(MyTable, TxId1)),
    ?assertEqual(true, mock_partition_fsm:if_applied(LocalParts, TxId1, CT, commit)),
    ?assertEqual(true, mock_partition_fsm:if_applied(RemoteParts, TxId1, CT, commit)),
    ?assertEqual(true, mock_partition_fsm:if_applied(LocalParts, TxId1, CT, repl_commit)),
    ?assertEqual(true, mock_partition_fsm:if_applied(RemoteParts, TxId1, CT, repl_commit)),
    ets:delete(MyTable).

abort_specula_list_test() ->
    MyTable = ets:new(whatever, [private, set]),
    DepDict = dict:new(),
    DepNum = dict:new(),
    T1 = tx_utilities:create_transaction_record(0),
    T2 = tx_utilities:create_transaction_record(0),
    T3 = tx_utilities:create_transaction_record(0),
    T4 = tx_utilities:create_transaction_record(0),
    DepDict1 = dict:store(T1, whatever, DepDict),
    DepDict2 = dict:store(T3, whatever, DepDict1),
    DepDict3 = dict:store(T4, whatever, DepDict2),
    DepNum1 = dict:store(T2, whatever, DepNum),
    DepNum2 = dict:store(T3, whatever, DepNum1),
    DepNum3 = dict:store(T4, whatever, DepNum2),
    LocalUps = [{p1, [{key1, 1}]}],
    RemoteUps = [{p2, [{key2, 2}]}],
    ets:insert(MyTable, {T1, {1, 1, LocalUps, RemoteUps}}),
    ets:insert(MyTable, {T2, {1, 1, LocalUps, RemoteUps}}),
    ets:insert(MyTable, {T3, {1, 1, LocalUps, RemoteUps}}),
    {DepDict4, DepNum4} = abort_specula_list([T1, T2, T3], DepDict3, DepNum3, true, MyTable, MyTable),
    ?assertEqual([], ets:lookup(MyTable, T1)),
    ?assertEqual([], ets:lookup(MyTable, T2)),
    ?assertEqual([], ets:lookup(MyTable, T3)),
    ?assertEqual(true, mock_partition_fsm:if_applied([p1], T1, nothing, abort)),
    ?assertEqual(true, mock_partition_fsm:if_applied([p2], T1, nothing, abort)),
    ?assertEqual(true, mock_partition_fsm:if_applied([p1], T1, nothing, repl_abort)),
    ?assertEqual(true, mock_partition_fsm:if_applied([p2], T1, nothing, repl_abort)),
    ?assertEqual(true, mock_partition_fsm:if_applied([p1], T2, nothing, abort)),
    ?assertEqual(true, mock_partition_fsm:if_applied([p2], T2, nothing, abort)),
    ?assertEqual(true, mock_partition_fsm:if_applied([p1], T2, nothing, repl_abort)),
    ?assertEqual(true, mock_partition_fsm:if_applied([p2], T2, nothing, repl_abort)),
    ?assertEqual({ok, whatever}, dict:find(T4, DepDict4)),
    ?assertEqual({ok, whatever}, dict:find(T4, DepNum4)),
    ?assertEqual(error, dict:find(T3, DepDict4)),
    ?assertEqual(error, dict:find(T3, DepNum4)),
    ets:delete(MyTable).

solve_read_dependency_test() ->
    T1 = tx_utilities:create_transaction_record(0), 
    CommitTime1 = tx_utilities:now_microsec(),
    T2 = tx_utilities:create_transaction_record(0),
    CommitTime2 = tx_utilities:now_microsec(),
    T3 = tx_utilities:create_transaction_record(0),
    %% T2 should be aborted
    DepNum = dict:new(),
    DepNum1 = dict:store(T2,1, DepNum),
    DepNum2 = dict:store(T3,2, DepNum1),
    {_, T} = solve_read_dependency(T1, CommitTime2, [T2, T3], DepNum2),
    ?assertEqual(T, T2),
    {DP1, TT} = solve_read_dependency(T1, CommitTime1, [T2, T3], DepNum2),
    ?assertEqual({[{T3,1}], ignore}, {dict:to_list(DP1), TT}).

deal_dependency_test() ->
    MyTable = ets:new(whatever, [private, set]),
    T1 = tx_utilities:create_transaction_record(0),
    %CommitTime1 = tx_utilities:now_microsec(),
    T2 = tx_utilities:create_transaction_record(0),
    MaxPT1 = T2#tx_id.snapshot_time+1,
    %CommitTime2 = tx_utilities:now_microsec(),
    T3 = tx_utilities:create_transaction_record(0),
    %MaxPT2 = tx_utilities:now_microsec(),
    DepDict1 = dict:store(T1, [T2, T3], dict:new()),
    DepDict2 = dict:store(T2, [T3], DepDict1),
    DepNum1 = dict:store(T2, 1, dict:new()),
    DepNum2 = dict:store(T3, 2, DepNum1),
    LP1 = [{lp1, [{key1, 1}]}], 
    RP1 = [{rp1, [{key3, 1}]}],
    LP2 = [{lp2, [{key2, 1}]}], 
    RP2 = [{rp2, [{key4, 1}]}],
    ets:insert(MyTable, {T2, {0, 2, LP1, RP1}}), 
    ets:insert(MyTable, {T3, {0, 2, LP2, RP2}}), 
    %% Get aborted because of large commit timestamp 
    io:format(user, "Abort ~w ~n", [MaxPT1]),
    {DD1, DP1, PD1, MPT1} = deal_dependency(T1, MaxPT1, [T2, T3], DepDict1, DepNum1, 
            true, MyTable, MyTable),
    ?assertEqual(DD1, dict:new()),
    ?assertEqual(DP1, dict:new()),
    ?assertEqual(PD1, []),
    ?assertEqual(MPT1, MaxPT1),
    ?assertEqual([], ets:lookup(MyTable, T2)),
    ?assertEqual([], ets:lookup(MyTable, T3)),
    ?assertEqual(true, mock_partition_fsm:if_applied([lp1], T2, nothing, abort)),
    ?assertEqual(true, mock_partition_fsm:if_applied([rp2], T3, nothing, repl_abort)),
    %% Abort due to wrong number dependency
    io:format(user, "Wrong num ~w ~n", [MPT1]),
    ets:insert(MyTable, {T2, {0, 3, LP1, RP1}}), 
    ets:insert(MyTable, {T3, {0, 4, LP2, RP2}}), 
    DepNum3 = dict:store(T2, 2, dict:new()),
    Result = deal_dependency(T1, MPT1-2, [T2, T3], DepDict1, DepNum3, 
            true, MyTable, MyTable),
    ?assertEqual(error, Result),
    %% Return due to need for more prepare
    io:format(user, "Return ~w ~n", [Result]),
    ets:insert(MyTable, {T2, {1, 5, LP1, RP1}}), 
    ets:insert(MyTable, {T3, {0, 6, LP2, RP2}}), 
    {DD2, DP2, PD2, MPT2} = deal_dependency(T1, MPT1-2, [T2, T3], DepDict2, DepNum2,
            true, MyTable, MyTable),
    ?assertEqual(DD2, dict:store(T2, [T3], dict:new())),
    ?assertEqual(DP2, dict:store(T3, 1, dict:new())),
    ?assertEqual(PD2, [T2, T3]),
    ?assertEqual(MPT2, MPT1-2),
    ?assertEqual(ets:lookup(MyTable, T2),  [{T2, {1, 5, LP1, RP1}}]), 
    ?assertEqual(ets:lookup(MyTable, T3),  [{T3, {0, 6, LP2, RP2}}]),
    %% Do not commit the second due to more prepare
    io:format(user, "More prepare ~w ~n", [Result]),
    ets:insert(MyTable, {T2, {0, 6, LP1, RP1}}), 
    ets:insert(MyTable, {T3, {1, 7, LP2, RP2}}), 
    {DD3, DP3, PD3, MPT3} = deal_dependency(T1, MPT1-2, [T2, T3], DepDict2, DepNum2, 
            true, MyTable, MyTable),
    ?assertEqual(DD3, dict:new()),
    ?assertEqual(DP3, dict:new()),
    ?assertEqual(PD3, [T3]),
    ?assertEqual(MPT3, MPT1-1),
    ?assertEqual(ets:lookup(MyTable, T2),  []), 
    ?assertEqual(ets:lookup(MyTable, T3),  [{T3, {1, 7, LP2, RP2}}]),
    %% Commit all
    ets:insert(MyTable, {T2, {0, 6, LP1, RP1}}), 
    ets:insert(MyTable, {T3, {0, 7, LP2, RP2}}), 
    {DD4, DP4, PD4, MPT4} = deal_dependency(T1, MPT1-2, [T2, T3], DepDict2, DepNum2, 
            true, MyTable, MyTable),
    ?assertEqual(DD4, dict:new()),
    ?assertEqual(DP4, dict:new()),
    ?assertEqual(PD4, []),
    ?assertEqual(MPT4, MPT1),
    ?assertEqual(true, mock_partition_fsm:if_applied([lp1], T2, MPT1-1, commit)),
    ?assertEqual(true, mock_partition_fsm:if_applied([rp2], T3, MPT1, repl_commit)).
    
-endif.
