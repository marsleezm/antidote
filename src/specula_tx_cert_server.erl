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
        read_aborted=0 :: non_neg_integer(),
        hit_cache=0 :: non_neg_integer(),
        committed=0 :: non_neg_integer(),
        aborted=0 :: non_neg_integer(),
        speculated=0 :: non_neg_integer(),
        sender :: term(),
        to_ack :: non_neg_integer()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name) ->
    lager:info("Specula tx cert started wit name ~w", [Name]),
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

handle_call({get_hash_fun}, _Sender, SD0) ->
    L = hash_fun:get_hash_fun(),
    {reply, L, SD0};

handle_call({start_tx}, _Sender, SD0) ->
    TxId = tx_utilities:create_transaction_record(0),
    {reply, TxId, SD0};

handle_call({get_stat}, _Sender, SD0=#state{aborted=Aborted, committed=Committed, read_aborted=ReadAborted, hit_cache=HitCache,
        speculated=Speculated}) ->
    lager:info("Hit cache is ~w, Num of read aborted ~w, Num of aborted is ~w, Num of committed is ~w, Speculated ~w", 
                [HitCache, ReadAborted, Aborted, Committed, Speculated]),
    {reply, {HitCache, ReadAborted, Aborted, Committed, Speculated}, SD0};

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
            {reply, ets:tab2list(SpeculaData), SD0};
        dependency ->
            {Reader, Creator} = Param,
            {reply, {dict:find(Reader, DepNum), dict:find(Creator, DepDict)}, SD0};
        all_dependency ->
            {reply, {dict:to_list(DepDict), dict:to_list(DepNum)}, SD0}
    end;

handle_call({certify, TxId, LocalUpdates0, RemoteUpdates0},  Sender, SD0=#state{dep_num=DepNum, 
            dep_dict=DepDict}) ->
    %LocalKeys = lists:map(fun({Node, Ups}) -> {Node, [Key || {Key, _} <- Ups]} end, LocalUpdates),
    %% If there was a legacy ongoing transaction.
    {LocalUpdates, RemoteUpdates} = case LocalUpdates0 of
                                        {raw, LList} ->
                                            {raw, RList} = RemoteUpdates0,
                                            {[{hash_fun:get_vnode_by_id(P, N), Ups}  || {N, P, Ups} <- LList],
                                             [{hash_fun:get_vnode_by_id(P, N), Ups}  || {N, P, Ups} <- RList]};
                                        _ ->
                                            {LocalUpdates0, RemoteUpdates0}
                                    end,
    %lager:info("Certifying, txId is ~w, local num is ~w, remote num is ~w", 
    %        [TxId, length(LocalUpdates), length(RemoteUpdates)]),
    %lager:info("Got req: localUpdates ~p, remote updates ~p", [LocalUpdates0, RemoteUpdates0]),
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

handle_call({read, Key, TxId, Node0}, Sender, SD0=#state{dep_num=DepNum, specula_data=SpeculaData, hit_cache=HitCache}) ->
    Node = case Node0 of
                {raw,  N, P} ->
                    hash_fun:get_vnode_by_id(P, N);
                _ ->
                    Node0
              end,
    %lager:info("~w Reading key ~w from ~w", [TxId, Key, Node]),
    case ets:lookup(SpeculaData, Key) of
        [{Key, Value, PendingTxId}] ->
            %lager:info("Has Value ~w", [Value]),
            DepNum1 = case dict:find(TxId, DepNum) of
                        {ok, Set} ->
                            dict:store(TxId, sets:add_element(PendingTxId, Set), DepNum);
                        error ->
                            dict:store(TxId, sets:add_element(PendingTxId, sets:new()), DepNum)
                      end,
            {reply, {ok, Value}, SD0#state{dep_num=DepNum1, hit_cache=HitCache+1}};
        [] ->
            %lager:info("No Value"),
            case Node of 
                {_,_} ->
                    ?CLOCKSI_VNODE:relay_read(Node, Key, TxId, Sender),
                    %lager:info("Well, from clocksi_vnode"),
                    {noreply, SD0};
                _ ->
                    {ok, V} = data_repl_serv:read(Node, TxId, Key),
                    {reply, V, SD0}
            end
    end;

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

handle_cast({prepared, ReceivedTxId, PrepareTime, local}, 
	    SD0=#state{to_ack=N, tx_id=TxId, local_updates=LocalUpdates, do_repl=DoRepl, last_commit_ts=LastCommitTS,
            remote_updates=RemoteUpdates, sender=Sender, pending_list=PendingList, speculated=Speculated,
            prepare_time=OldPrepTime, pending_txs=PendingTxs, specula_data=SpeculaData}) ->
    %lager:info("Got local prepare for ~w", [TxId]),
    case ReceivedTxId of
        TxId ->
            case N of
                1 -> 
                    %lager:info("Got all local prepare!"),
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
                                    {noreply, SD0#state{speculated=Speculated+1, tx_id=[], 
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
                            {noreply, SD0#state{tx_id=[], speculated=Speculated+1, pending_list=PendingList++[TxId]}}
                        end;
                        %%TODO: Maybe it does not speculatively-commit.
                _ ->
                    %lager:info("~w needs ~w replies", [TxId, N-1]),
                    MaxPrepTime = max(OldPrepTime, PrepareTime),
                    {noreply, SD0#state{to_ack=N-1, prepare_time=MaxPrepTime}}
            end;
        _ ->
            %lager:info("Got bad prepare for ~w, PT is ~w", [TxId, PrepareTime]),
            {noreply, SD0}
    end;

%handle_cast({prepared, TId, Time, local}, 
%	    SD0) ->
%    %lager:info("Received prepare of previous prepared txn! ~w", [OtherTxId]),
%    {noreply, SD0};

handle_cast({abort, ReceivedTxId, local}, SD0=#state{tx_id=TxId, local_updates=LocalUpdates, 
        do_repl=DoRepl, sender=Sender, pending_txs=PendingTxs}) ->
    case ReceivedTxId of
        TxId ->
            %lager:info("Received abort for ~w, setting tx_id to []", [TxId]),
            abort_tx(TxId, LocalUpdates, [], DoRepl, PendingTxs),
            gen_server:reply(Sender, {aborted, TxId}),
            {noreply, SD0#state{tx_id=[]}};
        _ ->
            {noreply, SD0}
    end;
%handle_cast({abort, _, local}, SD0) ->
    %lager:info("Received abort for previous txn ~w", [OtherTxId]),
%    {noreply, SD0};

%% If the last transaction gets a remote prepare right after the reply is sent back to the client and before the
%% next transaction arrives.. We will update the state in place, instead of always going to the ets table.
%% But if we can not commit the transaction immediately, we have to store the updated state to ets table.
%handle_cast({prepared, TxId, PrepareTime, remote}, 
%	    SD0=#state{sender=Sender, tx_id=TxId, do_repl=DoRepl, specula_data=SpeculaData, 
%            prepare_time=OldMaxPrep, last_commit_ts=LastCommitTS, remote_updates=RemoteUpdates, 
%            pending_txs=PendingTxs, committed=Committed, 
%            pending_list=PendingList, to_ack=N, local_updates=LocalUpdates}) ->
    %lager:info("Received remote prepare"),
%    NewMaxPrep = max(OldMaxPrep, PrepareTime),
%    case N of
%        1 ->
%            case PendingList of
%                [] -> 
%                    CommitTime = max(NewMaxPrep, LastCommitTS+1),
%                    commit_specula_tx(TxId, CommitTime, LocalUpdates, RemoteUpdates, 
%                        DoRepl, PendingTxs, SpeculaData),
%                    gen_server:reply(Sender, {ok, {committed, CommitTime}}),
%                    {noreply, SD0#state{last_commit_ts=CommitTime, tx_id=[], committed=Committed+1}};
%                [TxId] -> 
%                    CommitTime = max(NewMaxPrep, LastCommitTS+1),
%                    commit_specula_tx(TxId, CommitTime, LocalUpdates, RemoteUpdates, 
%                        DoRepl, PendingTxs, SpeculaData),
%                    {noreply, SD0#state{last_commit_ts=CommitTime, tx_id=[], pending_list=[], committed=Committed+1}};
%                _ ->
%                    %%This can not happen!!!
%                    ets:insert(PendingTxs, {TxId, {0, NewMaxPrep, LocalUpdates, RemoteUpdates}}),
%                    {noreply, SD0#state{to_ack=0, prepare_time=NewMaxPrep}}
%            end;
%        N ->
%            ets:insert(PendingTxs, {TxId, {N-1, NewMaxPrep, LocalUpdates, RemoteUpdates}}),
%            {noreply, SD0#state{to_ack=N-1, prepare_time=NewMaxPrep}}
%    end;
handle_cast({prepared, PendingTxId, PendingPT, remote}, 
	    SD0=#state{pending_list=PendingList, do_repl=DoRepl, last_commit_ts=LastCommitTS, pending_txs=PendingTxs,
            sender=Sender, tx_id=CurrentTxId,
             committed=Committed, dep_dict=DepDict, dep_num=DepNum, read_aborted=ReadAborted, specula_data=SpeculaData}) ->
    case ets:lookup(PendingTxs, PendingTxId) of
        [{PendingTxId, {1, PendingMaxPT, LocalUpdates, RemoteUpdates}}] ->
            MaxPT = max(PendingMaxPT, PendingPT),
            %% To make sure that a transaction commits with timestamp larger than its previous txn
            case hd(PendingList) of
                PendingTxId ->
                    CommitTime = max(LastCommitTS+1, MaxPT),
                    commit_specula_tx(PendingTxId, CommitTime, LocalUpdates, RemoteUpdates, DoRepl,
                             PendingTxs, SpeculaData),
                    {NewDepDict, NewDepNum, NewPendingList, NewMaxPT, NewReadAborted, NewCommitted} = deal_dependency(PendingTxId,
                                CommitTime, tl(PendingList), DepDict, DepNum, DoRepl, PendingTxs, SpeculaData, ReadAborted, Committed+1),
                    case NewReadAborted of
                        ReadAborted ->
                            %% No transaction is aborted
                            {noreply, SD0#state{ 
                                pending_list=NewPendingList, dep_dict=NewDepDict, dep_num=NewDepNum, 
                                last_commit_ts=NewMaxPT, read_aborted=NewReadAborted, committed=NewCommitted}};
                        _ ->
                            %% The current transaction must have been aborted due to flow dependency
                            abort_tx(CurrentTxId, LocalUpdates, RemoteUpdates, DoRepl, PendingTxs),
                            gen_server:reply(Sender, {aborted, CurrentTxId}),
                            {noreply, SD0#state{ 
                                pending_list=NewPendingList, tx_id=[], dep_dict=NewDepDict, dep_num=NewDepNum, 
                                last_commit_ts=NewMaxPT, read_aborted=NewReadAborted, committed=NewCommitted}}
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

%handle_cast({abort, TxId, remote}, 
%	    SD0=#state{remote_updates=RemoteUpdates, pending_txs=PendingTxs, pending_list=PendingList, 
%            do_repl=DoRepl, tx_id=TxId, local_updates=LocalUpdates, aborted=Aborted}) ->
%    abort_tx(TxId, LocalUpdates, RemoteUpdates, DoRepl, PendingTxs),
%    %gen_server:reply(Sender, {aborted, TxId}),
%    {L, _} = lists:split(length(PendingList) - 1, PendingList),
%    %lager:info("Abort remote"),
%    {noreply, SD0#state{tx_id=[], aborted=Aborted+1, 
%                pending_list=L}};

handle_cast({abort, PendingTxId, remote}, 
	    SD0=#state{pending_list=PendingList, tx_id=TxId, dep_dict=DepDict, specula_data=SpeculaData, sender=Sender,
            do_repl=DoRepl, dep_num=DepNum, pending_txs=PendingTxs, local_updates=LocalUpdates,
            remote_updates=RemoteUpdates, aborted=Aborted}) ->
    case start_from(PendingTxId, PendingList) of
        [] ->
            %lager:info("Not aborting anything"),
            {noreply, SD0};
        L ->
            %lager:info("Abort remote again, Pending list is ~w, PendingTxId is ~w, List is ~w", [PendingList, PendingTxId, L]),
            {DD, DN} = abort_specula_list(L, DepDict, DepNum, DoRepl, PendingTxs, SpeculaData),
            %% The current transaction is aborted! So replying to client.
            case TxId of
                [] ->
                    {noreply, SD0#state{dep_dict=DD, dep_num=DN, tx_id=[], 
                        pending_list=lists:sublist(PendingList, length(PendingList)-length(L)),
                        aborted=Aborted+length(L)}};
                _ ->
                    abort_tx(TxId, LocalUpdates, RemoteUpdates, DoRepl, PendingTxs),
                    gen_server:reply(Sender, {aborted, TxId}),
                    {noreply, SD0#state{dep_dict=DD, dep_num=DN, tx_id=[], 
                        pending_list=lists:sublist(PendingList, length(PendingList)-length(L)),
                        aborted=Aborted+length(L)}}
            end
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

deal_dependency([], _, [], DepDict, DepNum, MaxPT, _, _, ReadAborted, Committed) ->
    {DepDict, DepNum, [], MaxPT, ReadAborted, Committed};
deal_dependency(DependentTxId, CommitTime, PendingList, DepDict, DepNum, 
                        DoRepl, PendingTxs, SpeculaData, ReadAborted, Committed) ->
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
    ReadAborted1 = length(PendingList) - length(PendingList1) + ReadAborted,
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
                                    DoRepl, PendingTxs, SpeculaData, 
                                ReadAborted1, Committed+1);
                        %% The next txn can not be committed.. Then just return
                        _ ->
                            %lager:info("Next txn not prepared!, pending list 1 is ~w", [PendingList1]),
                            {DepDict2, DepNum2, PendingList1, CommitTime, ReadAborted1, Committed}
                    end;
                R ->
                    %% The next txn still depends on some txn.. Should not happen!!! 
                    lager:error("The next txn still depends on some other txns.. This is wrong! ~w", [R]), 
                    error
            end;
        [] ->
            {DepDict2, DepNum2, [], CommitTime, ReadAborted1, Committed}
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
    {DD1, DP1, PD1, MPT1, Aborted, Committed} = deal_dependency(T1, MaxPT1, [T2, T3], DepDict1, DepNum1, 
            true, MyTable, MyTable, 0, 0),
    ?assertEqual(Aborted, 2),
    ?assertEqual(Committed, 0),
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
            true, MyTable, MyTable, Aborted, Committed),
    ?assertEqual(error, Result),
    %% Return due to need for more prepare
    io:format(user, "Return ~w ~n", [Result]),
    ets:insert(MyTable, {T2, {1, 5, LP1, RP1}}), 
    ets:insert(MyTable, {T3, {0, 6, LP2, RP2}}), 
    {DD2, DP2, PD2, MPT2, Aborted1, Committed1} = deal_dependency(T1, MPT1-2, [T2, T3], DepDict2, DepNum2,
            true, MyTable, MyTable, Aborted, Committed),
    ?assertEqual(Aborted1, 2),
    ?assertEqual(Committed1, 0),
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
    {DD3, DP3, PD3, MPT3, Aborted2, Committed2} = deal_dependency(T1, MPT1-2, [T2, T3], DepDict2, DepNum2, 
            true, MyTable, MyTable, Aborted1, Committed1),
    ?assertEqual(Aborted2, Aborted1),
    ?assertEqual(Committed2, Committed1+1),
    ?assertEqual(DD3, dict:new()),
    ?assertEqual(DP3, dict:new()),
    ?assertEqual(PD3, [T3]),
    ?assertEqual(MPT3, MPT1-1),
    ?assertEqual(ets:lookup(MyTable, T2),  []), 
    ?assertEqual(ets:lookup(MyTable, T3),  [{T3, {1, 7, LP2, RP2}}]),
    %% Commit all
    ets:insert(MyTable, {T2, {0, 6, LP1, RP1}}), 
    ets:insert(MyTable, {T3, {0, 7, LP2, RP2}}), 
    {DD4, DP4, PD4, MPT4, Aborted3, Committed3} = deal_dependency(T1, MPT1-2, [T2, T3], DepDict2, DepNum2, 
            true, MyTable, MyTable, Aborted2, Committed2),
    ?assertEqual(Aborted3, Aborted2),
    ?assertEqual(Committed3, Committed2+2),
    ?assertEqual(DD4, dict:new()),
    ?assertEqual(DP4, dict:new()),
    ?assertEqual(PD4, []),
    ?assertEqual(MPT4, MPT1),
    ?assertEqual(true, mock_partition_fsm:if_applied([lp1], T2, MPT1-1, commit)),
    ?assertEqual(true, mock_partition_fsm:if_applied([rp2], T3, MPT1, repl_commit)).
    
-endif.
