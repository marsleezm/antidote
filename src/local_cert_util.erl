% -------------------------------------------------------------------
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
-module(local_cert_util).

-include("include/speculation.hrl").
-include("include/antidote.hrl").

-export([prepare_for_master_part/5, ready_or_block/4, prepare_for_other_part/7, update_store/9, clean_abort_prepared/7, 
            specula_commit/8, specula_read/4, insert_prepare/6]).

-define(SPECULA_THRESHOLD, 0).

prepare_for_master_part(TxId, TxWriteSet, CommittedTxs, PreparedTxs, InitPrepTime)->
    KeySet = [K || {K, _} <- TxWriteSet],
    case certification_check(InitPrepTime, TxId, KeySet, CommittedTxs, PreparedTxs, 0, 0) of
        false ->
            {error, write_conflict};
        %% Directly prepare
        {0, 0, PrepareTime} ->
            lager:warning("~p passed prepare with ~p", [TxId, PrepareTime]),
            lists:foreach(fun({K, V}) ->
                    ets:insert(PreparedTxs, {K, [{prepared, TxId, PrepareTime, PrepareTime, PrepareTime, 1, V, []}]})
                    end, TxWriteSet),
            true = ets:insert(PreparedTxs, {TxId, KeySet}),
            {ok, PrepareTime};
        %% Pend-prepare. 
        {PendPrepDep, PrepDep, PrepareTime} ->
            lager:warning("~p passed but has ~p pend dep, ~p prepdep, prepare with ~p", [TxId, PendPrepDep, PrepDep, PrepareTime]),
            %KeySet = [K || {K, _} <- TxWriteSet],  % set_prepared(PreparedTxs, TxWriteSet, TxId,PrepareTime, []),
            lists:foreach(fun({K, V}) ->
                          case ets:lookup(PreparedTxs, K) of
                          [] ->
                              ets:insert(PreparedTxs, {K, [{prepared, TxId, PrepareTime, PrepareTime, PrepareTime, 1, V, []}]});
                          [{K, [{Type, PrepTxId, OldPPTime, LastReaderTime, LastPrepTime, PrepNum, PrepValue, RWaiter}|PWaiter]}] ->
                              ets:insert(PreparedTxs, {K, [{Type, PrepTxId, OldPPTime, LastReaderTime, max(LastPrepTime, PrepareTime), PrepNum+1, PrepValue, RWaiter}|
                                       (PWaiter++[{prepared, TxId, PrepareTime, V, []}])]});
                          _ -> ets:insert(PreparedTxs, {K, [{prepared, TxId, PrepareTime, PrepareTime, PrepareTime, 1, V, []}]})
                          end
                    end, TxWriteSet),
            ets:insert(PreparedTxs, {TxId, KeySet}),
            {wait, PendPrepDep, PrepDep, PrepareTime}
    end.

prepare_for_other_part(TxId, Partition, TxWriteSet, CommittedTxs, PreparedTxs, InitPrepTime, PartitionType)->
    KeySet = case PartitionType of cache -> [{Partition,K} || {K, _} <- TxWriteSet];
                                   slave -> [K || {K, _} <- TxWriteSet]
             end,
    case certification_check(InitPrepTime, TxId, KeySet, CommittedTxs, PreparedTxs, 0, 0) of
        false ->
            {error, write_conflict};
        %% Directly prepare
        {0, 0, PrepareTime} ->
            lager:warning("~p passed prepare with ~p, KeySet is ~w", [TxId, PrepareTime, KeySet]),
            lists:foreach(fun({K, V}) ->
                    InsertKey = case PartitionType of cache -> {Partition, K}; slave -> K end,
                    ets:insert(PreparedTxs, {InsertKey, [{prepared, TxId, PrepareTime, PrepareTime, PrepareTime, 1, V, []}]})
                    end, TxWriteSet),
            true = ets:insert(PreparedTxs, {{TxId, Partition}, KeySet}),
            {ok, PrepareTime};
        %% Pend-prepare. 
        {PendPrepDep, PrepDep, PrepareTime} ->
            lager:warning("~p passed but has ~p pend prep deps, ~p prep dep, prepare with ~p, KeySet is ~w", [TxId, PendPrepDep, PrepDep, PrepareTime, KeySet]),
            %KeySet = [K || {K, _} <- TxWriteSet],  % set_prepared(PreparedTxs, TxWriteSet, TxId,PrepareTime, []),
            lists:foreach(fun({K, V}) ->
                          InsertKey = case PartitionType of cache -> {Partition, K}; slave -> K end,
                          case ets:lookup(PreparedTxs, InsertKey) of
                          [] ->  lager:warning("Inserting to empty"),
                              ets:insert(PreparedTxs, {InsertKey, [{prepared, TxId, PrepareTime, PrepareTime, PrepareTime, 1, V, []}]});
                          [{InsertKey, [{Type, PrepTxId, OldPPTime, LastRTime, LastPrepTime, PrepNum, PrepValue, RWaiter}|PWaiter]}] ->
                               lager:warning("Inserting to current"),
                              ets:insert(PreparedTxs, {InsertKey, [{Type, PrepTxId, OldPPTime, LastRTime, max(LastPrepTime, PrepareTime), PrepNum+1, PrepValue, RWaiter}|
                                       (PWaiter++[{prepared, TxId, PrepareTime, V, []}])]});
                          R -> 
                               lager:warning("Inserting to whatever, R is ~w", [R]),
                              ets:insert(PreparedTxs, {InsertKey, [{prepared, TxId, PrepareTime, PrepareTime, PrepareTime, 1, V, []}]})
                          end
                    end, TxWriteSet),
            true = ets:insert(PreparedTxs, {{TxId, Partition}, KeySet}),
            {wait, PendPrepDep, PrepDep, PrepareTime}
    end.

certification_check(FinalPrepTime, _, [], _, _, PendPrepDep, PrepDep) ->
    {PendPrepDep, PrepDep, FinalPrepTime};
certification_check(PrepareTime, TxId, [Key|T], CommittedTxs, PreparedTxs, PendPrepDep, PrepDep) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case CommittedTxs of
        ignore ->
            case check_prepared(TxId, PreparedTxs, Key, whatever) of
                {true, NewPrepTime} ->
                    certification_check(max(NewPrepTime, PrepareTime), TxId, T, CommittedTxs, PreparedTxs, PendPrepDep, PrepDep);
                {pend_prep_dep, NewPrepTime} ->
                    certification_check(max(NewPrepTime, PrepareTime), TxId, T, CommittedTxs, PreparedTxs, PendPrepDep+1, PrepDep);
                {prep_dep, NewPrepTime} ->
                    certification_check(max(NewPrepTime, PrepareTime), TxId, T, CommittedTxs, PreparedTxs, PendPrepDep, PrepDep+1);
                false ->
                      lager:warning("~p: False of prepared for ~p", [TxId, Key]),
                    false
            end;
        _ ->
            case ets:lookup(CommittedTxs, Key) of
                [{Key, CommitTime}] ->
                    case CommitTime > SnapshotTime of
                        true ->
                               lager:warning("False for committed key ~p, Snapshot is ~p, diff with commit ~p", [Key, TxId#tx_id.snapshot_time, CommitTime-TxId#tx_id.snapshot_time]),
                            false;
                        false ->
                            case check_prepared(TxId, PreparedTxs, Key, whatever) of
                                {true, NewPrepTime} ->
                                    certification_check(max(NewPrepTime, PrepareTime), TxId, T, CommittedTxs, PreparedTxs, PendPrepDep, PrepDep);
                                {pend_prep_dep, NewPrepTime} ->
                                    certification_check(max(NewPrepTime, PrepareTime), TxId, T, CommittedTxs, PreparedTxs, PendPrepDep+1, PrepDep);
                                {prep_dep, NewPrepTime} ->
                                    certification_check(max(NewPrepTime, PrepareTime), TxId, T, CommittedTxs, PreparedTxs, PendPrepDep, PrepDep+1);
                                false ->
                                    false
                            end
                    end;
                [] ->
                    case check_prepared(TxId, PreparedTxs, Key, whatever) of
                        {true, NewPrepTime} ->
                            certification_check(max(NewPrepTime, PrepareTime), TxId, T, CommittedTxs, PreparedTxs, PendPrepDep, PrepDep);
                        {pend_prep_dep, NewPrepTime} ->
                            certification_check(max(NewPrepTime, PrepareTime), TxId, T, CommittedTxs, PreparedTxs, PendPrepDep+1, PrepDep);
                        {prep_dep, NewPrepTime} ->
                            certification_check(max(NewPrepTime, PrepareTime), TxId, T, CommittedTxs, PreparedTxs, PendPrepDep, PrepDep+1);
                        false ->
                              lager:warning("~p: False of prepared for ~p", [TxId, Key]),
                            false
                    end
            end
    end.

check_prepared(TxId, PreparedTxs, Key, _Value) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(PreparedTxs, Key) of
        [] ->
            %ets:insert(PreparedTxs, {Key, [{TxId, PPTime, PPTime, PPTime, Value, []}]}),
            {true, 1};
        %%% The type of the record: prepared or specula-commit; TxId; PrepareTime; LastReaderTime;
        %% The specula-commit time of the last record
        [{Key, [{_Type, PrepTxId, PrepareTime, LastReaderTime, LastSCTime, PrepNum, _PrepValue, _RWaiter}|_PWaiter]=Record}] ->
            lager:warning("For key ~p: ~p prepared already! LastStTime is ~p, ~p may fail, whole record is ~p", [Key, PrepTxId, LastSCTime, TxId, Record]),
            case LastSCTime > SnapshotTime of
                true ->
                    false;
                false ->
                     lager:warning("~p: ~p waits for ~p with ~p", [Key, TxId, PrepTxId, PrepareTime]),
                    %% If all previous txns have specula-committed, then this txn can be directly pend-prepared and specula-committed 
                    %% Otherwise, this txn has to wait until all preceding prepared txn to be specula-committed
                    %% has_spec_commit means this txn can pend prep and then spec commit 
                    %% has_pend_prep basically means this txn can not even pend_prep
                    case PrepNum of 0 ->  {prep_dep, LastReaderTime+1};
                                        _ -> {pend_prep_dep, LastReaderTime+1}
                    end
            end;
        [{Key, LastReaderTime}] ->
            %ToPrepTime = max(LastReaderTime+1, PPTime),
            %ets:insert(PreparedTxs, {Key, [{TxId, ToPrepTime, ToPrepTime, PPTime, Value, []}]}),
            {true, LastReaderTime+1}
    end.


-spec update_store(Keys :: [{key()}],
                          TxId::txid(),TxCommitTime:: {term(), term()},
                                InMemoryStore :: cache_id(), CommittedTxs :: cache_id(),
                                PreparedTxs :: cache_id(), DepDict :: dict(), 
                        Partition :: integer(), PartitionType :: term() ) -> ok.
update_store([], _TxId, _TxCommitTime, _InMemoryStore, _CommittedTxs, _PreparedTxs, DepDict, _Partition, _PartitionType) ->
    DepDict;
    %dict:update(commit_diff, fun({Diff, Cnt}) -> {Diff+TxCommitTime-PrepareTime, Cnt+1} end, DepDict);
update_store([Key|Rest], TxId, TxCommitTime, InMemoryStore, CommittedTxs, PreparedTxs, DepDict, Partition, PartitionType) ->
     lager:warning("Trying to insert key ~p with for ~p, commit time is ~p", [Key, TxId, TxCommitTime]),
    MyNode = {Partition, node()},
    case ets:lookup(PreparedTxs, Key) of
        [{Key, [{Type, TxId, _PrepareTime, LRTime, LSCTime, PrepNum, Value, PendingReaders}|Deps]}] ->
            lager:warning("Trying to insert key ~p with for ~p, Type is ~p, prepnum is  is ~p, Commit time is ~p", [Key, TxId, Type, PrepNum, TxCommitTime]),
            lager:warning("~p Pending readers are ~p! Pending writers are ~p", [TxId, PendingReaders, Deps]),
            {Head, Record, DepDict1, AbortedReaders, RemoveDepType, AbortPrep} 
                = deal_pending_records(Deps, TxCommitTime, DepDict, MyNode, [], false, PartitionType, 0),
            RPrepNum = case Type of specula_commit -> PrepNum-AbortPrep; _ -> PrepNum-AbortPrep-1 end,
            case PartitionType of
                cache ->  
                    lists:foreach(fun({ReaderTxId, Node, Sender}) ->
                            SnapshotTime = ReaderTxId#tx_id.snapshot_time,
                            case SnapshotTime >= TxCommitTime of
                                true ->
                                    reply(Sender, {ok, Value});
                                false ->
                                    {_, RealKey} = Key,
                                    lager:warning("Relaying read of ~w to ~w", [ReaderTxId, Node]),
                                    clocksi_vnode:relay_read(Node, RealKey, ReaderTxId, Sender, false)
                            end end,
                        AbortedReaders++PendingReaders),
                    case Head of
                        [] -> 
                            true = ets:insert(PreparedTxs, {Key, max(TxCommitTime, LRTime)}),
                            update_store(Rest, TxId, TxCommitTime, InMemoryStore, CommittedTxs, PreparedTxs, 
                                DepDict1, Partition, PartitionType);
                        {HType, HTxId, HPTime, HValue, HReaders} -> 
                            DepDict2 = unblock_prepare(HTxId, DepDict1, Partition, RemoveDepType),
                            ets:insert(PreparedTxs, {Key, [{HType, HTxId, HPTime, LRTime, max(HPTime,LSCTime), RPrepNum, HValue, HReaders}|Record]}),
                            update_store(Rest, TxId, TxCommitTime, InMemoryStore, CommittedTxs, PreparedTxs,
                                DepDict2, Partition, PartitionType)
                    end;
                _ ->
                    Values = case ets:lookup(InMemoryStore, Key) of
                                [] ->
                                    true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, Value}]}),
                                    [[], Value];
                                [{Key, ValueList}] ->
                                    {RemainList, _} = lists:split(min(?NUM_VERSION,length(ValueList)), ValueList),
                                    [{_CommitTime, First}|_] = RemainList,
                                    true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, Value}|RemainList]}),
                                    [First, Value]
                            end,
                    ets:insert(CommittedTxs, {Key, TxCommitTime}),
                    lists:foreach(fun({ReaderTxId, ignore, Sender}) ->
                            case ReaderTxId#tx_id.snapshot_time >= TxCommitTime of
                                true ->
                                    reply(Sender, {ok, lists:nth(2,Values)});
                                false ->
                                    reply(Sender, {ok, hd(Values)})
                            end end,
                        AbortedReaders++PendingReaders),
                    lager:warning("Head is ~p, Record is ~p", [Head, Record]),
                    case Head of
                        [] -> ets:insert(PreparedTxs, {Key, max(TxCommitTime, LRTime)}),
                            update_store(Rest, TxId, TxCommitTime, InMemoryStore, CommittedTxs, PreparedTxs,
                                DepDict1, Partition, PartitionType);
                        {repl_prepare, HTxId, HPTime, HValue, HReaders} -> 
                            ets:insert(PreparedTxs, {Key, [{repl_prepare, HTxId, HPTime, LRTime, max(HPTime,LSCTime), RPrepNum, HValue, HReaders}|Record]}),
                            update_store(Rest, TxId, TxCommitTime, InMemoryStore, CommittedTxs, PreparedTxs, 
                                DepDict1, Partition, PartitionType);
                        {HType, HTxId, HPTime, HValue, HReaders} -> 
                            lager:warning("Head Id is ~w, Head type is ~w", [HTxId, HType]),
                            ets:insert(PreparedTxs, {Key, [{HType, HTxId, HPTime, LRTime, max(HPTime,LSCTime), RPrepNum, HValue, HReaders}|Record]}),
                            DepDict2 = case Type of specula_commit -> unblock_prepare(HTxId, DepDict1, Partition, RemoveDepType);
                                         prepared -> unblock_prepare(HTxId, DepDict1, Partition, remove_ppd) 
                            end,
                            update_store(Rest, TxId, TxCommitTime, InMemoryStore, CommittedTxs, PreparedTxs, 
                                DepDict2, Partition, PartitionType)
                    end
            end;
        [{Key, [First|Others]}] ->
            lager:warning("First is ~p, Others are ~p, PartitionType is ~p", [First, Others, PartitionType]),
            {Record, DepDict2, CAbortPrep} = delete_and_read(commit, InMemoryStore, TxCommitTime, Key, DepDict, PartitionType, Partition, Others, TxId, [], First, 0),
            case CAbortPrep of 
                0 -> ets:insert(PreparedTxs, {Key, Record});
                _ -> [{F1, F2, F3, F4, F5, FPrepNum, FValue, FPendReader}|FRest] = Record,
                    ets:insert(PreparedTxs, {Key, [{F1, F2, F3, F4, F5, FPrepNum-CAbortPrep, FValue, FPendReader}|FRest]}) 
            end,
            case PartitionType of cache -> ok; _ -> ets:insert(CommittedTxs, {Key, TxCommitTime}) end,
            update_store(Rest, TxId, TxCommitTime, InMemoryStore, CommittedTxs, PreparedTxs, DepDict2, Partition, PartitionType);
        [] ->
            %[{TxId, Keys}] = ets:lookup(PreparedTxs, TxId),
            lager:error("Something is wrong!!! A txn updated two same keys ~p!", [Key]),
            update_store(Rest, TxId, TxCommitTime, InMemoryStore, CommittedTxs, PreparedTxs, DepDict, Partition, PartitionType);
        R ->
            lager:error("Record is ~p", [R]),
            R = error,
            error
    end.

clean_abort_prepared(_PreparedTxs, [], _TxId, _InMemoryStore, DepDict, _, _) ->
    DepDict; 
clean_abort_prepared(PreparedTxs, [Key | Rest], TxId, InMemoryStore, DepDict, Partition, PartitionType) ->
    lager:warning("Aborting ~p for key ~p", [TxId, Key]),
    MyNode = {Partition, node()},
    case ets:lookup(PreparedTxs, Key) of
        [{Key, [{Type, TxId, _PrepTime, LastReaderTime, LastPPTime, PrepNum, _Value, PendingReaders}|RestRecords]}] ->
            {Head, RemainRecord, DepDict1, AbortedReaders, remove_pd, 0}
                  = deal_pending_records(RestRecords, 0, DepDict, MyNode, [], false, PartitionType, 0),
            case PartitionType of
                cache -> 
                    lists:foreach(fun({ReaderTxId, Node, {relay, Sender}}) -> 
                            lager:warning("Relaying read of ~w to ~w", [ReaderTxId, Node]),
                            clocksi_vnode:relay_read(Node, Key, ReaderTxId, Sender, false) end,
                              PendingReaders++AbortedReaders);
                _ ->
                    lists:foldl(fun({_, ignore, Sender}, ToReturn) -> 
                        case ToReturn of nil -> ToReturn1 = case ets:lookup(InMemoryStore, Key) of
                                                                [{Key, ValueList}] -> {_, V} = hd(ValueList), V;
                                                                [] -> []
                                                            end,
                                                reply(Sender, {ok, ToReturn1}), ToReturn1;
                                        _ ->  reply(Sender, {ok, ToReturn}), ToReturn end end, 
                    nil, PendingReaders++AbortedReaders)
            end,
            case Head of
                [] ->
                    true = ets:insert(PreparedTxs, {Key, LastReaderTime}),
                    clean_abort_prepared(PreparedTxs,Rest,TxId, InMemoryStore, DepDict1, Partition, PartitionType);
                {repl_prepare, HTxId, HPTime, HValue, HReaders} ->
                    true = ets:insert(PreparedTxs, {Key, [{repl_prepare, HTxId, HPTime, LastReaderTime, max(HPTime,LastPPTime), PrepNum-1,
                            HValue, HReaders}|RemainRecord]}),
                    clean_abort_prepared(PreparedTxs,Rest,TxId, InMemoryStore, DepDict1, Partition, PartitionType);
                {HType, HTxId, HPTime, HValue, HReaders} ->
                    case Type of
                        specula_commit -> 
                            DepDict2 = unblock_prepare(HTxId, DepDict1, Partition, remove_pd), 
                            ets:insert(PreparedTxs, {Key, [{HType, HTxId, HPTime, LastReaderTime, max(HPTime,LastPPTime), PrepNum,
                                    HValue, HReaders}|RemainRecord]}),
                            clean_abort_prepared(PreparedTxs,Rest,TxId, InMemoryStore, DepDict2, Partition, PartitionType);
                        prepared -> 
                            DepDict2 = unblock_prepare(HTxId, DepDict1, Partition, remove_ppd),
                            ets:insert(PreparedTxs, {Key, [{HType, HTxId, HPTime, LastReaderTime, HPTime, PrepNum-1,
                                    HValue, HReaders}|RemainRecord]}),
                            clean_abort_prepared(PreparedTxs,Rest,TxId, InMemoryStore, DepDict2, Partition, PartitionType)
                    end
            end;
        [{Key, [First|Others]}] -> 
            lager:warning("Aborting TxId is ~w, Key is ~w", [TxId, Key]),
            {Record, DepDict2, CAbortPrep} = delete_and_read(abort, InMemoryStore, 0, Key, DepDict, PartitionType, Partition, Others, TxId, [], First, 0),
            lager:warning("Record is ~w", [Record]),
            case Record of 
                [] -> clean_abort_prepared(PreparedTxs,Rest,TxId, InMemoryStore, DepDict, Partition, PartitionType);
                _ ->          
                    case CAbortPrep of 
                        0 -> ets:insert(PreparedTxs, {Key, Record});
                        _ -> [{F1, F2, F3, F4, F5, FPrepNum, FValue, FPendReader}|FRest] = Record,
                           ets:insert(PreparedTxs, {Key, [{F1, F2, F3, F4, F5, FPrepNum-CAbortPrep, FValue, FPendReader}|FRest]}) 
                    end,
                    clean_abort_prepared(PreparedTxs,Rest,TxId, InMemoryStore, DepDict2, Partition, PartitionType)
            end;
        R ->
            lager:warning("WTF? R is ~p", [R]),
            clean_abort_prepared(PreparedTxs,Rest,TxId, InMemoryStore, DepDict, Partition, PartitionType)
    end.

deal_pending_records([], _, DepDict, _, Readers, HasAbortPrep, _PartitionType, AbortedPrep) ->
    case HasAbortPrep of true ->  {[], [], DepDict, Readers, remove_ppd, AbortedPrep};
                         false ->  {[], [], DepDict, Readers, remove_pd, AbortedPrep}
    end;
deal_pending_records([{repl_prepare, TxId, PPTime, Value, PendingReaders}|PWaiter], _SCTime, 
            DepDict, _MyNode, Readers, HasAbortPrep, slave, AbortPrep) ->
    lager:warning("Returing something , has abort prep is ~p", [HasAbortPrep]),
    case HasAbortPrep of true -> {{repl_prepare, TxId, PPTime, Value, PendingReaders}, PWaiter, DepDict, Readers, remove_ppd, AbortPrep};
                        false -> {{repl_prepare, TxId, PPTime, Value, PendingReaders}, PWaiter, DepDict, Readers, remove_pd, AbortPrep}
    end;
deal_pending_records([{Type, TxId, PPTime, Value, PendingReaders}|PWaiter], SCTime, 
            DepDict, MyNode, Readers, HasAbortPrep, PartitionType, AbortPrep) ->
    lager:warning("Dealing with ~p, ~p, commit time is ~p", [TxId, PPTime, SCTime]),
    case SCTime > TxId#tx_id.snapshot_time of
        true ->
            %% Abort the current txn
            case dict:find(TxId, DepDict) of
                {ok, {_, _, _, Sender, _Type, _WriteSet}} ->
                    NewDepDict = dict:erase(TxId, DepDict),
                    lager:warning("Prepare not valid anymore! For ~p, abort to ~p, Type is ~w", [TxId, Sender, Type]),
                    gen_server:cast(Sender, {aborted, TxId, MyNode}),
                    %% Abort should be fast, so send abort to myself directly.. Coord won't send abort to me again.
                    clocksi_vnode:abort([MyNode], TxId),
                    case Type of
                        specula_commit ->
                            deal_pending_records(PWaiter, SCTime, NewDepDict, MyNode, PendingReaders++Readers, HasAbortPrep, PartitionType, AbortPrep);
                        prepared ->
                            deal_pending_records(PWaiter, SCTime, NewDepDict, MyNode, PendingReaders++Readers, max(HasAbortPrep, false), PartitionType, AbortPrep+1)
                    end;
                error ->
                     lager:warning("Prepare not valid anymore! For ~p, but it's aborted already", [TxId]),
                    %specula_utilities:deal_abort_deps(TxId),
                    deal_pending_records(PWaiter, SCTime, DepDict, MyNode, PendingReaders++Readers, HasAbortPrep, PartitionType, AbortPrep+1)
            end;
        false ->
            case Type of 
                master ->
                    case dict:find(TxId, DepDict) of
                        error ->
                            lager:warning("Can not find record for ~p", [TxId]),
                            deal_pending_records(PWaiter, SCTime, DepDict, MyNode, [], HasAbortPrep, PartitionType, AbortPrep);
                        _ ->
                            %{NewDepDict, Remaining} = abort_others(PPTime, PWaiter, DepDict, MyNode, []),
                            lager:warning("Returing something , has abort prep is ~p", [HasAbortPrep]),
                            case HasAbortPrep of 
                                true -> {{Type, TxId, PPTime, Value, PendingReaders}, PWaiter, DepDict, Readers, remove_ppd, AbortPrep};
                                false -> {{Type, TxId, PPTime, Value, PendingReaders}, PWaiter, DepDict, Readers, remove_pd, AbortPrep}
                            end
                            %[{prepared, TxId, PPTime, LastReaderTime, LastPPTime, Value, []}|Remaining]
                    end;
                _ -> %% slave or cache
                    case HasAbortPrep of 
                        true -> {{Type, TxId, PPTime, Value, PendingReaders}, PWaiter, DepDict, Readers, remove_ppd, AbortPrep};
                        false -> {{Type, TxId, PPTime, Value, PendingReaders}, PWaiter, DepDict, Readers, remove_pd, AbortPrep}
                    end
            end
    end.

%% Update its entry in DepDict.. If the transaction can be prepared already, prepare it
%% (or just replicate it).. Otherwise just update and do nothing. 
%% Three cases:
%%   false -> reduce pd
%%   true -> only reduce ppd but do not increase pd. This is because the current record is 
%%           already in the first of the queue for some reason
%%   convert_to_pd -> reducd pd, but because it is not preceded by a s.c. record, also increase
%%           pd.
%% Only recuce ppd but do not increase pd
unblock_prepare(TxId, DepDict, _Partition, convert_to_pd) ->
     lager:warning("Trying to unblocking transaction ~p", [TxId]),
    case dict:find(TxId, DepDict) of
        {ok, {PendPrepDep, PrepareTime, Sender}} ->
            lager:warning("PendPrepDep is ~w", [PendPrepDep]),
            case PendPrepDep of
                1 -> gen_server:cast(Sender, {prepared, TxId, PrepareTime, self()}), dict:erase(TxId, DepDict); 
                _ -> dict:store(TxId, {PendPrepDep-1, PrepareTime, Sender}, DepDict) 
            end;
        {ok, {1, PrepDep, PrepareTime, Sender, RepMode, WriteSet}} ->
            lager:warning("Repmode is ~w", [RepMode]),
            case RepMode of local -> gen_server:cast(Sender, {pending_prepared, TxId, PrepareTime, self()});
                            _ -> ok
            end,
            dict:store(TxId, {0, PrepDep+1, PrepareTime, Sender, RepMode, WriteSet}, DepDict);
        {ok, {N, PrepDep, PrepareTime, Sender, RepMode, WriteSet}} ->
            lager:warning("~p updates dep to ~p", [TxId, N-1]),
            dict:store(TxId, {N-1, PrepDep+1, PrepareTime, Sender, RepMode, WriteSet}, DepDict);
        error -> lager:warning("Unblock txn: no record in dep dict!"),  
            DepDict
    end;
%% Reduce prepared dependency 
unblock_prepare(TxId, DepDict, Partition, RemoveDepType) ->
    lager:warning("Trying to unblocking prepared transaction ~p, RemveDepType is ~p", [TxId, RemoveDepType]),
    case dict:find(TxId, DepDict) of
        {ok, {PendPrepDep, PrepareTime, Sender}} ->
            lager:warning("~p Removing in slave replica", [TxId]),
            case PendPrepDep of
                1 -> gen_server:cast(Sender, {prepared, TxId, PrepareTime, self()}), dict:erase(TxId, DepDict); 
                _ -> dict:store(TxId, {PendPrepDep-1, PrepareTime, Sender}, DepDict) 
            end;
        {ok, {0, 1, PrepareTime, Sender, local}} ->
            lager:warning("~p Removing in the last prep dep", [TxId]),
            RemoveDepType = remove_pd,
            gen_server:cast(Sender, {remove_pend_prepare, TxId, PrepareTime, self()}),
            dict:erase(TxId, DepDict);
        {ok, {0, N, PrepareTime, Sender, local}} ->
            lager:warning("~p Removing in the last prep dep", [TxId]),
            RemoveDepType = remove_pd,
            dict:store(TxId, {0, N-1, PrepareTime, Sender, local}, DepDict);
        {ok, {0, 1, PrepareTime, Sender, RepMode, TxWriteSet}} ->
            lager:warning("~p Removing in the last prep dep", [TxId]),
            RemoveDepType = remove_pd,
            gen_server:cast(Sender, {remove_pend_prepare, TxId, PrepareTime, self()}),
            RepMsg = {Sender, RepMode, TxWriteSet, PrepareTime}, 
            repl_fsm:repl_prepare(Partition, prepared, TxId, RepMsg), 
            %DepDict1 = dict:update_counter(success_wait, 1, DepDict), 
            dict:erase(TxId, DepDict);
        {ok, {1, PrepDep, PrepareTime, Sender, RepMode, TxWriteSet}} ->
            lager:warning("~w Herre", [TxId]),
            case RemoveDepType of 
                remove_ppd ->
                    lager:warning("~p Removing ppd, PrepDep is ~w", [TxId, PrepDep]),
                    case PrepDep of 
                        0 -> gen_server:cast(Sender, {prepared, TxId, PrepareTime, self()}), 
                            %case RepMode of local -> ok;
                            RepMsg = {Sender, RepMode, TxWriteSet, PrepareTime},
                            repl_fsm:repl_prepare(Partition, prepared, TxId, RepMsg),
                            %end,
                            dict:erase(TxId, DepDict);
                        _ ->
                            gen_server:cast(Sender, {pending_prepared, TxId, PrepareTime, self()}),
                            case RepMode of 
                                local ->
                                    RepMsg = {Sender, RepMode, TxWriteSet, PrepareTime},
                                    repl_fsm:repl_prepare(Partition, prepared, TxId, RepMsg),
                                    dict:store(TxId, {0, PrepDep, PrepareTime, Sender, RepMode}, DepDict);
                                _ ->
                                    dict:store(TxId, {0, PrepDep, PrepareTime, Sender, RepMode, TxWriteSet}, DepDict)
                            end
                    end;
                remove_pd -> 
                    lager:warning("~p Removing pd", [TxId]),
                    dict:store(TxId, {1, PrepDep-1, PrepareTime, Sender, RepMode}, DepDict)
            end;
        {ok, {PendPrepDep, PrepDep, PrepareTime, Sender, RepMode, WriteSet}} ->
            lager:warning("~w Herre", [TxId]),
            case RemoveDepType of 
                remove_ppd -> dict:store(TxId, {PendPrepDep-1, PrepDep, PrepareTime, Sender, RepMode, WriteSet}, DepDict);
                remove_pd -> dict:store(TxId, {PendPrepDep, PrepDep-1, PrepareTime, Sender, RepMode, WriteSet}, DepDict)
            end;
        error -> lager:warning("Unblock txn: no record in dep dict!"),  
            DepDict
    end.

reply({relay, Sender}, Result) ->
    gen_server:reply(Sender, Result);
reply(Sender, Result) ->
    riak_core_vnode:reply(Sender, Result).

specula_commit([], _TxId, _SCTime, _InMemoryStore, _PreparedTxs, DepDict, _Partition, _PartitionType) ->
    %dict:update(commit_diff, fun({Diff, Cnt}) -> {Diff+SCTime-PrepareTime, Cnt+1} end, DepDict);
    DepDict;
specula_commit([Key|Rest], TxId, SCTime, InMemoryStore, PreparedTxs, DepDict, Partition, PartitionType) ->
    lager:warning("Trying to insert key ~p with for ~p, specula commit time is ~p", [Key, TxId, SCTime]),
    MyNode = {Partition, node()},
    case ets:lookup(PreparedTxs, Key) of
        %% If this one is prepared, no one else can be specula-committed already, so sc-time should be the same as prep time 
        [{Key, [{prepared, TxId, _PrepareTime, LastReaderTime, LastPrepTime, PrepNum, Value, PendingReaders}|Deps] }] ->
            lager:warning("Here, deps are ~w", [Deps]),
            {Head, Record, DepDict1, AbortedReaders, _, AbortPrep} = deal_pending_records(Deps, SCTime, DepDict, MyNode, [], false, PartitionType, 0),
            case Head == [] of
                true -> Record = [],
                true = ets:insert(PreparedTxs, {Key, [{specula_commit, TxId, SCTime, LastReaderTime, max(SCTime, LastPrepTime), 0, Value, []}]});
                false ->
                true = ets:insert(PreparedTxs, {Key, [{specula_commit, TxId, SCTime, LastReaderTime, max(SCTime, LastPrepTime), PrepNum-1-AbortPrep, Value, []}|[Head|Record]]})
            end,
            DepDict2 = case Head of
                [] -> DepDict1;
                {repl_prepare, _, _, _, _} -> DepDict1;
                {prepared, HTxId, _HPTime, _HValue, _HReaders} -> unblock_prepare(HTxId, DepDict1, Partition, convert_to_pd)
            end,
            case PartitionType of
                cache ->
                      lists:foreach(fun({ReaderTxId, Node, {relay, Sender}}) ->
                                SnapshotTime = ReaderTxId#tx_id.snapshot_time,
                                case SnapshotTime >= SCTime of
                                    true ->
                                        reply({relay, Sender}, {ok, Value});
                                    false ->
                                        lager:warning("Relaying read of ~w to ~w", [ReaderTxId, Node]),
                                        {_, RealKey} = Key,
                                        clocksi_vnode:relay_read(Node, RealKey, ReaderTxId, Sender, false)
                                end end,
                            AbortedReaders++PendingReaders);
                _ ->
                    lists:foldl(fun({ReaderTxId, ignore, Sender}, PrevCommittedVal) ->
                              case ReaderTxId#tx_id.snapshot_time >= SCTime of
                                  true ->
                                      reply(Sender, {ok, Value}), PrevCommittedVal;
                                  false ->
                                      ToReplyVal = case PrevCommittedVal of
                                                        ignore ->
                                                            case ets:lookup(InMemoryStore, Key) of
                                                                [{Key, ValList}] ->
                                                                    {_, LastCommittedVal} = hd(ValList),
                                                                    LastCommittedVal;
                                                                [] -> []
                                                            end;
                                                        _ -> PrevCommittedVal
                                                   end,
                                      reply(Sender, {ok, ToReplyVal}),
                                      ToReplyVal
                              end end, ignore, PendingReaders ++ AbortedReaders)
            end,
            specula_commit(Rest, TxId, SCTime, InMemoryStore, 
                  PreparedTxs, DepDict2, Partition, PartitionType);
        [{Key, [{specula_commit, OtherTxId, MySCTime, LastReaderTime, LastPrepTime, PrepNum, Value, OtherPendReaders}|RecordList]}] ->
            lager:warning("iN Spec commit"),
            case find_prepare_record(RecordList, TxId) of
                [] -> 
                    specula_commit(Rest, TxId, SCTime, InMemoryStore, 
                          PreparedTxs, DepDict, Partition, PartitionType);
                {Prev, {TxId, TxPrepTime, TxSCValue, PendingReaders}, RestRecords} ->
                    lager:warning("After finding record"),
                    {Head, Record, DepDict1, AbortedReaders, _, AbortPrep} = deal_pending_records(RestRecords, SCTime, DepDict, MyNode, [], false, PartitionType, 0),
                    case PartitionType of
                        cache ->
                            lists:foreach(fun({ReaderTxId, Node, {relay, Sender}}) ->
                                  SnapshotTime = ReaderTxId#tx_id.snapshot_time,
                                  case SnapshotTime >= SCTime of
                                      true ->
                                          reply({relay, Sender}, {ok, Value});
                                      false ->
                                          {_, RealKey} = Key,
                                           lager:warning("Relaying read of ~w to ~w", [ReaderTxId, Node]),
                                          clocksi_vnode:relay_read(Node, RealKey, ReaderTxId, Sender, false)
                                  end end,
                              AbortedReaders++PendingReaders);
                        _ ->
                            lists:foldl(fun({ReaderTxId, ignore, Sender}, PrevSCValue) ->
                                    case ReaderTxId#tx_id.snapshot_time >= SCTime of
                                        true -> reply(Sender, {ok, TxSCValue}), PrevSCValue;
                                        false ->
                                                ToReplyV = case PrevSCValue of
                                                                ignore ->
                                                                    case Prev of [] -> Value;
                                                                                 _ -> {_, _, _, V, _} = lists:last(Prev), V
                                                                    end;
                                                                _ -> PrevSCValue
                                                            end,
                                                reply(Sender, {ok, ToReplyV}), ToReplyV
                                    end end, ignore, PendingReaders ++ AbortedReaders)
                    end,
                    DepDict2 = case Head of
                        [] -> DepDict1;
                        {repl_prepare, _, _, _, _} -> DepDict1;
                        {prepared, HTxId, _HPTime, _HValue, _HReaders} -> unblock_prepare(HTxId, DepDict1, Partition, convert_to_pd)
                    end,
                    RemainRecords = case Head of [] -> Prev ++ [{specula_commit, TxId, SCTime, TxSCValue, []}|Record];
                                                 _ ->  Prev ++ [{specula_commit, TxId, SCTime, TxSCValue, []}|[Head|Record]]
                                    end,
                    %%% Just to expose more erros if possible
                    true = TxPrepTime =< SCTime,
                    lager:warning("Other transaction is ~p, Head is ~p, Record is ~p", [OtherTxId, Head, Record]),
                    true = ets:insert(PreparedTxs, {Key, [{specula_commit, OtherTxId, MySCTime, LastReaderTime, max(SCTime, LastPrepTime), PrepNum-1-AbortPrep, Value, OtherPendReaders}|RemainRecords]}),
                    specula_commit(Rest, TxId, SCTime, InMemoryStore, PreparedTxs,
                        DepDict2, Partition, PartitionType)
            end;
        R ->
            %% TODO: Need to fix this
            lager:error("The txn is actually aborted already ~w, ~w", [Key, R]),
            specula_commit(Rest, TxId, SCTime, InMemoryStore, PreparedTxs,
                DepDict, Partition, PartitionType)
            %R = 2
    end.

find_prepare_record(RecordList, TxId) ->
    find_prepare_record(RecordList, TxId, []).
  
find_prepare_record([], _TxId, _Prev) ->
    [];
find_prepare_record([{prepared, TxId, TxPrepTime, Value, Readers}|Rest], TxId, Prev) ->
    {lists:reverse(Prev), {TxId, TxPrepTime, Value, Readers}, Rest};
find_prepare_record([Record|Rest], TxId, Prev) ->
    find_prepare_record(Rest, TxId, [Record|Prev]).

%% TODO: allowing all speculative read now! Maybe should not be so aggressive
specula_read(TxId, Key, PreparedTxs, SenderInfo) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(PreparedTxs, Key) of
        [] ->
             lager:warning("Nothing prepared!!"),
            ets:insert(PreparedTxs, {Key, SnapshotTime}),
            ready;
        [{Key, [{Type, PreparedTxId, PrepareTime, LastReaderTime, LastPPTime, CanPendPrep, Value, PendingReader}| PendingPrepare]}] ->
              lager:warning("~p: has ~p with ~p, Type is ~p, lastpp time is ~p, pending prepares are ~p",[Key, PreparedTxId, PrepareTime, Type, LastPPTime, PendingPrepare]),
            case PrepareTime =< SnapshotTime of
                true ->
                    %% The read is not ready, may read from speculative version 
                    {ToReadTx, ToReadValue, Record} =
                        read_appr_version(TxId, SnapshotTime, PendingPrepare, SenderInfo),
                    NextReaderTime = max(SnapshotTime, LastReaderTime),
                    case ToReadTx of
                        first ->
                            %% There is more than one speculative version
                            case sc_by_local(TxId) and (Type == specula_commit) of
                                true ->
                                    lager:warning("~p finally reading specula version ~p", [TxId, Value]),
                                    add_read_dep(TxId, PreparedTxId, Key, PreparedTxs),
                                    case SnapshotTime > LastReaderTime of
                                        true -> ets:insert(PreparedTxs, [{Key, [{specula_commit, PreparedTxId, PrepareTime, SnapshotTime, LastPPTime, CanPendPrep, Value, PendingReader}| PendingPrepare]}]);
                                        false -> ok
                                    end,
                                    {specula, Value};
                                false ->
                                    lager:warning("~p can not read this version, not by local or not specula commit, Type is ~p, PrepTx is ~p", [TxId, Type, PreparedTxId]),
                                    ets:insert(PreparedTxs, {Key, [{Type, PreparedTxId, PrepareTime, NextReaderTime, LastPPTime, CanPendPrep,
                                        Value, [SenderInfo|PendingReader]}| PendingPrepare]}),
                                    not_ready
                            end;
                        not_ready ->
                            lager:warning("Key ~p: ~p can not read this version, not ready, Record is ~w", [Key, TxId, Record]),
                            ets:insert(PreparedTxs, {Key, [{Type, PreparedTxId, PrepareTime, NextReaderTime, LastPPTime, CanPendPrep, Value,
                                PendingReader}| Record]}),
                            not_ready;
                        _ ->
                            %% There is more than one speculative version
                            lager:warning("Going to read specual value"),
                            add_read_dep(TxId, ToReadTx, Key, PreparedTxs),
                            case SnapshotTime > LastReaderTime of
                                true -> lager:warning("Inserting here"), 
                                        ets:insert(PreparedTxs, [{Key, [{Type, PreparedTxId, PrepareTime, SnapshotTime, LastPPTime, CanPendPrep, Value, PendingReader}| PendingPrepare]}]);
                                false -> lager:warning("No need to insert"),  
                                        ok
                            end,
                            {specula, ToReadValue}
                    end; 
                false ->
                    ready
            end;
        [{Key, LastReaderTime}] ->
            ets:insert(PreparedTxs, {Key, max(SnapshotTime, LastReaderTime)}),
            ready
    end.

add_read_dep(ReaderTx, WriterTx, _Key, PreparedTxs) ->
%    lager:warning("Inserting anti_dep from ~p to ~p", [ReaderTx, WriterTx]),
    case ets:lookup(PreparedTxs, {dep, WriterTx}) of
        [] ->  
            lager:warning("Add read dep of ~w for ~w", [WriterTx, ReaderTx]), 
            ets:insert(PreparedTxs, {{dep, WriterTx}, [ReaderTx]});
        [{_, V}] ->
            lager:warning("Add read dep of ~w for ~w, V is ~w", [WriterTx, ReaderTx, V]), 
            ets:insert(PreparedTxs, {{dep, WriterTx}, [ReaderTx|V]})
    end.
%    ets:insert(anti_dep, {ReaderTx, WriterTx}).


delete_and_read(DeleteType, InMemoryStore, TxCommitTime, Key, DepDict, PartitionType, Partition, [{Type, TxId, _Time, Value, PendingReaders}|Rest], TxId, [], {PType, PTxId, PPrepTime, PLastReaderTime, PSCTime, PrepNum, PValue, PPendingReaders}, 0) ->
    %% If can read previous version: read
    %% If can not read previous version: add to previous pending
    RemainReaders = case DeleteType of 
                        commit ->
                            case PartitionType of 
                                cache -> ok;
                                _ -> 
                                    case ets:lookup(InMemoryStore, Key) of
                                        [] ->
                                            true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, Value}]});
                                        [{Key, ValueList}] ->
                                            {RemainList, _} = lists:split(min(?NUM_VERSION,length(ValueList)), ValueList),
                                            true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, Value}|RemainList]})
                                    end
                            end,
                            lists:foldl(fun({ReaderTxId, Node, Sender}, BlockReaders) ->
                                                    case ReaderTxId#tx_id.snapshot_time >= TxCommitTime of
                                                        true -> reply(Sender, {ok, PValue}), BlockReaders;
                                                        false -> [{ReaderTxId, Node, Sender}|BlockReaders] 
                                                    end
                                        end, [], PendingReaders);
                            %% Add to store and add to committxed txn for master and slave partition
                        abort ->
                            PendingReaders
                    end,
    {Head, RRecord, DepDict1, AbortedReaders, RemoveDepType, AbortNum}
        = deal_pending_records(Rest, TxCommitTime, DepDict, {Partition, node()}, [], false, PartitionType, 0),
    lager:warning("Pending readers are ~w, aborted readers are ~w", [RemainReaders, AbortedReaders]),
    NewPendingReaders = 
        case PType of 
            specula_commit ->
                lists:foldl(fun({ReaderTxId, Node, Sender}, BlockReaders) ->
                    case sc_by_local(ReaderTxId) of
                        true ->
                            %add_read_dep(ReaderTxId, PTxId, Key),
                            reply(Sender, {specula, PValue}),
                            BlockReaders;
                        false -> [{ReaderTxId, Node, Sender}|BlockReaders]    
                    end 
                end, PPendingReaders, RemainReaders++AbortedReaders);
            _ -> PPendingReaders++RemainReaders++AbortedReaders
        end,
    RPrepNum = case Type of specula_commit -> PrepNum-AbortNum; _ -> PrepNum-AbortNum-1 end, 
    lager:warning("Head is ~w", [Head]),
    case Head of 
        [] -> RRecord = [], 
            {[{PType, PTxId, PPrepTime, PLastReaderTime, PSCTime, RPrepNum, PValue, NewPendingReaders}], DepDict1, 0}; 
        {repl_prepare, _HTxId, _HPTime, _HValue, _HReaders} -> 
            {[{PType, PTxId, PPrepTime, PLastReaderTime, PSCTime, RPrepNum, PValue, NewPendingReaders}|[Head|RRecord]], DepDict1, 0}; 
        {_, HTxId, _HPTime, _HValue, _HReaders} -> 
            DepDict2 = case Type of 
                            specula_commit -> unblock_prepare(HTxId, DepDict1, Partition, RemoveDepType);
                            _ -> %% Type is repl_prepare or prepared 
                                case PType of specula_commit ->unblock_prepare(HTxId, DepDict1, Partition, convert_to_pd);
                                           _ -> DepDict1 
                                end
                        end,
            {[{PType, PTxId, PPrepTime, PLastReaderTime, PSCTime, RPrepNum, PValue, NewPendingReaders}|[Head|RRecord]], DepDict2, 0}
    end;
delete_and_read(DeleteType, InMemoryStore, TxCommitTime, Key, DepDict, PartitionType, Partition, [{Type, TxId, _Time, Value, PendingReaders}|Rest], TxId, Prev, {PType, PTxId, PPrepTime, PValue, PPendingReaders}, CAbortPrep) ->
    RemainReaders = case DeleteType of 
                        commit ->
                            case PartitionType of 
                                cache -> ok;
                                _ -> 
                                    case ets:lookup(InMemoryStore, Key) of
                                        [] ->
                                            true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, Value}]});
                                        [{Key, ValueList}] ->
                                            {RemainList, _} = lists:split(min(?NUM_VERSION,length(ValueList)), ValueList),
                                            true = ets:insert(InMemoryStore, {Key, [{TxCommitTime, Value}|RemainList]})
                                    end
                            end,
                            lists:foldl(fun({ReaderTxId, Node, Sender}, BlockReaders) ->
                                            case ReaderTxId#tx_id.snapshot_time >= TxCommitTime of
                                                true -> reply(Sender, {ok, PValue}), BlockReaders;
                                                false -> [{ReaderTxId, Node, Sender}|BlockReaders] 
                                            end
                                        end, [], PendingReaders);
                            %% Add to store and add to committxed txn for master and slave partition
                        abort ->
                            PendingReaders
                    end,
    %% If can read previous version: read
    %% If can not read previous version: add to previous pending
    {Head, RRecord, DepDict1, AbortedReaders, RemoveDepType, AbortNum}
        = deal_pending_records(Rest, TxCommitTime, DepDict, {Partition, node()}, [], false, PartitionType, 0),
    lager:warning("Pending readers are ~w, aborted readers are ~w", [RemainReaders, AbortedReaders]),
    NewPendingReaders = 
        case PType of 
            specula_commit ->
                lists:foldl(fun({ReaderTxId, Node, Sender}, BlockReaders) ->
                    case sc_by_local(ReaderTxId) of
                        true ->
                            %add_read_dep(ReaderTxId, PTxId, Key),
                            reply(Sender, {specula, PValue}),
                            BlockReaders;
                        false -> [{ReaderTxId, Node, Sender}|BlockReaders]    
                    end 
                end, PPendingReaders, RemainReaders++AbortedReaders);
            _ -> PPendingReaders++RemainReaders++AbortedReaders
        end,
    NewCAbortPrep = case Type of specula_commit -> CAbortPrep+AbortNum; _ -> CAbortPrep+AbortNum+1 end, 
    case Head of 
        [] -> RRecord = [], 
            {lists:reverse(Prev)++[{PType, PTxId, PPrepTime, PValue, NewPendingReaders}], DepDict1, NewCAbortPrep};
        {repl_prepare, _HTxId, _HPTime, _HValue, _HReaders} -> 
            {lists:reverse(Prev)++[{PType, PTxId, PPrepTime, PValue, NewPendingReaders}|[Head|RRecord]], DepDict1, NewCAbortPrep};
        {Type, HTxId, _, _, _} -> 
            DepDict2 = case Type of specula_commit -> unblock_prepare(HTxId, DepDict1, Partition, RemoveDepType);
                         _ -> %% Type is repl_prepare or prepared 
                            case PType of specula_commit ->unblock_prepare(HTxId, DepDict1, Partition, convert_to_pd);
                                           _ -> DepDict1 
                            end
            end,
            {lists:reverse(Prev)++[{PType, PTxId, PPrepTime, PValue, NewPendingReaders}|[Head|RRecord]], DepDict2, NewCAbortPrep}
    end;
delete_and_read(DeleteType, InMemoryStore, TxCommitTime, Key, DepDict, PartitionType, MyNode, [{Type, OtherTxId, Time, Value, PendingReaders}|Rest], TxId, Prev, {PType, PTxId, PPrepTime, PLastReaderTime, PSCTime, PrepNum, PValue, PPendingReaders}, CAbortPrep) ->
    Prev1 = [{PType, PTxId, PPrepTime, PLastReaderTime, PSCTime, PrepNum, PValue, PPendingReaders}|Prev],
    delete_and_read(DeleteType, InMemoryStore, TxCommitTime, Key, DepDict, PartitionType, MyNode, Rest, TxId, Prev1, {Type, OtherTxId, Time, Value, PendingReaders}, CAbortPrep);
delete_and_read(DeleteType, InMemoryStore, TxCommitTime, Key, DepDict, PartitionType, MyNode, [{Type, OtherTxId, Time, Value, PendingReaders}|Rest], TxId, Prev, {PType, PTxId, PPrepTime, PValue, PPendingReaders}, CAbortPrep) ->
    Prev1 = [{PType, PTxId, PPrepTime, PValue, PPendingReaders}|Prev],
        delete_and_read(DeleteType, InMemoryStore, TxCommitTime, Key, DepDict, PartitionType, MyNode, Rest, TxId, Prev1, {Type, OtherTxId, Time, Value, PendingReaders}, CAbortPrep);
delete_and_read(abort, _InMemoryStore, _TxCommitTime, _Key, DepDict, _PartitionType, _MyNode, [], _TxId, _Prev, _Whatever, 0) ->
    lager:warning("Abort but got nothing"),
    {[], DepDict, 0}.

read_appr_version(ReaderTxId, SnapshotTime, PendingPrepare, SenderInfo) ->
    case PendingPrepare of
        [] ->
            {first, [], []};
        _ ->
            find(ReaderTxId, SnapshotTime, PendingPrepare, first, [], SenderInfo)
    end.

find(ReaderTxId, SnapshotTime, [], ToReturn, AllPrevious, SenderInfo) ->
    case ToReturn of
        first ->
            {first, [], []};
        {specula_commit, SCTxId, SCTime, SCValue, SCPendingReaders} ->
            case sc_by_local(ReaderTxId) of
                true ->
                    lager:warning("~p reads specula value ~p!", [SCTxId, SCValue]),
                    {SCTxId, SCValue, []};
                false ->
                    lager:warning("~p can read specula value, because not by local!", [SCTxId]),
                     lager:warning("Allprevious are ~w", [AllPrevious]),
                    case AllPrevious of 
                        [] -> {not_ready, [], [{specula_commit, SCTxId, SCTime, SCValue, [SenderInfo|SCPendingReaders]}]};
                        _ ->
                            {not_ready, [], lists:revese(AllPrevious)++ [{specula_commit, SCTxId, SCTime, SCValue, [SenderInfo|SCPendingReaders]}]}
                    end
            end;
        {prepared, PTxId, PTime, Value, PendingReaders} ->
            lager:warning("~p can not read specula value, because still prepared!", [PTxId]),
            case SnapshotTime >= PTime + ?SPECULA_THRESHOLD of
                true ->
                    lager:warning("Allprevious are ~w", [AllPrevious]),
                    case AllPrevious of
                        [] -> {not_ready, [], [{prepared, PTxId, PTime, Value, [SenderInfo|PendingReaders]}]};
                        _ -> {not_ready, [], lists:reverse(AllPrevious) ++ [{prepared, PTxId, PTime, Value, [SenderInfo|PendingReaders]}]}
                    end
            end;
        {repl_prepare, PTxId, PTime, Value, PendingReaders} ->
            lager:warning("~p can read specula value, because still prepared!", [PTxId]),
            case SnapshotTime >= PTime + ?SPECULA_THRESHOLD of
                true ->
                    lager:warning("Allprevious are ~w", [AllPrevious]),
                    case AllPrevious of
                        [] -> {not_ready, [], [{repl_prepare, PTxId, PTime, Value, [SenderInfo|PendingReaders]}]};
                        _ ->
                            {not_ready, [], lists:reverse(AllPrevious) ++ [{repl_prepare, PTxId, PTime, Value, [SenderInfo|PendingReaders]}]}
                    end
            end
    end;
find(ReaderTxId, SnapshotTime, [{Type, TxId, Time, Value, PendingReaders}|Rest]=AllRest, ToReturn, AllPrevious, SenderInfo) ->
    case SnapshotTime < Time of
        true ->
            case ToReturn of
                first ->
                    %% Just to test that the logic is not wrong
                    AllPrevious = [],
                    {first, [], []};
                {specula_commit, SCTxId, SCTime, SCValue, SCPendingReaders} ->
                    case sc_by_local(ReaderTxId) of
                        true ->
                            {SCTxId, SCValue, []};
                        false ->
                            {not_ready, [], lists:reverse(AllPrevious)++[{specula_commit, SCTxId, SCTime, SCValue, [SenderInfo|SCPendingReaders]}|AllRest]}
                    end;
                {repl_prepare, RPTxId, RPTime, RPValue, RPReaders} ->
                    case SnapshotTime >= RPTime + ?SPECULA_THRESHOLD of
                        true ->
                            {not_ready, [], lists:reverse(AllPrevious) ++ [{repl_prepare, RPTxId, RPTime, RPValue, [SenderInfo|RPReaders]}|AllRest]}
                    end;
                {prepared, PTxId, PTime, PValue, PReaders} ->
                    case SnapshotTime >= PTime + ?SPECULA_THRESHOLD of
                        true ->
                            {not_ready, [], lists:reverse(AllPrevious) ++ [{prepared, PTxId, PTime, PValue, [SenderInfo|PReaders]}|AllRest]}
                    end
            end;
        _ ->
            case ToReturn of 
                first ->
                    AllPrevious = [],
                    lager:warning("Here"),
                    find(ReaderTxId, SnapshotTime, Rest, {Type, TxId, Time, Value, PendingReaders}, [], SenderInfo);
                _ ->
                    lager:warning("Filling ~w", [[ToReturn|AllPrevious]]),
                    find(ReaderTxId, SnapshotTime, Rest, {Type, TxId, Time, Value, PendingReaders}, [ToReturn|AllPrevious], SenderInfo)
            end 
    end.

sc_by_local(TxId) ->
    [{dc_id, DcId}] = ets:lookup(meta_info, dc_id),
    TxId#tx_id.dc_id == DcId.

insert_prepare(PreparedTxs, TxId, Partition, WriteSet, TimeStamp, Sender) ->
    case ets:lookup(PreparedTxs, {TxId, Partition}) of
          [] ->
              {KeySet, ToPrepTS} =   lists:foldl(fun({Key, _Value}, {KS, Ts}) ->
                                          case ets:lookup(PreparedTxs, Key) of
                                              [] -> {[Key|KS], Ts};
                                              [{Key, [{_Type, _PrepTxId, _, LastReaderTS, _, _, _, _}|_Rest]}] ->
                                                  {[Key|KS], max(Ts, LastReaderTS+1)};
                                              [{Key, LastReaderTS}] ->  
                                                     lager:warning("For ~p, last reader ts is ~p", [Key, LastReaderTS]),
                                                  {[Key|KS], max(Ts, LastReaderTS+1)}
                                          end end, {[], TimeStamp}, WriteSet),
              lists:foreach(fun({Key, Value}) ->
                          lager:warning("Trying to insert key ~p", [Key]),
                          case ets:lookup(PreparedTxs, Key) of
                              [] ->
                                  true = ets:insert(PreparedTxs, {Key, [{repl_prepare, TxId, ToPrepTS, ToPrepTS, ToPrepTS, 1, Value, []}]});
                              [{Key, [{_, _, _, _, _, _, _, _}|_Rest]=L}] ->
                                  NewList = add_to_list(TxId, ToPrepTS, Value, L),
                                  true = ets:insert(PreparedTxs, {Key, NewList});
                              [{Key, _}] -> true = ets:insert(PreparedTxs, {Key, [{repl_prepare, TxId, ToPrepTS, ToPrepTS, ToPrepTS, 1, Value, []}]})
              end end,  WriteSet),
              lager:warning("Got repl prepare for ~p, propose ~p and replied", [TxId, ToPrepTS]),
              ets:insert(PreparedTxs, {{TxId, Partition}, KeySet}),
              gen_server:cast(Sender, {remove_pend_prepare, TxId, ToPrepTS, self()});
          R ->
              lager:warning("Not replying for ~p, ~p because already prepard, Record is ~p", [TxId, Partition, R]),
              ok
      end.

add_to_list(ToPrepTxId, ToPrepTS, ToPrepValue, [{Type, PrepTxId, PrepTS, LastReaderTS, LastSCTime, PrepNum, PrepValue, Reader}|Rest]) -> 
    [{Type, PrepTxId, PrepTS, LastReaderTS, max(LastSCTime, ToPrepTS), PrepNum+1, PrepValue, Reader}|
        add_to_list(ToPrepTxId, ToPrepTS, ToPrepValue, Rest)];
add_to_list(ToPrepTxId, ToPrepTS, ToPrepValue, [{Type, PrepTxId, PrepTS, PrepValue, Reader}|Rest] = L) ->
    case ToPrepTS < PrepTS of
        true -> [{repl_prepare, ToPrepTxId, ToPrepTS, ToPrepValue, []}|L]; 
        false -> [{Type, PrepTxId, PrepTS, PrepValue, Reader} | add_to_list(ToPrepTxId, ToPrepTS, ToPrepValue, Rest)]
    end;
add_to_list(ToPrepTxId, ToPrepTS, ToPrepValue, []) ->
    [{repl_prepare, ToPrepTxId, ToPrepTS, ToPrepValue, []}];
add_to_list(ToPrepTxId, ToPrepTS, ToPrepValue, [_LastReaderTS]) ->
    [{repl_prepare, ToPrepTxId, ToPrepTS, ToPrepTS, ToPrepTS, 1, ToPrepValue, []}].

ready_or_block(TxId, Key, PreparedTxs, SenderInfo) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(PreparedTxs, Key) of
        [] ->
            ets:insert(PreparedTxs, {Key, SnapshotTime}),
            ready;
        [{Key, [{Type, PreparedTxId, PrepareTime, LastReaderTime, LastPPTime, CanSC, Value, PendingReader}|PendingPrepare]}] ->
             lager:warning("~p Not ready.. ~p waits for ~p with ~p, others are ~p", [Key, TxId, PreparedTxId, PrepareTime, PendingReader]),
            case PrepareTime =< SnapshotTime of
                true ->
                    ets:insert(PreparedTxs, {Key, [{Type, PreparedTxId, PrepareTime, LastReaderTime, LastPPTime, CanSC, Value,
                        [SenderInfo|PendingReader]}| PendingPrepare]}),
                    %lager:error("~p non_specula reads ~p is blocked by ~p! PrepareTime is ~p", [TxId, Key, PreparedTxId, PrepareTime]),
                    not_ready;
                false ->
                    ready
            end;
        [{Key, LastReaderTime}] ->
            ets:insert(PreparedTxs, {Key, max(SnapshotTime, LastReaderTime)}),
            ready
    end.
