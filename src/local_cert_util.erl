%%-------------------------------------------------------------------
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
            pre_commit/9, specula_read/5, insert_prepare/6, reply/2]).

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
                          [{K, [{Type, PrepTxId, OldPPTime, LastReaderTime, FirstPrepTime, PrepNum, PrepValue, RWaiter}|Rest]}] ->
                              %ets:insert(PreparedTxs, {K, [{Type, PrepTxId, OldPPTime, LastReaderTime, max(LastPrepTime, PrepareTime), PrepNum+1, PrepValue, RWaiter}|
                              %         (PWaiter++[{prepared, TxId, PrepareTime, V, []}])]});
                              ets:insert(PreparedTxs, {K, [{prepared, TxId, PrepareTime, LastReaderTime, FirstPrepTime, PrepNum+1, V, []}|[{Type, PrepTxId, OldPPTime, PrepValue, RWaiter}|Rest]]});
                          _R -> 
                            lager:warning("R is ~w", [_R]),
                            ets:insert(PreparedTxs, {K, [{prepared, TxId, PrepareTime, PrepareTime, PrepareTime, 1, V, []}]})
                          end
                    end, TxWriteSet),
            ets:insert(PreparedTxs, {TxId, KeySet}),
            {wait, PendPrepDep, PrepDep, PrepareTime}
    end.

prepare_for_other_part(TxId, Partition, TxWriteSet, CommittedTxs, PreparedTxs, InitPrepTime, PartitionType)->
    KeySet = case PartitionType of cache -> [{Partition,K} || {K, _} <- TxWriteSet];
                                   slave -> [K || {K, _} <- TxWriteSet]
             end,
    lager:warning("~w: For ~w, keys: ~p", [TxId, Partition, KeySet]),
    case certification_check(InitPrepTime, TxId, KeySet, CommittedTxs, PreparedTxs, 0, 0) of
        false ->
            {error, write_conflict};
        %% Directly prepare
        {0, 0, PrepareTime} ->
            lager:warning("~p passed prepare with ~p, KeySet is ~p", [TxId, PrepareTime, KeySet]),
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
                          [] -> 
                              ets:insert(PreparedTxs, {InsertKey, [{prepared, TxId, PrepareTime, PrepareTime, PrepareTime, 1, V, []}]});
                          [{InsertKey, [{Type, PrepTxId, OldPPTime, LastRTime, FirstPrepTime, PrepNum, PrepValue, RWaiter}|PWaiter]}] ->
                              lager:warning("Key is ~w, After insertion is ~w", [K, [{prepared, TxId, PrepareTime, LastRTime, FirstPrepTime, PrepNum+1, V, []}|[{Type, PrepTxId, OldPPTime, PrepValue, RWaiter}|PWaiter]]]),
                              ets:insert(PreparedTxs, {InsertKey, [{prepared, TxId, PrepareTime, LastRTime, FirstPrepTime, PrepNum+1, V, []}|[{Type, PrepTxId, OldPPTime, PrepValue, RWaiter}|PWaiter]]});
                          _R -> 
                              lager:warning("R is ~w", [_R]),
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
        [{Key, [{_Type, _PrepTxId, _PrepareTime, LastReaderTime, _FirstPrepTime, PrepNum, _PrepValue, _RWaiter}|_PWaiter]=_Record}] ->
             lager:warning("For key ~p: ~p prepared already! FirstPrepTime is ~p, ~p may fail, PrepNum is ~w", [Key, _PrepTxId, _FirstPrepTime, TxId, PrepNum]),
            case _PrepareTime > SnapshotTime of
                true ->
                    false;
                false ->
                     lager:warning("~p: ~p waits for ~p that is ~p with ~p, PrepNum is ~w", [Key, TxId, _PrepTxId, _Type, _PrepareTime, PrepNum]),
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
    case ets:lookup(PreparedTxs, Key) of
        [{Key, [{_Type, TxId, _PrepareTime, LRTime, _FirstPrepTime, _PrepNum, Value, PendingReaders}|Others]}] ->
            lager:warning("Key ~p: ~p Pending readers are ~p! Others are ~p", [Key, TxId, PendingReaders, Others]),
             lager:warning("Trying to insert key ~p with for ~p, Type is ~p, prepnum is  is ~p, Commit time is ~p", [Key, TxId, _Type, _PrepNum, TxCommitTime]),
            AllPendingReaders = lists:foldl(fun({_, _, _, _ , Readers}, CReaders) ->
                                       Readers++CReaders end, PendingReaders, Others), 
            case PartitionType of
                cache ->  
                    lists:foreach(fun({ReaderTxId, Node, Sender}) ->
                            SnapshotTime = ReaderTxId#tx_id.snapshot_time,
                            case SnapshotTime >= TxCommitTime of
                                true -> reply(Sender, {ok, Value});
                                false ->
                                    {_, RealKey} = Key,
                                    clocksi_vnode:relay_read(Node, RealKey, ReaderTxId, Sender, false)
                            end end, AllPendingReaders);
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
                        AllPendingReaders)
            end,
            %% This is problematic! If commits arrive out-of-order, need to use a queue to store it
            ets:insert(PreparedTxs, {Key, max(TxCommitTime, LRTime)}),
            update_store(Rest, TxId, TxCommitTime, InMemoryStore, CommittedTxs, PreparedTxs,
                DepDict, Partition, PartitionType);
        [{Key, [First|Others]}] ->
            lager:warning("Trying to insert key ~p with for ~p, commit time is ~p, First is ~p, Others are ~p, PartitionType is ~p", [Key, TxId, TxCommitTime, First, Others, PartitionType]),
            DepDict2 = delete_and_read(commit, PreparedTxs, InMemoryStore, TxCommitTime, Key, DepDict, PartitionType, Partition, Others, TxId, [], First, 0),
            case PartitionType of cache -> ok; _ -> ets:insert(CommittedTxs, {Key, TxCommitTime}) end,
            update_store(Rest, TxId, TxCommitTime, InMemoryStore, CommittedTxs, PreparedTxs, DepDict2, Partition, PartitionType);
        R ->
            %[{TxId, Keys}] = ets:lookup(PreparedTxs, TxId),
            lager:error("For key ~w, txn ~w come first! Record is ~w", [Key, TxId, R]),
            update_store(Rest, TxId, TxCommitTime, InMemoryStore, CommittedTxs, PreparedTxs, DepDict, Partition, PartitionType)
    end.

clean_abort_prepared(_PreparedTxs, [], _TxId, _InMemoryStore, DepDict, _, _) ->
    DepDict; 
clean_abort_prepared(PreparedTxs, [Key | Rest], TxId, InMemoryStore, DepDict, Partition, PartitionType) ->
    case ets:lookup(PreparedTxs, Key) of
        [{Key, [{Type, TxId, _PrepTime, LastReaderTime, FirstPPTime, PrepNum, _Value, PendingReaders}|RestRecords]}] ->
            lager:warning("Aborting ~p for key ~p, PrepNum is ~w, Type is ~w", [TxId, Key, PrepNum, Type]),
            case PartitionType of
                cache -> 
                    lists:foreach(fun({ReaderTxId, Node, {relay, Sender}}) -> 
                            {_, RealKey} = Key,
                            clocksi_vnode:relay_read(Node, RealKey, ReaderTxId, Sender, false) end,
                              PendingReaders);
                _ ->
                    lists:foldl(fun({_, ignore, Sender}, ToReturn) -> 
                        case ToReturn of nil -> ToReturn1 = case ets:lookup(InMemoryStore, Key) of
                                                                [{Key, ValueList}] -> {_, V} = hd(ValueList), V;
                                                                [] -> []
                                                            end,
                                                reply(Sender, {ok, ToReturn1}), ToReturn1;
                                        _ ->  reply(Sender, {ok, ToReturn}), ToReturn end end, 
                    nil, PendingReaders)
            end,
            case RestRecords of
                [] ->
                    true = ets:insert(PreparedTxs, {Key, LastReaderTime}),
                    clean_abort_prepared(PreparedTxs,Rest,TxId, InMemoryStore, DepDict, Partition, PartitionType);
                [{HType, HTxId, HPTime, HValue, HReaders}|Others] ->
                    RPrepNum = case Type of pre_commit -> PrepNum; _ -> PrepNum-1 end,
                    true = ets:insert(PreparedTxs, {Key, [{HType, HTxId, HPTime, LastReaderTime, FirstPPTime, RPrepNum,
                            HValue, HReaders}|Others]}),
                    clean_abort_prepared(PreparedTxs,Rest,TxId, InMemoryStore, DepDict, Partition, PartitionType)
            end;
        [{Key, [First|Others]}] -> 
            lager:warning("Aborting TxId ~w, Key is ~p, First is ~w, Others are ~w", [TxId, Key, First, Others]),
            DepDict2 = delete_and_read(abort, PreparedTxs, InMemoryStore, 0, Key, DepDict, PartitionType, Partition, Others, TxId, [], First, 0),
            clean_abort_prepared(PreparedTxs,Rest,TxId, InMemoryStore, DepDict2, Partition, PartitionType);
        _R ->
            lager:warning("WTF? R is ~p", [_R]),
            clean_abort_prepared(PreparedTxs,Rest,TxId, InMemoryStore, DepDict, Partition, PartitionType)
    end.

deal_pending_records([], {Type, TxId, MySCTime, LastReaderTime, _FirstPrepTime, PrepNum, Value, PendReaders}, SCTime, DepDict, MyNode, Readers, PartitionType, NumRemovePrep, RemoveDepType) ->
    case SCTime > TxId#tx_id.snapshot_time of
        true -> 
            lager:warning("Dealing pening ~w, should abort! SCTime is ~w", [TxId, SCTime]),
            case Type of 
                repl_prepare ->
                    {{repl_prepare, TxId, MySCTime, LastReaderTime, MySCTime, PrepNum-NumRemovePrep, Value, PendReaders}, [], Readers, DepDict};
                _ ->
                    DepDict1 = case dict:find(TxId, DepDict) of
                                {ok, {_, _, Sender}} ->
                                    gen_server:cast(Sender, {aborted, TxId}),
                                    dict:erase(TxId, DepDict);
                                {ok, {_, _, _, Sender, _Type, _WriteSet}} ->
                                    gen_server:cast(Sender, {aborted, TxId, MyNode}),
                                    case PartitionType of master -> clocksi_vnode:abort([MyNode], TxId); _ -> false end,
                                    dict:erase(TxId, DepDict);
                                {ok, {_, _, _, Sender, local}} ->
                                    gen_server:cast(Sender, {aborted, TxId}),
                                    clocksi_vnode:abort([MyNode], TxId),
                                    case PartitionType of master -> clocksi_vnode:abort([MyNode], TxId); _ -> false end,
                                    dict:erase(TxId, DepDict);
                                error ->
                                    DepDict
                            end,
                    case Type of
                        pre_commit ->
                            {{LastReaderTime, PrepNum-NumRemovePrep}, [], PendReaders++Readers, DepDict1};
                        prepared ->
                            {{LastReaderTime, PrepNum-NumRemovePrep-1}, [], PendReaders++Readers, DepDict1}
                    end
            end;
        false ->
            lager:warning("Dealing pening ~w, should not abort, Partition is ~w, remove dev type ~w!", [TxId, PartitionType, RemoveDepType]),
            case PartitionType of
                master ->
                    {Partition, _} = MyNode,
                    DepDict1 = case RemoveDepType of
                                   not_remove -> DepDict;
                                   remove_pd -> 
                                        case NumRemovePrep of 
                                            0 -> unblock_prepare(TxId, DepDict, Partition, RemoveDepType);
                                            _ -> unblock_prepare(TxId, DepDict, Partition, remove_ppd)
                                        end;
                                    _ ->
                                        unblock_prepare(TxId, DepDict, Partition, RemoveDepType)
                               end,
                    {{Type, TxId, MySCTime, LastReaderTime, MySCTime, PrepNum-NumRemovePrep, Value, PendReaders}, [], Readers, DepDict1};
                _ -> %% slave or cache
                        %% May unblock him now
                      {Partition, _} = MyNode,
                      DepDict1 = case RemoveDepType of
                                   not_remove -> DepDict;
                                   remove_pd -> 
                                        case NumRemovePrep of 
                                            0 -> unblock_prepare(TxId, DepDict, Partition, RemoveDepType);
                                            _ -> unblock_prepare(TxId, DepDict, Partition, remove_ppd)
                                        end;
                                    _ ->
                                        unblock_prepare(TxId, DepDict, Partition, RemoveDepType)
                               end,
                      {{Type, TxId, MySCTime, LastReaderTime, MySCTime, PrepNum-NumRemovePrep, Value, PendReaders}, [], Readers, DepDict1}
              end
    end;
deal_pending_records([{repl_prepare, _TxId, _PPTime, _Value, _PendingReaders}|_]=List, First, _SCTime, 
            DepDict, _MyNode, Readers, slave, RemovePrepNum, _) ->
    {FType, FTxId, FPTime, LastReaderTime, FirstPrepTime, PrepNum, FValue, FPendReaders} = First,
    {{FType, FTxId, FPTime, LastReaderTime, FirstPrepTime, PrepNum-RemovePrepNum, FValue, FPendReaders},
        lists:reverse(List), Readers, DepDict}; 
deal_pending_records([{Type, TxId, _PPTime, _Value, PendingReaders}|PWaiter]=List, First, SCTime, 
            DepDict, MyNode, Readers, PartitionType, RemovePrepNum, RemoveDepType) ->
    lager:warning("Dealing with ~p, Type is ~p, ~p, commit time is ~p", [TxId, Type, _PPTime, SCTime]),
    case SCTime > TxId#tx_id.snapshot_time of
        true ->
            %% Abort the current txn
            NewDepDict = case dict:find(TxId, DepDict) of
                            {ok, {_, _, Sender}} ->
                                gen_server:cast(Sender, {aborted, TxId}),
                                dict:erase(TxId, DepDict);
                            {ok, {_, _, _, Sender, _Type, _WriteSet}} ->
                                lager:warning("Prepare not valid anymore! For ~p, abort to ~p, Type is ~w", [TxId, Sender, Type]),
                                gen_server:cast(Sender, {aborted, TxId, MyNode}),
                                case PartitionType of master -> clocksi_vnode:abort([MyNode], TxId); _ -> ok end,
                                dict:erase(TxId, DepDict);
                            {ok, {_, _, _, Sender, local}} ->
                                gen_server:cast(Sender, {aborted, TxId}),
                                case PartitionType of master -> clocksi_vnode:abort([MyNode], TxId); _ -> ok end,
                                dict:erase(TxId, DepDict);
                            error ->
                                DepDict
                        end,
            case Type of
                pre_commit ->
                    deal_pending_records(PWaiter, First, SCTime, NewDepDict, MyNode, PendingReaders++Readers, PartitionType, RemovePrepNum, RemoveDepType);
                prepared ->
                    deal_pending_records(PWaiter, First, SCTime, NewDepDict, MyNode, PendingReaders++Readers, PartitionType, RemovePrepNum+1, RemoveDepType)
            end;
        false ->
            case Type of 
                master ->
                    case dict:find(TxId, DepDict) of
                        error ->
                            deal_pending_records(PWaiter, First, SCTime, DepDict, MyNode, [], PartitionType, RemovePrepNum, RemoveDepType);
                        _ ->
                            {Partition, _} = MyNode,
                            %{NewDepDict, Remaining} = abort_others(PPTime, PWaiter, DepDict, MyNode, []),
                            DepDict1 = case RemoveDepType of
                                           not_remove -> DepDict;
                                           remove_pd -> 
                                                case RemovePrepNum of 
                                                    0 -> unblock_prepare(TxId, DepDict, Partition, RemoveDepType);
                                                    _ -> unblock_prepare(TxId, DepDict, Partition, remove_ppd)
                                                end;
                                            _ ->
                                                unblock_prepare(TxId, DepDict, Partition, RemoveDepType)
                                       end,
                            {FType, FTxId, FPTime, LastReaderTime, FirstPrepTime, PrepNum, FValue, FPendReaders} = First,
                            %[{_, _, TPrepTime, _, _}|_T] = List,
                            {{FType, FTxId, FPTime, LastReaderTime, FirstPrepTime, PrepNum-RemovePrepNum, FValue, FPendReaders},
                                    lists:reverse(List), Readers, DepDict1}
                            %[{prepared, TxId, PPTime, LastReaderTime, LastPPTime, Value, []}|Remaining]
                    end;
                _ -> %% slave or cache
                    {Partition, _} = MyNode,
                    %{NewDepDict, Remaining} = abort_others(PPTime, PWaiter, DepDict, MyNode, []),
                    DepDict1 = case RemoveDepType of
                                   not_remove -> DepDict;
                                   remove_pd -> 
                                        case RemovePrepNum of 
                                            0 -> unblock_prepare(TxId, DepDict, Partition, RemoveDepType);
                                            _ -> unblock_prepare(TxId, DepDict, Partition, remove_ppd)
                                        end;
                                    _ ->
                                        unblock_prepare(TxId, DepDict, Partition, RemoveDepType)
                               end,
                    %[{_, _, TPrepTime, _, _}|_T] = List,
                    {FType, FTxId, FPTime, LastReaderTime, FirstPrepTime, PrepNum, FValue, FPendReaders} = First,
                    {{FType, FTxId, FPTime, LastReaderTime, FirstPrepTime, PrepNum-RemovePrepNum, FValue, FPendReaders},
                            lists:reverse(List), Readers, DepDict1}
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
            gen_server:cast(Sender, {solve_pending_prepared, TxId, PrepareTime, self()}),
            dict:erase(TxId, DepDict);
        {ok, {0, N, PrepareTime, Sender, local}} ->
             lager:warning("~p Removing in the last prep dep", [TxId]),
            RemoveDepType = remove_pd,
            dict:store(TxId, {0, N-1, PrepareTime, Sender, local}, DepDict);
        {ok, {0, 1, PrepareTime, Sender, RepMode, TxWriteSet}} ->
             lager:warning("~p Removing in the last prep dep", [TxId]),
            RemoveDepType = remove_pd,
            gen_server:cast(Sender, {solve_pending_prepared, TxId, PrepareTime, self()}),
            RepMsg = {Sender, RepMode, TxWriteSet, PrepareTime}, 
            repl_fsm:repl_prepare(Partition, prepared, TxId, RepMsg), 
            %DepDict1 = dict:update_counter(success_wait, 1, DepDict), 
            dict:erase(TxId, DepDict);
        {ok, {1, PrepDep, PrepareTime, Sender, RepMode, TxWriteSet}} ->
            lager:warning("PrepDep is ~w", [PrepDep]),
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
                    dict:store(TxId, {1, PrepDep-1, PrepareTime, Sender, RepMode, TxWriteSet}, DepDict)
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

pre_commit([], _TxId, _SCTime, _InMemoryStore, _PreparedTxs, DepDict, _Partition, _PartitionType, _MaxLen) ->
    %dict:update(commit_diff, fun({Diff, Cnt}) -> {Diff+SCTime-PrepareTime, Cnt+1} end, DepDict);
    DepDict;
pre_commit([Key|Rest], TxId, SCTime, InMemoryStore, PreparedTxs, DepDict, Partition, PartitionType, MaxLen) ->
    MyNode = {Partition, node()},
     lager:warning("Trying to insert key ~p with for ~p, specula commit time is ~p", [Key, TxId, SCTime]),
    case ets:lookup(PreparedTxs, Key) of
        %% If this one is prepared, no one else can be specula-committed already, so sc-time should be the same as prep time 
        [{Key, [{prepared, TxId, PrepareTime, LastReaderTime, LastPrepTime, PrepNum, Value, PendingReaders}|Deps]=_Record}] ->
            lager:warning("In prep, PrepNum is ~w, record is ~w", [PrepNum, _Record]),
            {StillPend, ToPrev} = reply_pre_commit(PartitionType, PendingReaders, Key, SCTime, Value, TxId, MaxLen),
            case ToPrev of
                [] ->
                    ets:insert(PreparedTxs, [{Key, [{pre_commit, TxId, PrepareTime, LastReaderTime, 
                            LastPrepTime, PrepNum-1, Value, StillPend}|Deps] }]),
                    pre_commit(Rest, TxId, SCTime, InMemoryStore, 
                          PreparedTxs, DepDict, Partition, PartitionType, MaxLen);
                _ ->
                    %% Let these readers read the previous guy...
                    lager:warning("In multi read version, Deps is ~w, ToPrev is ~w", [Deps, ToPrev]),
                    AfterReadRecord = case ToPrev of
                                          [] -> Deps;
                                          _ -> multi_read_version(Key, Deps, ToPrev, InMemoryStore, MaxLen)
                                      end,
                    ets:insert(PreparedTxs, [{Key, [{pre_commit, TxId, PrepareTime, LastReaderTime, 
                            LastPrepTime, PrepNum-1, Value, StillPend}|AfterReadRecord] }]),
                    pre_commit(Rest, TxId, SCTime, InMemoryStore, 
                          PreparedTxs, DepDict, Partition, PartitionType, MaxLen)
            end;
        [{Key, [{_Type, _OtherTxId, _, LastReaderTime, FirstPrepTime, _PrepNum, _Value, _OtherPendReaders}=FirstOne|RecordList]}] ->
            lager:warning("SC commit for ~w, ~p, prepnum are ~w", [TxId, Key, _PrepNum]),
            case find_prepare_record(RecordList, TxId) of
                [] -> 
                    lager:warning("Did not find record! Record list is ~w", [RecordList]),
                    pre_commit(Rest, TxId, SCTime, InMemoryStore, 
                          PreparedTxs, DepDict, Partition, PartitionType, MaxLen);
                {Prev, {TxId, TxPrepTime, TxSCValue, PendingReaders}, RestRecords} ->
                    {First, RemainRecords, AbortedReaders, DepDict1} = 
                            deal_pending_records(Prev, FirstOne, SCTime, DepDict, MyNode, [], PartitionType, 1, convert_to_pd),
                    lager:warning("Found record! Prev is ~w, First is ~w, RemainRecords is ~w", [Prev, First, RemainRecords]),
                    {StillPend, ToPrev} = reply_pre_commit(PartitionType, PendingReaders++AbortedReaders, Key, SCTime, TxSCValue, TxId, MaxLen),
                    lager:warning("Before multi read, RestRecords ~w, ToPrev ~w", [RestRecords, InMemoryStore]),
                    AfterReadRecord = case ToPrev of
                                          [] -> RestRecords;
                                          _ -> multi_read_version(Key, RestRecords, ToPrev, InMemoryStore, MaxLen)
                                      end,
                    true = TxPrepTime =< SCTime,
                    case First of
                        {LastReaderTime, RemainPrepNum} ->
                            case RemainRecords of 
                                [] ->
                                    case AfterReadRecord of
                                        [] ->
                                            ets:insert(PreparedTxs, {Key, [{pre_commit, TxId, SCTime, LastReaderTime, SCTime, RemainPrepNum, TxSCValue, StillPend}]});
                                        _ ->
                                            ets:insert(PreparedTxs, {Key, [{pre_commit, TxId, SCTime, LastReaderTime, FirstPrepTime, RemainPrepNum, TxSCValue, StillPend}|AfterReadRecord]})
                                    end;
                                [{TType, TTxId, TSCTime, TValue, TPendReaders}|RT] -> 
                                    case AfterReadRecord of
                                        [] ->
                                            ets:insert(PreparedTxs, {Key, [{TType, TTxId, TSCTime, LastReaderTime, SCTime, RemainPrepNum, TValue, TPendReaders}|RT]++[{pre_commit, TxId, SCTime, TxSCValue, StillPend}]});
                                        _ ->
                                            ets:insert(PreparedTxs, {Key, [{TType, TTxId, TSCTime, LastReaderTime, FirstPrepTime, RemainPrepNum, TValue, TPendReaders}|RT]++[{pre_commit, TxId, SCTime, TxSCValue, StillPend}|AfterReadRecord]})
                                    end
                            end;
                        {F1, F2, F3, F4, _, F6, F7, F8} -> 
                            case AfterReadRecord of
                                [] ->
                                    ets:insert(PreparedTxs, {Key, [{F1, F2, F3, F4, SCTime, F6, F7, F8}|RemainRecords]++[{pre_commit, TxId, SCTime, TxSCValue, StillPend}]});
                                _ ->
                                    ets:insert(PreparedTxs, {Key, [{F1, F2, F3, F4, FirstPrepTime, F6, F7, F8}|RemainRecords]++[{pre_commit, TxId, SCTime, TxSCValue, StillPend}|AfterReadRecord]})
                            end
                    end,
                    pre_commit(Rest, TxId, SCTime, InMemoryStore, PreparedTxs,
                        DepDict1, Partition, PartitionType, MaxLen)
                    %%% Just to expose more erros if possible
            end;
        R ->
            %% TODO: Need to fix this
            lager:error("The txn is actually aborted already ~w, ~w", [Key, R]),
            pre_commit(Rest, TxId, SCTime, InMemoryStore, PreparedTxs,
                DepDict, Partition, PartitionType, MaxLen)
            %R = 2
    end.

reply_pre_commit(_, [], _, _, _, _, _) ->
    {[],[]};
reply_pre_commit(PartitionType, PendingReaders, Key, SCTime, Value, TxId, MaxLen) ->
    PrepLen = case ets:lookup(dep_len, TxId) of
               [{TxId, Len, spec_commit, _, _Dep}] -> Len;
                _ -> 0
            end,
    case PartitionType of
        cache ->
              lists:foreach(fun({ReaderTxId, Node, {relay, Sender}}) ->
                        SnapshotTime = ReaderTxId#tx_id.snapshot_time,
                        case SnapshotTime >= SCTime of
                            true ->
                                case PrepLen < MaxLen of
                                    true ->
                                        %ets:insert(dep_len, add_to_entry(Entry, ReaderTxId, PrepLen+1, SCTxId, [])),
                                        lager:warning("~w reads from ~w, add dep len to ~w now" , [ReaderTxId, TxId, PrepLen+1]),
                                        %dep_server:add_dep(ReaderTxId, SCTxId),
                                        ets:insert(anti_dep, {ReaderTxId, TxId}),
                                        ets:insert(dependency, {TxId, ReaderTxId}),
                                        reply({relay, Sender}, {ok, Value});
                                    false ->
                                        dep_server:add_block_dep(TxId, {Sender, {ok, Value}}),
                                        ets:insert(anti_dep, {ReaderTxId, TxId}),
                                        ets:insert(dependency, {TxId, ReaderTxId})
                                end;
                            false ->
                                {_, RealKey} = Key,
                                clocksi_vnode:relay_read(Node, RealKey, ReaderTxId, Sender, false)
                        end end,
                    PendingReaders),
                {[], []};
        _ ->
            lists:foldl(fun({ReaderTxId, ignore, Sender}=ReaderInfo, {Pend, ToPrev}) ->
                case ReaderTxId#tx_id.snapshot_time >= SCTime of
                    true ->
                        case sc_by_local(ReaderTxId) of
                            true ->
                                case PrepLen < MaxLen of
                                    true ->
                                        %ets:insert(dep_len, add_to_entry(Entry, ReaderTxId, PrepLen+1, SCTxId, [])),
                                        %dep_server:add_dep(ReaderTxId, SCTxId),
                                        ets:insert(anti_dep, {ReaderTxId, TxId}),
                                        ets:insert(dependency, {TxId, ReaderTxId}),
                                        reply({relay, Sender}, {ok, Value});
                                    false ->
                                        dep_server:add_block_dep(TxId, {Sender, {ok, Value}}),
                                        ets:insert(anti_dep, {ReaderTxId, TxId}),
                                        ets:insert(dependency, {TxId, ReaderTxId})
                                end,
                                {Pend, ToPrev};
                            false ->
                                {[ReaderInfo|Pend], ToPrev}
                        end;
                    false ->
                            {Pend, [ReaderInfo|ToPrev]}
                end
            end, {[], []}, PendingReaders)
    end.

reply_to_all(PartitionType, PendingReaders, Key, CommitTime, Value) ->
    case PartitionType of
        cache ->
              lists:foreach(fun({ReaderTxId, Node, {relay, Sender}}) ->
                        SnapshotTime = ReaderTxId#tx_id.snapshot_time,
                        case SnapshotTime >= CommitTime of
                            true ->
                                reply({relay, Sender}, {ok, Value});
                            false ->
                                {_, RealKey} = Key,
                                clocksi_vnode:relay_read(Node, RealKey, ReaderTxId, Sender, false)
                        end end,
                    PendingReaders),
                [];
        _ ->
            lists:foldl(fun({ReaderTxId, ignore, Sender}=ReaderInfo, ToPrev) ->
                case ReaderTxId#tx_id.snapshot_time >= CommitTime of
                    true ->
                        reply(Sender, {ok, Value}),
                        ToPrev;
                    false ->
                        [ReaderInfo|ToPrev]
                end
            end, [], PendingReaders)
    end.

find_prepare_record(RecordList, TxId) ->
    find_prepare_record(RecordList, TxId, []).
  
find_prepare_record([], _TxId, _Prev) ->
    [];
find_prepare_record([{prepared, TxId, TxPrepTime, Value, Readers}|Rest], TxId, Prev) ->
    {Prev, {TxId, TxPrepTime, Value, Readers}, Rest};
find_prepare_record([Record|Rest], TxId, Prev) ->
    find_prepare_record(Rest, TxId, [Record|Prev]).

%% TODO: allowing all speculative read now! Maybe should not be so aggressive
specula_read(TxId, Key, PreparedTxs, SenderInfo, LenLimit) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(PreparedTxs, Key) of
        [] ->
            ets:insert(PreparedTxs, {Key, SnapshotTime}),
            ready;
        [{Key, [{Type, PreparedTxId, PrepareTime, LastReaderTime, FirstPrepTime, PendPrepNum, Value, PendingReader}| PendingPrepare]}] ->
            lager:warning("~p: has ~p with ~p, Type is ~p, lastpp time is ~p, pend prep num is ~p, pending prepares are ~p",[Key, PreparedTxId, PrepareTime, Type, FirstPrepTime, PendPrepNum, PendingPrepare]),
            case SnapshotTime >= PrepareTime of
                true ->
                    %% Read current version
                    case (Type == pre_commit) and sc_by_local(TxId) of
                        true ->
                            case ets:lookup(dep_len, PreparedTxId) of
                                [{PreparedTxId, PrepLen, spec_commit, _, _Dep}] ->
                                    %Entry = ets:lookup(dep_len, TxId),
                                    %case IfSpecCommit of
                                    %    [] -> ets:insert(dep_len, {PreparedTxId, PrepLen, spec_commit, Dep});
                                    %    _ -> ok
                                    %end,
                                    case PrepLen < LenLimit of
                                        true -> 
                                            %NewEntry = add_to_entry(Entry, TxId, PrepLen+1, PreparedTxId, []),
                                            % lager:warning("~p reading specula ~p, pend prep num is ~w, entry is ~w, new entry is ~w", [TxId, Value, PendPrepNum, Entry, NewEntry]),
                                            %ets:insert(dep_len, NewEntry), 
                                            lager:warning("~w reads from ~w, add dep len to ~w now" , [TxId, PreparedTxId, PrepLen+1]),
                                            ets:insert(anti_dep, {TxId, PreparedTxId}),
                                            ets:insert(dependency, {PreparedTxId, TxId}),
                                            {specula, Value};
                                        false ->
                                            {_ReaderTxId, ignore, Sender} = SenderInfo,
                                            %NewEntry = add_to_entry(Entry, TxId, PrepLen+1, PreparedTxId, {Sender, {ok, Value}}),
                                            lager:warning("~w reads from ~w, add dep block len to ~w now" , [TxId, PreparedTxId, PrepLen+1]),
                                            dep_server:add_block_dep(PreparedTxId, {Sender, {ok, Value}}),
                                            %ets:insert(dep_len, NewEntry), 
                                            ets:insert(anti_dep, {TxId, PreparedTxId}),
                                            ets:insert(dependency, {PreparedTxId, TxId}),
                                            specula_wait
                                    end;
                                _ ->
                                    ets:insert(PreparedTxs, {Key, [{Type, PreparedTxId, PrepareTime, max(SnapshotTime, LastReaderTime), FirstPrepTime, PendPrepNum, Value, [SenderInfo|PendingReader]}| PendingPrepare]}),
                                    not_ready
                            end;
                        false ->
                             lager:warning("~p can not read this version, not by local or not specula commit, Type is ~p, PrepTx is ~p, PendPrepNum is ~w", [TxId, Type, PreparedTxId, PendPrepNum]),
                            ets:insert(PreparedTxs, {Key, [{Type, PreparedTxId, PrepareTime, max(SnapshotTime, LastReaderTime), FirstPrepTime, PendPrepNum,
                                Value, [SenderInfo|PendingReader]}| PendingPrepare]}),
                            not_ready
                    end;
                false ->
                    case SnapshotTime < FirstPrepTime of
                        true ->
                            ready;
                        false ->
                            %% Read previous version
                            lager:warning("Trying to read appr version, pending preapre is ~w", [PendingPrepare]),
                            {IfReady, Record} = read_appr_version(TxId, Key, PendingPrepare, [], SenderInfo, LenLimit),
                            case IfReady of
                                not_ready ->
                                    ets:insert(PreparedTxs, [{Key, [{Type, PreparedTxId, PrepareTime, max(SnapshotTime, LastReaderTime), FirstPrepTime, PendPrepNum, Value, PendingReader}|Record]}]),
                                    not_ready;
                                specula ->
                                    case SnapshotTime > LastReaderTime of
                                        true -> ets:insert(PreparedTxs, {Key, [{Type, PreparedTxId, PrepareTime, SnapshotTime, FirstPrepTime, PendPrepNum, Value, PendingReader}| PendingPrepare]});
                                        false -> ok
                                    end,
                                    SCValue = Record,
                                    {specula, SCValue};
                                specula_wait ->
                                    case SnapshotTime > LastReaderTime of
                                        true -> ets:insert(PreparedTxs, {Key, [{Type, PreparedTxId, PrepareTime, SnapshotTime, FirstPrepTime, PendPrepNum, Value, PendingReader}| PendingPrepare]});
                                        false -> ok
                                    end,
                                    not_ready;
                                ready ->
                                    case SnapshotTime > LastReaderTime of
                                        true -> ets:insert(PreparedTxs, {Key, [{Type, PreparedTxId, PrepareTime, SnapshotTime, FirstPrepTime, PendPrepNum, Value, PendingReader}| PendingPrepare]});
                                        false -> ok
                                    end,
                                    ready
                            end
                    end
            end;
        [{Key, LastReaderTime}] ->
            ets:insert(PreparedTxs, {Key, max(SnapshotTime, LastReaderTime)}),
            ready
    end.

%% What wans to delete/commit is not found, This is problematic! If commits arrive out-of-order, need to use a queue to store it
delete_and_read(_DeleteType, _, _, _, _Key, DepDict, _, _, [], _TxId, _, _, _) ->
    lager:warning("Want to ~w ~w for key ~w, but arrived already", [_DeleteType, _TxId, _Key]),
    DepDict;
delete_and_read(DeleteType, PreparedTxs, InMemoryStore, TxCommitTime, Key, DepDict, PartitionType, Partition, [{Type, TxId, _Time, Value, PendingReaders}|Rest], TxId, Prev, FirstOne, 0) ->
    %% If can read previous version: read
    %% If can not read previous version: add to previous pending
    lager:warning("Delete and read ~w for ~w, prev is ~w", [DeleteType, TxId, Prev]),
    ToRemovePrep = case Type of prepared -> 1; repl_prepare -> 1; _ -> 0 end,
    RemoveDepType = case DeleteType of 
                        commit ->
                            case Type of pre_commit -> remove_pd; _ -> remove_ppd end;
                            %case Rest of 
                            %    [] -> case Type of pre_commit -> remove_pd; _ -> remove_ppd end;
                            %    _ -> 
                            %end;
                        abort ->
                            case Rest of 
                                [] -> case Type of pre_commit -> remove_pd; _ -> remove_ppd end;
                                [{prepared, _, _, _, _}|_] -> not_remove;
                                [{repl_prepare, _, _, _, _}|_] -> not_remove; 
                                [{pre_commit, _, _, _, _}|_] ->
                                    case Type of pre_commit -> not_remove; _ -> convert_to_pd end
                            end
                    end,
    {First, RemainPrev, AbortReaders, DepDict1} 
        = deal_pending_records(Prev, FirstOne, TxCommitTime, DepDict, {Partition, node()}, [], PartitionType, ToRemovePrep, RemoveDepType),
    ToPrev = case DeleteType of 
                abort -> AbortReaders++PendingReaders; 
                commit -> reply_to_all(PartitionType, AbortReaders++PendingReaders, Key, TxCommitTime, Value)
             end,
    AfterReadRecord = case ToPrev of 
                            [] -> Rest;
                            _ -> [{length, MaxLen}] = ets:lookup(meta_info, length),
                                 multi_read_version(Key, Rest, ToPrev, InMemoryStore, MaxLen)
                      end,
    lager:warning("After read record is ~w, First is ~w, remain previ is ~w", [AfterReadRecord, First, RemainPrev]),
    case AfterReadRecord of
        [] ->
            Rest = [],
            case First of
                {LastReaderTime, RemainPrepNum} ->
                    RemainPrev = [],
                    %case RemainPrev of
                    %    [] ->
                            RemainPrepNum = 0,
                            ets:insert(PreparedTxs, {Key, LastReaderTime}),
                    %    [{TType, TTxId, TSCTime, TValue, TPendReaders}|RT] ->
                    %        ets:insert(PreparedTxs, {Key, [{TType, TTxId, TSCTime, LastReaderTime, NewFirstPrep, RemainPrepNum, TValue, TPendReaders}|RT]})
                    %end,
                    DepDict1;
                _ ->
                    ets:insert(PreparedTxs, {Key, [First|RemainPrev]}),
                    DepDict1
            end;
        [{TType, TTxId, TSCTime, TValue, TPendReaders}|TT] -> 
            case First of
                {LastReaderTime, FirstPrepTime, RemainPrepNum} ->
                    case RemainPrev of
                        [] ->
                            ets:insert(PreparedTxs, {Key, [{TType, TTxId, TSCTime, LastReaderTime, FirstPrepTime, RemainPrepNum, TValue, TPendReaders}|TT]});
                        [{RType, RTxId, RSCTime, RValue, RPendReaders}|RT] ->
                            ets:insert(PreparedTxs, {Key, [{RType, RTxId, RSCTime, LastReaderTime, FirstPrepTime, RemainPrepNum, RValue, RPendReaders}|RT]++AfterReadRecord})
                    end,
                    DepDict1;
                {F1, F2, F3, F4, F5, F6, F7, F8} ->
                    case RemainPrev of
                        [] ->
                            ets:insert(PreparedTxs, {Key, [First|RemainPrev]++AfterReadRecord});
                        _ ->
                            F5 = F3,
                            {_, _, _, _, _, FirstPrepTime, _, _} = FirstOne,
                            ets:insert(PreparedTxs, {Key, [{F1, F2, F3, F4, FirstPrepTime, F6, F7, F8}|RemainPrev]++AfterReadRecord})
                    end,
                    DepDict1
            end
    end;
delete_and_read(DeleteType, PreparedTxs, InMemoryStore, TxCommitTime, Key, DepDict, PartitionType, MyNode, [Current|Rest], TxId, Prev, FirstOne, CAbortPrep) ->
    delete_and_read(DeleteType, PreparedTxs, InMemoryStore, TxCommitTime, Key, DepDict, PartitionType, MyNode, Rest, TxId, [Current|Prev], FirstOne, CAbortPrep);
delete_and_read(abort, _PreparedTxs, _InMemoryStore, _TxCommitTime, _Key, DepDict, _PartitionType, _MyNode, [], _TxId, _Prev, _Whatever, 0) ->
    DepDict.

read_appr_version(_ReaderTxId, _Key, [], _Prev, _SenderInfo, _) ->
    {ready, []}; 
read_appr_version(ReaderTxId, _Key, [{Type, SCTxId, SCTime, SCValue, SCPendingReaders}|Rest], Prev, SenderInfo, LenLimit) when ReaderTxId#tx_id.snapshot_time >= SCTime ->
    case Type of
        prepared ->
            {not_ready, lists:reverse(Prev)++[{Type, SCTxId, SCTime, SCValue, [SenderInfo|SCPendingReaders]}|Rest]};
        repl_prepare ->
            {not_ready, lists:reverse(Prev)++[{Type, SCTxId, SCTime, SCValue, [SenderInfo|SCPendingReaders]}|Rest]};
        pre_commit ->
            DepLen = ets:lookup(dep_len, SCTxId),
            case {sc_by_local(ReaderTxId), DepLen} of
                {true, [{SCTxId, PrepLen, spec_commit, _, _}]} ->
                    [{SCTxId, PrepLen, spec_commit, _, _}] = DepLen,
                    %Entry = ets:lookup(dep_len, ReaderTxId),
                    case PrepLen < LenLimit of
                        true ->
                            %NewEntry = add_to_entry(Entry, ReaderTxId, PrepLen+1, SCTxId, []),
                            %ets:insert(dep_len, NewEntry),
                            lager:warning("~w reads from ~w, add dep len to ~w now" , [ReaderTxId, SCTxId, PrepLen+1]),
                            %dep_server:add_dep(ReaderTxId, SCTxId),
                            ets:insert(anti_dep, {ReaderTxId, SCTxId}),
                            ets:insert(dependency, {SCTxId, ReaderTxId}),
                            lager:warning("Inserting anti_dep from ~p to ~p", [ReaderTxId, SCTxId]),
                            {specula, SCValue};
                        false ->
                            {_ReaderTxId, ignore, Sender} = SenderInfo,
                            %NewEntry = add_to_entry(Entry, ReaderTxId, PrepLen+1, SCTxId, {Sender, {ok, SCValue}}),
                            lager:warning("~w reads from ~w, add dep block len to ~w now" , [ReaderTxId, SCTxId, PrepLen+1]),
                            dep_server:add_block_dep(SCTxId, {Sender, {ok, SCValue}}),
                            %ets:insert(dep_len, NewEntry),
                            ets:insert(anti_dep, {ReaderTxId, SCTxId}),
                            ets:insert(dependency, {SCTxId, ReaderTxId}),
                            lager:warning("Inserting anti_dep from ~p to ~p", [ReaderTxId, SCTxId]),
                            {specula_wait, []}
                    end;
                _ ->
                    {not_ready, lists:reverse(Prev)++[{Type, SCTxId, SCTime, SCValue, [SenderInfo|SCPendingReaders]}|Rest]}
            end
    end;
read_appr_version(ReaderTxId, Key, [H|Rest], Prev, SenderInfo, LenLmit) -> 
    read_appr_version(ReaderTxId, Key, Rest, [H|Prev], SenderInfo, LenLmit).

multi_read_version(_Key, List, [], _, _) -> 
    List;
multi_read_version(Key, [], SenderInfos, InMemoryStore, _) -> 
    %% Let all senders read
    Value = case ets:lookup(InMemoryStore, Key) of
                [] ->
                    [];
                [{Key, ValueList}] ->
                    [{_CommitTime, First}|_T] = ValueList,
                    First
            end,
    lists:foreach(fun({_ReaderTxId, ignore, Sender}) ->
                        reply(Sender, {ok, Value})
                  end, SenderInfos),
    [];
multi_read_version(Key, [{pre_commit, SCTxId, SCTime, SCValue, SCPendingReaders}|Rest], SenderInfos, CommittedTxs, MaxLen) -> 
    lager:warning("Multi read version for ~p ~w", [Key, SCTxId]),
    RemainReaders = lists:foldl(fun({ReaderTxId, ignore, Sender}, Pend) ->
            case (ReaderTxId#tx_id.snapshot_time >= SCTime)  and sc_by_local(ReaderTxId) of
                true ->
                    case ets:lookup(dep_len, SCTxId) of
                      [{SCTxId, PrepLen, spec_commit, _, _Dep}] ->
                          %Entry = ets:lookup(dep_len, ReaderTxId),
                          %case IfSpecCommit of
                          %    [] -> ets:insert(dep_len, {SCTxId, PrepLen, spec_commit, Dep});
                          %    _ -> ok
                          %end,
                          case PrepLen < MaxLen of
                              true ->
                                  %ets:insert(dep_len, add_to_entry(Entry, ReaderTxId, PrepLen+1, SCTxId, [])),
                                  lager:warning("~w reads from ~w, add dep len to ~w now" , [ReaderTxId, SCTxId, PrepLen+1]),
                                  %dep_server:add_dep(ReaderTxId, SCTxId),
                                  ets:insert(anti_dep, {ReaderTxId, SCTxId}),
                                  ets:insert(dependency, {SCTxId, ReaderTxId}),
                                  reply(Sender, {ok, SCValue});
                              false ->
                                  lager:warning("~p reading specula ~p, but blocked! LenLimit is ~w", [ReaderTxId, SCValue, MaxLen]),
                                  %ets:insert(dep_len, add_to_entry(Entry, ReaderTxId, PrepLen+1, SCTxId, {Sender, {ok, SCValue}})),
                                  lager:warning("~w reads from ~w, add dep block len to ~w now" , [ReaderTxId, SCTxId, PrepLen+1]),
                                  dep_server:add_block_dep(SCTxId, {Sender, {ok, SCValue}}),
                                  ets:insert(anti_dep, {ReaderTxId, SCTxId}),
                                  ets:insert(dependency, {SCTxId, ReaderTxId})
                          end,
                          Pend;
                      _ ->
                          %Entry = ets:lookup(dep_len, ReaderTxId),
                          %NewEntry = add_to_entry(Entry, ReaderTxId, 1, SCTxId, []),
                          %ets:insert(dep_len, NewEntry),
                          [{ReaderTxId, ignore, Sender}|Pend] 
                      %Whatever ->
                      %    lager:error("Got some dep_len weird ~w", [Whatever]),
                      %    Whatever = error
                    end;
                false ->
                    [{ReaderTxId, ignore, Sender}|Pend] 
            end end, [], SenderInfos),
    [{pre_commit, SCTxId, SCTime, SCValue, SCPendingReaders}|multi_read_version(Key, Rest, RemainReaders, CommittedTxs, MaxLen)];
multi_read_version(_Key, [{Type, SCTxId, SCTime, SCValue, SCPendingReaders}|Rest], SenderInfos, _CommittedTxs, _MaxLen) -> 
    [{Type, SCTxId, SCTime, SCValue, SenderInfos++SCPendingReaders}|Rest].

sc_by_local(TxId) ->
    node(TxId#tx_id.server_pid) == node().

insert_prepare(PreparedTxs, TxId, Partition, WriteSet, TimeStamp, Sender) ->
    case ets:lookup(PreparedTxs, {TxId, Partition}) of
          [] ->
              {KeySet, ToPrepTS} = lists:foldl(fun({Key, _Value}, {KS, Ts}) ->
                                          case ets:lookup(PreparedTxs, Key) of
                                              [] -> {[Key|KS], Ts};
                                              [{Key, [{_Type, _PrepTxId, _, LastReaderTS, _, _, _, _}|_Rest]}] ->
                                                  {[Key|KS], max(Ts, LastReaderTS+1)};
                                              [{Key, LastReaderTS}] ->  
                                                  {[Key|KS], max(Ts, LastReaderTS+1)}
                                          end end, {[], TimeStamp}, WriteSet),
              lists:foreach(fun({Key, Value}) ->
                          case ets:lookup(PreparedTxs, Key) of
                              [] ->
                                  true = ets:insert(PreparedTxs, {Key, [{repl_prepare, TxId, ToPrepTS, ToPrepTS, ToPrepTS, 1, Value, []}]});
                              [{Key, [{_, _, _, _, _, _, _, _}|_Rest]=L}] ->
                                  NewList = add_to_list(TxId, ToPrepTS, Value, L),
                                  true = ets:insert(PreparedTxs, {Key, NewList});
                              [{Key, _}] -> 
                                  true = ets:insert(PreparedTxs, {Key, [{repl_prepare, TxId, ToPrepTS, ToPrepTS, ToPrepTS, 1, Value, []}]})
              end end,  WriteSet),
              lager:warning("Got repl prepare for ~p, propose ~p and replied", [TxId, ToPrepTS]),
              ets:insert(PreparedTxs, {{TxId, Partition}, KeySet}),
              gen_server:cast(Sender, {solve_pending_prepared, TxId, ToPrepTS, self()});
          _R ->
               lager:warning("Not replying for ~p, ~p because already prepard, Record is ~p", [TxId, Partition, _R]),
              ok
      end.

add_to_list(ToPrepTxId, ToPrepTS, ToPrepValue, [{Type, PrepTxId, PrepTS, LastReaderTS, LastSCTime, PrepNum, PrepValue, Reader}|Rest]) -> 
    lager:warning("Insert prepare for ~w, previous tx is ~w, type is ~w, new prep num is ~w", [ToPrepTxId, PrepTxId, Type, PrepNum]),
    case ToPrepTxId#tx_id.snapshot_time > PrepTxId#tx_id.snapshot_time of
        true ->
            [{repl_prepare, ToPrepTxId, ToPrepTS, LastReaderTS, min(LastSCTime, ToPrepTS), PrepNum+1, ToPrepValue, []}|
                [{Type, PrepTxId, PrepTS, PrepValue, Reader}|Rest]];
        false ->
            [{Type, PrepTxId, PrepTS, LastReaderTS, min(LastSCTime, ToPrepTS), PrepNum+1, PrepValue, Reader}|
                add_to_list(ToPrepTxId, ToPrepTS, ToPrepValue, Rest)]
    end;
add_to_list(ToPrepTxId, ToPrepTS, ToPrepValue, [{Type, PrepTxId, PrepTS, PrepValue, Reader}|Rest] = L) ->
    case ToPrepTS > PrepTS of
        true -> 
                %case PrepTxId#tx_id.snapshot_time > ToPrepTxId#tx_id.snapshot_time of
                %    true -> lager:warning("L ~w must have been aborted already!", [L]), 
                %            [{repl_prepare, ToPrepTxId, ToPrepTS, ToPrepValue, []}]; 
                [{repl_prepare, ToPrepTxId, ToPrepTS, ToPrepValue, []}|L];
                %end;
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
        [{Key, [{Type, PreparedTxId, PrepareTime, LastReaderTime, FirstPrepTime, CanSC, Value, PendingReader}|PendingPrepare]}] ->
             lager:warning("~p Not ready.. ~p waits for ~p with ~p, others are ~p, FirstPrepTime is ~w, pending prepares are ~w", [Key, TxId, PreparedTxId, PrepareTime, PendingReader, FirstPrepTime, PendingPrepare]),
            case SnapshotTime < FirstPrepTime of
                true ->
                    ready;
                false ->
                    case SnapshotTime >= PrepareTime of
                        true ->
                            ets:insert(PreparedTxs, {Key, [{Type, PreparedTxId, PrepareTime, LastReaderTime, FirstPrepTime, CanSC, Value, [SenderInfo|PendingReader]}| PendingPrepare]}),
                            not_ready;

                        false ->
                            Record = insert_pend_reader(PendingPrepare, SnapshotTime, SenderInfo),
                            ets:insert(PreparedTxs, {Key, [{Type, PreparedTxId, PrepareTime, LastReaderTime, FirstPrepTime, CanSC, Value, PendingReader}|Record]}),
                    %lager:error("~p non_specula reads ~p is blocked by ~p! PrepareTime is ~p", [TxId, Key, PreparedTxId, PrepareTime]),
                            not_ready
                    end
            end;
        [{Key, LastReaderTime}] ->
            ets:insert(PreparedTxs, {Key, max(SnapshotTime, LastReaderTime)}),
            ready
    end.

insert_pend_reader([{Type, PrepTxId, PrepareTime, PrepValue, RWaiter}|Rest], SnapshotTime, SenderInfo) when SnapshotTime >= PrepareTime ->
    [{Type, PrepTxId, PrepareTime, PrepValue, [SenderInfo|RWaiter]}|Rest]; 
insert_pend_reader([Record|Rest], SnapshotTime, SenderInfo) ->
    [Record|insert_pend_reader(Rest, SnapshotTime, SenderInfo)].
