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
            pre_commit/10, specula_read/4, insert_prepare/6, read_value/4, remote_read_value/4]).

-define(SPECULA_THRESHOLD, 0).

prepare_for_master_part(TxId, TxWriteSet, CommittedTxs, PreparedTxs, InitPrepTime)->
    KeySet = [K || {K, _} <- TxWriteSet],
    case certification_check(InitPrepTime, TxId, KeySet, CommittedTxs, PreparedTxs, 0, 0) of
        false ->
            {error, write_conflict};
        %% Directly prepare
        {0, 0, PrepareTime} ->
          %lager:warning("~p passed prepare with ~p", [TxId, PrepareTime]),
            lists:foreach(fun({K, V}) ->
                    ets:insert(PreparedTxs, {K, {PrepareTime, PrepareTime, 1}, [{prepared, TxId, PrepareTime, V, []}]})
                    end, TxWriteSet),
            true = ets:insert(PreparedTxs, {TxId, KeySet}),
            {ok, PrepareTime};
        %% Pend-prepare. 
        {PendPrepDep, PrepDep, PrepareTime} ->
          %lager:warning("~p passed but has ~p pend dep, ~p prepdep, prepare with ~p", [TxId, PendPrepDep, PrepDep, PrepareTime]),
            %KeySet = [K || {K, _} <- TxWriteSet],  % set_prepared(PreparedTxs, TxWriteSet, TxId,PrepareTime, []),
            lists:foreach(fun({K, V}) ->
                  case ets:lookup(PreparedTxs, K) of
                  [] ->
                      ets:insert(PreparedTxs, {K, {PrepareTime, PrepareTime, 1}, [{prepared, TxId, PrepareTime, V, []}]});
                  [{K, {LastReaderTime, FirstPrepTime, PrepNum}, [{Type, PrepTxId, OldPPTime, PrepValue, RWaiter}|Rest]}] ->
                      %ets:insert(PreparedTxs, {K, [{Type, PrepTxId, OldPPTime, LastReaderTime, max(LastPrepTime, PrepareTime), PrepNum+1, PrepValue, RWaiter}|
                      %         (PWaiter++[{prepared, TxId, PrepareTime, V, []}])]});
                      ets:insert(PreparedTxs, {K, {LastReaderTime, FirstPrepTime, PrepNum+1}, [{prepared, TxId, PrepareTime, V, []}|[{Type, PrepTxId, OldPPTime, PrepValue, RWaiter}|Rest]]});
                  _R -> 
                    %lager:warning("R is ~w", [_R]),
                    ets:insert(PreparedTxs, {K, {PrepareTime, PrepareTime, 1}, [{prepared, TxId, PrepareTime, V, []}]})
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
          %lager:warning("~p passed prepare with ~p, KeySet is ~p", [TxId, PrepareTime, KeySet]),
            lists:foreach(fun({K, V}) ->
                    InsertKey = case PartitionType of cache -> {Partition, K}; slave -> K end,
                    ets:insert(PreparedTxs, {InsertKey, {PrepareTime, PrepareTime, 1}, [{prepared, TxId, PrepareTime, V, []}]})
                    end, TxWriteSet),
            true = ets:insert(PreparedTxs, {{TxId, Partition}, KeySet}),
            {ok, PrepareTime};
        %% Pend-prepare. 
        {PendPrepDep, PrepDep, PrepareTime} ->
          %lager:warning("~p passed but has ~p pend prep deps, ~p prep dep, prepare with ~p, KeySet is ~w", [TxId, PendPrepDep, PrepDep, PrepareTime, KeySet]),
            %KeySet = [K || {K, _} <- TxWriteSet],  % set_prepared(PreparedTxs, TxWriteSet, TxId,PrepareTime, []),
            lists:foreach(fun({K, V}) ->
                          InsertKey = case PartitionType of cache -> {Partition, K}; slave -> K end,
                          case ets:lookup(PreparedTxs, InsertKey) of
                          [] -> 
                              ets:insert(PreparedTxs, {InsertKey, {PrepareTime, PrepareTime, 1}, [{prepared, TxId, PrepareTime, V, []}]});
                          [{InsertKey,  {LastRTime, FirstPrepTime, PrepNum},[{Type, PrepTxId, OldPPTime, PrepValue, RWaiter}|PWaiter]}] ->
                              %ets:insert(PreparedTxs, {InsertKey, [{Type, PrepTxId, OldPPTime, LastRTime, max(LastPrepTime, PrepareTime), PrepNum+1, PrepValue, RWaiter}|(PWaiter++[{prepared, TxId, PrepareTime, V, []}])]});
                             %lager:warning("Key is ~w, After insertion is ~w", [K, [{prepared, TxId, PrepareTime, LastRTime, FirstPrepTime, PrepNum+1, V, []}|[{Type, PrepTxId, OldPPTime, PrepValue, RWaiter}|PWaiter]]]),
                              ets:insert(PreparedTxs, {InsertKey, {LastRTime, FirstPrepTime, PrepNum+1}, [{prepared, TxId, PrepareTime, V, []}|[{Type, PrepTxId, OldPPTime, PrepValue, RWaiter}|PWaiter]]});
                          _R -> 
                             %lager:warning("R is ~w", [_R]),
                              ets:insert(PreparedTxs, {InsertKey, {PrepareTime, PrepareTime, 1}, [{prepared, TxId, PrepareTime, V, []}]})
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
                     %lager:warning("~p: False of prepared for ~p", [TxId, Key]),
                    false
            end;
        _ ->
            case ets:lookup(CommittedTxs, Key) of
                [{Key, CommitTime}] ->
                    case CommitTime > SnapshotTime of
                        true ->
                              %lager:warning("False for committed key ~p, Snapshot is ~p, diff with commit ~p", [Key, TxId#tx_id.snapshot_time, CommitTime-TxId#tx_id.snapshot_time]),
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
                             %lager:warning("~p: False of prepared for ~p", [TxId, Key]),
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
        [{Key, {LastReaderTime, _FirstPrepTime, PrepNum}, [{_Type, _PrepTxId, _PrepareTime, _PrepValue, _RWaiter}|_PWaiter]=_Record}] ->
            %lager:warning("For key ~p: record is ~w! LastReaderTime is ~w, FirstPrepTime is ~p, ~p may fail, PrepNum is ~w", [Key, _Record, _PrepTxId, _FirstPrepTime, TxId, PrepNum]),
            case _PWaiter of
                [] -> _FirstPrepTime = _PrepareTime;
                _ -> ok
            end,
            case _PrepareTime > SnapshotTime of
                true ->
                    false;
                false ->
                   %lager:warning("~p: ~p waits for ~p that is ~p with ~p, PrepNum is ~w", [Key, TxId, _PrepTxId, _Type, _PrepareTime, PrepNum]),
                    %% If all previous txns have specula-committed, then this txn can be directly pend-prepared and specula-committed 
                    %% Otherwise, this txn has to wait until all preceding prepared txn to be specula-committed
                    %% has_spec_commit means this txn can pend prep and then spec commit 
                    %% has_pend_prep basically means this txn can not even pend_prep
                    case _PWaiter of
                        [{_,_,F,_,_}] -> _FirstPrepTime = F;
                        _ -> ok
                    end,
                    case PrepNum of 0 ->  {prep_dep, LastReaderTime+1};
                                    _ -> {pend_prep_dep, LastReaderTime+1}
                    end
            end;
        [{Key, LastReaderTime}] ->
            %lager:warning("LastReader Time is ~w", [LastReaderTime]),
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
        [{Key, {LRTime, _FirstPrepTime, _PrepNum}, [{Type, TxId, _PrepareTime, MValue, PendingReaders}|Others]}] ->
            %lager:warning("~p Pending readers are ~p! Others are ~p", [TxId, PendingReaders, Others]),
            AllPendingReaders = lists:foldl(fun({_, _, _, _ , Readers}, CReaders) ->
                                       Readers++CReaders end, PendingReaders, Others), 
            Value = case Type of pre_commit -> {_, _, V}=MValue, V; _ -> MValue end,
            case Value of
                read ->
                    AllPendingReaders = [],
                    ets:insert(CommittedTxs, {Key, TxCommitTime});
                _ ->
                    case PartitionType of
                        cache ->  
                            lists:foreach(fun({ReaderTxId, Node, Sender}) ->
                                    SnapshotTime = ReaderTxId#tx_id.snapshot_time,
                                    case SnapshotTime >= TxCommitTime of
                                        true -> gen_server:reply(Sender, {ok, Value});
                                        false ->
                                            {_, RealKey} = Key,
                                            clocksi_vnode:remote_read(Node, RealKey, ReaderTxId, Sender)
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
                            lists:foreach(fun({ReaderTxId, Sender}) ->
                                    RValue = case ReaderTxId#tx_id.snapshot_time >= TxCommitTime of
                                              true ->
                                                  lists:nth(2,Values);
                                              false ->
                                                  hd(Values)
                                            end,
                                    case Sender of
                                        {remote, Client} -> 
                                            %lager:warning("For ~w of reader ~w, replying to its server", [TxId, Client]),
                                            gen_server:cast(TxId#tx_id.server_pid, {rr_value, TxId, Client, TxCommitTime, RValue});
                                        _ -> gen_server:reply(Sender, {ok, RValue})
                                    end end,
                                AllPendingReaders)
                    end
            end,
            ets:insert(PreparedTxs, {Key, max(TxCommitTime, LRTime)}),
            update_store(Rest, TxId, TxCommitTime, InMemoryStore, CommittedTxs, PreparedTxs,
                DepDict, Partition, PartitionType);
        [{Key, Metadata, Records}] ->
          %lager:warning("Trying to insert key ~p with for ~p, commit time is ~p, Records are ~p, PartitionType is ~p", [Key, TxId, TxCommitTime, Records, PartitionType]),
            DepDict2 = delete_and_read(commit, PreparedTxs, InMemoryStore, TxCommitTime, Key, DepDict, PartitionType, Partition, Records, TxId, [], Metadata),
            case PartitionType of cache -> ok; _ -> ets:insert(CommittedTxs, {Key, TxCommitTime}) end,
            update_store(Rest, TxId, TxCommitTime, InMemoryStore, CommittedTxs, PreparedTxs, DepDict2, Partition, PartitionType);
        _R ->
            %[{TxId, Keys}] = ets:lookup(PreparedTxs, TxId),
            %lager:error("For key ~w, txn ~w come first! Record is ~w", [Key, TxId, R]),
            update_store(Rest, TxId, TxCommitTime, InMemoryStore, CommittedTxs, PreparedTxs, DepDict, Partition, PartitionType)
    end.

clean_abort_prepared(_PreparedTxs, [], _TxId, _InMemoryStore, DepDict, _, _) ->
    DepDict; 
clean_abort_prepared(PreparedTxs, [Key | Rest], TxId, InMemoryStore, DepDict, Partition, PartitionType) ->
    case ets:lookup(PreparedTxs, Key) of
        [{Key, {LastReaderTime, FirstPPTime, PrepNum}, [{Type, TxId, _PrepTime, _Value, PendingReaders}|RestRecords]}] ->
           %lager:warning("Aborting ~p for key ~p, PrepNum is ~w, Type is ~w", [TxId, Key, PrepNum, Type]),
            %case RestRecords of
            %    [{FTxId,_,F,_,_}] ->%lager:warning("FTxId is ~w, F is ~w, FirstPPTime is ~w", [FTxId, F, FirstPPTime]), 
            %                     F=FirstPPTime;
            %    _ -> ok
            %end,
            case PendingReaders of
                [] -> ok;
                _ ->
                    case PartitionType of
                        cache -> 
                            lists:foreach(fun({ReaderTxId, Node, Sender}) -> 
                                    {_, RealKey} = Key,
                                    clocksi_vnode:remote_read(Node, RealKey, ReaderTxId, Sender) end,
                                      PendingReaders);
                        _ ->
                            {TS, RValue} = case ets:lookup(InMemoryStore, Key) of
                                                  [{Key, ValueList}] -> hd(ValueList);
                                                  [] -> {0, []}
                                              end,
                            lists:foreach(fun({RTx, Sender}) -> 
                                case Sender of
                                    {remote, Client} -> gen_server:cast(RTx#tx_id.server_pid, {rr_value, RTx, Client, TS, RValue}); 
                                    _ -> gen_server:reply(Sender, {ok, RValue})
                                end end, PendingReaders)
                    end
            end,
            case RestRecords of
                [] ->
                    true = ets:insert(PreparedTxs, {Key, LastReaderTime}),
                    clean_abort_prepared(PreparedTxs,Rest,TxId, InMemoryStore, DepDict, Partition, PartitionType);
                [{HType, HTxId, HPTime, HValue, HReaders}|Others] ->
                    RPrepNum = case Type of pre_commit -> PrepNum; _ -> PrepNum-1 end,
                    true = ets:insert(PreparedTxs, {Key, {LastReaderTime, FirstPPTime, RPrepNum}, [{HType, HTxId, HPTime, 
                            HValue, HReaders}|Others]}),
                    clean_abort_prepared(PreparedTxs,Rest,TxId, InMemoryStore, DepDict, Partition, PartitionType)
            end;
        [{Key, Metadata, Records}] -> 
           %lager:warning("Aborting TxId ~w, Key is ~p, metadata are ~w", [TxId, Key, Records]),
            DepDict2 = delete_and_read(abort, PreparedTxs, InMemoryStore, 0, Key, DepDict, PartitionType, Partition, Records, TxId, [], Metadata),
            clean_abort_prepared(PreparedTxs,Rest,TxId, InMemoryStore, DepDict2, Partition, PartitionType);
        _R ->
           %lager:warning("WTF? R is ~p", [_R]),
            clean_abort_prepared(PreparedTxs,Rest,TxId, InMemoryStore, DepDict, Partition, PartitionType)
    end.

deal_pending_records([], {LastReaderTime, FirstPrepTime, PrepNum}, _, DepDict, _, Readers, _, NumRemovePrep, _, _) ->
    {[], {LastReaderTime, FirstPrepTime, PrepNum-NumRemovePrep}, ignore, Readers, DepDict};
deal_pending_records([{repl_prepare, _TxId, PPTime, _Value, _PendingReaders}|_]=List, {LastReaderTime, FirstPrepTime, PrepNum}, _SCTime, DepDict, _MyNode, Readers, slave, RemovePrepNum, _, _) ->
    {lists:reverse(List), {LastReaderTime, FirstPrepTime, PrepNum-RemovePrepNum}, PPTime, Readers, DepDict}; 
deal_pending_records([{Type, TxId, PPTime, Value, PendingReaders}|PWaiter]=List, Metadata, SCTime, 
            DepDict, MyNode, Readers, PartitionType, RemovePrepNum, RemoveDepType, read) ->
    case {SCTime > TxId#tx_id.snapshot_time, Value == read} of
        {true, false} ->
            %% Abort the current txn
            NewDepDict = abort_from_dep_dict(TxId, DepDict, PartitionType, MyNode),
            case Type of
                pre_commit ->
                    deal_pending_records(PWaiter, Metadata, SCTime, NewDepDict, MyNode, PendingReaders++Readers, PartitionType, RemovePrepNum, RemoveDepType, read);
                prepared ->
                    deal_pending_records(PWaiter, Metadata, SCTime, NewDepDict, MyNode, PendingReaders++Readers, PartitionType, RemovePrepNum+1, RemoveDepType, read)
            end;
        _ ->
            case Type of 
                master ->
                    case dict:find(TxId, DepDict) of
                        error ->
                            deal_pending_records(PWaiter, Metadata, SCTime, DepDict, MyNode, [], PartitionType, RemovePrepNum, RemoveDepType, read);
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
                            {LastReaderTime, FirstPrepTime, PrepNum} = Metadata,
                            {lists:reverse(List), {LastReaderTime, FirstPrepTime, PrepNum-RemovePrepNum}, PPTime, Readers, DepDict1}
                            %[{prepared, TxId, PPTime, LastReaderTime, LastPPTime, Value, []}|Remaining]
                    end;
                _ -> %% slave or cache
                    {Partition, _} = MyNode,
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
                    {LastReaderTime, FirstPrepTime, PrepNum} = Metadata,
                    {lists:reverse(List), {LastReaderTime, FirstPrepTime, PrepNum-RemovePrepNum}, PPTime, Readers, DepDict1}
            end
    end;
deal_pending_records([{Type, TxId, PPTime, _Value, PendingReaders}|PWaiter]=List, Metadata, SCTime, 
            DepDict, MyNode, Readers, PartitionType, RemovePrepNum, RemoveDepType, update) ->
  %lager:warning("Dealing with ~p, Type is ~p, ~p, commit time is ~p", [TxId, Type, PPTime, SCTime]),
    case SCTime > TxId#tx_id.snapshot_time of
        true ->
            %% Abort the current txn
            NewDepDict = abort_from_dep_dict(TxId, DepDict, PartitionType, MyNode),
            case Type of
                pre_commit ->
                    deal_pending_records(PWaiter, Metadata, SCTime, NewDepDict, MyNode, PendingReaders++Readers, PartitionType, RemovePrepNum, RemoveDepType, update);
                prepared ->
                    deal_pending_records(PWaiter, Metadata, SCTime, NewDepDict, MyNode, PendingReaders++Readers, PartitionType, RemovePrepNum+1, RemoveDepType, update)
            end;
        false ->
            case Type of 
                master ->
                    case dict:find(TxId, DepDict) of
                        error ->
                            deal_pending_records(PWaiter, Metadata, SCTime, DepDict, MyNode, [], PartitionType, RemovePrepNum, RemoveDepType, update);
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
                            {LastReaderTime, FirstPrepTime, PrepNum} = Metadata,
                            {lists:reverse(List), {LastReaderTime, FirstPrepTime, PrepNum-RemovePrepNum}, PPTime, Readers, DepDict1}
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
                    {LastReaderTime, FirstPrepTime, PrepNum} = Metadata,
                    {lists:reverse(List), {LastReaderTime, FirstPrepTime, PrepNum-RemovePrepNum}, PPTime, Readers, DepDict1}
            end
    end.

abort_from_dep_dict(TxId, DepDict, PartitionType, MyNode) ->
    case dict:find(TxId, DepDict) of
        {ok, {_, _, Sender}} ->
            gen_server:cast(Sender, {aborted, TxId}),
            dict:erase(TxId, DepDict);
        {ok, {_, _, _, Sender, _Type, _WriteSet}} ->
            gen_server:cast(Sender, {aborted, TxId, MyNode}),
            case PartitionType of master -> clocksi_vnode:abort([MyNode], TxId); _ -> ok end,
            dict:erase(TxId, DepDict);
        {ok, {_, _, _, Sender, local}} ->
            gen_server:cast(Sender, {aborted, TxId}),
            case PartitionType of master -> clocksi_vnode:abort([MyNode], TxId); _ -> ok end,
            dict:erase(TxId, DepDict);
        error ->
            DepDict
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
   %lager:warning("Trying to unblocking transaction ~p", [TxId]),
    case dict:find(TxId, DepDict) of
        {ok, {PendPrepDep, PrepareTime, Sender}} ->
           %lager:warning("PendPrepDep is ~w", [PendPrepDep]),
            case PendPrepDep of
                1 -> gen_server:cast(Sender, {prepared, TxId, PrepareTime, {node(), self()}}), dict:erase(TxId, DepDict); 
                _ -> dict:store(TxId, {PendPrepDep-1, PrepareTime, Sender}, DepDict) 
            end;
        {ok, {1, PrepDep, PrepareTime, Sender, RepMode, WriteSet}} ->
            case RepMode of local -> gen_server:cast(Sender, {pending_prepared, TxId, PrepareTime});
                            _ -> ok
            end,
            dict:store(TxId, {0, PrepDep+1, PrepareTime, Sender, RepMode, WriteSet}, DepDict);
        {ok, {N, PrepDep, PrepareTime, Sender, RepMode, WriteSet}} ->
          %lager:warning("~p updates dep to ~p", [TxId, N-1]),
            dict:store(TxId, {N-1, PrepDep+1, PrepareTime, Sender, RepMode, WriteSet}, DepDict);
        error -> %lager:warning("Unblock txn: no record in dep dict!"),  
            DepDict
    end;
%% Reduce prepared dependency 
unblock_prepare(TxId, DepDict, Partition, RemoveDepType) ->
   %lager:warning("Trying to unblocking prepared transaction ~p, RemveDepType is ~p, dep dict is ~w", [TxId, RemoveDepType, DepDict]),
    case dict:find(TxId, DepDict) of
        {ok, {PendPrepDep, PrepareTime, Sender}} ->
           %lager:warning("~p Removing in slave replica", [TxId]),
            case PendPrepDep of
                1 -> gen_server:cast(Sender, {prepared, TxId, PrepareTime, {node(), self()}}), dict:erase(TxId, DepDict); 
                _ -> dict:store(TxId, {PendPrepDep-1, PrepareTime, Sender}, DepDict) 
            end;
        {ok, {0, 1, PrepareTime, Sender, local}} ->
           %lager:warning("~p Removing in the last prep dep", [TxId]),
            RemoveDepType = remove_pd,
            gen_server:cast(Sender, {solve_pending_prepared, TxId, PrepareTime, {node(), self()}}),
            dict:erase(TxId, DepDict);
        {ok, {0, N, PrepareTime, Sender, local}} ->
           %lager:warning("~p Removing in the last prep dep", [TxId]),
            RemoveDepType = remove_pd,
            dict:store(TxId, {0, N-1, PrepareTime, Sender, local}, DepDict);
        {ok, {0, 1, PrepareTime, Sender, RepMode, TxWriteSet}} ->
           %lager:warning("~p Removing in the last prep dep", [TxId]),
            RemoveDepType = remove_pd,
            gen_server:cast(Sender, {solve_pending_prepared, TxId, PrepareTime, {node(), self()}}),
            RepMsg = {Sender, RepMode, TxWriteSet, PrepareTime}, 
            repl_fsm:repl_prepare(Partition, prepared, TxId, RepMsg), 
            %DepDict1 = dict:update_counter(success_wait, 1, DepDict), 
            dict:erase(TxId, DepDict);
        {ok, {1, PrepDep, PrepareTime, Sender, RepMode, TxWriteSet}} ->
           %lager:warning("PrepDep is ~w", [PrepDep]),
            case RemoveDepType of 
                remove_ppd ->
                   %lager:warning("~p Removing ppd, PrepDep is ~w", [TxId, PrepDep]),
                    case PrepDep of 
                        0 -> gen_server:cast(Sender, {prepared, TxId, PrepareTime, {node(), self()}}), 
                            %case RepMode of local -> ok;
                            RepMsg = {Sender, RepMode, TxWriteSet, PrepareTime},
                            repl_fsm:repl_prepare(Partition, prepared, TxId, RepMsg),
                            %end,
                            dict:erase(TxId, DepDict);
                        _ ->
                            gen_server:cast(Sender, {pending_prepared, TxId, PrepareTime}),
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
                   %lager:warning("~p Removing pd", [TxId]),
                    dict:store(TxId, {1, PrepDep-1, PrepareTime, Sender, RepMode, TxWriteSet}, DepDict)
            end;
        {ok, {PendPrepDep, PrepDep, PrepareTime, Sender, RepMode, WriteSet}} ->
          %lager:warning("~w Herre", [TxId]),
            case RemoveDepType of 
                remove_ppd -> dict:store(TxId, {PendPrepDep-1, PrepDep, PrepareTime, Sender, RepMode, WriteSet}, DepDict);
                remove_pd -> dict:store(TxId, {PendPrepDep, PrepDep-1, PrepareTime, Sender, RepMode, WriteSet}, DepDict)
            end;
        error ->%lager:warning("Unblock txn: no record in dep dict!"),  
            DepDict
    end.

%reply(Sender, Result) ->
%    riak_core_vnode:reply(Sender, Result).

pre_commit([], _TxId, _SCTime, _InMemoryStore, _PreparedTxs, DepDict, _Partition, _, _, _PartitionType) ->
    %dict:update(commit_diff, fun({Diff, Cnt}) -> {Diff+SCTime-PrepareTime, Cnt+1} end, DepDict);
    DepDict;
pre_commit([Key|Rest], TxId, SCTime, InMemoryStore, PreparedTxs, DepDict, Partition, LOC, FFC, PartitionType) ->
    MyNode = {Partition, node()},
   %lager:warning("Trying to insert key ~p with for ~p, specula commit time is ~p", [Key, TxId, SCTime]),
    case ets:lookup(PreparedTxs, Key) of
        %% If this one is prepared, no one else can be specula-committed already, so sc-time should be the same as prep time 
        [{Key, {LastReaderTime, LastPrepTime, PrepNum}, [{prepared, TxId, PrepareTime, Value, PendingReaders}|Deps]=_Record}] ->
          %lager:warning("In prep, PrepNum is ~w, record is ~w", [PrepNum, _Record]),
            %case Deps of
            %    [{FTxId,_,F,_,_}] ->%lager:warning("FTxId is ~w, F is ~w, FirstPPTime is ~w", [FTxId, F, LastPrepTime]), 
            %                     F=LastPrepTime;
            %    _ -> ok
            %end,
            {StillPend, ToPrev} = reply_pre_commit(PartitionType, PendingReaders, SCTime, {LOC, FFC, Value}, TxId),
            case ToPrev of
                [] ->
                    ets:insert(PreparedTxs, [{Key, {LastReaderTime, LastPrepTime, PrepNum-1}, [{pre_commit, TxId, PrepareTime, 
                            {LOC, FFC, Value}, StillPend}|Deps] }]),
                    pre_commit(Rest, TxId, SCTime, InMemoryStore, 
                          PreparedTxs, DepDict, Partition, LOC, FFC, PartitionType);
                _ ->
                    %% Let these readers read the previous guy...
                   %lager:warning("In multi read version, Key is ~w, Deps is ~w, ToPrev is ~w", [Key, Deps, ToPrev]),
                    AfterReadRecord = multi_read_version(Key, Deps, ToPrev, InMemoryStore),
                    ets:insert(PreparedTxs, [{Key, {LastReaderTime, LastPrepTime, PrepNum-1}, [{pre_commit, TxId, PrepareTime, 
                            {LOC, FFC, Value}, StillPend}|AfterReadRecord] }]),
                    pre_commit(Rest, TxId, SCTime, InMemoryStore, 
                          PreparedTxs, DepDict, Partition, LOC, FFC, PartitionType)
            end;
        [{Key, Metadata, RecordList}] ->
          %lager:warning("SC commit for ~w, ~p, meta data is ~w, RC list is ~w", [TxId, Key, Metadata, RecordList]),
            %case RecordList of
            %    [{_,_, F, _, _}] -> F = FirstPrepTime;
            %    _ -> ok
            %end,
            case find_prepare_record(RecordList, TxId) of
                [] -> 
                   %lager:warning("Did not find record! Record list is ~w", [RecordList]),
                    pre_commit(Rest, TxId, SCTime, InMemoryStore, 
                          PreparedTxs, DepDict, Partition, LOC, FFC, PartitionType);
                {Prev, {TxId, TxPrepTime, TxSCValue, PendingReaders}, RestRecords} ->
                    OpType = case TxSCValue of read -> read; _ -> update end,
                    {RemainRecords, Metadata1, _, AbortedReaders, DepDict1} = deal_pending_records(Prev, Metadata, SCTime, DepDict, MyNode, [], PartitionType, 1, convert_to_pd, OpType),
                   %lager:warning("Found record! Prev is ~w, Newmeta is ~w, RemainRecords is ~w", [Prev, Metadata1, RemainRecords]),
                    {StillPend, ToPrev} = reply_pre_commit(PartitionType, PendingReaders++AbortedReaders, SCTime, {LOC, FFC, TxSCValue}, TxId),
                    AfterReadRecord = multi_read_version(Key, RestRecords, ToPrev, InMemoryStore),
                    true = TxPrepTime =< SCTime,
                    case AfterReadRecord of
                        [] ->
                            {LastReaderTime, TxPrepTime, NewPrepNum} = Metadata1,
                            ets:insert(PreparedTxs, {Key, {LastReaderTime, SCTime, NewPrepNum}, RemainRecords++[{pre_commit, TxId, SCTime, {LOC, FFC, TxSCValue}, StillPend}]});
                        _ ->
                            ets:insert(PreparedTxs, {Key, Metadata1, RemainRecords++[{pre_commit, TxId, SCTime, {LOC, FFC, TxSCValue}, StillPend}|AfterReadRecord]})
                    end,
                    pre_commit(Rest, TxId, SCTime, InMemoryStore, PreparedTxs,
                        DepDict1, Partition, LOC, FFC, PartitionType)
                    %%% Just to expose more erros if possible
            end;
        _R ->
            %% TODO: Need to fix this
           %lager:error("The txn is actually aborted already ~w, ~w", [Key, R]),
            pre_commit(Rest, TxId, SCTime, InMemoryStore, PreparedTxs,
                DepDict, Partition, LOC, FFC, PartitionType)
            %R = 2
    end.

reply_pre_commit(PartitionType, PendingReaders, SCTime, MValue, PreparedTxId) ->
   %lager:warning("Replying specula commit: pendiing readers are ~w", [PendingReaders]),
    case PartitionType of
        cache ->
              ToPrev = lists:foldl(fun({ReaderTxId, _, Sender}=Reader, TP) ->
                        SnapshotTime = ReaderTxId#tx_id.snapshot_time,
                        case SnapshotTime >= SCTime of
                            true ->
                                {PLOC, PFFC, Value} = MValue,
                                %lager:warning("Trying to reply"),
                                case is_safe_read(PLOC, PFFC, PreparedTxId, ReaderTxId) of
                                    true -> gen_server:reply(Sender, {ok, Value}), TP;
                                    false -> 
                                        gen_server:cast(ReaderTxId#tx_id.server_pid, 
                                            {read_blocked, ReaderTxId, PLOC, PFFC, Value, Sender}),
                                        TP
                                        %% Maybe place in waiting queue instead of reading it?
                                end;
                            false ->
                               %lager:warning("Block it"),
                                [Reader|TP]
                        end end,
                    [], PendingReaders),
                {[], ToPrev};
        _ ->
            lists:foldl(fun({ReaderTxId, Sender}=ReaderInfo, {Pend, ToPrev}) ->
                case ReaderTxId#tx_id.snapshot_time >= SCTime of
                    true ->
                        case sc_by_local(ReaderTxId) of
                            true ->
                                {PLOC, PFFC, Value} = MValue,
                                case is_safe_read(PLOC, PFFC, PreparedTxId, ReaderTxId) of
                                    true -> gen_server:reply(Sender, {ok, Value});
                                    false -> 
                                        gen_server:cast(ReaderTxId#tx_id.server_pid, 
                                            {read_blocked, ReaderTxId, PLOC, PFFC, Value, Sender})
                                        %% Maybe place in waiting queue instead of reading it?
                                end,
                                {Pend, ToPrev};
                            false ->
                                 %lager:warning("Adding ~w to pend", [Sender]),
                                {[ReaderInfo|Pend], ToPrev}
                        end;
                    false ->
                             %lager:warning("Adding ~w to to-prev", [Sender]),
                            {Pend, [ReaderInfo|ToPrev]}
                end
            end, {[], []}, PendingReaders)
    end.

reply_to_all(PartitionType, PendingReaders, CommitTime, Value) ->
    case PartitionType of
        cache ->
              lists:foldl(fun({ReaderTxId, _Node, Sender}=ReaderInfo, TP) ->
                        SnapshotTime = ReaderTxId#tx_id.snapshot_time,
                        case SnapshotTime >= CommitTime of
                            true ->
                                gen_server:reply(Sender, {ok, Value}),
                                TP;
                            false -> [ReaderInfo|TP]
                        end end,
                    [], PendingReaders);
        _ ->
            lists:foldl(fun({RTxId, Sender}=ReaderInfo, ToPrev) ->
                case RTxId#tx_id.snapshot_time >= CommitTime of
                    true ->
                        case Sender of
                            {remote, Client} -> 
                                gen_server:cast(RTxId#tx_id.server_pid, {rr_value, RTxId, Client, CommitTime, Value});
                            _ -> gen_server:reply(Sender, {ok, Value})
                        end,
                        ToPrev;
                    false ->
                        [ReaderInfo|ToPrev]
                end
            end, [], PendingReaders)
    end.

find_prepare_record(RecordList, TxId) ->
    R = find_prepare_record(RecordList, TxId, []),
   %lager:warning("Returning record ~w", [R]),
    R.
  
find_prepare_record([], _TxId, _Prev) ->
    [];
find_prepare_record([{prepared, TxId, TxPrepTime, Value, Readers}|Rest], TxId, Prev) ->
    {Prev, {TxId, TxPrepTime, Value, Readers}, Rest};
find_prepare_record([Record|Rest], TxId, Prev) ->
    find_prepare_record(Rest, TxId, [Record|Prev]).

%% TODO: allowing all speculative read now! Maybe should not be so aggressive
specula_read(TxId, Key, PreparedTxs, SenderInfo) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(PreparedTxs, Key) of
        [] ->
            ets:insert(PreparedTxs, {Key, SnapshotTime}),
            ready;
        [{Key, {LastReaderTime, FirstPrepTime, PendPrepNum}, [{Type, PreparedTxId, PrepareTime, MValue, PendingReader}| PendingPrepare]}] ->
          %lager:warning("~p: has ~p with ~p, Type is ~p, lastpp time is ~p, pend prep num is ~p, pending prepares are ~p",[Key, PreparedTxId, PrepareTime, Type, FirstPrepTime, PendPrepNum, PendingPrepare]),
            case SnapshotTime < FirstPrepTime of
                true ->
                    ready;
                false ->
                    %% Read previous version
                   %lager:warning("Trying to read appr version, pending preapre is ~w", [PendingPrepare]),
                    {IfReady, Record} = read_appr_version(TxId, PendingPrepare, [], SenderInfo),
                    case IfReady of
                        not_ready ->
                            ets:insert(PreparedTxs, [{Key, {max(SnapshotTime, LastReaderTime), FirstPrepTime, PendPrepNum}, [{Type, PreparedTxId, PrepareTime, MValue, PendingReader}|Record]}]),
                            not_ready;
                        wait ->
                            wait;
                        specula ->
                            case SnapshotTime > LastReaderTime of
                                true -> ets:insert(PreparedTxs, {Key, {SnapshotTime, FirstPrepTime, PendPrepNum}, [{Type, PreparedTxId, PrepareTime, MValue, PendingReader}| PendingPrepare]});
                                false -> ok
                            end,
                            SCValue = Record,
                            case SCValue of
                                read ->lager:warning("~w spec reading read!", [TxId]);
                                _ -> ok
                            end,
                            {specula, SCValue};
                        ready ->
                            case SnapshotTime > LastReaderTime of
                                true -> ets:insert(PreparedTxs, {Key, {SnapshotTime, FirstPrepTime, PendPrepNum}, [{Type, PreparedTxId, PrepareTime, MValue, PendingReader}| PendingPrepare]});
                                false -> ok
                            end,
                            ready
                    end
            end;
        [{Key, LastReaderTime}] ->
            ets:insert(PreparedTxs, {Key, max(SnapshotTime, LastReaderTime)}),
            ready
    end.

%add_read_dep(ReaderTx, WriterTx, _Key) ->
%    %lager:warning("Inserting anti_dep from ~p to ~p", [ReaderTx, WriterTx]),
%    ets:insert(dependency, {WriterTx, ReaderTx}),
%    ets:insert(anti_dep, {ReaderTx, WriterTx}).

%% What wans to delete/commit is not found, This is problematic! If commits arrive out-of-order, need to use a queue to store it
delete_and_read(_DeleteType, _, _, _, _Key, DepDict, _, _, [], _TxId, _, _) ->
    %lager:warning("Want to ~w ~w for key ~w, but arrived already", [_DeleteType, _TxId, _Key]),
    DepDict;
delete_and_read(DeleteType, PreparedTxs, InMemoryStore, TxCommitTime, Key, DepDict, PartitionType, Partition, [{Type, TxId, _Time, MValue, PendingReaders}|Rest], TxId, Prev, Metadata) ->
    %% If can read previous version: read
    %% If can not read previous version: add to previous pending
    %lager:warning("Delete and read ~w for ~w, prev is ~w, metadata is ~w, rest is ~w, type is ~w, MValue is ~w", [DeleteType, TxId, Prev, Metadata, Rest, Type, MValue]),
    Value = case Type of pre_commit -> {_, _, V} = MValue, V; _ -> MValue end, 
    case Value of
        read ->
            ToRemovePrep = case Type of prepared -> 1; repl_prepare -> 1; _ -> 0 end,
            RemoveDepType = case DeleteType of 
                                commit ->
                                    case Type of pre_commit -> remove_pd; _ -> remove_ppd end;
                                abort ->
                                    case Rest of 
                                        [] -> case Type of pre_commit -> remove_pd; _ -> remove_ppd end;
                                        [{prepared, _, _, _, _}|_] -> not_remove;
                                        [{repl_prepare, _, _, _, _}|_] -> not_remove; 
                                        [{pre_commit, _, _, _, _}|_] ->
                                            case Type of pre_commit -> not_remove; _ -> convert_to_pd end
                                    end
                            end,
           %lager:warning("~w read before pending record", [TxId]),
            RR 
                = deal_pending_records(Prev, Metadata, TxCommitTime, DepDict, {Partition, node()}, [], PartitionType, ToRemovePrep, RemoveDepType, read),
           %lager:warning("~w got ~w", [TxId, RR]),
            {RemainPrev, Metadata1, LastPrepTime, AbortReaders, DepDict1} = RR, 
            ToPrev = case DeleteType of 
                        abort -> AbortReaders++PendingReaders; 
                        commit -> reply_to_all(PartitionType, AbortReaders++PendingReaders, TxCommitTime, Value)
                     end,
            AfterReadRecord = multi_read_version(Key, Rest, ToPrev, InMemoryStore),
            case AfterReadRecord of
                [] ->
                    Rest = [],
                    case RemainPrev of
                        [] ->
                            {LastReaderTime, _, 0} = Metadata1,
                            ets:insert(PreparedTxs, {Key, LastReaderTime});
                        _ ->
                            %{_,_,NewFirstPrepTime,_,_} = lists:last(RemainPrev),
                            {LastReaderTime, _OldFirstPrep, NewPrepNum} = Metadata1,
                            ets:insert(PreparedTxs, {Key, {LastReaderTime, LastPrepTime, NewPrepNum}, RemainPrev})
                    end;
                _ ->
                    ets:insert(PreparedTxs, {Key, Metadata1, RemainPrev++AfterReadRecord})
            end,
            DepDict1;
        _ ->
            ToRemovePrep = case Type of prepared -> 1; repl_prepare -> 1; _ -> 0 end,
            RemoveDepType = case DeleteType of 
                                commit ->
                                    case Type of pre_commit -> remove_pd; _ -> remove_ppd end;
                                abort ->
                                    case Rest of 
                                        [] -> case Type of pre_commit -> remove_pd; _ -> remove_ppd end;
                                        [{prepared, _, _, _, _}|_] -> not_remove;
                                        [{repl_prepare, _, _, _, _}|_] -> not_remove; 
                                        [{pre_commit, _, _, _, _}|_] ->
                                            case Type of pre_commit -> not_remove; _ -> convert_to_pd end
                                    end
                            end,
           %lager:warning("~w normal before pending record", [TxId]),
            {RemainPrev, Metadata1, LastPrepTime, AbortReaders, DepDict1} 
                = deal_pending_records(Prev, Metadata, TxCommitTime, DepDict, {Partition, node()}, [], PartitionType, ToRemovePrep, RemoveDepType, update),
            ToPrev = case DeleteType of 
                        abort -> AbortReaders++PendingReaders; 
                        commit -> reply_to_all(PartitionType, AbortReaders++PendingReaders, TxCommitTime, Value)
                     end,
            AfterReadRecord = multi_read_version(Key, Rest, ToPrev, InMemoryStore),
            case AfterReadRecord of
                [] ->
                    Rest = [],
                    case RemainPrev of
                        [] ->
                            {LastReaderTime, _, 0} = Metadata1,
                            ets:insert(PreparedTxs, {Key, LastReaderTime});
                        _ ->
                            %{_,_,NewFirstPrepTime,_,_} = lists:last(RemainPrev),
                            {LastReaderTime, OldFirstPrep, NewPrepNum} = Metadata1,
                            %LastPrepTime = NewFirstPrepTime,
                            case LastPrepTime =< OldFirstPrep of
                                true ->
                                  lager:warning("OldFirstPrep is ~w, RemainPrev is ~w, LastPrepTime is ~w, Key is ~w, TxId is ~w", [OldFirstPrep, RemainPrev, LastPrepTime, Key, TxId]);
                                false -> ok
                            end,
                            %true = OldFirstPrep < LastPrepTime,
                            ets:insert(PreparedTxs, {Key, {LastReaderTime, LastPrepTime, NewPrepNum}, RemainPrev})
                    end;
                _ ->
                    ets:insert(PreparedTxs, {Key, Metadata1, RemainPrev++AfterReadRecord})
            end,
            DepDict1
    end;
delete_and_read(DeleteType, PreparedTxs, InMemoryStore, TxCommitTime, Key, DepDict, PartitionType, MyNode, [Current|Rest], TxId, Prev, FirstOne) ->
    delete_and_read(DeleteType, PreparedTxs, InMemoryStore, TxCommitTime, Key, DepDict, PartitionType, MyNode, Rest, TxId, [Current|Prev], FirstOne);
delete_and_read(abort, _PreparedTxs, _InMemoryStore, _TxCommitTime, _Key, DepDict, _PartitionType, _MyNode, [], _TxId, _Prev, _Whatever) ->
    %lager:warning("Abort but got nothing"),
    DepDict.

read_appr_version(_ReaderTxId, [], _Prev, _SenderInfo) ->
    {ready, []}; 
read_appr_version(ReaderTxId, [H={Type, SCTxId, SCTime, SCValue, SCPendingReaders}|Rest], Prev, SenderInfo) when ReaderTxId#tx_id.snapshot_time >= SCTime ->
    case Type of
        prepared ->
            {not_ready, lists:reverse(Prev)++[{Type, SCTxId, SCTime, SCValue, [SenderInfo|SCPendingReaders]}|Rest]};
        repl_prepare ->
            {not_ready, lists:reverse(Prev)++[{Type, SCTxId, SCTime, SCValue, [SenderInfo|SCPendingReaders]}|Rest]};
        pre_commit ->
            case sc_by_local(ReaderTxId) of
                true ->
                   %lager:warning("~p reads specula value ~p!", [SCTxId, SCValue]),
                    {PLOC, PFFC, Value} = SCValue,
                    case Value of
                        read ->
                            read_appr_version(ReaderTxId, Rest, [H|Prev], SenderInfo);
                        _ ->
                            case is_safe_read(PLOC, PFFC, SCTxId, ReaderTxId) of
                                true -> {specula, Value}; 
                                false -> 
                                        Sender = case SenderInfo of {_, _, S} -> S; {_, S} -> S end,   
                                        gen_server:cast(ReaderTxId#tx_id.server_pid, 
                                            {read_blocked, ReaderTxId, PLOC, PFFC, Value, Sender}),
                                        {wait, []} 
                            end
                    end;
                false ->
                   %lager:warning("~p can read specula value, because not by local!", [SCTxId]),
                    {not_ready, lists:reverse(Prev)++[{Type, SCTxId, SCTime, SCValue, [SenderInfo|SCPendingReaders]}|Rest]}
            end
    end;
read_appr_version(ReaderTxId, [H|Rest], Prev, SenderInfo) -> 
    read_appr_version(ReaderTxId, Rest, [H|Prev], SenderInfo).

multi_read_version(_Key, List, [], _) -> 
    List;
multi_read_version({_, RealKey}, [], SenderInfos, ignore) -> %% In cache 
    lists:foreach(fun({ReaderTxId, Node, Sender}) ->
            %lager:warning("Send read of ~w from ~w to ~w", [RealKey, ReaderTxId, Sender]),
            clocksi_vnode:remote_read(Node, RealKey, ReaderTxId, Sender)
                  end, SenderInfos),
    [];
multi_read_version(Key, [], SenderInfos, InMemoryStore) -> 
    %% Let all senders read
    Value = case ets:lookup(InMemoryStore, Key) of
                [] ->
                    [];
                [{Key, ValueList}] ->
                    [{_CommitTime, First}|_T] = ValueList,
                    First
            end,
    lists:foreach(fun({_ReaderTxId, Sender}) ->
                        gen_server:reply(Sender, {ok, Value})
                  end, SenderInfos),
    [];
multi_read_version(Key, [{pre_commit, SCTxId, SCTime, SCValue, SCPendingReaders}|Rest], SenderInfos, InMemoryStore) -> 
    RemainReaders = lists:foldl(fun(Reader, Pend) ->
            {TxId, Sender} = case Reader of {RTxId, _, S} -> {RTxId, S}; {RTxId, S} -> {RTxId, S} end,
            case TxId#tx_id.snapshot_time >= SCTime of
                true ->
                    {PLOC, PFFC, Value} = SCValue,
                    case is_safe_read(PLOC, PFFC, SCTxId, TxId) of
                        true -> gen_server:reply(Sender, {ok, Value});
                        false ->
                                gen_server:cast(TxId#tx_id.server_pid, 
                                    {read_blocked, TxId, PLOC, PFFC, Value, Sender})
                            %% Maybe place in waiting queue instead of reading it?
                    end,
                    Pend;
                false ->
                    [Reader|Pend] 
            end end, [], SenderInfos),
    [{pre_commit, SCTxId, SCTime, SCValue, SCPendingReaders}|multi_read_version(Key, Rest, RemainReaders, InMemoryStore)];
multi_read_version(_Key, [{Type, SCTxId, SCTime, SCValue, SCPendingReaders}|Rest], SenderInfos, _InMemoryStore) -> 
    [{Type, SCTxId, SCTime, SCValue, SenderInfos++SCPendingReaders}|Rest].

sc_by_local(TxId) ->
    node(TxId#tx_id.server_pid) == node().

insert_prepare(PreparedTxs, TxId, Partition, WriteSet, TimeStamp, Sender) ->
    case ets:lookup(PreparedTxs, {TxId, Partition}) of
          [] ->
              {KeySet, ToPrepTS} = lists:foldl(fun({Key, _Value}, {KS, Ts}) ->
                                          case ets:lookup(PreparedTxs, Key) of
                                              [] -> {[Key|KS], Ts};
                                              [{Key, {LastReaderTS, _, _}, [{_Type, _PrepTxId, _, _, _}|_Rest]}] ->
                                                  {[Key|KS], max(Ts, LastReaderTS+1)};
                                              [{Key, LastReaderTS}] ->  
                                                  {[Key|KS], max(Ts, LastReaderTS+1)}
                                          end end, {[], TimeStamp}, WriteSet),
              lists:foreach(fun({Key, Value}) ->
                          case ets:lookup(PreparedTxs, Key) of
                              [] ->
                                  true = ets:insert(PreparedTxs, {Key, {ToPrepTS, ToPrepTS, 1}, [{repl_prepare, TxId, ToPrepTS, Value, []}]});
                              [{Key, LastReaderTime}] -> 
                                  true = ets:insert(PreparedTxs, {Key, {max(LastReaderTime, ToPrepTS), ToPrepTS, 1}, [{repl_prepare, TxId, ToPrepTS, Value, []}]});
                              [{Key, {LastReaderTS, LastSCTime, PrepNum}, List}] ->
                                  NewList = add_to_list(TxId, ToPrepTS, Value, List),
                                  true = ets:insert(PreparedTxs, {Key, {LastReaderTS, min(LastSCTime, ToPrepTS), PrepNum+1}, NewList})
              end end,  WriteSet),
             %lager:warning("Got repl prepare for ~p, propose ~p and replied", [TxId, ToPrepTS]),
              ets:insert(PreparedTxs, {{TxId, Partition}, KeySet}),
              gen_server:cast(Sender, {solve_pending_prepared, TxId, ToPrepTS, {node(), self()}});
          _R ->
              %lager:warning("Not replying for ~p, ~p because already prepard, Record is ~p", [TxId, Partition, _R]),
              ok
      end.

add_to_list(ToPrepTxId, ToPrepTS, ToPrepValue, [{Type, PrepTxId, PrepTS, PrepValue, Reader}|Rest] = L) ->
    case ToPrepTS > PrepTS of
        true -> 
                %case PrepTxId#tx_id.snapshot_time > ToPrepTxId#tx_id.snapshot_time of
                %    true ->%lager:warning("L ~w must have been aborted already!", [L]), 
                %            [{repl_prepare, ToPrepTxId, ToPrepTS, ToPrepValue, []}]; 
                [{repl_prepare, ToPrepTxId, ToPrepTS, ToPrepValue, []}|L];
                %end;
        false -> [{Type, PrepTxId, PrepTS, PrepValue, Reader} | add_to_list(ToPrepTxId, ToPrepTS, ToPrepValue, Rest)]
    end;
add_to_list(ToPrepTxId, ToPrepTS, ToPrepValue, []) ->
    [{repl_prepare, ToPrepTxId, ToPrepTS, ToPrepValue, []}].

ready_or_block(TxId, Key, PreparedTxs, SenderInfo) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    case ets:lookup(PreparedTxs, Key) of
        [] ->
            ets:insert(PreparedTxs, {Key, SnapshotTime}),
            ready;
        [{Key, {LastReaderTime, FirstPrepTime, PrepNum}, CurrentRecord}] ->
            case SnapshotTime < FirstPrepTime of
                true -> ready;
                false ->
                  %lager:warning("~p Not ready.. ~p FirstPrepTime is ~w", [Key, TxId, FirstPrepTime]),
                    {Res, Record} = insert_pend_reader(CurrentRecord, SnapshotTime, SenderInfo),
                    ets:insert(PreparedTxs, {Key, {max(SnapshotTime, LastReaderTime), FirstPrepTime, PrepNum}, Record}),
                    Res
            end;
        [{Key, LastReaderTime}] ->
            ets:insert(PreparedTxs, {Key, max(SnapshotTime, LastReaderTime)}),
            ready
    end.

insert_pend_reader([], _, _) ->
    {ready, []};
insert_pend_reader([{Type, PrepTxId, PrepareTime, PrepValue, RWaiter}|Rest], SnapshotTime, SenderInfo) when SnapshotTime >= PrepareTime andalso PrepValue /= read ->
    {not_ready, [{Type, PrepTxId, PrepareTime, PrepValue, [SenderInfo|RWaiter]}|Rest]}; 
insert_pend_reader([Record|Rest], SnapshotTime, SenderInfo) ->
    {Res, ListRes} = insert_pend_reader(Rest, SnapshotTime, SenderInfo),
    {Res, [Record|ListRes]}.


is_safe_read(PLOC, PFFC, PreparedTxId, TxId) ->
    ets:insert(dependency, {PreparedTxId, TxId}),
    case ets:lookup(anti_dep, TxId) of
        [] ->
            %lager:warning("Insert ~w ~w for ~w, of ~w", [PLOC, PFFC, TxId, PreparedTxId]),
            ets:insert(anti_dep, {TxId, {PLOC, [PLOC]}, PFFC, [PreparedTxId]}),
            true;
        [{TxId, {OldLOC, LOCList}, FFC, Deps}] ->
            NLOC = min(OldLOC, PLOC),
            NFFC = max(FFC, PFFC),
            case NLOC >= NFFC of
                true ->
                    %lager:warning("NewLOC is ~w, NewFFC is ~w, safe!", [NLOC, NFFC]),
                    NLOCList = case PLOC of inf -> LOCList; _ -> [PLOC|LOCList] end,
                    ets:insert(anti_dep, {TxId, {NLOC, NLOCList}, NFFC, [PreparedTxId|Deps]}),
                    true;
                false ->
                    %lager:warning("NewLOC is ~w, NewFFC is ~w, unsafe!", [NLOC, NFFC]),
                    false
            end
    end.

read_value(Key, TxId, Sender, InMemoryStore) ->
    case ets:lookup(InMemoryStore, Key) of
        [] ->
            gen_server:reply(Sender, {ok, []});
        [{Key, ValueList}] ->
            %lager:warning("~w trying to read ~w", [TxId, Key]),
            MyClock = TxId#tx_id.snapshot_time,
            find_version(ValueList, MyClock, TxId, Sender)
    end.

remote_read_value(Key, TxId, Sender, InMemoryStore) ->
   %lager:warning("~w remote reading ~w", [TxId, Key]),
    case ets:lookup(InMemoryStore, Key) of
        [] ->
            %lager:warning("Directly reply to ~w", [Sender]),
            gen_server:reply(Sender, {ok, []});
        [{Key, ValueList}] ->
            %lager:warning("~w trying to read ~w", [TxId, Key]),
            MyClock = TxId#tx_id.snapshot_time,
            find_version_for_remote(ValueList, MyClock, TxId, Sender)
    end.

find_version([],  _SnapshotTime, _, Sender) ->
    gen_server:reply(Sender, {ok, []});
find_version([{TS, Value}|Rest], SnapshotTime, TxId, Sender) ->
    case SnapshotTime >= TS of
        true ->
            case ets:lookup(anti_dep, TxId) of
                [] ->
                    ets:insert(anti_dep, {TxId, {inf, []}, TS, []}),
                    gen_server:reply(Sender, {ok, Value});
                [{TxId, {LOC, LOCList}, FFC, Deps}] ->
                    case TS =< FFC of
                        true ->
                            gen_server:reply(Sender, {ok, Value});
                        false ->
                            case TS =< LOC of
                                true ->
                                    ets:insert(anti_dep, {TxId, {LOC, LOCList}, TS, Deps}),
                                    gen_server:reply(Sender, {ok, Value});
                                false ->
                                   %lager:warning("~w has ~w, ~w, ~w, ~w", [TxId, LOC, LOCList, FFC, Deps]),
                                   %lager:warning("~w blocked when trying to read, TS is ~w, LOC is ~w", [TxId, TS, LOC]),
                                    case Value of
                                        read -> lager:warning("~w reading read!! TS is ~w", [TxId, TS]);
                                        _ -> ok
                                    end,
                                    gen_server:cast(TxId#tx_id.server_pid, {read_blocked, TxId, inf, TS, Value, Sender})
                            end
                    end 
            end;
        false ->
            find_version(Rest, SnapshotTime, TxId, Sender)
    end.


find_version_for_remote([],  _SnapshotTime, _, Sender) ->
    gen_server:reply(Sender, {ok, []});
find_version_for_remote([{TS, Value}|Rest], SnapshotTime, TxId, Sender) ->
    case SnapshotTime >= TS of
        true ->
           %lager:warning("Send ~w of ~w to its coord, It's TS is ~w", [Value, TxId, TS]),
            gen_server:cast(TxId#tx_id.server_pid, {rr_value, TxId, Sender, TS, Value});
        false ->
            find_version_for_remote(Rest, SnapshotTime, TxId, Sender)
    end.
