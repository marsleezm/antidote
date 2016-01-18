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
-module(helper).

-include("antidote.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
        verify_table/2,
        gather_abort_stat/1,
        check_top_aborted/2,
        check_key_record/3,
        if_prepared/3,
        relay_read_stat/1,
	    check_tables_ready/0,
	    check_prepared_empty/0,
        num_specula_read/1,
        print_stat/0]).

-export([
        handle_verify_table/2,
        handle_relay_read_stat/1,
        handle_check_key_record/4,
        handle_if_prepared/3,
        handle_command_print_stat/8,
        handle_command_check_tables_ready/1,
        handle_command_check_prepared_empty/1,
        handle_check_top_aborted/4
        ]).



relay_read_stat(Node) ->
    riak_core_vnode_master:sync_command(Node,
                           {relay_read_stat},
                           ?CLOCKSI_MASTER, infinity).

gather_abort_stat(Len) ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    PartitionList = chashbin:to_list(CHBin),
    lists:map(fun(Partition) ->
            R = check_top_aborted(Partition, Len),
            lager:info("~w: ~p", [Partition, R]),
            {Partition, R}
            end, PartitionList).
  
check_top_aborted(Partition, Len) ->
      riak_core_vnode_master:sync_command(Partition,
                   {check_top_aborted, Len},
                   ?CLOCKSI_MASTER,
                   infinity).

verify_table(Node, Repl) ->
    riak_core_vnode_master:sync_command(Node,
                           {verify_table, Repl},
                           ?CLOCKSI_MASTER, infinity).

check_key_record(Node, Key, Type) ->
    riak_core_vnode_master:sync_command(Node,
                       {check_key_record, Key, Type},
                       ?CLOCKSI_MASTER, infinity).

if_prepared(Node, TxId, Keys) ->
    riak_core_vnode_master:sync_command(Node,
                                   {if_prepared, TxId, Keys},
                                   ?CLOCKSI_MASTER, infinity).

check_tables_ready() ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    PartitionList = chashbin:to_list(CHBin),
    check_table_ready(PartitionList).

check_table_ready([]) ->
    true;
check_table_ready([{Partition,Node}|Rest]) ->
    Result = riak_core_vnode_master:sync_command({Partition,Node},
						 {check_tables_ready},
						 ?CLOCKSI_MASTER,
						 infinity),
    case Result of true -> check_table_ready(Rest);
                   false -> false
    end.

print_stat() ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    PartitionList = chashbin:to_list(CHBin),
    print_stat(PartitionList, {0,0,0,0,0,0,0}).

print_stat([], {CommitAcc, AbortAcc, CertFailAcc, BlockedAcc, TimeAcc, CntAcc, BlockedTime}) ->
    lager:info("Total number committed is ~w, total number aborted is ~w, cer fail is ~w, num blocked is ~w,Avg time is ~w, Avg blocked time is ~w", [CommitAcc, AbortAcc, CertFailAcc, BlockedAcc, TimeAcc div max(1,CntAcc), BlockedTime div max(1,BlockedAcc)]),
    {CommitAcc, AbortAcc, CertFailAcc, BlockedAcc, TimeAcc, CntAcc, BlockedTime};
print_stat([{Partition,Node}|Rest], {CommitAcc, AbortAcc, CertFailAcc, BlockedAcc, TimeAcc, CntAcc, BlockedTime}) ->
    {Commit, Abort, Cert, BlockedA, TimeA, CntA, BlockedTimeA} = riak_core_vnode_master:sync_command({Partition,Node},
						 {print_stat},
						 ?CLOCKSI_MASTER,
						 infinity),
	print_stat(Rest, {CommitAcc+Commit, AbortAcc+Abort, CertFailAcc+Cert, BlockedAcc+BlockedA, TimeAcc+TimeA, CntAcc+CntA, BlockedTimeA+BlockedTime}).

check_prepared_empty() ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    PartitionList = chashbin:to_list(CHBin),
    check_prepared_empty(PartitionList).

num_specula_read(Node) ->
    riak_core_vnode_master:sync_command(Node,
						 {num_specula_read},
						 ?CLOCKSI_MASTER,
						 infinity).

check_prepared_empty([]) ->
    ok;
check_prepared_empty([{Partition,Node}|Rest]) ->
    Result = riak_core_vnode_master:sync_command({Partition,Node},
						 {check_prepared_empty},
						 ?CLOCKSI_MASTER,
						 infinity),
    case Result of
	    true ->
            ok;
	    false ->
            lager:info("Prepared not empty!")
    end,
	check_prepared_empty(Rest).

handle_verify_table(Repl, InMemoryStore) ->
    L = ets:tab2list(InMemoryStore),
    data_repl_serv:verify_table(Repl, L),
    ok.

handle_relay_read_stat(RelayRead) ->
    {N, R} = RelayRead,
    lager:info("Relay read value is ~w, ~w, ~w", [N, R, R div N]),
    R div N.

handle_check_key_record(Key, Type, PreparedTxs, CommittedTxs) ->
    R = case Type of
            prepared ->  ets:lookup(PreparedTxs, Key);
            _ -> ets:lookup(CommittedTxs, Key)
        end,
    R.

handle_if_prepared(TxId, Keys, PreparedTxs) ->
    Result = lists:all(fun(Key) ->
            case ets:lookup(PreparedTxs, Key) of
                [{Key, [{TxId, _, _, _, _}|_]}] -> lager:info("Found sth for key ~w", [Key]), true;
                _ -> lager:info("Nothing for key ~w", [Key]), false
            end end, Keys),
    Result.

handle_command_check_tables_ready(Partition) ->
    Result = case ets:info(tx_utilities:get_table_name(Partition,prepared)) of
		 undefined ->
		     false;
		 _ ->
		     true
	     end,
    Result.

handle_command_print_stat(Partition, NumAborted, BlockedTime,
                    NumCommitted, NumCertFail, NumBlocked, A6, A7) ->
    lager:info("~w: committed is ~w, aborted is ~w, num cert fail ~w, num blocked ~w, avg blocked time ~w",[Partition, NumCommitted, NumAborted, NumCertFail, NumBlocked, BlockedTime div max(1,NumBlocked)]),
    {NumCommitted, NumAborted, NumCertFail, NumBlocked, A6, A7, BlockedTime}.
    
handle_command_check_prepared_empty(PreparedTxs) ->
    PreparedList = ets:tab2list(PreparedTxs),
    case length(PreparedList) of
		 0 ->  true;
		 _ ->  false
    end.

handle_check_top_aborted(Len, LocalAbort, RemoteAbort, DepDict) ->
    {LPC, LPT} = dict:fetch(pa, LocalAbort),
    {LCC, LCT} = dict:fetch(ca, LocalAbort),
    {RPC, RPT} = dict:fetch(pa, RemoteAbort),
    {RCC, RCT} = dict:fetch(ca, RemoteAbort),
    LA1 = dict:erase(pa, LocalAbort),
    LA2 = dict:erase(ca, LA1),
    RA1 = dict:erase(pa, RemoteAbort),
    RA2 = dict:erase(ca, RA1),
    L = dict:to_list(LA2),
    R = dict:to_list(RA2),
    LS = lists:keysort(2, L),
    RS = lists:keysort(2, R),
    RLS = lists:reverse(LS),
    RRS = lists:reverse(RS),
    NumSuccessWait = dict:fetch(success_wait, DepDict),
    {FailByCommit, FCC} = dict:fetch(fucked_by_commit, DepDict),
    {FailByPrep, FPC} = dict:fetch(fucked_by_badprep, DepDict),
    {CommitDiff, DC} = dict:fetch(commit_diff, DepDict),
    lists:flatten(io_lib:format("LA Top:~p, RATop:~p, LocalPrepAbort ~p, ~p, LocalCommitAbort ~p, ~p, RemotePrepAbort ~p, ~p, RemoteCommitAbort ~p, ~p, NumSuccWait: ~p, TimeCFailDiff: ~p, NumCFail: ~p, TimePFailDiff: ~p, NumPFail: ~p, CommitDiff: ~p, CommitNum: ~p ~n", [lists:sublist(RLS, Len), lists:sublist(RRS, Len), LPT div max(1,LPC), LPC, LCT div max(1,LCC), LCC, RPT div RPC, RPC, RCT div RCC, RCC, NumSuccessWait, FailByCommit div max(1,FCC), FCC, FailByPrep div max(1,FPC), FPC, CommitDiff div max(1,DC), DC])).
