%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
% This file is provided to you under the Apache License,
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
-module(specula_test).

-export([confirm/0, spawn_receive/5, replicates/1, specula_test2/1]).

-include_lib("eunit/include/eunit.hrl").
-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Nodes] = rt:build_clusters([4]),
    lager:info("Nodes: ~p", [Nodes]),
    lists:foreach(fun(N) ->
             rpc:call(N, hash_fun, init_hash_fun, [])
            end, Nodes),
    specula_test1(Nodes),
    specula_test2(Nodes),
    specula_test3(Nodes),
    rt:clean_cluster(Nodes),
    pass.

%% Test that writing to local replica works. Basic, updates to local replicas do not need to
%% go through master. The updates in cache_serv will be clean, in local replicas will be changed
%% to stable data. 
specula_test1(Nodes) ->
    lager:info("Test1 started"),
    Node1 = hd(Nodes),
    Node2 = lists:nth(2, Nodes),
    Node3 = lists:nth(3, Nodes),
    Node4 = lists:nth(4, Nodes),
    TxId1 = rpc:call(Node1, tx_cert_sup, start_tx, [1]),
    lager:info("TxId1 is ~w", [TxId1]),
    PartNode1 = rpc:call(Node1, hash_fun, get_local_vnode_by_id, [1]),
    PartNode2 = rpc:call(Node2, hash_fun, get_local_vnode_by_id, [1]),
    PartNode3 = rpc:call(Node3, hash_fun, get_local_vnode_by_id, [1]),
    PartNode4 = rpc:call(Node4, hash_fun, get_local_vnode_by_id, [1]),
    LUps1 = [{PartNode1, [{t1p1k1, 11}, {t1p1k2, 12}]}],
    PartUp2 = {PartNode2, [{t1p2k1, 21}, {t1p2k2, 22}]},
    PartUp3 = {PartNode3, [{t1p3k1, 31}, {t1p3k2, 32}]},
    PartUp4 = {PartNode4, [{t1p4k1, 41}, {t1p4k2, 42}]},
    RUps1 = [PartUp2, PartUp3, PartUp4],
    %% Only partition Part3 is normal.
    ok = rpc:call(Node1, clocksi_vnode, set_debug, [PartNode2, true]),
    ok = rpc:call(Node1, clocksi_vnode, set_debug, [PartNode4, true]),
    {ok, Result1} = rpc:call(Node1, tx_cert_sup, certify, [1, TxId1, LUps1, RUps1]),
    ?assertMatch({specula_commit, _}, Result1),

    timer:sleep(500),
    %% Local master is prepared
    %% The fourth is if prepared in master, fifth is if prepared in slaves 
    check_txn_preparing_state(TxId1, Node1, LUps1, true, true),
    %% Local slave is prepared due to specula prepare
    check_txn_local_states(TxId1, Node1, RUps1, true),

    %% Replica of partition 3 is prepared since prepare is not blocked 
    check_txn_preparing_state(TxId1, Node1, PartUp3, true, true),
    %% Replica of partition 2 and 4 are blocked
    check_txn_preparing_state(TxId1, Node1, PartUp2, true, false),
    check_txn_preparing_state(TxId1, Node1, PartUp4, true, false),

    lager:info("Before replying first"),
    %% After allowing partnode2 to prepare, some of its replicas get prepared
    ok = rpc:call(Node1, clocksi_vnode, do_reply, [PartNode2, TxId1]),
    timer:sleep(500),
    lager:info("After replying first"),
    check_txn_preparing_state(TxId1, Node1, PartUp2, true, true),
    check_txn_preparing_state(TxId1, Node1, PartUp4, true, false),

    %% The transaction will be committed.
    lager:info("Before replying second"),
    ok = rpc:call(Node1, clocksi_vnode, do_reply, [PartNode4, TxId1]),
    timer:sleep(500),
    lager:info("After replying second"),
    
    %% Waits until all value gets committed.
    %% Check that prepared values get cleaned. 
    check_txn_local_states(TxId1, Node1, RUps1, false),
    check_txn_preparing_state(TxId1, Node1, LUps1, false, false),
    check_txn_preparing_state(TxId1, Node1, PartUp2, false, false),
    check_txn_preparing_state(TxId1, Node1, PartUp3, false, false),
    check_txn_preparing_state(TxId1, Node1, PartUp4, false, false),

    read_txn_data(Node1, LUps1, RUps1),

    ok = rpc:call(Node1, clocksi_vnode, set_debug, [PartNode2, false]),
    ok = rpc:call(Node1, clocksi_vnode, set_debug, [PartNode4, false]).

%% Test that read values from previous transaction in the same thread works.
%% Dependency will be created, so if the previous transaction commits with high timestamp,
%% descendent transaction will be aborted; if commit with small timestamp, will be committed. 
specula_test2(Nodes) ->
    lager:info("Test2 started"),
    Node1 = hd(Nodes),
    Node2 = lists:nth(2, Nodes),
    Node3 = lists:nth(3, Nodes),
    Node4 = lists:nth(4, Nodes),
    TxId1 = rpc:call(Node1, tx_cert_sup, start_tx, [1]),
    PartNode1 = rpc:call(Node1, hash_fun, get_local_vnode_by_id, [1]),
    PartNode2 = rpc:call(Node2, hash_fun, get_local_vnode_by_id, [1]),
    PartNode3 = rpc:call(Node3, hash_fun, get_local_vnode_by_id, [1]),
    PartNode4 = rpc:call(Node4, hash_fun, get_local_vnode_by_id, [1]),
    %{Part4, _} = PartNode4,
    
    K1=t2p1k1, K2=t2p2k1, K3=t2p3k1, K4=t2p4k1,
    K5=t2p1k2, K6=t2p2k2, K7=t2p3k2, K8=t2p4k2,
    K9=t2p1k3, K10=t2p2k3, K11=t2p3k3, _K12=t2p4k3,
    K13=t2p1k4, _K14=t2p2k4, K15=t2p3k4, K16=t2p4k4,
    K17=t2p1k5, _K18=t2p2k5, _K19=t2p3k5, K20=t2p4k5,
    LUp1 = [{PartNode1, [{K1, 1}]}],
    PartUp2 = {PartNode2, [{K2, 2}]},
    PartUp3 = {PartNode3, [{K3, 3}]},
    PartUp4 = {PartNode4, [{K4, 4}]},
    RUp1 = [PartUp2, PartUp3, PartUp4],

    DataRepl1='dev1@127.0.0.1repldev2@127.0.0.1',
    DataRepl2='dev1@127.0.0.1repldev3@127.0.0.1',

    ok = rpc:call(Node1, clocksi_vnode, set_debug, [PartNode2, true]),
    ok = rpc:call(Node1, clocksi_vnode, set_debug, [PartNode3, true]),

    lager:info("Cert t1: ~w", [TxId1]),
    {ok, Result0} = rpc:call(Node1, tx_cert_sup, certify, [1, TxId1, LUp1, RUp1]),
    ?assertMatch({specula_commit, _}, Result0),

    TxId2 = rpc:call(Node1, tx_cert_sup, start_tx, [1]),

    Dep1 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, dependency, TxId1]),
    ?assertEqual([], Dep1),
    AntiDep1 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, anti_dep, TxId2]),
    ?assertEqual([], AntiDep1),
    ReadR1 = rpc:call(Node1, cache_serv, read, [K4, TxId2]),
    ?assertEqual({ok, 4}, ReadR1),
    ReadR2 = rpc:call(Node1, data_repl_serv, read, [DataRepl1, K2, TxId2]),
    ?assertEqual({ok, 2}, ReadR2),
    ReadR3 = rpc:call(Node1, data_repl_serv, read, [DataRepl1, K3, TxId2]),
    ?assertEqual({ok, []}, ReadR3),
    Dep2 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, dependency, TxId1]),
    ?assertEqual([{TxId1, TxId2}], Dep2),
    AntiDep2 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, anti_dep, TxId2]),
    ?assertEqual([{TxId2, TxId1}], AntiDep2),

    LUp2 = [{PartNode1, [{K5, 25}]}],
    PartUp22 = {PartNode2, [{K2, 22}, {K6, 26}]},
    PartUp32 = {PartNode3, [{K7, 23}]},
    PartUp42 = {PartNode4, [{K8, 28}]},
    RUp2 = [PartUp22, PartUp32, PartUp42], 
    lager:info("Cert t2 ~w", [TxId2]),
    {ok, Result2} = rpc:call(Node1, tx_cert_sup, certify, [1, TxId2, LUp2, RUp2]),
    ?assertMatch({specula_commit, _}, Result2),
    lager:info("Specula commit for t2"),
    IntDep = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, read_dep, TxId2]),
    ?assertEqual([TxId1], IntDep),
    
    TxId3 = rpc:call(Node1, tx_cert_sup, start_tx, [1]),
    ReadR4 = rpc:call(Node1, data_repl_serv, read, [DataRepl2, K3, TxId3]),
    ?assertEqual({ok, 3}, ReadR4),
    ReadR5 = rpc:call(Node1, tx_cert_sup, read, [1, TxId3, K5, PartNode1]),
    ?assertEqual({ok, 25}, ReadR5),
    Dep3 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, dependency, TxId1]),
    ?assertEqual([{TxId1, TxId2}, {TxId1, TxId3}], Dep3),
    Dep4 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, dependency, TxId2]),
    ?assertEqual([{TxId2, TxId3}], Dep4),
    AntiDep3 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, anti_dep, TxId3]),
    ?assertEqual([{TxId3, TxId1}, {TxId3, TxId2}], AntiDep3),

    LUp3 = [{PartNode1, [{K9, 31}]}],
    PartUp23 = {PartNode2, [{K10, 32}]},
    PartUp33 = {PartNode4, [{K11, 33}]},
    RUp3 = [PartUp23, PartUp33], 
    lager:info("Cert t3 ~w", [TxId3]),
    {ok, Result3} = rpc:call(Node1, tx_cert_sup, certify, [1, TxId3, LUp3, RUp3]),
    ?assertMatch({specula_commit, _}, Result3),
    IntDep2 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, read_dep, TxId3]),
    ?assertEqual([TxId1, TxId2], IntDep2),

    %% This txn will not read anything from the previous txn. And its previous txn
    %% will commit with larger ts then its snapshot, but since it has no read dependency
    %% with its previous txn, it can still commit.
    TxId4 = rpc:call(Node1, tx_cert_sup, start_tx, [1]),
    LUp4 = [{PartNode1, [{K13, 41}]}],
    PartUp34 = {PartNode3, [{K15, 43}]},
    PartUp44 = {PartNode4, [{K16, 44}]},
    RUp4 = [PartUp34, PartUp44], 
    lager:info("Cert t4 ~w", [TxId4]),
    {ok, Result4} = rpc:call(Node1, tx_cert_sup, certify, [1, TxId4, LUp4, RUp4]),
    ?assertMatch({specula_commit, _}, Result4),

    %% Because of read dedendency, although has received all preps, still can not commit
    TxId5 = rpc:call(Node1, tx_cert_sup, start_tx, [1]),
    ReadR6 = rpc:call(Node1, data_repl_serv, read, [DataRepl2, K15, TxId5]),
    ?assertEqual({ok, 43}, ReadR6),
    PartUp45 = {PartNode4, [{K20, 54}]},
    RUp5 = [PartUp45], 
    lager:info("Cert t5 ~w", [TxId5]),
    {ok, Result5} = rpc:call(Node1, tx_cert_sup, certify, [1, TxId5, [], RUp5]),
    ?assertMatch({specula_commit, _}, Result5),

    TxId6 = rpc:call(Node1, tx_cert_sup, start_tx, [1]),
    LUp6 = [{PartNode1, [{K17, 61}]}],
    RUp6 = [], 
    lager:info("Cert t6 ~w", [TxId6]),
    spawn(specula_test, spawn_receive, [self(), Node1, tx_cert_sup, certify, [1, TxId6, LUp6, RUp6]]),
    receive 
        Unexpected ->
            ?assertMatch({ok, {specula_commit, _}}, Unexpected)
    after
        500 ->
            lager:info("Didn't receive anything!")
    end,

    %% Tx1 gets committed.
    lager:info("Commit t1"),
    ok = rpc:call(Node1, clocksi_vnode, do_reply, [PartNode2, TxId1]),
    ok = rpc:call(Node1, clocksi_vnode, do_reply, [PartNode3, TxId1]),
    timer:sleep(500),
    read_txn_data(Node1, LUp1, RUp1),
    check_txn_local_states(TxId1, Node1, RUp1, false),
    check_txn_preparing_state(TxId1, Node1, LUp1, false, false),
    check_txn_preparing_state(TxId1, Node1, PartUp2, false, false),
    check_txn_preparing_state(TxId1, Node1, PartUp3, false, false),
    check_txn_preparing_state(TxId1, Node1, PartUp4, false, false),

    receive 
        Something ->
            ?assertMatch({ok, {specula_commit, _}}, Something)
    end,
    lager:info("Got specula commit!"),

    %% Tx2 gets committed
    lager:info("Commit t2"),
    ReadDep1 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, read_dep, TxId2]),
    ?assertEqual([], ReadDep1),
    ok = rpc:call(Node1, clocksi_vnode, do_reply, [PartNode3, TxId2]),
    timer:sleep(500),
    read_txn_data(Node1, LUp2, RUp2),

    %% Tx3 gets aborted 
    lager:info("Commit t3"),
    ok = rpc:call(Node1, clocksi_vnode, do_reply, [PartNode2, TxId3]),
    timer:sleep(500),
    read_txn_data(Node1, LUp3, RUp3),

    {tx_id, T, _} =TxId4,
    rpc:call(Node1, tx_cert_sup, set_internal_data, [1, last_commit_time, T+10]),

    %% Tx4 gets committed, before committing check that TxId5 still depends on Tx4
    lager:info("Commit t4"),
    ReadDep2 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, read_dep, TxId5]),
    ?assertEqual([TxId4], ReadDep2),
    {tx_id, T1, _} =TxId5,
    rpc:call(Node1, tx_cert_sup, set_internal_data, [1, last_commit_time, T1+10]),

    ok = rpc:call(Node1, clocksi_vnode, do_reply, [PartNode3, TxId4]),
    timer:sleep(500),
    read_txn_data(Node1, LUp4, RUp4),
    LastCommitT1 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, last_commit_time, ignore]),
    ?assertEqual(T1+11, LastCommitT1),

    %% Tx5 will be aborted, due to its commit time being smaller than the commit time of its previous txn
    %% that it has read from.
    lager:info("Abort t5"),
    timer:sleep(500),
    read_txn_data(Node1, LUp4, RUp4),
    check_txn_local_states(TxId5, Node1, RUp5, false),
    check_txn_preparing_state(TxId5, Node1, PartUp45, false, false),

    timer:sleep(500),
    %% Tx6 will also be aborted because TxId5 is aborted!
    lager:info("Abort t6"),
    read_txn_data(Node1, LUp4, RUp4),
    ReadAborted1 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, read_aborted, ignore]),
    ?assertEqual(2, ReadAborted1),
    Committed1 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, committed, ignore]),
    ?assertEqual(5, Committed1),
    check_txn_preparing_state(TxId6, Node1, LUp6, false, false),

    ok = rpc:call(Node1, clocksi_vnode, set_debug, [PartNode2, false]),
    ok = rpc:call(Node1, clocksi_vnode, set_debug, [PartNode3, false]).
        
%% Test that read values from previous transaction across threads works.
specula_test3(Nodes) ->
    lager:info("Test3 started"),
    Node1 = hd(Nodes),
    Node2 = lists:nth(2, Nodes),
    Node3 = lists:nth(3, Nodes),
    Node4 = lists:nth(4, Nodes),
    PartNode1 = rpc:call(Node1, hash_fun, get_local_vnode_by_id, [1]),
    PartNode2 = rpc:call(Node2, hash_fun, get_local_vnode_by_id, [1]),
    PartNode3 = rpc:call(Node3, hash_fun, get_local_vnode_by_id, [1]),
    PartNode4 = rpc:call(Node4, hash_fun, get_local_vnode_by_id, [1]),
    %{Part4, _} = PartNode4,

    ReadAborted1 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, aborted, ignore]),
    ReadAborted2 = rpc:call(Node1, tx_cert_sup, get_internal_data, [2, aborted, ignore]),
    ReadAborted3 = rpc:call(Node1, tx_cert_sup, get_internal_data, [3, aborted, ignore]),

    K1=t3p1k1, K2=t3p2k1, K3=t3p3k1, K4=t3p4k1,
    K5=t3p1k2, _K6=t3p2k2, K7=t3p3k2, K8=t3p4k2,
    K9=t3p1k3, K10=t3p2k3, K11=t3p3k3, _K12=t3p4k3,
    K13=t3p1k4, K14=t3p2k4, _K15=t3p3k4, K16=t3p4k4,
    _K17=t3p1k5, _K18=t3p2k5, _K19=t3p3k5, _K20=t3p4k5,

    DataRepl1='dev1@127.0.0.1repldev2@127.0.0.1',
    %DataRepl2='dev1@127.0.0.1repldev3@127.0.0.1',

    ok = rpc:call(Node1, clocksi_vnode, set_debug, [PartNode2, true]),
    ok = rpc:call(Node1, clocksi_vnode, set_debug, [PartNode3, true]),

    LUp1 = [{PartNode1, [{K1, 1}]}],
    PartUp2 = {PartNode2, [{K2, 2}]},
    PartUp3 = {PartNode3, [{K3, 3}]},
    PartUp4 = {PartNode4, [{K4, 4}]},
    RUp1 = [PartUp2, PartUp3, PartUp4],

    TxId1 = rpc:call(Node1, tx_cert_sup, start_tx, [1]),
    lager:info("Cert tx1 ~w", [TxId1]),
    {ok, Result1} = rpc:call(Node1, tx_cert_sup, certify, [1, TxId1, LUp1, RUp1]),
    ?assertMatch({specula_commit, _}, Result1),

    %% Tx2 reads two keys from Tx1.. Although it can should have received all prepares,
    %% it can not commit due to pending read dependency
    TxId2 = rpc:call(Node1, tx_cert_sup, start_tx, [2]),
    lager:info("Cert tx2 ~w", [TxId2]),
    ReadR1 = rpc:call(Node1, cache_serv, read, [K4, TxId2]),
    ?assertEqual({ok, 4}, ReadR1),
    ReadR2 = rpc:call(Node1, data_repl_serv, read, [DataRepl1, K2, TxId2]),
    ?assertEqual({ok, 2}, ReadR2),

    LUp2 = [{PartNode1, [{K5, 11}]}],
    PartUp42 = {PartNode4, [{K8, 14}]},
    RUp2 = [PartUp42],
    {ok, Result2} = rpc:call(Node1, tx_cert_sup, certify, [2, TxId2, LUp2, RUp2]),
    ?assertMatch({specula_commit, _}, Result2),

    %% Tx3 reads from Tx1 and Tx2
    TxId3 = rpc:call(Node1, tx_cert_sup, start_tx, [3]),
    lager:info("Cert tx3 ~w", [TxId3]),
    ReadR3 = rpc:call(Node1, cache_serv, read, [K8, TxId3]),
    ?assertEqual({ok, 14}, ReadR3),
    ReadR4 = rpc:call(Node1, tx_cert_sup, read, [3, TxId3, K1, PartNode1]),
    ?assertEqual({ok, 1}, ReadR4),

    LUp3 = [{PartNode1, [{K9, 21}]}],
    PartUp23 = {PartNode3, [{K7, 13}]},
    RUp3 = [PartUp23],
    {ok, Result3} = rpc:call(Node1, tx_cert_sup, certify, [3, TxId3, LUp3, RUp3]),
    ?assertMatch({specula_commit, _}, Result3),

    %% Tx4 is from thread 1(the same thread as Tx1) and reads from Tx1 and Tx3
    TxId4 = rpc:call(Node1, tx_cert_sup, start_tx, [1]),
    lager:info("Cert tx4 ~w", [TxId4]),
    ReadR5 = rpc:call(Node1, data_repl_serv, read, [DataRepl1, K2, TxId4]),
    ?assertEqual({ok, 2}, ReadR5),
    ReadR6 = rpc:call(Node1, tx_cert_sup, read, [1, TxId4, K9, PartNode1]),
    ?assertEqual({ok, 21}, ReadR6),

    LUp4 = [{PartNode1, [{K10, 25}]}],
    PartUp43 = {PartNode4, [{K16, 44}]},
    RUp4 = [PartUp43],
    {ok, Result4} = rpc:call(Node1, tx_cert_sup, certify, [1, TxId4, LUp4, RUp4]),
    ?assertMatch({specula_commit, _}, Result4),

    %% Tx5 is in the same thread as Tx4. It will be aborted because Tx4 is aborted.
    TxId5 = rpc:call(Node1, tx_cert_sup, start_tx, [1]),
    lager:info("Cert tx5 ~w", [TxId5]),
    LUp5 = [{PartNode1, [{K11, 22222}]}],
    RUp5 = [],
    {ok, Result5} = rpc:call(Node1, tx_cert_sup, certify, [1, TxId5, LUp5, RUp5]),
    ?assertMatch({specula_commit, _}, Result5),

    %% T6 reads from T4 and will get aborted. It's from thread 2
    TxId6 = rpc:call(Node1, tx_cert_sup, start_tx, [2]),
    lager:info("Cert tx6 ~w", [TxId6]),
    ReadR7 = rpc:call(Node1, cache_serv, read, [K16, TxId6]),
    ?assertEqual({ok, 44}, ReadR7),

    LUp6 = [{PartNode1, [{K13, 42}]}],
    RUp6 = [],
    {ok, Result6} = rpc:call(Node1, tx_cert_sup, certify, [2, TxId6, LUp6, RUp6]),
    ?assertMatch({specula_commit, _}, Result6),

    %% T7 reads from T6 and will gets aborted also.
    TxId7 = rpc:call(Node1, tx_cert_sup, start_tx, [3]),
    lager:info("Cert tx7 ~w", [TxId7]),
    ReadR8 = rpc:call(Node1, tx_cert_sup, read, [3, TxId7, K13, PartNode1]),
    ?assertEqual({ok, 42}, ReadR8),

    LUp7 = [],
    RUp7 = [{PartNode2, [{K14, 43}]}],
    {ok, Result7} = rpc:call(Node1, tx_cert_sup, certify, [3, TxId7, LUp7, RUp7]),
    ?assertMatch({specula_commit, _}, Result7),
    

    %% Both T1 and T2 gets committed
    ok = rpc:call(Node1, clocksi_vnode, do_reply, [PartNode2, TxId1]),
    ok = rpc:call(Node1, clocksi_vnode, do_reply, [PartNode3, TxId1]),
    timer:sleep(500),

    read_txn_data(Node1, LUp1, RUp1),
    read_txn_data(Node1, LUp2, RUp2),

    %% Start the test now!!!!
    {tx_id, T4, _} =TxId4,
    rpc:call(Node1, tx_cert_sup, set_internal_data, [3, last_commit_time, T4+10]),    
    %% T3 gets committed 
    ok = rpc:call(Node1, clocksi_vnode, do_reply, [PartNode3, TxId3]),

    timer:sleep(500),
    read_txn_data(Node1, LUp3, RUp3, true),
    %% T4, T5, T6, T7 all gets aborted
    read_txn_data(Node1, LUp4, RUp4, false),
    read_txn_data(Node1, LUp5, RUp5, false),
    read_txn_data(Node1, LUp6, RUp6, false),
    read_txn_data(Node1, LUp7, RUp7, false),
    check_txn_local_states(TxId4, Node1, RUp4, false),
    check_txn_preparing_state(TxId4, Node1, LUp4, false, false),
    check_txn_preparing_state(TxId4, Node1, PartUp43, false, false),

    check_txn_preparing_state(TxId5, Node1, LUp5, false, false),
    check_txn_preparing_state(TxId6, Node1, LUp6, false, false),
    check_txn_local_states(TxId7, Node1, RUp7, false),
    check_txn_preparing_state(TxId7, Node1, RUp7, false, false),

    ReadAborted12 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, aborted, ignore]),
    ?assertEqual(ReadAborted12, ReadAborted1+2),
    ReadAborted22 = rpc:call(Node1, tx_cert_sup, get_internal_data, [2, aborted, ignore]),
    ?assertEqual(ReadAborted22, ReadAborted2+1),
    ReadAborted32 = rpc:call(Node1, tx_cert_sup, get_internal_data, [3, aborted, ignore]),
    ?assertEqual(ReadAborted32, ReadAborted3+1),

    ok = rpc:call(Node1, clocksi_vnode, set_debug, [PartNode2, false]),
    ok = rpc:call(Node1, clocksi_vnode, set_debug, [PartNode3, false]).

%% Basically test speculation and bad speculation.. 

spawn_receive(Myself, Node, Mod, Fun, Param) ->
    Result = rpc:call(Node, Mod, Fun, Param),
    Myself ! Result.

read_txn_data(Node, LocalUpdates, RemoteUpdates) ->
    read_txn_data(Node, LocalUpdates, RemoteUpdates, true).

read_txn_data(Node, LocalUpdates, RemoteUpdates, IfCommitted) ->
    TxId = rpc:call(Node, tx_utilities, create_tx_id, [1]),
    lists:foreach(fun(PartUpdate) ->
            {{UpPart, UpNode}, Updates} = PartUpdate,
            Replicas = replicas(UpNode),
            lists:foreach(fun({K, V}) ->
                lager:info("Trying to read ~w", [K]),
                R = rpc:call(Node, clocksi_vnode, debug_read, [{UpPart, UpNode}, K, TxId]),
                %?assertEqual({ok, V}, R),
                assert_by_condition(IfCommitted, {ok, V}, R),
                lists:foreach(fun(Rep) ->
                    R2 = rpc:call(Node, data_repl_serv, debug_read, [Rep, K, TxId]),
                    assert_by_condition(IfCommitted, {ok, V}, R2)
                    %?assertEqual({ok, V}, R2)
                        end, Replicas)
                end, Updates)
            end, LocalUpdates),
    lists:foreach(fun(PartUpdate) ->
            {{UpPart, UpNode}, Updates} = PartUpdate,
            Replicas = replicas(UpNode),
            lists:foreach(fun({K, V}) ->
                R = rpc:call(Node, clocksi_vnode, debug_read, [{UpPart, UpNode}, K, TxId]),
                lager:info("Trying to read ~w", [K]),
                %?assertEqual({ok, V}, R),
                assert_by_condition(IfCommitted, {ok, V}, R),
                %R1 = rpc:call(Node, cache_serv, read, [K, TxId]),
                %?assertEqual({ok, []}, R1),
                lists:foreach(fun(Rep) ->
                    lager:info("Tringto read ~w from ~w", [K, Rep]),
                    R2 = rpc:call(Node, data_repl_serv, debug_read, [Rep, K, TxId]),
                    %?assertEqual({ok, V}, R2)
                    assert_by_condition(IfCommitted, {ok, V}, R2)
                        end, Replicas)
                end, Updates)
            end, RemoteUpdates).

check_txn_local_states(TxId, LocalNode, PartUpdates, IfPrepared) ->
    lists:foreach(fun(PartUpdate) ->
            {{UpdatedPart, UpdatedNode}, Updates} = PartUpdate,
            Replicas = replicas(UpdatedNode),
            Keys = [K  || {K, _} <- Updates],
            Rs = lists:filter(fun(R) ->
                    LocalNodeStr= atom_to_list(LocalNode),
                    L = length(LocalNodeStr),
                    RStr = atom_to_list(R),
                    lists:sublist(RStr, 1, L) == LocalNodeStr
                    end, Replicas),
            lager:info("Replicas are ~w, Rs are ~w", [Replicas, Rs]),
            case Rs of
                [] ->
                    R = rpc:call(LocalNode, cache_serv, if_prepared, [TxId, Keys]),
                    ?assertEqual(IfPrepared, R);
                [DataRepl] ->
                    R1 = rpc:call(LocalNode, data_repl_serv, if_prepared, [DataRepl, TxId, Keys]),
                    ?assertEqual(IfPrepared, R1),
                    R2 = rpc:call(LocalNode, data_repl_serv, if_bulk_prepared, [DataRepl, TxId, UpdatedPart]),
                    ?assertEqual(false, R2)
            end
        end, PartUpdates).

check_txn_preparing_state(TxId, LocalNode, PartUpdates, IfMPrepared, IfSPrepared) ->
    {{UpdatedPart, UpdatedNode}, Updates} = case PartUpdates of
                                             [{{A,B},C}] -> {{A, B}, C};
                                              {{A,B},C} ->  {{A, B}, C}
                                            end,
    Keys = [K  || {K, _} <- Updates],
    Replicas = replicas(UpdatedNode),
    lists:foreach(fun(DataRepl) ->
            DataReplStr= atom_to_list(DataRepl),
            LocalNodeStr = atom_to_list(LocalNode),
            L = length(LocalNodeStr),
            case lists:sublist(DataReplStr, 1, L) of
                LocalNodeStr ->%%Is replicated locally, it's already checked by previous function 
                    ok;
                _ ->
                    RR2 = rpc:call(LocalNode, data_repl_serv, if_bulk_prepared, [DataRepl, TxId, UpdatedPart]),
                    ?assertEqual(IfSPrepared, RR2)
            end
        end, Replicas),
    R3 = rpc:call(LocalNode, clocksi_vnode, if_prepared, [{UpdatedPart, UpdatedNode}, TxId, Keys]),
    ?assertEqual(IfMPrepared, R3).

replicas('dev1@127.0.0.1') ->
    ['dev3@127.0.0.1repldev1@127.0.0.1', 'dev4@127.0.0.1repldev1@127.0.0.1'];
replicas('dev2@127.0.0.1') ->
    ['dev4@127.0.0.1repldev2@127.0.0.1', 'dev1@127.0.0.1repldev2@127.0.0.1'];
replicas('dev3@127.0.0.1') ->
    ['dev1@127.0.0.1repldev3@127.0.0.1', 'dev2@127.0.0.1repldev3@127.0.0.1'];
replicas('dev4@127.0.0.1') ->
    ['dev2@127.0.0.1repldev4@127.0.0.1', 'dev3@127.0.0.1repldev4@127.0.0.1'].
    
replicates('dev1@127.0.0.1') ->
    ['dev1@127.0.0.1repldev2@127.0.0.1', 'dev1@127.0.0.1repldev3@127.0.0.1'];
replicates('dev2@127.0.0.1') ->
    ['dev2@127.0.0.1repldev3@127.0.0.1', 'dev2@127.0.0.1repldev4@127.0.0.1'];
replicates('dev3@127.0.0.1') ->
    ['dev3@127.0.0.1repldev4@127.0.0.1', 'dev3@127.0.0.1repldev1@127.0.0.1'];
replicates('dev4@127.0.0.1') ->
    ['dev4@127.0.0.1repldev1@127.0.0.1', 'dev4@127.0.0.1repldev2@127.0.0.1'].

assert_by_condition(false, A, B) ->
    lager:info("A is ~w, B is ~w", [A, B]),
    ?assertNotEqual(A, B);
assert_by_condition(true, A, B) ->
    ?assertEqual(A, B).
