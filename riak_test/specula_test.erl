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

-export([confirm/0, spawn_receive/5]).

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
    TxId1 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    PartNode1 = rpc:call(Node1, hash_fun, get_local_vnode_by_id, [1]),
    PartNode2 = rpc:call(Node2, hash_fun, get_local_vnode_by_id, [1]),
    {Part2, _} = PartNode2,
    PartNode3 = rpc:call(Node3, hash_fun, get_local_vnode_by_id, [1]),
    {Part3, _} = PartNode3,
    PartNode4 = rpc:call(Node4, hash_fun, get_local_vnode_by_id, [1]),
    {Part4, _} = PartNode4,
    DataRepl2 = list_to_atom(atom_to_list(Node1)++"repl"++atom_to_list(Node2)),
    DataRepl3 = list_to_atom(atom_to_list(Node1)++"repl"++atom_to_list(Node3)),
    Data3Repl4 = list_to_atom(atom_to_list(Node3)++"repl"++atom_to_list(Node4)),
    Data4Repl2 = list_to_atom(atom_to_list(Node4)++"repl"++atom_to_list(Node2)),
    Data2Repl3 = list_to_atom(atom_to_list(Node2)++"repl"++atom_to_list(Node3)),
    LUps1 = [{PartNode1, [{t1p1k1, 11}, {t1p1k2, 12}]}],
    RUps1 = [{PartNode2, [{t1p2k1, 21}, {t1p2k2, 22}]}, {PartNode3, [{t1p3k1, 31}, {t1p3k2, 32}]}, 
                {PartNode4, [{t1p4k1, 41}, {t1p4k2, 42}]}],
    %% Only partition Part3 is normal.
    ok = rpc:call(Node1, clocksi_vnode, set_debug, [PartNode2, true]),
    ok = rpc:call(Node1, clocksi_vnode, set_debug, [PartNode4, true]),
    {ok, Result1} = rpc:call(Node1, tx_cert_sup, certify, [1, TxId1, LUps1, RUps1]),
    ?assertMatch({specula_commit, _}, Result1),

    timer:sleep(500),
    %% Local master is prepared
    IfPrep1 = rpc:call(Node1, clocksi_vnode, if_prepared, [PartNode1, TxId1, [t1p1k1, t1p1k2]]),
    ?assertEqual(true, IfPrep1),
    %% Local slave is prepared due to specula prepare
    IfPrep2 = rpc:call(Node1, data_repl_serv, if_prepared, [DataRepl2, TxId1, [t1p2k1, t1p2k2]]),
    ?assertEqual(true, IfPrep2),
    IfPrep3 = rpc:call(Node1, data_repl_serv, if_prepared, [DataRepl3, TxId1, [t1p3k1, t1p3k2]]),
    ?assertEqual(true, IfPrep3),
    %% Local cache is prepared
    IfPrep5 = rpc:call(Node1, cache_serv, if_prepared, [TxId1, [t1p4k1, t1p4k2]]),
    ?assertEqual(true, IfPrep5),

    %% Replica of partition 3 is prepared since prepare is not blocked 
    If2Prep3 = rpc:call(Node1, data_repl_serv, if_bulk_prepared, [Data2Repl3, TxId1, Part3]),
    ?assertEqual(true, If2Prep3),
    %% Replica of partition 2 and 4 are blocked
    If4Prep2 = rpc:call(Node1, data_repl_serv, if_bulk_prepared, [Data4Repl2, TxId1, Part2]),
    ?assertEqual(false, If4Prep2),
    If3Prep4 = rpc:call(Node1, data_repl_serv, if_bulk_prepared, [Data3Repl4, TxId1, Part4]),
    ?assertEqual(false, If3Prep4),

    %% After allowing partnode2 to prepare, some of its replicas get prepared
    ok = rpc:call(Node1, clocksi_vnode, do_reply, [PartNode2, TxId1]),
    timer:sleep(500),
    If4Prep2_1 = rpc:call(Node1, data_repl_serv, if_bulk_prepared, [Data4Repl2, TxId1, Part2]),
    ?assertEqual(true, If4Prep2_1),
    If3Prep4_1 = rpc:call(Node1, data_repl_serv, if_bulk_prepared, [Data3Repl4, TxId1, Part4]),
    ?assertEqual(false, If3Prep4_1),
    IfPrep2_1 = rpc:call(Node1, data_repl_serv, if_bulk_prepared, [DataRepl2, TxId1, Part2]),
    ?assertEqual(false, IfPrep2_1),

    %% The transaction will be committed.
    ok = rpc:call(Node1, clocksi_vnode, do_reply, [PartNode4, TxId1]),
    timer:sleep(500),
    
    %% Waits until all value gets committed.
    %% Check that prepared values get cleaned. 
    If4Prep2_3 = rpc:call(Node1, data_repl_serv, if_bulk_prepared, [Data4Repl2, TxId1, Part2]),
    ?assertEqual(false, If4Prep2_3),
    IfPrep1_1 = rpc:call(Node1, clocksi_vnode, if_prepared, [PartNode1, TxId1, [t1p1k1, t1p1k2]]),
    ?assertEqual(false, IfPrep1_1),
    IfPrep2_1 = rpc:call(Node1, data_repl_serv, if_prepared, [DataRepl2, TxId1, [t1p2k1, t1p2k2]]),
    ?assertEqual(false, IfPrep2_1),
    IfPrep3_1 = rpc:call(Node1, data_repl_serv, if_prepared, [DataRepl3, TxId1, [t1p3k1, t1p3k2]]),
    ?assertEqual(false, IfPrep3_1),
    If2Prep3_1 = rpc:call(Node1, data_repl_serv, if_bulk_prepared, [Data2Repl3, TxId1, Part3]),
    ?assertEqual(false, If2Prep3_1),
    If3Prep4_1 = rpc:call(Node1, data_repl_serv, if_bulk_prepared, [Data3Repl4, TxId1, Part4]),
    ?assertEqual(false, If3Prep4_1),

    TxId2 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    ReadResult1 = rpc:call(Node1, clocksi_vnode, read_data_item, [PartNode1, t1p1k1, TxId2]),
    ?assertEqual({ok, 11}, ReadResult1),
    ReadResult2 = rpc:call(Node1, clocksi_vnode, read_data_item, [PartNode2, t1p2k2, TxId2]),
    ?assertEqual({ok, 22}, ReadResult2),
    ReadResult3 = rpc:call(Node1, data_repl_serv, read, [DataRepl2, t1p2k1, TxId2]),
    ?assertEqual({ok, 21}, ReadResult3),
    ReadResult4 = rpc:call(Node1, cache_serv, read, [t1p4k1, TxId2]),
    ?assertEqual({ok, []}, ReadResult4),
    ReadResult5 = rpc:call(Node1, clocksi_vnode, read_data_item, [PartNode4, t1p4k1, TxId2]),
    ?assertEqual({ok, 41}, ReadResult5),
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
    TxId1 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    PartNode1 = rpc:call(Node1, hash_fun, get_local_vnode_by_id, [1]),
    PartNode2 = rpc:call(Node2, hash_fun, get_local_vnode_by_id, [1]),
    {Part2, _} = PartNode2,
    PartNode3 = rpc:call(Node3, hash_fun, get_local_vnode_by_id, [1]),
    {Part3, _} = PartNode3,
    PartNode4 = rpc:call(Node4, hash_fun, get_local_vnode_by_id, [1]),
    {Part4, _} = PartNode4,
    DataRepl2 = list_to_atom(atom_to_list(Node1)++"repl"++atom_to_list(Node2)),
    DataRepl3 = list_to_atom(atom_to_list(Node1)++"repl"++atom_to_list(Node3)),
    Data3Repl4 = list_to_atom(atom_to_list(Node3)++"repl"++atom_to_list(Node4)),
    Data4Repl2 = list_to_atom(atom_to_list(Node4)++"repl"++atom_to_list(Node2)),
    Data2Repl3 = list_to_atom(atom_to_list(Node2)++"repl"++atom_to_list(Node3)),




    Node1 = hd(Nodes),
    TxId0 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    TxId1 = TxId0,
    [Part1] = rpc:call(Node1, hash_fun, get_preflist_from_key, [1]),
    [Part2] = rpc:call(Node1, hash_fun, get_preflist_from_key, [2]),

    LUps1 = [{Part1, [{t5_k1, 1}, {t5_k2, 2}, {t5_k3, 3}]}],
    RUps1 = [{Part2, [{t5_k4, 1}, {t5_k5, 2}, {t5_k6, 3}]}],
    {ok, Result0} = rpc:call(Node1, tx_cert_sup, certify, [1, TxId0, RUps1, []]),
    ?assertMatch({committed, _}, Result0),

    ok = rpc:call(Node1, clocksi_vnode, set_debug, [Part2, true]),
    {ok, Result1} = rpc:call(Node1, tx_cert_sup, certify, [1, TxId1, LUps1, RUps1]),
    ?assertMatch({specula_commit, _}, Result1),

    TxId2 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    LUps2 = [{Part1, [{t5_k01, 1}, {t5_k02, 2}, {t5_k03, 3}]}],
    RUps2 = [{Part2, [{t5_k04, 1}, {t5_k05, 2}, {t5_k06, 3}]}],
    {ok, Result2} = rpc:call(Node1, tx_cert_sup, certify, [1, TxId2, LUps2, RUps2]),
    ?assertMatch({specula_commit, _}, Result2),

    TxId3 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    LUps3 = [{Part1, [{t5_k11, 1}, {t5_k12, 2}, {t5_k13, 3}]}],
    RUps3 = [{Part2, [{t5_k14, 1}, {t5_k15, 2}, {t5_k16, 3}]}],
    {ok, Result3} = rpc:call(Node1, tx_cert_sup, certify, [1, TxId3, LUps3, RUps3]),
    ?assertMatch({specula_commit, _}, Result3),
    
    ok = rpc:call(Node1, clocksi_vnode, do_reply, [Part2, TxId1]),
    IntData1 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, pending_list, ignore]),
    ?assertEqual([], IntData1),
    IntData2 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, pending_txs, TxId1]),
    ?assertEqual([], IntData2),
    IntData3 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, pending_txs, TxId2]),
    ?assertMatch([], IntData3),
    IntData4 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, pending_txs, TxId3]),
    ?assertMatch([], IntData4),

    IntData5 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, specula_data, t5_k1]),
    ?assertEqual([], IntData5),
    IntData6 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, specula_data, t5_k6]),
    ?assertEqual([], IntData6),

    IntData7 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, specula_data, t5_k04]),
    ?assertEqual([], IntData7),
    IntData8 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, specula_data, t5_k16]),
    ?assertEqual([], IntData8),
    ok = rpc:call(Node1, clocksi_vnode, set_debug, [Part2, false]).
        
%% Test that read values from previous transaction across threads works.
specula_test3(Nodes) ->
    lager:info("Test6 started"),
    Node1 = hd(Nodes),
    TxId1 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    [Part1] = rpc:call(Node1, hash_fun, get_preflist_from_key, [1]),
    [Part2] = rpc:call(Node1, hash_fun, get_preflist_from_key, [2]),
    LUps1 = [{Part1, [{t6_k1, 1}, {t6_k2, 2}, {t6_k3, 3}]}],
    RUps1 = [{Part2, [{t6_k4, 1}, {t6_k5, 2}, {t6_k6, 3}]}],
    ok = rpc:call(Node1, clocksi_vnode, set_debug, [Part2, true]),
    {ok, Result1} = rpc:call(Node1, tx_cert_sup, certify, [1, TxId1, LUps1, RUps1]),
    ?assertMatch({specula_commit, _}, Result1),

    %Tx2 is Valid read, Tx3 is invalid read
    TxId2 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    {tx_id, T, _} = TxId2,
    T3 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    {tx_id, _, S} = T3,
    TxId3 = {tx_id, T, S},

    {ok, Value1} = rpc:call(Node1, tx_cert_sup, read, [1, TxId2, t6_k2, []]),
    ?assertEqual(Value1, 2),
    LUps2 = [{Part1, [{t6_k01, 1}, {t6_k02, 2}, {t6_k03, 3}]}],
    RUps2 = [{Part2, [{t6_k04, 1}, {t6_k05, 2}, {t6_k06, 3}]}],
    {ok, Result2} = rpc:call(Node1, tx_cert_sup, certify, [1, TxId2, LUps2, RUps2]),
    ?assertMatch({specula_commit, _}, Result2),
    Result3 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, dependency, {TxId2, TxId1}]),
    ?assertEqual({{ok, 1}, {ok, [TxId2]}}, Result3),

    {ok, Value2} = rpc:call(Node1, tx_cert_sup, read, [1, TxId3, t6_k3, []]),
    ?assertEqual(Value2, 3),
    {ok, Value3} = rpc:call(Node1, tx_cert_sup, read, [1, TxId3, t6_k02, []]),
    ?assertEqual(Value3, 2),
    LUps3 = [{Part1, [{t6_k11, 1}, {t6_k12, 2}, {t6_k13, 3}]}],
    RUps3 = [{Part2, [{t6_k14, 1}, {t6_k15, 2}, {t6_k16, 3}]}],
    {ok, Result4} = rpc:call(Node1, tx_cert_sup, certify, [1, TxId3, LUps3, RUps3]),
    ?assertMatch({specula_commit, _}, Result4),
    Result5 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, dependency, {TxId3, TxId1}]),
    ?assertEqual({{ok, 2}, {ok, [TxId2, TxId3]}}, Result5),

    ok = rpc:call(Node1, clocksi_vnode, do_reply, [Part2, TxId1]),
    ok = rpc:call(Node1, clocksi_vnode, do_reply, [Part2, TxId2]),
    
    TxId4 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    %lager:info("TxId1 ~w, TxId2 ~w, TxId3 ~w, TxId4 ~w", [TxId1, TxId2, TxId3, TxId4]),
    Result6 = rpc:call(Node1, clocksi_vnode, read_data_item, [Part1, t6_k1, TxId4]),
    ?assertEqual({ok, 1}, Result6),
    Result7 = rpc:call(Node1, clocksi_vnode, read_data_item, [Part2, t6_k05, TxId4]),
    ?assertEqual({ok, 2}, Result7),
    Result8 = rpc:call(Node1, clocksi_vnode, read_data_item, [Part2, t6_k16, TxId4]),
    ?assertEqual({ok, []}, Result8),

    IntData1 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, pending_list, ignore]),
    ?assertEqual([], IntData1),
    IntData2 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, specula_data, t6_k1]),
    ?assertEqual([], IntData2),
    IntData3 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, specula_data, t6_k06]),
    ?assertEqual([], IntData3),
    IntData4 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, specula_data, t6_k12]),
    ?assertEqual([], IntData4),
    IntData5 = rpc:call(Node1, tx_cert_sup, get_internal_data, [1, pending_txs, TxId1]),
    ?assertEqual([], IntData5),
    ok = rpc:call(Node1, clocksi_vnode, set_debug, [Part2, false]).

%% Basically test speculation and bad speculation.. 

spawn_receive(Myself, Node, Mod, Fun, Param) ->
    Result = rpc:call(Node, Mod, Fun, Param),
    Myself ! Result.

check_txn_local_states(TxId, LocalNode, PartUpdates, IfPrepared) ->
    Replicates = replicates(LocalNode),
    lists:foreach(fun(PartUpdate) ->
            {{UpdatedPart, UpdatedNode}, Updates} = PartUpdate,
            Rs = lists:filter(fun(R) ->
                    UPNodeStr= atom_to_list(UpdatedNode),
                    L = length(UPNodeStr),
                    RStr = atom_to_list(R),
                    lists:sublist(RStr, 1, L) == UPNodeStr
                    end, Replicates),
            case Rs of
                [] ->
                    R = rpc:call(LocalNode, cache_serv, if_prepared, [TxId, Updates]),
                    ?assertEqual(IfPrepared, R);
                [DataRepl] ->
                    R1 = rpc:call(LocalNode, data_repl_serv, if_prepared, [DataRepl, TxId, Updates]),
                    ?assertEqual(IfPrepared, R1),
                    R2 = rpc:call(LocalNode, data_repl_serv, if_bulk_prepared, [DataRepl, TxId, UpdatedPart]),
                    ?assertEqual(false, R2)
            end
        end, PartUpdates).

check_txn_preparing_state(TxId, LocalNode, PartUpdates, IfPrepared) ->
    Replicates = replicates(LocalNode),
    {{UpdatedPart, UpdatedNode}, Updates} = PartUpdates,
    Replicas = replicas(UpdatedNode),
    lists:foreach(fun(DataRepl) ->
            RR1 = rpc:call(LocalNode, data_repl_serv, if_bulk_prepared, [DataRepl, TxId, UpdatedPart]),
            ?assertEqual(false, RR1),
            RR2 = rpc:call(LocalNode, data_repl_serv, if_prepared, [DataRepl, TxId, Updates]),
            ?assertEqual(IfPrepared, RR2)
        end, Replicas),
    R3 = rpc:call(LocalNode, clocksi_vnode, if_prepared, [{UpdatedPart, UpdatedNode}, TxId, Updates]),
    ?assertEqual(IfPrepared, R3).

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
